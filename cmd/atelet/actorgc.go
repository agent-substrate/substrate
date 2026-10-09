// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// The actor-state orphan sweep.
//
// Terminate reclaims an actor's directory on the graceful path, but no
// graceful path can be guaranteed: a node can be evicted, an atelet can crash
// mid-teardown, a control plane can be uninstalled and reinstalled on top of
// live state. The control plane comes back for an actor's state when it
// learns the actor's workload is gone, but only while it still records the
// actor and can reach this node's atelet. Whatever falls outside that is left
// behind, and nothing else walks actors/.
//
// The image cache has the same shape of problem and the same shape of answer
// (see imagegc.go): a serialized pass on a fixed period, plus one at startup.
// The root set is the difference: the image cache reads it off the node, while
// only the control plane knows which actors are placed here. Each pass asks it
// once, with WorkerService.ListNodeActorUIDs, which it scopes to this node by
// atelet's certificate. A pass that cannot read that set deletes nothing at
// all.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
)

var (
	actorGCPeriod = pflag.Duration("actor-gc-period", 30*time.Minute, "How often to sweep orphaned actor state directories. 0 disables the periodic pass (the startup pass still runs).")
	actorGCMinAge = pflag.Duration("actor-gc-min-age", 10*time.Minute, "Actor directories younger than this are never swept, which covers the window between atelet creating one and the control plane recording where the actor was placed.")
	actorGCDryRun = pflag.Bool("actor-gc-dry-run", false, "Log what the sweep would reclaim without deleting anything.")
)

// actorGCListTimeout bounds the call that reads the live set. Passes are
// serialized, so a call that never returns would stall every pass after it.
const actorGCListTimeout = time.Minute

// retiredActorPrefix marks an actor dir the sweep has renamed aside and is
// about to delete. Actor dirs are named by UID, which never starts with a dot,
// so a retired dir can never be mistaken for a live one — by this sweep, or by
// the image cache's root-set scan, which reads bundle specs out of the same
// tree and treats anything it still finds there as in use.
const retiredActorPrefix = ".rm-"

func validateActorGCFlags() error {
	if *actorGCPeriod < 0 {
		// A negative period would silently disable the periodic pass: it
		// fails the > 0 guard at the launch site, which also protects the
		// ticker.
		return fmt.Errorf("--actor-gc-period %v must be >= 0", *actorGCPeriod)
	}
	if *actorGCMinAge < 0 {
		// A negative min-age inverts the veto — the cutoff lands in the
		// future, making a directory created moments ago sweepable.
		return fmt.Errorf("--actor-gc-min-age %v must be >= 0", *actorGCMinAge)
	}
	return nil
}

// liveActorLister reports the UIDs of the actors placed on this node.
type liveActorLister interface {
	liveActorUIDs(ctx context.Context) (map[string]bool, error)
}

// nodeActorsClient is the one WorkerService read the live set needs.
// ateapipb.WorkerServiceClient satisfies it.
type nodeActorsClient interface {
	ListNodeActorUIDs(ctx context.Context, in *ateapipb.ListNodeActorUIDsRequest, opts ...grpc.CallOption) (*ateapipb.ListNodeActorUIDsResponse, error)
}

// controlPlaneActors answers the live set from the control plane: the actors
// assigned to the workers it records on this node. The control plane takes the
// node from atelet's certificate, so the set cannot be another node's.
//
// The control plane is the only authority for this. atelet's own view is
// in-memory and starts empty (a restarted atelet on a busy node knows about
// none of the actors still running there), which is why it is a supplement to
// this set and never a substitute for it.
type controlPlaneActors struct {
	client nodeActorsClient
}

// liveActorUIDs reads the actors placed on this node.
//
// A failed call fails the pass rather than yielding an empty set: an empty set
// is indistinguishable from "no actor is placed here", and acting on it would
// delete live actors' state.
func (c *controlPlaneActors) liveActorUIDs(ctx context.Context) (map[string]bool, error) {
	ctx, cancel := context.WithTimeout(ctx, actorGCListTimeout)
	defer cancel()
	resp, err := c.client.ListNodeActorUIDs(ctx, &ateapipb.ListNodeActorUIDsRequest{})
	if err != nil {
		return nil, fmt.Errorf("while listing the actors placed on this node: %w", err)
	}
	live := make(map[string]bool, len(resp.GetActorUids()))
	for _, uid := range resp.GetActorUids() {
		live[uid] = true
	}
	return live, nil
}

// actorGC is the sweep's state: its configuration snapshotted from the flags
// at construction, so the pass logic never reads globals and is testable
// without flag juggling.
type actorGC struct {
	actorsDir string
	live      liveActorLister
	// resident reports the actors this atelet is currently hosting. Authoritative
	// only in the positive direction — it starts empty after a restart — so it
	// can protect a directory but never condemn one.
	resident func() []string
	// mountPoints lists the mount points in atelet's own mount namespace.
	mountPoints func() ([]string, error)
	period      time.Duration
	minAge      time.Duration
	dryRun      bool
}

func newActorGC(actorsDir string, live liveActorLister, resident func() []string) *actorGC {
	return &actorGC{
		actorsDir:   actorsDir,
		live:        live,
		resident:    resident,
		mountPoints: selfMountPoints,
		period:      *actorGCPeriod,
		minAge:      *actorGCMinAge,
		dryRun:      *actorGCDryRun,
	}
}

// Run sweeps on the configured period until ctx is done. Passes are strictly
// serialized: a slow pass delays the next tick rather than overlapping it.
func (g *actorGC) Run(ctx context.Context) {
	// First pass immediately: the debris of the previous atelet's life is on
	// disk now, and a node that lost its actors abruptly should not carry
	// their state for a full period.
	g.runPass(ctx)

	if g.period <= 0 {
		return
	}
	ticker := time.NewTicker(g.period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		g.runPass(ctx)
	}
}

// actorGCStats counts what one pass did, for its single log line.
type actorGCStats struct {
	Reclaimed      int
	ReclaimedBytes int64
	Live           int
	Resident       int
	Fresh          int
	LocalSnapshot  int
	Mounted        int
	Failed         int
}

// runPass performs one sweep. It recovers from panics: this is a background
// janitor, and a bug here — or a directory an operator dropped into the tree —
// must not take atelet down and strand every actor on the node.
func (g *actorGC) runPass(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Actor GC pass panicked; skipping this pass",
				slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()

	entries, err := os.ReadDir(g.actorsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// No actor has ever run here.
			return
		}
		slog.WarnContext(ctx, "Actor GC: listing the actors dir failed; skipping this pass",
			slog.String("dir", g.actorsDir), slog.Any("err", err))
		return
	}

	// Read before anything is deleted, and required to succeed: a pass with no
	// trustworthy root set has nothing to distinguish an orphan from a running
	// actor, so it deletes nothing. This is the state during a control-plane
	// outage, and the cost of waiting it out is disk that is already spent.
	live, err := g.live.liveActorUIDs(ctx)
	if err != nil {
		slog.WarnContext(ctx, "Actor GC: reading the live actor set failed; skipping this pass (no directory is swept without one)",
			slog.Any("err", err))
		return
	}
	// Required for the same reason: without it, a volume still mounted under
	// an orphan looks like the orphan's own files.
	mounts, err := g.mountPoints()
	if err != nil {
		slog.WarnContext(ctx, "Actor GC: reading the mount table failed; skipping this pass (no directory is swept without one)",
			slog.Any("err", err))
		return
	}
	resident := map[string]bool{}
	if g.resident != nil {
		for _, uid := range g.resident() {
			resident[uid] = true
		}
	}

	tStart := time.Now()
	cutoff := tStart.Add(-g.minAge)
	var stats actorGCStats
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, retiredActorPrefix) {
			// Debris from a pass that died between the rename and the
			// delete. It is already unreachable; finish the job.
			dir := filepath.Join(g.actorsDir, name)
			if !g.holdsMount(ctx, dir, mounts, &stats) {
				g.remove(ctx, dir, &stats)
			}
			continue
		}
		if live[name] {
			stats.Live++
			continue
		}
		if resident[name] {
			// The control plane does not (yet) place this actor here, but
			// this atelet is running it: a bind committed after the listing,
			// or a release racing a teardown in flight. Either way its
			// directory is in use.
			stats.Resident++
			continue
		}
		info, err := entry.Info()
		if err != nil {
			// Vanished under us, or unreadable. Retention is the safe answer
			// and the next pass sees it again.
			stats.Failed++
			continue
		}
		if info.ModTime().After(cutoff) {
			// Young enough that the control plane may not have recorded the
			// placement yet.
			stats.Fresh++
			continue
		}
		dir := filepath.Join(g.actorsDir, name)
		if hasLocalSnapshot(name) {
			// A pause snapshot is node-pinned state held deliberately, and a
			// PAUSED actor holds no worker assignment, so it is absent from
			// the live set by construction. Deleting one is unrecoverable;
			// keeping it costs disk that a later delete still reclaims.
			stats.LocalSnapshot++
			continue
		}
		if g.holdsMount(ctx, dir, mounts, &stats) {
			continue
		}
		g.retireAndRemove(ctx, dir, name, &stats)
	}

	attrs := []any{
		slog.Int("reclaimed_dirs", stats.Reclaimed),
		slog.Int64("reclaimed_bytes", stats.ReclaimedBytes),
		slog.Int("live_actors", stats.Live),
		slog.Int("skipped_resident", stats.Resident),
		slog.Int("skipped_fresh", stats.Fresh),
		slog.Int("skipped_local_snapshot", stats.LocalSnapshot),
		slog.Int("skipped_mounted", stats.Mounted),
		slog.Int("failed", stats.Failed),
		slog.Bool("dry_run", g.dryRun),
		slog.Duration("took", time.Since(tStart)),
	}
	if stats.Reclaimed == 0 && stats.Failed == 0 {
		// The steady state on a healthy node: nothing to say at INFO every
		// period.
		slog.DebugContext(ctx, "Actor GC pass complete", attrs...)
		return
	}
	slog.InfoContext(ctx, "Actor GC pass complete", attrs...)
}

// holdsMount reports whether a mount sits at or under dir, which the pass then
// leaves in place. Removing dir would delete through the mount, and renaming
// it would move the mount away from the path its unmount names.
func (g *actorGC) holdsMount(ctx context.Context, dir string, mounts []string, stats *actorGCStats) bool {
	var under []string
	for _, m := range mounts {
		if m == dir || strings.HasPrefix(m, dir+"/") {
			under = append(under, m)
		}
	}
	if len(under) == 0 {
		return false
	}
	slog.WarnContext(ctx, "Actor GC: leaving an orphaned actor directory with a mount under it",
		slog.String("dir", dir), slog.Any("mounts", under))
	stats.Mounted++
	return true
}

// retireAndRemove reclaims one orphaned actor directory in two phases: a
// rename out of the UID namespace, then the slow delete. A crash in between
// leaves a ".rm-*" dir the next pass finishes, rather than a half-emptied
// directory still named after an actor.
func (g *actorGC) retireAndRemove(ctx context.Context, dir, uid string, stats *actorGCStats) {
	size := dirSize(dir)
	if g.dryRun {
		slog.InfoContext(ctx, "Actor GC would reclaim an orphaned actor directory",
			slog.String("actor_uid", uid), slog.Int64("bytes", size))
		stats.Reclaimed++
		stats.ReclaimedBytes += size
		return
	}
	retired := filepath.Join(g.actorsDir, fmt.Sprintf("%s%s-%d", retiredActorPrefix, uid, time.Now().UnixNano()))
	if err := os.Rename(dir, retired); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		slog.WarnContext(ctx, "Actor GC: retiring an orphaned actor directory failed",
			slog.String("actor_uid", uid), slog.Any("err", err))
		stats.Failed++
		return
	}
	slog.InfoContext(ctx, "Actor GC reclaiming an orphaned actor directory",
		slog.String("actor_uid", uid), slog.Int64("bytes", size))
	before := stats.Failed
	g.remove(ctx, retired, stats)
	if stats.Failed == before {
		stats.Reclaimed++
		stats.ReclaimedBytes += size
	}
}

// remove deletes a retired directory. RemoveAllWritable, not os.RemoveAll: a
// bundle's upper dir can hold copied-up image directories carrying the image's
// read-only modes, which atelet cannot remove as plain root without making
// them writable first (same reason resetActorDirs uses it).
func (g *actorGC) remove(ctx context.Context, dir string, stats *actorGCStats) {
	if g.dryRun {
		return
	}
	if err := imagecache.RemoveAllWritable(dir); err != nil {
		// It is out of the UID namespace already, so it is nothing but bytes;
		// the next pass retries.
		slog.WarnContext(ctx, "Actor GC: deleting a retired actor directory failed",
			slog.String("dir", dir), slog.Any("err", err))
		stats.Failed++
	}
}

// hasLocalSnapshot reports whether the actor holds at least one local (pause)
// snapshot on this node.
func hasLocalSnapshot(actorUID string) bool {
	entries, err := os.ReadDir(ateletpath.LocalCheckpointsDir(actorUID))
	if err != nil {
		// Missing is the ordinary case. Anything else is unreadable, which
		// this reports as "has one" so the directory is kept.
		return !errors.Is(err, os.ErrNotExist)
	}
	return len(entries) > 0
}

// dirSize sums the apparent size of a tree, for the reclaimed-bytes figure in
// the pass log. Best-effort: it is telemetry, not a decision input, so an
// unreadable entry is skipped rather than failing the reclaim.
func dirSize(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

// selfMountPoints reads the mount points in atelet's own mount namespace.
//
// atelet's own table, not the host's, is the one that decides what a delete
// descends into. /var/lib/ate is a hostPath without mount propagation, so the
// mounts atelet sees under it are the ones present when its container started,
// such as an external volume whose unmount failed before an atelet restart. A
// mount it cannot see is only detached when its directory is removed.
func selfMountPoints() ([]string, error) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	return parseMountPoints(string(data)), nil
}

// parseMountPoints returns the mount point, field 5, of each line of a
// mountinfo table, decoding the kernel's octal escapes (\040 for a space).
func parseMountPoints(mountinfo string) []string {
	var points []string
	for line := range strings.Lines(mountinfo) {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		points = append(points, unescapeMountPoint(fields[4]))
	}
	return points
}

func unescapeMountPoint(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if c, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(c))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
