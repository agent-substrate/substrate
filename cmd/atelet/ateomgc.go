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

// The ateom directory janitor removes ateoms/<pod-UID>/ directories left
// behind by worker pods that no longer exist. A gracefully terminated ateom
// removes its own; one killed outright leaves it, and the stats sweep pays a
// dial and a probe for each leftover, every sweep.
//
// A wrong removal is severe and self-hiding: the live ateom keeps serving on
// the unlinked socket inode, but every new connect fails ENOENT forever while
// the worker keeps advertising capacity. So a directory goes only when its
// pod is absent from the node's pod list, its ateom does not answer a probe,
// and both have held for several consecutive passes.

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/pflag"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

var ateomGCEnabled = pflag.Bool("ateom-gc-enabled", true, "Remove the directories of worker pods that no longer exist from this node.")

// ateomGCPeriod is how often a pass runs. Growth is slow, so nothing tunes it.
const ateomGCPeriod = 10 * time.Minute

// ateomGCMinAge is the youngest a directory can be and still be removed.
const ateomGCMinAge = 10 * time.Minute

// ateomGCStrikes is how many consecutive passes must find a directory
// orphaned before it is removed.
const ateomGCStrikes = 3

// ateomGCProbeTimeout is the stats sweep's bound for the same RPC: a live
// micro-VM ateom with a hung guest can take most of it, and is not dead.
const ateomGCProbeTimeout = statsRPCTimeout

// ateomGCListTimeout bounds the per-pass pod list so a hung apiserver skips
// the pass instead of parking the janitor.
const ateomGCListTimeout = 10 * time.Second

// ateomGC holds the janitor's seams and the strike counters that carry
// evidence across passes.
type ateomGC struct {
	ateomsDir string
	period    time.Duration
	minAge    time.Duration

	// listNodePodUIDs returns the UIDs of every pod on this node. An error
	// aborts the pass: deletion evidence cannot come from a failed list.
	listNodePodUIDs func(ctx context.Context) (map[string]struct{}, error)

	// probe asks the ateom behind podUID whether it is alive; see
	// ateomAnswered for what counts as an answer.
	probe func(ctx context.Context, podUID string) error

	// now is the clock, injected so tests can age directories.
	now func() time.Time

	// strikes counts consecutive orphaned passes per pod UID.
	strikes map[string]int
}

func newAteomGC(client kubernetes.Interface, nodeName string) *ateomGC {
	return &ateomGC{
		ateomsDir:       ateompath.AteomsDir(),
		period:          ateomGCPeriod,
		minAge:          ateomGCMinAge,
		listNodePodUIDs: nodePodUIDLister(client, nodeName),
		probe:           probeAteom,
		now:             time.Now,
		strikes:         make(map[string]int),
	}
}

// Run executes serialized passes on the period until ctx is done. The first
// pass waits one period: a node that just booted has nothing to reap.
func (g *ateomGC) Run(ctx context.Context) {
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

// runPass performs one pass. It recovers from panics so a bug here cannot
// take the node's lifecycle daemon down.
func (g *ateomGC) runPass(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Ateom GC pass panicked; skipping this pass",
				slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()

	entries, err := os.ReadDir(g.ateomsDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "Ateom GC: listing the ateoms directory failed; skipping this pass", slog.Any("err", err))
		}
		return
	}

	podUIDs, err := g.listNodePodUIDs(ctx)
	if err != nil {
		slog.WarnContext(ctx, "Ateom GC: listing the node's pods failed; skipping this pass", slog.Any("err", err))
		return
	}

	seen := make(map[string]struct{}, len(entries))
	var removed, kept int
	for _, e := range entries {
		name := e.Name()
		// Only directories named by a pod UID are ours to remove.
		if !e.IsDir() {
			continue
		}
		if !isCanonicalUUID(name) {
			slog.WarnContext(ctx, "Ateom GC: skipping an entry that is not a pod UID", slog.String("name", name))
			continue
		}
		seen[name] = struct{}{}

		if _, live := podUIDs[name]; live {
			delete(g.strikes, name)
			continue
		}
		// A pod list can be momentarily incomplete; an ateom that answers is
		// alive whatever the list said.
		if ateomAnswered(g.probeWithTimeout(ctx, name)) {
			delete(g.strikes, name)
			continue
		}
		if ctx.Err() != nil {
			// Our own shutdown cut the probe short; that says nothing about
			// the socket.
			return
		}

		g.strikes[name]++
		if g.strikes[name] < ateomGCStrikes {
			kept++
			continue
		}
		dir := filepath.Join(g.ateomsDir, name)
		if age, ok := g.dirAge(dir); !ok || age < g.minAge {
			kept++
			continue
		}

		if err := os.RemoveAll(dir); err != nil {
			slog.WarnContext(ctx, "Ateom GC: removing the orphaned directory failed",
				slog.String("pod_uid", name), slog.Any("err", err))
			continue
		}
		delete(g.strikes, name)
		removed++
		slog.InfoContext(ctx, "Ateom GC: removed the orphaned directory", slog.String("pod_uid", name))
	}

	// Strikes describe directories that exist; drop the rest.
	for name := range g.strikes {
		if _, ok := seen[name]; !ok {
			delete(g.strikes, name)
		}
	}

	if removed > 0 || kept > 0 {
		slog.InfoContext(ctx, "Ateom GC pass complete",
			slog.Int("removed", removed), slog.Int("pending", kept))
	}
}

// probeWithTimeout runs the configured probe under ateomGCProbeTimeout.
func (g *ateomGC) probeWithTimeout(ctx context.Context, podUID string) error {
	probeCtx, cancel := context.WithTimeout(ctx, ateomGCProbeTimeout)
	defer cancel()
	return g.probe(probeCtx, podUID)
}

// dirAge is how long ago dir was last modified. Not ok when dir cannot be
// stated, which makes a vanished directory a non-candidate this pass.
func (g *ateomGC) dirAge(dir string) (time.Duration, bool) {
	info, err := os.Stat(dir)
	if err != nil {
		return 0, false
	}
	return g.now().Sub(info.ModTime()), true
}

// isCanonicalUUID reports whether name is a hyphenated lower-case UUID, the
// one form a pod UID takes.
func isCanonicalUUID(name string) bool {
	u, err := uuid.Parse(name)
	return err == nil && u.String() == name
}

// ateomAnswered reports whether a probe result proves a bound server. Any
// status error other than the transport-level codes does: a live ateom
// whose sandbox read fails answers Internal, and must not read as dead.
// Canceled is our own context ending; the caller handles that separately.
func ateomAnswered(err error) bool {
	if err == nil {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return false
	}
	return true
}

// nodePodUIDLister returns the real pod lister: every pod on nodeName, by UID.
// No label selector, so a pod that lost a label stays protected.
func nodePodUIDLister(client kubernetes.Interface, nodeName string) func(ctx context.Context) (map[string]struct{}, error) {
	return func(ctx context.Context) (map[string]struct{}, error) {
		listCtx, cancel := context.WithTimeout(ctx, ateomGCListTimeout)
		defer cancel()
		pods, err := client.CoreV1().Pods(metav1.NamespaceAll).List(listCtx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=" + nodeName,
		})
		if err != nil {
			return nil, err
		}
		uids := make(map[string]struct{}, len(pods.Items))
		for _, pod := range pods.Items {
			uids[string(pod.UID)] = struct{}{}
		}
		return uids, nil
	}
}

// probeAteom is the real probe: the stats sweep's discovery read over a
// short-lived connection. The reply's content does not matter.
func probeAteom(ctx context.Context, podUID string) error {
	conn, closer, err := dialAteomStats(podUID)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = ateompb.NewAteomClient(conn).GetActiveWorkloadStats(ctx, &ateompb.GetActiveWorkloadStatsRequest{})
	return err
}
