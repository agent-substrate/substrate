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

// The image-cache GC loop.
//
// A single serialized pass on a fixed period (the kubelet's shape — the
// heavy deletion work happens outside the pull path's locks, so there is
// nothing to duty-cycle). Each pass measures the cache volume with statfs
// and the pool's own recorded size, computes how many bytes to free —
// down to the low watermark when volume usage crossed the high one, and/or
// down to --image-cache-max-bytes when the pool outgrew it — and hands the
// larger target to Store.EvictUnused.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/spf13/pflag"
	"golang.org/x/sys/unix"
)

var (
	imageCacheGCPeriod = pflag.Duration("image-cache-gc-period", 5*time.Minute, "How often to run the image cache eviction pass. 0 disables the periodic pass (startup orphan recovery still runs at every atelet start).")
	imageCacheHighPct  = pflag.Int("image-cache-high-percent", 85, "Cache-volume usage percentage above which eviction starts.")
	imageCacheLowPct   = pflag.Int("image-cache-low-percent", 80, "Cache-volume usage percentage eviction frees down to. Must be lower than --image-cache-high-percent.")
	imageCacheMaxBytes = pflag.Int64("image-cache-max-bytes", 0, "Absolute cap on the summed size of cached layers, evicted down to independently of the volume watermarks. 0 means no cap.")
	imageCacheMinAge   = pflag.Duration("image-cache-min-age", 2*time.Minute, "Layers and image records younger than this are never evicted (protects images pulled but not yet mounted). Governs startup orphan recovery too, so it is live even with the periodic pass disabled.")
	imageCacheGCDryRun = pflag.Bool("image-cache-gc-dry-run", false, "Compute and log eviction decisions without deleting anything.")
)

const (
	// shortfallWarnLimit is how many consecutive shortfalls warn before the
	// loop backs off to shortfallReminderEvery.
	shortfallWarnLimit = 3
	// shortfallReminderEvery keeps a persistent shortfall visible without a
	// line per tick (at the 5m default: roughly hourly).
	shortfallReminderEvery = 12
)

func validateImageCacheGCFlags() error {
	if *imageCacheGCPeriod < 0 {
		// A negative period would silently disable the loop (it fails the
		// > 0 guard at the launch site, which also protects the ticker).
		return fmt.Errorf("--image-cache-gc-period %v must be >= 0", *imageCacheGCPeriod)
	}
	if *imageCacheHighPct < 1 || *imageCacheHighPct > 100 {
		return fmt.Errorf("--image-cache-high-percent %d out of range [1,100]", *imageCacheHighPct)
	}
	if *imageCacheLowPct < 0 || *imageCacheLowPct >= *imageCacheHighPct {
		return fmt.Errorf("--image-cache-low-percent %d must be in [0,%d)", *imageCacheLowPct, *imageCacheHighPct)
	}
	if *imageCacheMinAge < 0 {
		// A negative min-age inverts the veto (the cutoff lands in the
		// future), making just-pulled layers evictable.
		return fmt.Errorf("--image-cache-min-age %v must be >= 0", *imageCacheMinAge)
	}
	if imageCacheDirOutsideBasePath(*imageCacheDir) {
		slog.Warn("Image cache dir is outside the ateom base path; its volume watermarks are measured separately from actor state",
			slog.String("image_cache_dir", *imageCacheDir),
			slog.String("actors_dir", ateompath.ActorsDir))
	}
	return nil
}

// imageCacheDirOutsideBasePath reports whether the cache dir is outside
// the ateom base path — the watermarks then measure a different volume
// than actor state. Warn-worthy, not an error: a separate cache volume is
// legitimate (recommended for IOPS).
func imageCacheDirOutsideBasePath(dir string) bool {
	abs, err := filepath.Abs(dir)
	if err != nil {
		abs = filepath.Clean(dir)
	}
	return !strings.HasPrefix(abs, ateompath.BasePath+string(os.PathSeparator))
}

// imageCacheGCTarget computes the bytes a pass should free: the larger
// of the watermark shortfall (kubelet's formula — usage at highPct frees
// down to lowPct) and the pool's overage past maxBytes, but never more
// than the cache actually holds.
//
// The cache-size ceiling is the difference from kubelet, which owns its
// imagefs. This cache is one tenant of a shared volume, so the raw
// watermark target can dwarf it (measured: a 105 GiB volume at 98% asks
// an 11 MiB cache for 18.9 GiB), and an uncapped pass would evict
// everything every tick for a permanent 0% hit rate. Capped, the cache
// gives back all it can; the residual shortfall is someone else's disk —
// reported, not chased.
func imageCacheGCTarget(capacity, available uint64, cacheSize, maxBytes int64, highPct, lowPct int) int64 {
	var target int64
	if capacity > 0 {
		// Integer floor of the available fraction: usage reads up to ~1%
		// high, so eviction can trigger just before the nominal watermark.
		usedPct := 100 - int(available*100/capacity)
		if usedPct >= highPct {
			// Free enough that available climbs back to (100-lowPct)% of
			// capacity.
			target = int64(capacity)*int64(100-lowPct)/100 - int64(available)
		}
	}
	if maxBytes > 0 && cacheSize > maxBytes {
		if over := cacheSize - maxBytes; over > target {
			target = over
		}
	}
	if target > cacheSize {
		target = cacheSize
	}
	if target < 0 {
		target = 0
	}
	return target
}

// gcStore is what the loop needs from *imagecache.Store — a seam so
// runPass's skip and recovery paths are testable without a real pool.
type gcStore interface {
	CacheSize() (int64, error)
	EvictUnused(ctx context.Context, targetBytes int64, dryRun bool) (imagecache.EvictStats, error)
}

// imageCacheGC is the loop's state: configuration snapshotted from the
// flags at construction (the pass logic never reads globals, so it is
// testable without flag juggling) plus the shortfall-backoff counter.
type imageCacheGC struct {
	store    gcStore
	cacheDir string
	period   time.Duration
	highPct  int
	lowPct   int
	maxBytes int64
	dryRun   bool

	consecutiveShortfalls int
}

func newImageCacheGC(store *imagecache.Store, cacheDir string) *imageCacheGC {
	return &imageCacheGC{
		store:    store,
		cacheDir: cacheDir,
		period:   *imageCacheGCPeriod,
		highPct:  *imageCacheHighPct,
		lowPct:   *imageCacheLowPct,
		maxBytes: *imageCacheMaxBytes,
		dryRun:   *imageCacheGCDryRun,
	}
}

// Run executes eviction passes on the configured period until ctx is
// done. Passes are strictly serialized: a slow pass delays the next tick
// rather than overlapping it.
//
// atelet passes its root context (the StartMetricsServer convention), so
// the loop dies with the process; a pass cut off there leaves only .rm-*
// dirs for the startup sweep. Cancellation is honored for tests.
func (g *imageCacheGC) Run(ctx context.Context) {
	// First pass immediately: a node booting under disk pressure must not
	// wait a full period (startup recovery reclaims debris, not pressure).
	g.runPass(ctx)

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

// runPass performs one pass. It recovers from panics: this is a
// background janitor, and a bug here (or a malformed directory an operator
// dropped into the pool) must not take atelet down with it and strand every
// actor on the node.
func (g *imageCacheGC) runPass(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Image cache GC pass panicked; skipping this pass",
				slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()

	var st unix.Statfs_t
	if err := unix.Statfs(g.cacheDir, &st); err != nil {
		slog.WarnContext(ctx, "Image cache GC: statfs failed", slog.String("dir", g.cacheDir), slog.Any("err", err))
		return
	}
	capacity := st.Blocks * uint64(st.Bsize)
	available := st.Bavail * uint64(st.Bsize)

	// Same failure class as the enumeration gates (ReadDir of the layer
	// pool): fail toward retention, retry next tick.
	cacheSize, err := g.store.CacheSize()
	if err != nil {
		slog.WarnContext(ctx, "Image cache GC: sizing the pool failed; skipping this pass",
			slog.Any("err", err))
		return
	}

	target := imageCacheGCTarget(capacity, available, cacheSize, g.maxBytes, g.highPct, g.lowPct)

	tStart := time.Now()
	// Runs even at target 0: the enumeration gates should surface a
	// corrupt record or spec on the next tick, not first under disk
	// pressure. Cost: the full root-set scan (a ReadDir per actor, a
	// read per bundle spec) plus a read per image record — hundreds of
	// small reads on a busy node. Deliberate, and cheap at this period.
	stats, err := g.store.EvictUnused(ctx, target, g.dryRun)
	attrs := []any{
		slog.Int64("target_bytes", target),
		slog.Int64("freed_bytes", stats.FreedBytes),
		slog.Int("evicted_images", stats.EvictedImages),
		slog.Int("evicted_layers", stats.EvictedLayers),
		slog.Int("candidates", stats.Candidates),
		slog.Int("rooted_images", stats.RootedImages),
		slog.Int("orphan_layers", stats.OrphanLayers),
		slog.Int("skipped_rooted", stats.SkippedRooted),
		slog.Int("skipped_fresh", stats.SkippedFresh),
		slog.Int64("cache_size_bytes", cacheSize),
		slog.Bool("dry_run", g.dryRun),
		slog.Duration("took", time.Since(tStart)),
	}
	g.noteOutcome(ctx, classifyGCPass(err, target, stats.FreedBytes), err, attrs)
}

// noteOutcome logs one finished pass and advances the shortfall backoff.
// The counter survives a gated pass (which says nothing about whether the
// cache can meet a target) and resets when a target is met or absent.
func (g *imageCacheGC) noteOutcome(ctx context.Context, outcome gcPassOutcome, err error, attrs []any) {
	if outcome == gcPassSkipped {
		slog.ErrorContext(ctx, "Image cache GC pass skipped", append(attrs, slog.Any("err", err))...)
		return
	}
	if err != nil {
		// Per-item failures; each retries next pass.
		slog.WarnContext(ctx, "Image cache GC pass finished with errors", append(attrs, slog.Any("err", err))...)
	}
	switch outcome {
	case gcPassShortfall:
		// The capped target means a shortfall is "the cache cannot give
		// more" — on a volume under foreign pressure, the steady state.
		// Warn on the first few, then a periodic reminder, never
		// ERROR-per-tick.
		g.consecutiveShortfalls++
		switch {
		case g.consecutiveShortfalls <= shortfallWarnLimit:
			slog.WarnContext(ctx, "Image cache GC could not reach target",
				append(attrs, slog.Int("consecutive", g.consecutiveShortfalls))...)
		case g.consecutiveShortfalls%shortfallReminderEvery == 0:
			slog.WarnContext(ctx, "Image cache GC still short of target; the remaining pressure is not the image cache's to free",
				append(attrs, slog.Int("consecutive", g.consecutiveShortfalls))...)
		}
	case gcPassComplete:
		g.consecutiveShortfalls = 0
		slog.InfoContext(ctx, "Image cache GC pass complete", attrs...)
	default: // gcPassQuiet: no target, nothing to say.
		g.consecutiveShortfalls = 0
	}
}

// gcPassOutcome classifies one finished pass for logging and backoff.
type gcPassOutcome int

const (
	gcPassSkipped   gcPassOutcome = iota // gated: nothing was attempted
	gcPassShortfall                      // ran; target not met
	gcPassComplete                       // ran; target met
	gcPassQuiet                          // no target
)

// classifyGCPass keeps the gate-vs-shortfall distinction testable and on
// contract (the engine's sentinel), not inferred from stats: a gated pass
// means "repair the named file", never "the cache cannot give more".
func classifyGCPass(err error, target, freed int64) gcPassOutcome {
	switch {
	case errors.Is(err, imagecache.ErrIncompleteEnumeration):
		return gcPassSkipped
	case target > 0 && freed < target:
		return gcPassShortfall
	case target > 0:
		return gcPassComplete
	default:
		return gcPassQuiet
	}
}
