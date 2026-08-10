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
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/spf13/pflag"
	"golang.org/x/sys/unix"
)

var (
	imageCacheGCPeriod = pflag.Duration("image-cache-gc-period", 5*time.Minute, "How often to run the image cache eviction pass. 0 disables eviction entirely.")
	imageCacheHighPct  = pflag.Int("image-cache-high-percent", 85, "Cache-volume usage percentage above which eviction starts.")
	imageCacheLowPct   = pflag.Int("image-cache-low-percent", 80, "Cache-volume usage percentage eviction frees down to. Must be lower than --image-cache-high-percent.")
	imageCacheMaxBytes = pflag.Int64("image-cache-max-bytes", 0, "Absolute cap on the summed size of cached layers, evicted down to independently of the volume watermarks. 0 means no cap.")
	imageCacheMinAge   = pflag.Duration("image-cache-min-age", 2*time.Minute, "Layers and image records younger than this are never evicted (protects images pulled but not yet mounted).")
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
	if *imageCacheHighPct < 1 || *imageCacheHighPct > 100 {
		return fmt.Errorf("--image-cache-high-percent %d out of range [1,100]", *imageCacheHighPct)
	}
	if *imageCacheLowPct < 0 || *imageCacheLowPct >= *imageCacheHighPct {
		return fmt.Errorf("--image-cache-low-percent %d must be in [0,%d)", *imageCacheLowPct, *imageCacheHighPct)
	}
	// The watermark is measured on the cache dir's filesystem while the root
	// set is read from the actors dir. If an operator points the cache at a
	// different volume than BasePath, those are different filesystems: the
	// pass would evict based on disk pressure the cache doesn't contribute
	// to. Warn rather than fail — a separate cache volume is a legitimate
	// (and recommended, for IOPS) configuration, it just wants its own
	// watermarks.
	if !strings.HasPrefix(*imageCacheDir, ateompath.BasePath+string(os.PathSeparator)) {
		slog.Warn("Image cache dir is outside the ateom base path; its volume watermarks are measured separately from actor state",
			slog.String("image_cache_dir", *imageCacheDir),
			slog.String("actors_dir", ateompath.ActorsDir))
	}
	return nil
}

// imageCacheGCTarget computes the bytes an eviction pass should free.
//
// Watermark half (kubelet's formula): when volume usage is at or above the
// high watermark, free down to the low one. Cap half: when the pool's
// recorded size exceeds maxBytes, free the difference. The pass pursues the
// larger — but never more than the cache actually holds.
//
// That cache-size ceiling is the important difference from kubelet, which
// owns its imagefs and can therefore assume the whole shortfall is its to
// free. Our cache is one tenant of a volume it shares with containerd's
// image store, kubelet, logs, actor uppers and local snapshots, so the raw
// watermark target asks the cache to free far more than it holds (measured:
// a 105 GiB volume at 98% yields an 18.9 GiB target against an 11 MiB
// cache). The pass would then evict every unrooted image on every tick —
// a permanent 0% hit rate, turning every actor start back into a full
// re-pull — while barely moving disk usage. Capped at its own size, the
// cache gives back everything it can and no more; the residual shortfall
// is reported (it is someone else's disk), not chased.
func imageCacheGCTarget(capacity, available uint64, cacheSize, maxBytes int64, highPct, lowPct int) int64 {
	var target int64
	if capacity > 0 {
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

// runImageCacheGC runs eviction passes on the configured period until ctx
// is done. Passes are strictly serialized: a slow pass delays the next
// tick rather than overlapping it.
func runImageCacheGC(ctx context.Context, store *imagecache.Store, cacheDir string) {
	ticker := time.NewTicker(*imageCacheGCPeriod)
	defer ticker.Stop()

	consecutiveShortfalls := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		runImageCacheGCPass(ctx, store, cacheDir, &consecutiveShortfalls)
	}
}

// runImageCacheGCPass performs one pass. It recovers from panics: this is a
// background janitor, and a bug here (or a malformed directory an operator
// dropped into the pool) must not take atelet down with it and strand every
// actor on the node.
func runImageCacheGCPass(ctx context.Context, store *imagecache.Store, cacheDir string, consecutiveShortfalls *int) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Image cache GC pass panicked; skipping this pass",
				slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()

	var st unix.Statfs_t
	if err := unix.Statfs(cacheDir, &st); err != nil {
		slog.WarnContext(ctx, "Image cache GC: statfs failed", slog.String("dir", cacheDir), slog.Any("err", err))
		return
	}
	capacity := st.Blocks * uint64(st.Bsize)
	available := st.Bavail * uint64(st.Bsize)

	// A sizing error is not fatal to the pass: the watermark half of the
	// target needs only statfs.
	cacheSize, err := store.CacheSize()
	if err != nil {
		slog.WarnContext(ctx, "Image cache GC: sizing the pool failed; watermark target only this pass",
			slog.Any("err", err))
		cacheSize = 0
	}

	target := imageCacheGCTarget(capacity, available, cacheSize, *imageCacheMaxBytes, *imageCacheHighPct, *imageCacheLowPct)

	tStart := time.Now()
	stats, err := store.EvictUnused(ctx, target, *imageCacheGCDryRun)
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
		slog.Bool("dry_run", *imageCacheGCDryRun),
		slog.Duration("took", time.Since(tStart)),
	}
	if err != nil {
		if stats.Candidates == 0 && stats.EvictedImages == 0 {
			// Enumeration-gated pass: nothing was attempted, so this is a
			// wedged pass, not a shortfall — the shortfall accounting below
			// would misread the zero stats as "cache cannot give more".
			slog.ErrorContext(ctx, "Image cache GC pass skipped", append(attrs, slog.Any("err", err))...)
			return
		}
		// Per-item failures were already skipped inside the pass; log the
		// aggregate and let the next pass retry.
		slog.WarnContext(ctx, "Image cache GC pass finished with errors", append(attrs, slog.Any("err", err))...)
	}
	switch {
	case target > 0 && stats.FreedBytes < target:
		// Everything eligible was evicted and the target still wasn't
		// met: the remainder is rooted, pinned, or fresh. Now that the
		// target is capped at the cache's own size, a shortfall means
		// the cache genuinely cannot give back more — which on a volume
		// under foreign pressure is the steady state, so this must not
		// log at ERROR every tick. Warn on the first few, then drop to a
		// periodic reminder.
		*consecutiveShortfalls++
		switch {
		case *consecutiveShortfalls <= shortfallWarnLimit:
			slog.WarnContext(ctx, "Image cache GC could not reach target",
				append(attrs, slog.Int("consecutive", *consecutiveShortfalls))...)
		case *consecutiveShortfalls%shortfallReminderEvery == 0:
			slog.WarnContext(ctx, "Image cache GC still short of target; the remaining pressure is not the image cache's to free",
				append(attrs, slog.Int("consecutive", *consecutiveShortfalls))...)
		}
	case target > 0:
		*consecutiveShortfalls = 0
		slog.InfoContext(ctx, "Image cache GC pass complete", attrs...)
	default:
		// No disk pressure and no orphans: stay quiet. The gauges are
		// the "GC is alive" signal, not a log line per tick.
		*consecutiveShortfalls = 0
	}
}
