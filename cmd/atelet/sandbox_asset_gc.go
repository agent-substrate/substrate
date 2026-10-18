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

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel/metric"

	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/ateattr"
)

var (
	sandboxAssetCacheGCPeriod = pflag.Duration("sandbox-asset-cache-gc-period", 5*time.Minute, "How often to run the sandbox asset cache eviction pass. 0 disables it.")
	sandboxAssetCacheMaxBytes = pflag.Int64("sandbox-asset-cache-max-bytes", 0, "Budget for the sandbox asset cache, in bytes of disk the cached assets occupy. A pass that finds the cache over it evicts least recently used assets that no actor and no worker's default runtime links, down to 90% of it. 0 means no budget: the cache only grows.")
	sandboxAssetCacheGCDryRun = pflag.Bool("sandbox-asset-cache-gc-dry-run", false, "Compute and log eviction decisions for the sandbox asset cache without deleting anything.")
)

// sandboxAssetCacheGCLowWater is the fraction of the budget a pass evicts
// down to once the cache is over it, so passes do not fire on every tick
// while usage hovers at the budget.
const sandboxAssetCacheGCLowWater = 0.9

const (
	sandboxAssetCacheSizeMetric      = "ate.sandboxassetcache.size"
	sandboxAssetCacheEvictionsMetric = "ate.sandboxassetcache.evictions"
	sandboxAssetCacheGCPassesMetric  = "ate.sandboxassetcache.gc.passes"
)

func validateSandboxAssetCacheGCFlags() error {
	if *sandboxAssetCacheGCPeriod < 0 {
		return fmt.Errorf("--sandbox-asset-cache-gc-period %v must be >= 0", *sandboxAssetCacheGCPeriod)
	}
	if *sandboxAssetCacheMaxBytes < 0 {
		return fmt.Errorf("--sandbox-asset-cache-max-bytes %d must be >= 0", *sandboxAssetCacheMaxBytes)
	}
	return nil
}

// sandboxAssetCacheGCTarget computes the bytes a pass should free: nothing
// while the cache is within its budget, otherwise enough to bring it down
// to the low-water fraction of the budget. A zero budget never asks for
// anything.
func sandboxAssetCacheGCTarget(total, maxBytes int64) int64 {
	if maxBytes <= 0 || total <= maxBytes {
		return 0
	}
	target := total - int64(float64(maxBytes)*sandboxAssetCacheGCLowWater)
	if target > total {
		target = total
	}
	return target
}

// assetGCStore is what the loop needs from *filecache.Store, a seam so the
// pass's skip and recovery paths are testable without a real cache.
type assetGCStore interface {
	TotalBytes(ctx context.Context) (int64, error)
	EvictUnused(ctx context.Context, targetBytes int64, dryRun bool) (filecache.EvictStats, error)
}

// sandboxAssetCacheInstruments are the metrics of the sandbox asset cache:
// how much disk it holds, how many entries its passes evict, and how each
// pass ended. Nil-safe: a nil receiver records nothing.
type sandboxAssetCacheInstruments struct {
	evictions metric.Int64Counter
	passes    metric.Int64Counter
}

// newSandboxAssetCacheInstruments creates the cache's instruments on meter.
// size is read on every collection through the callback, which walks the
// store's entries; that is a few hundred stats on a full node.
func newSandboxAssetCacheInstruments(meter metric.Meter, store assetGCStore) (*sandboxAssetCacheInstruments, error) {
	if _, err := meter.Int64ObservableUpDownCounter(
		sandboxAssetCacheSizeMetric,
		metric.WithUnit("By"),
		metric.WithDescription("Bytes of disk the node-local sandbox asset cache occupies, as the sum of its entries' allocated sizes."),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			total, err := store.TotalBytes(ctx)
			if err != nil {
				return err
			}
			o.Observe(total)
			return nil
		}),
	); err != nil {
		return nil, fmt.Errorf("create %s updowncounter: %w", sandboxAssetCacheSizeMetric, err)
	}
	evictions, err := meter.Int64Counter(
		sandboxAssetCacheEvictionsMetric,
		metric.WithUnit("{entry}"),
		metric.WithDescription("Number of entries eviction passes removed from the node-local sandbox asset cache."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", sandboxAssetCacheEvictionsMetric, err)
	}
	passes, err := meter.Int64Counter(
		sandboxAssetCacheGCPassesMetric,
		metric.WithUnit("{pass}"),
		metric.WithDescription("Number of eviction passes over the node-local sandbox asset cache that had work to do, by outcome."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", sandboxAssetCacheGCPassesMetric, err)
	}
	return &sandboxAssetCacheInstruments{evictions: evictions, passes: passes}, nil
}

// recordPass records one pass that had work to do, with what it evicted.
func (i *sandboxAssetCacheInstruments) recordPass(ctx context.Context, outcome string, evicted int) {
	if i == nil {
		return
	}
	i.passes.Add(ctx, 1, metric.WithAttributes(ateattr.SandboxAssetCacheGCOutcomeKey.String(outcome)))
	if evicted > 0 {
		i.evictions.Add(ctx, int64(evicted))
	}
}

// sandboxAssetCacheGC is the eviction loop's state: configuration
// snapshotted from the flags at construction, and the shortfall backoff.
type sandboxAssetCacheGC struct {
	store       assetGCStore
	instruments *sandboxAssetCacheInstruments
	period      time.Duration
	maxBytes    int64
	dryRun      bool

	consecutiveShortfalls int
}

func newSandboxAssetCacheGC(store assetGCStore, instruments *sandboxAssetCacheInstruments) *sandboxAssetCacheGC {
	return &sandboxAssetCacheGC{
		store:       store,
		instruments: instruments,
		period:      *sandboxAssetCacheGCPeriod,
		maxBytes:    *sandboxAssetCacheMaxBytes,
		dryRun:      *sandboxAssetCacheGCDryRun,
	}
}

// Run executes eviction passes on the configured period until ctx is done,
// the first one immediately so a node booting over budget does not wait a
// full period. Passes are strictly serialized.
func (g *sandboxAssetCacheGC) Run(ctx context.Context) {
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

// runPass performs one pass. It recovers from panics: this is a background
// janitor, and a bug here must not take atelet down with it and strand every
// actor on the node.
func (g *sandboxAssetCacheGC) runPass(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Sandbox asset cache GC pass panicked; skipping this pass",
				slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()

	total, err := g.store.TotalBytes(ctx)
	if err != nil {
		// Fail toward retention: retry next tick.
		slog.WarnContext(ctx, "Sandbox asset cache GC: sizing the cache failed; skipping this pass", slog.Any("err", err))
		g.instruments.recordPass(ctx, ateattr.SandboxAssetCacheGCOutcomeSkipped, 0)
		return
	}
	target := sandboxAssetCacheGCTarget(total, g.maxBytes)
	if target == 0 {
		// Within budget: nothing to do, nothing to say.
		g.consecutiveShortfalls = 0
		return
	}

	tStart := time.Now()
	stats, err := g.store.EvictUnused(ctx, target, g.dryRun)
	attrs := []any{
		slog.Int64("cache_bytes", total),
		slog.Int64("max_bytes", g.maxBytes),
		slog.Int64("target_bytes", target),
		slog.Int64("freed_bytes", stats.FreedBytes),
		slog.Int("evicted", stats.Retired),
		slog.Int("skipped_linked", stats.SkippedLinked),
		slog.Int("skipped_young", stats.SkippedYoung),
		slog.Int("skipped_busy", stats.SkippedBusy),
		slog.Bool("dry_run", g.dryRun),
		slog.Duration("took", time.Since(tStart)),
	}
	// A dry run retires nothing: its Retired is what a real pass would have
	// taken, which is logged above and must not count as an eviction.
	evicted := stats.Retired
	if g.dryRun {
		evicted = 0
	}
	if err != nil {
		// Per-entry failures; each retries next pass.
		slog.WarnContext(ctx, "Sandbox asset cache GC pass finished with errors", append(attrs, slog.Any("err", err))...)
	}
	if stats.FreedBytes < target {
		// Everything left is linked, young, or busy: the remaining usage is
		// not the cache's to free. Warn on the first few, then a periodic
		// reminder, never a line per tick.
		g.consecutiveShortfalls++
		switch {
		case g.consecutiveShortfalls <= shortfallWarnLimit:
			slog.WarnContext(ctx, "Sandbox asset cache GC could not reach its budget",
				append(attrs, slog.Int("consecutive", g.consecutiveShortfalls))...)
		case g.consecutiveShortfalls%shortfallReminderEvery == 0:
			slog.WarnContext(ctx, "Sandbox asset cache GC still over its budget; what remains is linked by actors or by workers for their default runtime",
				append(attrs, slog.Int("consecutive", g.consecutiveShortfalls))...)
		}
		g.instruments.recordPass(ctx, ateattr.SandboxAssetCacheGCOutcomeShortfall, evicted)
		return
	}
	g.consecutiveShortfalls = 0
	slog.InfoContext(ctx, "Sandbox asset cache GC pass complete", attrs...)
	g.instruments.recordPass(ctx, ateattr.SandboxAssetCacheGCOutcomeComplete, evicted)
}
