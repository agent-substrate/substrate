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

// Package filecachegc runs the eviction loop of a node-local file cache: a
// filecache store with a byte budget, evicted least recently used first,
// down to a low-water fraction of the budget, never taking an entry a
// consumer still links. One loop per cache, one set of flags per cache under
// the cache's flag prefix, and one set of instruments for all of them,
// labeled by cache name.
package filecachegc

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

// lowWater is the fraction of the budget a pass evicts down to once the
// cache is over it, so passes do not fire on every tick while usage hovers
// at the budget.
const lowWater = 0.9

const (
	// shortfallWarnLimit is how many consecutive shortfalls warn before the
	// loop backs off to shortfallReminderEvery.
	shortfallWarnLimit = 3
	// shortfallReminderEvery keeps a persistent shortfall visible without a
	// line per tick (at the 5m default: roughly hourly).
	shortfallReminderEvery = 12
)

const (
	sizeMetric      = "ate.filecache.size"
	evictionsMetric = "ate.filecache.evictions"
	gcPassesMetric  = "ate.filecache.gc.passes"
)

// Store is what the loop needs from a *filecache.Store, a seam so a pass's
// skip and recovery paths are testable without a real cache.
type Store interface {
	TotalBytes(ctx context.Context) (int64, error)
	EvictUnused(ctx context.Context, targetBytes int64, dryRun bool) (filecache.EvictStats, error)
}

// Flags are one cache's eviction flags: --<prefix>-gc-period,
// --<prefix>-max-bytes and --<prefix>-gc-dry-run.
type Flags struct {
	Period   *time.Duration
	MaxBytes *int64
	DryRun   *bool
	prefix   string
}

// NewFlags registers the eviction flags of the cache called what, for
// example "sandbox asset cache", under prefix, for example
// "sandbox-asset-cache", on fs.
func NewFlags(fs *pflag.FlagSet, prefix, what string) *Flags {
	return &Flags{
		prefix:   prefix,
		Period:   fs.Duration(prefix+"-gc-period", 5*time.Minute, fmt.Sprintf("How often to run the %s eviction pass. 0 disables it.", what)),
		MaxBytes: fs.Int64(prefix+"-max-bytes", 0, fmt.Sprintf("Budget for the %s, in bytes of disk the cached files occupy. A pass that finds the cache over it evicts least recently used entries that nothing links, down to %d%% of it. 0 means no budget: the cache only grows.", what, int(lowWater*100))),
		DryRun:   fs.Bool(prefix+"-gc-dry-run", false, fmt.Sprintf("Compute and log eviction decisions for the %s without deleting anything.", what)),
	}
}

// Validate rejects values the loop could not act on.
func (f *Flags) Validate() error {
	if *f.Period < 0 {
		return fmt.Errorf("--%s-gc-period %v must be >= 0", f.prefix, *f.Period)
	}
	if *f.MaxBytes < 0 {
		return fmt.Errorf("--%s-max-bytes %d must be >= 0", f.prefix, *f.MaxBytes)
	}
	return nil
}

// target computes the bytes a pass should free: nothing while the cache is
// within its budget, otherwise enough to bring it down to the low-water
// fraction of the budget. A zero budget never asks for anything.
func target(total, maxBytes int64) int64 {
	if maxBytes <= 0 || total <= maxBytes {
		return 0
	}
	target := total - int64(float64(maxBytes)*lowWater)
	if target > total {
		target = total
	}
	return target
}

// Instruments are the metrics of every file cache on the node, one set
// labeled by cache name: how much disk each holds, how many entries its
// passes evict, and how each pass ended. Nil-safe: a nil receiver records
// nothing.
type Instruments struct {
	meter     metric.Meter
	size      metric.Int64ObservableUpDownCounter
	evictions metric.Int64Counter
	passes    metric.Int64Counter
}

// NewInstruments creates the instruments on meter, once per process.
func NewInstruments(meter metric.Meter) (*Instruments, error) {
	size, err := meter.Int64ObservableUpDownCounter(
		sizeMetric,
		metric.WithUnit("By"),
		metric.WithDescription("Bytes of disk a node-local file cache occupies, as the sum of its entries' allocated sizes, by cache."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s updowncounter: %w", sizeMetric, err)
	}
	evictions, err := meter.Int64Counter(
		evictionsMetric,
		metric.WithUnit("{entry}"),
		metric.WithDescription("Number of entries eviction passes removed from a node-local file cache, by cache."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", evictionsMetric, err)
	}
	passes, err := meter.Int64Counter(
		gcPassesMetric,
		metric.WithUnit("{pass}"),
		metric.WithDescription("Number of eviction passes over a node-local file cache that had work to do, by cache and outcome."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", gcPassesMetric, err)
	}
	return &Instruments{meter: meter, size: size, evictions: evictions, passes: passes}, nil
}

// observe has the size of store reported under name on every collection.
// The callback walks the store's entries; that is a few hundred stats on a
// full node. Called whether or not the cache has a budget, so the size has
// a series on every install.
func (i *Instruments) observe(name string, store Store) error {
	if i == nil {
		return nil
	}
	attrs := metric.WithAttributes(ateattr.FileCacheNameKey.String(name))
	_, err := i.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		total, err := store.TotalBytes(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(i.size, total, attrs)
		return nil
	}, i.size)
	if err != nil {
		return fmt.Errorf("register %s callback for %s: %w", sizeMetric, name, err)
	}
	return nil
}

// recordPass records one pass over name that had work to do, with what it
// evicted.
func (i *Instruments) recordPass(ctx context.Context, name, outcome string, evicted int) {
	if i == nil {
		return
	}
	i.passes.Add(ctx, 1, metric.WithAttributes(ateattr.FileCacheNameKey.String(name), ateattr.FileCacheGCOutcomeKey.String(outcome)))
	if evicted > 0 {
		i.evictions.Add(ctx, int64(evicted), metric.WithAttributes(ateattr.FileCacheNameKey.String(name)))
	}
}

// gc is one cache's eviction loop: configuration snapshotted from the flags
// at construction, and the shortfall backoff.
type gc struct {
	name        string
	store       Store
	instruments *Instruments
	period      time.Duration
	maxBytes    int64
	dryRun      bool

	consecutiveShortfalls int
}

// newGC returns the loop for the cache called name, a value of
// ateattr.FileCacheNameKey.
func newGC(name string, store Store, flags *Flags, instruments *Instruments) *gc {
	return &gc{
		name:        name,
		store:       store,
		instruments: instruments,
		period:      *flags.Period,
		maxBytes:    *flags.MaxBytes,
		dryRun:      *flags.DryRun,
	}
}

// Start validates the cache's flags, reports its size through instruments,
// and runs its eviction loop in the background when the flags give it a
// budget and a period. Without a budget it warns once: the cache then only
// grows.
func Start(ctx context.Context, name string, store Store, flags *Flags, instruments *Instruments) error {
	if err := flags.Validate(); err != nil {
		return err
	}
	if err := instruments.observe(name, store); err != nil {
		return err
	}
	switch {
	case *flags.MaxBytes == 0:
		slog.WarnContext(ctx, "File cache has no budget; it only grows", slog.String("cache", name), slog.String("flag", "--"+flags.prefix+"-max-bytes"))
	case *flags.Period > 0:
		go newGC(name, store, flags, instruments).run(ctx)
	}
	return nil
}

// run executes eviction passes on the configured period until ctx is done,
// the first one immediately so a node booting over budget does not wait a
// full period. Passes are strictly serialized.
func (g *gc) run(ctx context.Context) {
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
func (g *gc) runPass(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "File cache GC pass panicked; skipping this pass",
				slog.String("cache", g.name), slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()

	total, err := g.store.TotalBytes(ctx)
	if err != nil {
		// Fail toward retention: retry next tick.
		slog.WarnContext(ctx, "File cache GC: sizing the cache failed; skipping this pass", slog.String("cache", g.name), slog.Any("err", err))
		g.instruments.recordPass(ctx, g.name, ateattr.FileCacheGCOutcomeSkipped, 0)
		return
	}
	target := target(total, g.maxBytes)
	if target == 0 {
		// Within budget: nothing to do, nothing to say.
		g.consecutiveShortfalls = 0
		return
	}

	tStart := time.Now()
	stats, err := g.store.EvictUnused(ctx, target, g.dryRun)
	attrs := []any{
		slog.String("cache", g.name),
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
		slog.WarnContext(ctx, "File cache GC pass finished with errors", append(attrs, slog.Any("err", err))...)
	}
	if stats.FreedBytes < target {
		// Everything left is linked, young, or busy: the remaining usage is
		// not the cache's to free. Warn on the first few, then a periodic
		// reminder, never a line per tick.
		g.consecutiveShortfalls++
		switch {
		case g.consecutiveShortfalls <= shortfallWarnLimit:
			slog.WarnContext(ctx, "File cache GC could not reach its budget",
				append(attrs, slog.Int("consecutive", g.consecutiveShortfalls))...)
		case g.consecutiveShortfalls%shortfallReminderEvery == 0:
			slog.WarnContext(ctx, "File cache GC still over its budget; what remains is linked by a consumer, younger than the min age, or busy",
				append(attrs, slog.Int("consecutive", g.consecutiveShortfalls))...)
		}
		g.instruments.recordPass(ctx, g.name, ateattr.FileCacheGCOutcomeShortfall, evicted)
		return
	}
	g.consecutiveShortfalls = 0
	slog.InfoContext(ctx, "File cache GC pass complete", attrs...)
	g.instruments.recordPass(ctx, g.name, ateattr.FileCacheGCOutcomeComplete, evicted)
}
