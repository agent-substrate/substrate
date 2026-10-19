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

package filecachegc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/pflag"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/ateattr"
)

const testCache = "test_cache"

func TestTarget(t *testing.T) {
	for _, tc := range []struct {
		name            string
		total, maxBytes int64
		want            int64
	}{
		{"no budget", 500, 0, 0},
		{"within budget", 500, 1000, 0},
		{"at budget", 1000, 1000, 0},
		{"over budget frees down to the low water", 1500, 1000, 600},
		{"far over budget frees down to the low water", 10_000, 1000, 9100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := target(tc.total, tc.maxBytes); got != tc.want {
				t.Errorf("Target(%d, %d) = %d, want %d", tc.total, tc.maxBytes, got, tc.want)
			}
		})
	}
}

func TestFlagsValidate(t *testing.T) {
	f := NewFlags(pflag.NewFlagSet("test", pflag.ContinueOnError), "test-cache", "test cache")
	for _, tc := range []struct {
		name     string
		period   time.Duration
		maxBytes int64
		wantErr  bool
	}{
		{"defaults", *f.Period, *f.MaxBytes, false},
		{"disabled period", 0, 1 << 30, false},
		{"negative period", -time.Second, 1 << 30, true},
		{"negative budget", time.Minute, -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*f.Period, *f.MaxBytes = tc.period, tc.maxBytes
			if err := f.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// fakeStore stands in for the cache: a size to report and a canned eviction
// result.
type fakeStore struct {
	size         int64
	sizeErr      error
	sizeCalls    int
	evictCalls   int
	gotTarget    int64
	gotDryRun    bool
	stats        filecache.EvictStats
	evictErr     error
	panicOnEvict bool
}

func (f *fakeStore) TotalBytes(context.Context) (int64, error) {
	f.sizeCalls++
	return f.size, f.sizeErr
}

func (f *fakeStore) EvictUnused(_ context.Context, target int64, dryRun bool) (filecache.EvictStats, error) {
	f.evictCalls++
	f.gotTarget = target
	f.gotDryRun = dryRun
	if f.panicOnEvict {
		panic("boom")
	}
	return f.stats, f.evictErr
}

// newTestGC returns a loop over fake with instruments on a manual reader, so
// a test can assert what a pass recorded.
func newTestGC(t *testing.T, fake *fakeStore, maxBytes int64) (*gc, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := NewInstruments(mp.Meter("atelet"))
	if err != nil {
		t.Fatalf("NewInstruments: %v", err)
	}
	if err := inst.observe(testCache, fake); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	return &gc{name: testCache, store: fake, instruments: inst, maxBytes: maxBytes, period: time.Hour}, reader
}

// collectMetric returns the named metric from one collection, or nil if it
// was not emitted.
func collectMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) *metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

// sumByOutcome returns a counter's data points keyed by the GC outcome,
// checking that every point carries the cache's name.
func sumByOutcome(t *testing.T, m *metricdata.Metrics) map[string]int64 {
	t.Helper()
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok || !sum.IsMonotonic {
		t.Fatalf("%s is %T (monotonic=%v), want a monotonic int64 sum", m.Name, m.Data, ok && sum.IsMonotonic)
	}
	out := map[string]int64{}
	for _, dp := range sum.DataPoints {
		if name, ok := dp.Attributes.Value(ateattr.FileCacheNameKey); !ok || name.AsString() != testCache {
			t.Fatalf("%s datapoint without the cache's name: %v", m.Name, dp.Attributes.ToSlice())
		}
		v, ok := dp.Attributes.Value(ateattr.FileCacheGCOutcomeKey)
		if !ok {
			t.Fatalf("%s datapoint without an outcome: %v", m.Name, dp.Attributes.ToSlice())
		}
		out[v.AsString()] += dp.Value
	}
	return out
}

func TestPassWithinBudgetDoesNothing(t *testing.T) {
	fake := &fakeStore{size: 500}
	g, reader := newTestGC(t, fake, 1000)
	g.runPass(context.Background())
	if fake.evictCalls != 0 {
		t.Errorf("evictCalls = %d, want 0 within budget", fake.evictCalls)
	}
	if m := collectMetric(t, reader, gcPassesMetric); m != nil {
		t.Errorf("a quiet pass recorded %v, want nothing", m)
	}
}

func TestPassSkipsWhenSizingFails(t *testing.T) {
	fake := &fakeStore{sizeErr: errors.New("unreadable")}
	g, reader := newTestGC(t, fake, 1000)
	g.runPass(context.Background())
	if fake.evictCalls != 0 {
		t.Errorf("evictCalls = %d, want 0 when the cache cannot be sized", fake.evictCalls)
	}
	fake.sizeErr = nil // so the size callback does not fail the collection
	m := collectMetric(t, reader, gcPassesMetric)
	if m == nil {
		t.Fatal("skipped pass not recorded")
	}
	if got := sumByOutcome(t, m); got[ateattr.FileCacheGCOutcomeSkipped] != 1 || len(got) != 1 {
		t.Errorf("passes by outcome = %v, want one skipped", got)
	}
}

func TestPassEvictsDownToTheLowWater(t *testing.T) {
	fake := &fakeStore{size: 1500, stats: filecache.EvictStats{Retired: 3, FreedBytes: 600}}
	g, reader := newTestGC(t, fake, 1000)
	g.runPass(context.Background())
	if fake.evictCalls != 1 || fake.gotTarget != 600 || fake.gotDryRun {
		t.Errorf("evict call: calls=%d target=%d dryRun=%v, want 1/600/false", fake.evictCalls, fake.gotTarget, fake.gotDryRun)
	}
	passes := collectMetric(t, reader, gcPassesMetric)
	if passes == nil {
		t.Fatal("pass not recorded")
	}
	if got := sumByOutcome(t, passes); got[ateattr.FileCacheGCOutcomeComplete] != 1 || len(got) != 1 {
		t.Errorf("passes by outcome = %v, want one complete", got)
	}
	evictions := collectMetric(t, reader, evictionsMetric)
	if evictions == nil {
		t.Fatal("evictions not recorded")
	}
	sum, ok := evictions.Data.(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 3 {
		t.Fatalf("evictions = %+v, want a single datapoint of 3", evictions.Data)
	}
	if name, ok := sum.DataPoints[0].Attributes.Value(ateattr.FileCacheNameKey); !ok || name.AsString() != testCache || sum.DataPoints[0].Attributes.Len() != 1 {
		t.Errorf("evictions labels = %v, want the cache's name only", sum.DataPoints[0].Attributes.ToSlice())
	}
	if evictions.Unit != "{entry}" || passes.Unit != "{pass}" {
		t.Errorf("units = %q / %q, want {entry} / {pass}", evictions.Unit, passes.Unit)
	}
}

// TestDryRunCountsNoEvictions pins the dry run's metrics: the pass is
// recorded with its outcome, since it ran and decided, but the entries it
// would have taken are not evictions, because none was removed.
func TestDryRunCountsNoEvictions(t *testing.T) {
	fake := &fakeStore{size: 1500, stats: filecache.EvictStats{Retired: 3, FreedBytes: 600}}
	g, reader := newTestGC(t, fake, 1000)
	g.dryRun = true
	g.runPass(context.Background())
	if fake.evictCalls != 1 || !fake.gotDryRun {
		t.Errorf("evict call: calls=%d dryRun=%v, want 1/true", fake.evictCalls, fake.gotDryRun)
	}
	passes := collectMetric(t, reader, gcPassesMetric)
	if passes == nil {
		t.Fatal("pass not recorded")
	}
	if got := sumByOutcome(t, passes); got[ateattr.FileCacheGCOutcomeComplete] != 1 || len(got) != 1 {
		t.Errorf("passes by outcome = %v, want one complete", got)
	}
	if evictions := collectMetric(t, reader, evictionsMetric); evictions != nil {
		t.Errorf("evictions = %+v after a dry run, want none recorded", evictions.Data)
	}
}

func TestPassShortfall(t *testing.T) {
	// Everything left is linked: the pass frees less than asked.
	fake := &fakeStore{size: 1500, stats: filecache.EvictStats{Retired: 1, FreedBytes: 100, SkippedLinked: 4}}
	g, reader := newTestGC(t, fake, 1000)
	for range 3 {
		g.runPass(context.Background())
	}
	if g.consecutiveShortfalls != 3 {
		t.Errorf("consecutiveShortfalls = %d, want 3", g.consecutiveShortfalls)
	}
	if got := sumByOutcome(t, collectMetric(t, reader, gcPassesMetric)); got[ateattr.FileCacheGCOutcomeShortfall] != 3 {
		t.Errorf("passes by outcome = %v, want three shortfalls", got)
	}
	// A pass that meets the budget resets the streak.
	fake.size = 500
	g.runPass(context.Background())
	if g.consecutiveShortfalls != 0 {
		t.Errorf("consecutiveShortfalls after a quiet pass = %d, want 0", g.consecutiveShortfalls)
	}
}

// TestSizeIsObservedPerCache pins the size instrument: one series per cache
// name, read from each store on collection.
func TestSizeIsObservedPerCache(t *testing.T) {
	fake := &fakeStore{size: 4096}
	_, reader := newTestGC(t, fake, 0)
	m := collectMetric(t, reader, sizeMetric)
	if m == nil {
		t.Fatal("size not observed")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok || sum.IsMonotonic || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 4096 {
		t.Fatalf("size = %+v, want a non-monotonic int64 sum of 4096", m.Data)
	}
	if m.Unit != "By" {
		t.Errorf("size unit = %q, want By", m.Unit)
	}
	if name, ok := sum.DataPoints[0].Attributes.Value(ateattr.FileCacheNameKey); !ok || name.AsString() != testCache || sum.DataPoints[0].Attributes.Len() != 1 {
		t.Errorf("size labels = %v, want the cache's name only", sum.DataPoints[0].Attributes.ToSlice())
	}
}

// TestTwoCachesShareTheInstruments pins that a second cache on the same
// instruments gets its own series under its own name.
func TestTwoCachesShareTheInstruments(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := NewInstruments(mp.Meter("atelet"))
	if err != nil {
		t.Fatal(err)
	}
	a, b := &fakeStore{size: 1}, &fakeStore{size: 2}
	for name, store := range map[string]*fakeStore{"a": a, "b": b} {
		if err := inst.observe(name, store); err != nil {
			t.Fatal(err)
		}
	}
	m := collectMetric(t, reader, sizeMetric)
	if m == nil {
		t.Fatal("size not observed")
	}
	got := map[string]int64{}
	for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
		name, _ := dp.Attributes.Value(ateattr.FileCacheNameKey)
		got[name.AsString()] = dp.Value
	}
	if got["a"] != 1 || got["b"] != 2 || len(got) != 2 {
		t.Errorf("size by cache = %v, want a=1 b=2", got)
	}
}

// TestStartRejectsInvalidFlags pins that Start validates the flags itself,
// so a caller cannot start a loop on values it would silently ignore: a
// negative period with a budget would leave the cache unbounded unnoticed.
func TestStartRejectsInvalidFlags(t *testing.T) {
	f := NewFlags(pflag.NewFlagSet("test", pflag.ContinueOnError), "test-cache", "test cache")
	*f.Period, *f.MaxBytes = -time.Second, 1<<30
	reader := sdkmetric.NewManualReader()
	inst, err := NewInstruments(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("atelet"))
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeStore{size: 1}
	if err := Start(context.Background(), testCache, fake, f, inst); err == nil {
		t.Fatal("Start with a negative period = nil, want an error")
	}
	if m := collectMetric(t, reader, sizeMetric); m != nil {
		t.Error("Start observed the size of a cache whose flags it rejected")
	}
}

func TestRunFirstPassIsImmediate(t *testing.T) {
	// A cancelled context and an hour-long period: the single call can
	// only be the immediate first pass, never a tick.
	fake := &fakeStore{size: 1500}
	g, _ := newTestGC(t, fake, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.run(ctx)
	if fake.evictCalls != 1 {
		t.Errorf("evictCalls = %d, want exactly 1 (the immediate first pass)", fake.evictCalls)
	}
}

func TestRunTicks(t *testing.T) {
	fake := &fakeStore{size: 1500}
	g, _ := newTestGC(t, fake, 1000)
	g.period = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	g.run(ctx)
	if fake.evictCalls < 2 {
		t.Errorf("evictCalls = %d, want >= 2 (first pass plus at least one tick)", fake.evictCalls)
	}
}

func TestPassRecoversPanic(t *testing.T) {
	g, _ := newTestGC(t, &fakeStore{size: 1500, panicOnEvict: true}, 1000)
	g.runPass(context.Background()) // must not propagate the panic
}
