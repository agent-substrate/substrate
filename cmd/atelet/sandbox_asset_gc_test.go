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
	"errors"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/ateattr"
)

func TestSandboxAssetCacheGCTarget(t *testing.T) {
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
			if got := sandboxAssetCacheGCTarget(tc.total, tc.maxBytes); got != tc.want {
				t.Errorf("sandboxAssetCacheGCTarget(%d, %d) = %d, want %d", tc.total, tc.maxBytes, got, tc.want)
			}
		})
	}
}

func TestValidateSandboxAssetCacheGCFlags(t *testing.T) {
	origPeriod, origMax := *sandboxAssetCacheGCPeriod, *sandboxAssetCacheMaxBytes
	t.Cleanup(func() { *sandboxAssetCacheGCPeriod, *sandboxAssetCacheMaxBytes = origPeriod, origMax })
	for _, tc := range []struct {
		name     string
		period   time.Duration
		maxBytes int64
		wantErr  bool
	}{
		{"defaults", origPeriod, origMax, false},
		{"disabled period", 0, 1 << 30, false},
		{"negative period", -time.Second, 1 << 30, true},
		{"negative budget", time.Minute, -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*sandboxAssetCacheGCPeriod, *sandboxAssetCacheMaxBytes = tc.period, tc.maxBytes
			if err := validateSandboxAssetCacheGCFlags(); (err != nil) != tc.wantErr {
				t.Errorf("validateSandboxAssetCacheGCFlags() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// fakeAssetGCStore stands in for the cache: a size to report and a canned
// eviction result.
type fakeAssetGCStore struct {
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

func (f *fakeAssetGCStore) TotalBytes(context.Context) (int64, error) {
	f.sizeCalls++
	return f.size, f.sizeErr
}

func (f *fakeAssetGCStore) EvictUnused(_ context.Context, target int64, dryRun bool) (filecache.EvictStats, error) {
	f.evictCalls++
	f.gotTarget = target
	f.gotDryRun = dryRun
	if f.panicOnEvict {
		panic("boom")
	}
	return f.stats, f.evictErr
}

// newTestAssetCacheGC returns a loop over fake with instruments on a manual
// reader, so a test can assert what a pass recorded.
func newTestAssetCacheGC(t *testing.T, fake *fakeAssetGCStore, maxBytes int64) (*sandboxAssetCacheGC, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := newSandboxAssetCacheInstruments(mp.Meter("atelet"), fake)
	if err != nil {
		t.Fatalf("newSandboxAssetCacheInstruments: %v", err)
	}
	return &sandboxAssetCacheGC{store: fake, instruments: inst, maxBytes: maxBytes, period: time.Hour}, reader
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

// sumByOutcome returns a counter's data points keyed by the GC outcome.
func sumByOutcome(t *testing.T, m *metricdata.Metrics) map[string]int64 {
	t.Helper()
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok || !sum.IsMonotonic {
		t.Fatalf("%s is %T (monotonic=%v), want a monotonic int64 sum", m.Name, m.Data, ok && sum.IsMonotonic)
	}
	out := map[string]int64{}
	for _, dp := range sum.DataPoints {
		v, ok := dp.Attributes.Value(ateattr.SandboxAssetCacheGCOutcomeKey)
		if !ok {
			t.Fatalf("%s datapoint without an outcome: %v", m.Name, dp.Attributes.ToSlice())
		}
		out[v.AsString()] += dp.Value
	}
	return out
}

func TestSandboxAssetCacheGCPassWithinBudgetDoesNothing(t *testing.T) {
	fake := &fakeAssetGCStore{size: 500}
	g, reader := newTestAssetCacheGC(t, fake, 1000)
	g.runPass(context.Background())
	if fake.evictCalls != 0 {
		t.Errorf("evictCalls = %d, want 0 within budget", fake.evictCalls)
	}
	if m := collectMetric(t, reader, sandboxAssetCacheGCPassesMetric); m != nil {
		t.Errorf("a quiet pass recorded %v, want nothing", m)
	}
}

func TestSandboxAssetCacheGCPassSkipsWhenSizingFails(t *testing.T) {
	fake := &fakeAssetGCStore{sizeErr: errors.New("unreadable")}
	g, reader := newTestAssetCacheGC(t, fake, 1000)
	g.runPass(context.Background())
	if fake.evictCalls != 0 {
		t.Errorf("evictCalls = %d, want 0 when the cache cannot be sized", fake.evictCalls)
	}
	fake.sizeErr = nil // so the size callback does not fail the collection
	m := collectMetric(t, reader, sandboxAssetCacheGCPassesMetric)
	if m == nil {
		t.Fatal("skipped pass not recorded")
	}
	if got := sumByOutcome(t, m); got[ateattr.SandboxAssetCacheGCOutcomeSkipped] != 1 || len(got) != 1 {
		t.Errorf("passes by outcome = %v, want one skipped", got)
	}
}

func TestSandboxAssetCacheGCPassEvictsDownToTheLowWater(t *testing.T) {
	fake := &fakeAssetGCStore{size: 1500, stats: filecache.EvictStats{Retired: 3, FreedBytes: 600}}
	g, reader := newTestAssetCacheGC(t, fake, 1000)
	g.runPass(context.Background())
	if fake.evictCalls != 1 || fake.gotTarget != 600 || fake.gotDryRun {
		t.Errorf("evict call: calls=%d target=%d dryRun=%v, want 1/600/false", fake.evictCalls, fake.gotTarget, fake.gotDryRun)
	}
	passes := collectMetric(t, reader, sandboxAssetCacheGCPassesMetric)
	if passes == nil {
		t.Fatal("pass not recorded")
	}
	if got := sumByOutcome(t, passes); got[ateattr.SandboxAssetCacheGCOutcomeComplete] != 1 || len(got) != 1 {
		t.Errorf("passes by outcome = %v, want one complete", got)
	}
	evictions := collectMetric(t, reader, sandboxAssetCacheEvictionsMetric)
	if evictions == nil {
		t.Fatal("evictions not recorded")
	}
	if sum, ok := evictions.Data.(metricdata.Sum[int64]); !ok || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 3 || sum.DataPoints[0].Attributes.Len() != 0 {
		t.Errorf("evictions = %+v, want a single unlabeled datapoint of 3", evictions.Data)
	}
	if evictions.Unit != "{entry}" || passes.Unit != "{pass}" {
		t.Errorf("units = %q / %q, want {entry} / {pass}", evictions.Unit, passes.Unit)
	}
}

// TestSandboxAssetCacheGCDryRunCountsNoEvictions pins the dry run's metrics:
// the pass is recorded with its outcome, since it ran and decided, but the
// entries it would have taken are not evictions, because none was removed.
func TestSandboxAssetCacheGCDryRunCountsNoEvictions(t *testing.T) {
	fake := &fakeAssetGCStore{size: 1500, stats: filecache.EvictStats{Retired: 3, FreedBytes: 600}}
	g, reader := newTestAssetCacheGC(t, fake, 1000)
	g.dryRun = true
	g.runPass(context.Background())
	if fake.evictCalls != 1 || !fake.gotDryRun {
		t.Errorf("evict call: calls=%d dryRun=%v, want 1/true", fake.evictCalls, fake.gotDryRun)
	}
	passes := collectMetric(t, reader, sandboxAssetCacheGCPassesMetric)
	if passes == nil {
		t.Fatal("pass not recorded")
	}
	if got := sumByOutcome(t, passes); got[ateattr.SandboxAssetCacheGCOutcomeComplete] != 1 || len(got) != 1 {
		t.Errorf("passes by outcome = %v, want one complete", got)
	}
	if evictions := collectMetric(t, reader, sandboxAssetCacheEvictionsMetric); evictions != nil {
		t.Errorf("evictions = %+v after a dry run, want none recorded", evictions.Data)
	}
}

func TestSandboxAssetCacheGCPassShortfall(t *testing.T) {
	// Everything left is linked: the pass frees less than asked.
	fake := &fakeAssetGCStore{size: 1500, stats: filecache.EvictStats{Retired: 1, FreedBytes: 100, SkippedLinked: 4}}
	g, reader := newTestAssetCacheGC(t, fake, 1000)
	for range 3 {
		g.runPass(context.Background())
	}
	if g.consecutiveShortfalls != 3 {
		t.Errorf("consecutiveShortfalls = %d, want 3", g.consecutiveShortfalls)
	}
	if got := sumByOutcome(t, collectMetric(t, reader, sandboxAssetCacheGCPassesMetric)); got[ateattr.SandboxAssetCacheGCOutcomeShortfall] != 3 {
		t.Errorf("passes by outcome = %v, want three shortfalls", got)
	}
	// A pass that meets the budget resets the streak.
	fake.size = 500
	g.runPass(context.Background())
	if g.consecutiveShortfalls != 0 {
		t.Errorf("consecutiveShortfalls after a quiet pass = %d, want 0", g.consecutiveShortfalls)
	}
}

func TestSandboxAssetCacheSizeIsObserved(t *testing.T) {
	fake := &fakeAssetGCStore{size: 4096}
	_, reader := newTestAssetCacheGC(t, fake, 0)
	m := collectMetric(t, reader, sandboxAssetCacheSizeMetric)
	if m == nil {
		t.Fatal("size not observed")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok || sum.IsMonotonic || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 4096 {
		t.Errorf("size = %+v, want a non-monotonic int64 sum of 4096", m.Data)
	}
	if m.Unit != "By" {
		t.Errorf("size unit = %q, want By", m.Unit)
	}
	if sum.DataPoints[0].Attributes.Len() != 0 {
		t.Errorf("size carries labels %v, want none", sum.DataPoints[0].Attributes.ToSlice())
	}
}

func TestSandboxAssetCacheGCRunFirstPassIsImmediate(t *testing.T) {
	// A cancelled context and an hour-long period: the single call can
	// only be the immediate first pass, never a tick.
	fake := &fakeAssetGCStore{size: 1500}
	g, _ := newTestAssetCacheGC(t, fake, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.Run(ctx)
	if fake.evictCalls != 1 {
		t.Errorf("evictCalls = %d, want exactly 1 (the immediate first pass)", fake.evictCalls)
	}
}

func TestSandboxAssetCacheGCRunTicks(t *testing.T) {
	fake := &fakeAssetGCStore{size: 1500}
	g, _ := newTestAssetCacheGC(t, fake, 1000)
	g.period = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	g.Run(ctx)
	if fake.evictCalls < 2 {
		t.Errorf("evictCalls = %d, want >= 2 (first pass plus at least one tick)", fake.evictCalls)
	}
}

func TestSandboxAssetCacheGCPassRecoversPanic(t *testing.T) {
	g, _ := newTestAssetCacheGC(t, &fakeAssetGCStore{size: 1500, panicOnEvict: true}, 1000)
	g.runPass(context.Background()) // must not propagate the panic
}
