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
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// fakeStatsAteom answers GetActiveWorkloadStats with a canned response or
// error, standing in for one ateom socket.
type fakeStatsAteom struct {
	resp *ateompb.GetActiveWorkloadStatsResponse
	err  error

	// mu guards the recordings below: the sweep probes ateoms concurrently.
	mu sync.Mutex
	// calls counts probes, so tests can tell "skipped" from "never found".
	calls int
	// sawDeadline records whether the probe's context carried one, pinning the
	// per-call timeout.
	sawDeadline bool
}

func (f *fakeStatsAteom) GetActiveWorkloadStats(ctx context.Context, req *ateompb.GetActiveWorkloadStatsRequest, opts ...grpc.CallOption) (*ateompb.GetActiveWorkloadStatsResponse, error) {
	f.mu.Lock()
	f.calls++
	_, f.sawDeadline = ctx.Deadline()
	f.mu.Unlock()
	return f.resp, f.err
}

// executingResponse builds the sample an executing ateom would echo.
func executingResponse(templateNS, templateName string, class ateompb.SandboxClass, source ateompb.StatsSource, current, workingSet uint64) *ateompb.GetActiveWorkloadStatsResponse {
	return &ateompb.GetActiveWorkloadStatsResponse{
		Result: &ateompb.GetActiveWorkloadStatsResponse_Sample{Sample: &ateompb.WorkloadStatsSample{
			ActorTemplateAtespace: templateNS,
			ActorTemplateName:     templateName,
			SandboxClass:          class,
			Source:                source,
			MemoryCurrentBytes:    current,
			MemoryWorkingSetBytes: workingSet,
		}},
	}
}

func noSampleResponse(reason ateompb.NoSampleReason) *ateompb.GetActiveWorkloadStatsResponse {
	return &ateompb.GetActiveWorkloadStatsResponse{
		Result: &ateompb.GetActiveWorkloadStatsResponse_NoSampleReason{NoSampleReason: reason},
	}
}

// closeRecorder counts Close calls, standing in for a probe's connection.
type closeRecorder struct {
	mu     sync.Mutex
	closes int
}

func (c *closeRecorder) Close() error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	return nil
}

// newPollerFixture builds a poller over a fixture ateoms directory with one
// subdirectory (and one fake) per entry in fakes. Dialing a UID without a fake
// fails, which is the shape of a stale directory whose socket is gone. Every
// successful dial hands out a recorded closer; assertClosed checks the
// connections-live-exactly-one-probe contract.
func newPollerFixture(t *testing.T, fakes map[string]*fakeStatsAteom) (*statsPoller, map[string]*closeRecorder) {
	t.Helper()
	dir := t.TempDir()
	closers := make(map[string]*closeRecorder)
	for uid := range fakes {
		if err := os.Mkdir(filepath.Join(dir, uid), 0o700); err != nil {
			t.Fatalf("creating fixture ateom dir %q: %v", uid, err)
		}
		closers[uid] = &closeRecorder{}
	}
	return &statsPoller{
		ateomsDir: dir,
		dial: func(_ context.Context, podUID string) (activeStatsClient, io.Closer, error) {
			f, ok := fakes[podUID]
			if !ok || f == nil {
				return nil, nil, errors.New("no such socket")
			}
			return f, closers[podUID], nil
		},
	}, closers
}

// assertClosed checks that every successfully dialed probe closed its
// connection exactly once per sweep -- the RPC failing must not leak it.
func assertClosed(t *testing.T, fakes map[string]*fakeStatsAteom, closers map[string]*closeRecorder, sweeps int) {
	t.Helper()
	for uid, f := range fakes {
		if f == nil {
			continue // dial fails: no connection to close
		}
		if got := closers[uid].closes; got != sweeps {
			t.Errorf("ateom %s connection closed %d times over %d sweeps, want %d", uid, got, sweeps, sweeps)
		}
	}
}

func TestStatsPollerCollectAggregates(t *testing.T) {
	// Two actors of the same template on this node, one of another, one idle
	// worker, one mid-boot: the same-template pair sums, the others contribute
	// nothing.
	fakes := map[string]*fakeStatsAteom{
		"uid-1": {resp: executingResponse("ns-a", "tmpl-a", ateompb.SandboxClass_SANDBOX_CLASS_GVISOR, ateompb.StatsSource_STATS_SOURCE_CGROUP, 1000, 700)},
		"uid-2": {resp: executingResponse("ns-a", "tmpl-a", ateompb.SandboxClass_SANDBOX_CLASS_GVISOR, ateompb.StatsSource_STATS_SOURCE_CGROUP, 500, 300)},
		"uid-3": {resp: executingResponse("ns-b", "tmpl-b", ateompb.SandboxClass_SANDBOX_CLASS_MICROVM, ateompb.StatsSource_STATS_SOURCE_GUEST_AGENT, 42, 40)},
		"uid-4": {resp: noSampleResponse(ateompb.NoSampleReason_NO_SAMPLE_REASON_NO_WORKLOAD)},
		"uid-5": {resp: noSampleResponse(ateompb.NoSampleReason_NO_SAMPLE_REASON_NOT_MEASURABLE_YET)},
	}
	p, closers := newPollerFixture(t, fakes)

	got := p.collect(context.Background())

	want := map[templateKey]*templateAggregate{
		{templateNamespace: "ns-a", templateName: "tmpl-a", sandboxClass: "gvisor", source: "cgroup"}: {
			sampledActors: 2, memoryCurrentBytes: 1500, memoryWorkingSetBytes: 1000,
		},
		{templateNamespace: "ns-b", templateName: "tmpl-b", sandboxClass: "microvm", source: "guest-agent"}: {
			sampledActors: 1, memoryCurrentBytes: 42, memoryWorkingSetBytes: 40,
		},
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(templateAggregate{}, templateKey{}, workerPoolRef{})); diff != "" {
		t.Errorf("collect() mismatch (-want +got):\n%s", diff)
	}

	for uid, f := range fakes {
		if f.calls != 1 {
			t.Errorf("ateom %s probed %d times, want 1", uid, f.calls)
		}
		if !f.sawDeadline {
			t.Errorf("ateom %s probed without a deadline; every probe must carry the per-call timeout", uid)
		}
	}
	assertClosed(t, fakes, closers, 1)
}

// TestStatsPollerCollectSkipsFailures pins the scan's one tolerance rule: a
// dial or call failure means "not a target this tick", never a failed sweep.
// The healthy ateom's sample must still be aggregated.
func TestStatsPollerCollectSkipsFailures(t *testing.T) {
	fakes := map[string]*fakeStatsAteom{
		"uid-healthy": {resp: executingResponse("ns-a", "tmpl-a", ateompb.SandboxClass_SANDBOX_CLASS_GVISOR, ateompb.StatsSource_STATS_SOURCE_CGROUP, 100, 80)},
		"uid-stale":   nil, // directory with no reachable socket: dial fails
		"uid-broken":  {err: errors.New("rpc error: connection refused")},
	}
	p, closers := newPollerFixture(t, fakes)

	got := p.collect(context.Background())

	want := map[templateKey]*templateAggregate{
		{templateNamespace: "ns-a", templateName: "tmpl-a", sandboxClass: "gvisor", source: "cgroup"}: {
			sampledActors: 1, memoryCurrentBytes: 100, memoryWorkingSetBytes: 80,
		},
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(templateAggregate{}, templateKey{}, workerPoolRef{})); diff != "" {
		t.Errorf("collect() mismatch (-want +got):\n%s", diff)
	}
	// The broken ateom's RPC failed, but its connection was dialed -- it must
	// be closed all the same.
	assertClosed(t, fakes, closers, 1)
}

// TestStatsPollerCollectNoAteomsDir: a node whose first workload has not
// arrived has no ateoms directory, which is empty coverage, not an error.
func TestStatsPollerCollectNoAteomsDir(t *testing.T) {
	p := &statsPoller{ateomsDir: filepath.Join(t.TempDir(), "does-not-exist")}
	if got := p.collect(context.Background()); len(got) != 0 {
		t.Errorf("collect() with no ateoms dir = %v, want empty", got)
	}
}

func TestClampActorStatsPollInterval(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "zero stays disabled", in: 0, want: 0},
		{name: "below floor clamps", in: time.Second, want: minActorStatsPollInterval},
		{name: "at floor passes", in: minActorStatsPollInterval, want: minActorStatsPollInterval},
		{name: "above floor passes", in: 5 * time.Minute, want: 5 * time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampActorStatsPollInterval(context.Background(), tc.in); got != tc.want {
				t.Errorf("clampActorStatsPollInterval(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestStatsInstrumentsObserveLatestSnapshotOnly pins the reason the gauges are
// observable rather than synchronous: each collection reports exactly the
// groups the latest sweep found. A synchronous gauge would re-export its last
// recorded value on every collection until process exit, so a template whose
// actors left the node would keep reporting their memory forever.
func TestStatsInstrumentsObserveLatestSnapshotOnly(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	inst, err := newStatsInstruments(mp.Meter("test"))
	if err != nil {
		t.Fatalf("newStatsInstruments() error = %v", err)
	}

	key := templateKey{templateNamespace: "ns-a", templateName: "tmpl-a", sandboxClass: "gvisor", source: "cgroup"}
	inst.publish(map[templateKey]*templateAggregate{
		key: {sampledActors: 2, memoryCurrentBytes: 1500, memoryWorkingSetBytes: 1000},
	})

	if got := gaugePointCount(t, reader, workingSetMetric); got != 1 {
		t.Fatalf("after publish: %s has %d datapoints, want 1", workingSetMetric, got)
	}

	// The template's actors leave the node: an empty sweep must make the
	// series disappear, not freeze at its last value.
	inst.publish(map[templateKey]*templateAggregate{})
	if got := gaugePointCount(t, reader, workingSetMetric); got != 0 {
		t.Errorf("after empty sweep: %s has %d datapoints, want 0", workingSetMetric, got)
	}
}

// gaugePointCount collects once and returns how many datapoints name has.
func gaugePointCount(t *testing.T, reader *sdkmetric.ManualReader, name string) int {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("metric %s has data type %T, want Gauge[int64]", name, m.Data)
			}
			return len(g.DataPoints)
		}
	}
	return 0
}

// cpuResponse is executingResponse with only the CPU counter set, for the
// delta tests.
func cpuResponse(actorUID string, cpuUsec uint64) *ateompb.GetActiveWorkloadStatsResponse {
	return &ateompb.GetActiveWorkloadStatsResponse{
		Result: &ateompb.GetActiveWorkloadStatsResponse_Sample{Sample: &ateompb.WorkloadStatsSample{
			ActorUid:              actorUID,
			ActorTemplateAtespace: "ns-a",
			ActorTemplateName:     "tmpl-a",
			SandboxClass:          ateompb.SandboxClass_SANDBOX_CLASS_GVISOR,
			Source:                ateompb.StatsSource_STATS_SOURCE_CGROUP,
			CpuUsageUsec:          cpuUsec,
		}},
	}
}

// TestStatsPollerCPUDeltas pins the increase computation across sweeps: the
// first sight of an actor establishes a baseline and charges nothing (atelet
// cannot tell a new actor from its own restart, and re-charging an epoch the
// previous atelet counted would spike the counter), a later sweep charges
// only the increase, a decrease is an epoch reset whose new value is the
// usage since the reset, and an actor that disappears stops contributing and
// is dropped from the baselines.
func TestStatsPollerCPUDeltas(t *testing.T) {
	key := templateKey{templateNamespace: "ns-a", templateName: "tmpl-a", sandboxClass: "gvisor", source: "cgroup"}
	fake := &fakeStatsAteom{resp: cpuResponse("uid-a", 1000)}
	p, _ := newPollerFixture(t, map[string]*fakeStatsAteom{"uid-1": fake})

	if got := p.collect(context.Background())[key].cpuDeltaUsec; got != 0 {
		t.Errorf("first sweep delta = %d, want 0 (baseline only on first sight)", got)
	}

	fake.resp = cpuResponse("uid-a", 1600)
	if got := p.collect(context.Background())[key].cpuDeltaUsec; got != 600 {
		t.Errorf("second sweep delta = %d, want 600 (the increase)", got)
	}

	// Epoch reset: the counter went backwards, so the new value is the usage
	// since the reset.
	fake.resp = cpuResponse("uid-a", 250)
	if got := p.collect(context.Background())[key].cpuDeltaUsec; got != 250 {
		t.Errorf("post-reset sweep delta = %d, want 250", got)
	}

	// The actor leaves: nothing to contribute, and its baseline must be
	// dropped so a later return re-baselines instead of comparing against a
	// dead value.
	fake.resp = noSampleResponse(ateompb.NoSampleReason_NO_SAMPLE_REASON_NO_WORKLOAD)
	if got := p.collect(context.Background()); len(got) != 0 {
		t.Errorf("empty sweep aggregates = %v, want none", got)
	}
	if len(p.lastCPU) != 0 {
		t.Errorf("baselines after empty sweep = %v, want pruned empty", p.lastCPU)
	}
}

// TestStatsPollerWorkerPoolLabels pins the pool enrichment: a resolved pod
// groups under its pool, an unresolved one groups without pool labels rather
// than vanishing, and the two never merge.
func TestStatsPollerWorkerPoolLabels(t *testing.T) {
	fakes := map[string]*fakeStatsAteom{
		"uid-pooled":   {resp: executingResponse("ns-a", "tmpl-a", ateompb.SandboxClass_SANDBOX_CLASS_GVISOR, ateompb.StatsSource_STATS_SOURCE_CGROUP, 100, 80)},
		"uid-unpooled": {resp: executingResponse("ns-a", "tmpl-a", ateompb.SandboxClass_SANDBOX_CLASS_GVISOR, ateompb.StatsSource_STATS_SOURCE_CGROUP, 10, 8)},
	}
	p, _ := newPollerFixture(t, fakes)
	p.workerPools = func(context.Context) map[string]workerPoolRef {
		return map[string]workerPoolRef{"uid-pooled": {namespace: "pool-ns", name: "pool-a"}}
	}

	got := p.collect(context.Background())

	base := templateKey{templateNamespace: "ns-a", templateName: "tmpl-a", sandboxClass: "gvisor", source: "cgroup"}
	pooled := base
	pooled.workerPool = workerPoolRef{namespace: "pool-ns", name: "pool-a"}
	want := map[templateKey]*templateAggregate{
		pooled: {sampledActors: 1, memoryCurrentBytes: 100, memoryWorkingSetBytes: 80},
		base:   {sampledActors: 1, memoryCurrentBytes: 10, memoryWorkingSetBytes: 8},
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(templateAggregate{}, templateKey{}, workerPoolRef{})); diff != "" {
		t.Errorf("collect() mismatch (-want +got):\n%s", diff)
	}
}

func TestAddSat(t *testing.T) {
	tests := []struct {
		name string
		agg  int64
		v    uint64
		want int64
	}{
		{name: "normal add", agg: 100, v: 50, want: 150},
		{name: "zero add", agg: 100, v: 0, want: 100},
		// A wire value above MaxInt64 -- a corrupt or hostile guest reading --
		// must pin at the ceiling, not wrap the aggregate negative.
		{name: "value above MaxInt64 saturates", agg: 0, v: math.MaxUint64, want: math.MaxInt64},
		// The addition itself can also overflow once inputs are clamped.
		{name: "sum overflow saturates", agg: math.MaxInt64 - 10, v: 100, want: math.MaxInt64},
		{name: "exactly at ceiling", agg: math.MaxInt64 - 5, v: 5, want: math.MaxInt64},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := addSat(tc.agg, tc.v); got != tc.want {
				t.Errorf("addSat(%d, %d) = %d, want %d", tc.agg, tc.v, tc.want, got)
			}
		})
	}
}

// TestStatsPollerCollectSaturatesCorruptSamples pins the end-to-end behavior:
// one guest reporting absurd counters must not flip a template's aggregates
// negative -- a negative gauge misreads as "no memory", and a negative CPU
// delta is a spec-violating counter Add. Everything pins at MaxInt64 instead.
func TestStatsPollerCollectSaturatesCorruptSamples(t *testing.T) {
	key := templateKey{templateNamespace: "ns-a", templateName: "tmpl-a", sandboxClass: "gvisor", source: "cgroup"}
	fakes := map[string]*fakeStatsAteom{
		"uid-1": {resp: executingResponse("ns-a", "tmpl-a", ateompb.SandboxClass_SANDBOX_CLASS_GVISOR, ateompb.StatsSource_STATS_SOURCE_CGROUP, math.MaxUint64, math.MaxUint64)},
		"uid-2": {resp: executingResponse("ns-a", "tmpl-a", ateompb.SandboxClass_SANDBOX_CLASS_GVISOR, ateompb.StatsSource_STATS_SOURCE_CGROUP, 1000, 700)},
	}
	p, _ := newPollerFixture(t, fakes)

	got := p.collect(context.Background())[key]
	if got == nil {
		t.Fatal("collect() returned no aggregate for the template")
	}
	if got.memoryCurrentBytes != math.MaxInt64 || got.memoryWorkingSetBytes != math.MaxInt64 {
		t.Errorf("memory aggregates = %d/%d, want both pinned at MaxInt64",
			got.memoryCurrentBytes, got.memoryWorkingSetBytes)
	}
	if got.memoryCurrentBytes < 0 || got.memoryWorkingSetBytes < 0 || got.cpuDeltaUsec < 0 {
		t.Errorf("aggregate went negative: %+v", got)
	}
}

// TestStatsPollerCPUDeltaSaturatesCorruptCounter: a baseline followed by an
// absurd counter value is a huge "increase"; it must clamp, not go negative.
func TestStatsPollerCPUDeltaSaturatesCorruptCounter(t *testing.T) {
	key := templateKey{templateNamespace: "ns-a", templateName: "tmpl-a", sandboxClass: "gvisor", source: "cgroup"}
	fake := &fakeStatsAteom{resp: cpuResponse("uid-a", 1000)}
	p, _ := newPollerFixture(t, map[string]*fakeStatsAteom{"uid-1": fake})

	if got := p.collect(context.Background())[key].cpuDeltaUsec; got != 0 {
		t.Fatalf("first sweep delta = %d, want 0", got)
	}

	fake.resp = cpuResponse("uid-a", math.MaxUint64)
	got := p.collect(context.Background())[key].cpuDeltaUsec
	if got != math.MaxInt64 {
		t.Errorf("corrupt-counter sweep delta = %d, want pinned at MaxInt64", got)
	}
}
