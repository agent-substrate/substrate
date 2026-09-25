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

package overcommit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/overcommit/predictor"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
)

// fakeSampler returns whatever samples a test last set.
type fakeSampler struct {
	samples []*ateworkerpb.WorkloadStatsSample
	err     error
}

func (f *fakeSampler) sample(context.Context) (*ateworkerpb.GetActiveWorkloadStatsResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &ateworkerpb.GetActiveWorkloadStatsResponse{Samples: f.samples}, nil
}

// fakeAdjuster records every accepted call and refuses all of them while err
// is set.
type fakeAdjuster struct {
	added      []*ateapi.Resources
	subtracted []*ateapi.Resources
	err        error
}

func (f *fakeAdjuster) AddWorkerAllocated(_ context.Context, delta *ateapi.Resources) error {
	if f.err != nil {
		return f.err
	}
	f.added = append(f.added, delta)
	return nil
}

func (f *fakeAdjuster) SubtractWorkerAllocated(_ context.Context, delta *ateapi.Resources) error {
	if f.err != nil {
		return f.err
	}
	f.subtracted = append(f.subtracted, delta)
	return nil
}

// scriptedStrategy predicts whatever a test last set and records the
// snapshots it was given.
type scriptedStrategy struct {
	prediction *ateapi.Resources
	err        error
	snapshots  []predictor.GroupUsageSnapshot
}

func (s *scriptedStrategy) Name() string { return "scripted" }

func (s *scriptedStrategy) Predict(_ time.Time, snapshot predictor.GroupUsageSnapshot) (*ateapi.Resources, error) {
	s.snapshots = append(s.snapshots, snapshot)
	if s.err != nil {
		return nil, s.err
	}
	return s.prediction, nil
}

func newTestController() (*Controller, *fakeSampler, *scriptedStrategy, *fakeAdjuster) {
	sampler := &fakeSampler{}
	strategy := &scriptedStrategy{}
	adjuster := &fakeAdjuster{}
	return NewController(sampler.sample, strategy, adjuster, time.Second), sampler, strategy, adjuster
}

func step(t *testing.T, c *Controller, now time.Time) {
	t.Helper()
	if err := c.Step(context.Background(), now); err != nil {
		t.Fatalf("Step() failed: %v", err)
	}
}

func wantTracked(t *testing.T, c *Controller, want *ateapi.Resources) {
	t.Helper()
	if diff := cmp.Diff(want, c.TrackedAllocated(), protocmp.Transform()); diff != "" {
		t.Errorf("trackedAllocated mismatch (-want +got):\n%s", diff)
	}
}

func wantCalls(t *testing.T, what string, want, got []*ateapi.Resources) {
	t.Helper()
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("%s calls mismatch (-want +got):\n%s", what, diff)
	}
}

// pending is a sample for an actor with no measurements yet, declaring the
// given limits.
func pending(uid string, cpuMilli, memoryBytes int64) *ateworkerpb.WorkloadStatsSample {
	return &ateworkerpb.WorkloadStatsSample{ActorUid: uid, CpuLimitMilli: cpuMilli, MemoryLimitBytes: memoryBytes}
}

// measured is a sample with measurements for an actor declaring 2 CPUs and
// 1Gi.
func measured(uid string, cpuUsec, memoryBytes uint64, at time.Time) *ateworkerpb.WorkloadStatsSample {
	return &ateworkerpb.WorkloadStatsSample{
		ActorUid:              uid,
		Source:                ateworkerpb.StatsSource_STATS_SOURCE_CGROUP,
		CpuUsageUsec:          cpuUsec,
		MemoryWorkingSetBytes: memoryBytes,
		ObservedAtUnixNano:    at.UnixNano(),
		CpuLimitMilli:         2000,
		MemoryLimitBytes:      1 << 30,
	}
}

// The whole cycle: an actor is booked at its limits when first sampled, the
// Worker hands back what it is not predicted to use, and once the actor is no
// longer sampled, the rest goes back too.
func TestControllerLifecycleAndDifferentialReclamation(t *testing.T) {
	c, sampler, strategy, adjuster := newTestController()
	now := time.Now()

	sampler.samples = []*ateworkerpb.WorkloadStatsSample{pending("uid-a", 2000, 4<<30)}
	strategy.prediction = resources.CPUMemory(500, 1<<30)
	step(t, c, now)
	if !c.Tracking("uid-a") {
		t.Fatal("a sampled actor is not tracked")
	}
	wantCalls(t, "SubtractWorkerAllocated", []*ateapi.Resources{resources.CPUMemory(1500, 3<<30)}, adjuster.subtracted)
	wantTracked(t, c, resources.CPUMemory(500, 1<<30))

	// A steady prediction changes nothing, and the actor is not booked again.
	step(t, c, now.Add(time.Second))
	if len(adjuster.subtracted) != 1 || len(adjuster.added) != 0 {
		t.Errorf("a steady prediction adjusted again: subtracted %v, added %v", adjuster.subtracted, adjuster.added)
	}

	sampler.samples = nil
	strategy.prediction = nil
	step(t, c, now.Add(2*time.Second))
	if c.Tracking("uid-a") {
		t.Error("still tracking an actor no longer sampled")
	}
	wantCalls(t, "SubtractWorkerAllocated", []*ateapi.Resources{
		resources.CPUMemory(1500, 3<<30),
		resources.CPUMemory(500, 1<<30),
	}, adjuster.subtracted)
	wantTracked(t, c, nil)
}

// Each call carries only non-negative quantities, so a tick where one
// dimension is over-allocated and another under-allocated makes two calls.
func TestControllerSplitsMixedDeltaIntoReclaimAndGrow(t *testing.T) {
	c, sampler, strategy, adjuster := newTestController()
	sampler.samples = []*ateworkerpb.WorkloadStatsSample{pending("uid-a", 2000, 1<<30)}

	strategy.prediction = resources.CPUMemory(500, 3<<30)
	step(t, c, time.Now())

	wantCalls(t, "SubtractWorkerAllocated", []*ateapi.Resources{resources.CPUMemory(1500, 0)}, adjuster.subtracted)
	wantCalls(t, "AddWorkerAllocated", []*ateapi.Resources{resources.CPUMemory(0, 2<<30)}, adjuster.added)
	wantTracked(t, c, resources.CPUMemory(500, 3<<30))
}

// A refused change leaves trackedAllocated alone, so the next tick sends the
// same difference again.
func TestControllerRetriesUnappliedDeltaAfterAdjustError(t *testing.T) {
	c, sampler, strategy, adjuster := newTestController()
	now := time.Now()
	sampler.samples = []*ateworkerpb.WorkloadStatsSample{pending("uid-a", 2000, 0)}
	strategy.prediction = resources.CPUMemory(500, 0)

	adjuster.err = errors.New("atelet unreachable")
	if err := c.Step(context.Background(), now); err == nil {
		t.Fatal("Step() succeeded while every adjustment was refused")
	}
	wantTracked(t, c, resources.CPUMemory(2000, 0))

	adjuster.err = nil
	step(t, c, now.Add(time.Second))
	wantCalls(t, "SubtractWorkerAllocated", []*ateapi.Resources{resources.CPUMemory(1500, 0)}, adjuster.subtracted)
	wantTracked(t, c, resources.CPUMemory(500, 0))
}

// Until a real strategy exists, the Controller must leave the full limits
// booked rather than act on no prediction.
func TestControllerSkipsWhenPredictorNotImplemented(t *testing.T) {
	sampler := &fakeSampler{samples: []*ateworkerpb.WorkloadStatsSample{pending("uid-a", 2000, 4<<30)}}
	adjuster := &fakeAdjuster{}
	c := NewController(sampler.sample, predictor.NewWindowedMaxPredictor(DefaultMaturationWindow, DefaultHistoryWindow), adjuster, time.Second)

	step(t, c, time.Now())
	if len(adjuster.subtracted)+len(adjuster.added) != 0 {
		t.Errorf("adjusted without a prediction: subtracted %v, added %v", adjuster.subtracted, adjuster.added)
	}
	wantTracked(t, c, resources.CPUMemory(2000, 4<<30))
}

func TestControllerStepFailures(t *testing.T) {
	now := time.Now()
	t.Run("sampler", func(t *testing.T) {
		c, sampler, _, adjuster := newTestController()
		sampler.err = errors.New("stats unavailable")
		if err := c.Step(context.Background(), now); err == nil {
			t.Error("Step() succeeded with a failing sampler")
		}
		if len(adjuster.subtracted)+len(adjuster.added) != 0 {
			t.Error("adjusted without a sample")
		}
	})
	t.Run("predictor", func(t *testing.T) {
		c, sampler, strategy, adjuster := newTestController()
		sampler.samples = []*ateworkerpb.WorkloadStatsSample{pending("uid-a", 2000, 0)}
		strategy.err = errors.New("no history")
		if err := c.Step(context.Background(), now); err == nil {
			t.Error("Step() succeeded with a failing predictor")
		}
		if len(adjuster.subtracted)+len(adjuster.added) != 0 {
			t.Error("adjusted without a prediction")
		}
	})
}

// An actor is booked once, at the limits of the first sample that shows it,
// however many samples follow and whatever they declare.
func TestControllerBooksEachActorOnce(t *testing.T) {
	c, sampler, strategy, _ := newTestController()
	strategy.err = predictor.ErrNotImplemented
	now := time.Now()

	sampler.samples = []*ateworkerpb.WorkloadStatsSample{pending("uid-a", 1000, 1<<30)}
	step(t, c, now)
	sampler.samples = []*ateworkerpb.WorkloadStatsSample{pending("uid-a", 3000, 1<<30)}
	step(t, c, now.Add(time.Second))
	wantTracked(t, c, resources.CPUMemory(1000, 1<<30))

	sampler.samples = []*ateworkerpb.WorkloadStatsSample{pending("uid-a", 3000, 1<<30), pending("uid-b", 500, 0)}
	step(t, c, now.Add(2*time.Second))
	wantTracked(t, c, resources.CPUMemory(1500, 1<<30))

	// An actor with no declared limits, or nonsense ones, books nothing.
	sampler.samples = append(sampler.samples, pending("uid-c", 0, 0), pending("uid-d", -1, -1))
	step(t, c, now.Add(3*time.Second))
	wantTracked(t, c, resources.CPUMemory(1500, 1<<30))
	if !c.Tracking("uid-c") || !c.Tracking("uid-d") {
		t.Error("actors without limits are not tracked")
	}
}

// The first sample's observation time stands in for the actor's start.
func TestControllerStartsActorAtFirstSample(t *testing.T) {
	c, sampler, strategy, _ := newTestController()
	t0 := time.Unix(1_000_000, 0)
	sampler.samples = []*ateworkerpb.WorkloadStatsSample{measured("uid-a", 0, 0, t0), pending("uid-b", 1000, 0)}
	step(t, c, t0.Add(time.Second))
	sampler.samples = []*ateworkerpb.WorkloadStatsSample{measured("uid-a", 0, 0, t0.Add(5*time.Second)), pending("uid-b", 1000, 0)}
	step(t, c, t0.Add(6*time.Second))

	last := strategy.snapshots[len(strategy.snapshots)-1]
	if got := last.Actors["uid-a"].StartedAt; !got.Equal(t0) {
		t.Errorf("uid-a StartedAt = %v, want its first sample's %v", got, t0)
	}
	// A sample with no observation time falls back to the step's time.
	if got, want := last.Actors["uid-b"].StartedAt, t0.Add(time.Second); !got.Equal(want) {
		t.Errorf("uid-b StartedAt = %v, want the first step's %v", got, want)
	}
}

func TestControllerComputesPerActorCPURate(t *testing.T) {
	c, sampler, strategy, _ := newTestController()
	t0 := time.Unix(1_000_000, 0)
	strategy.prediction = resources.CPUMemory(6000, 3<<30)

	// First sample: memory is known, but there is no earlier CPU reading.
	// uid-b is still booting.
	sampler.samples = []*ateworkerpb.WorkloadStatsSample{
		measured("uid-a", 1_000_000, 256<<20, t0),
		pending("uid-b", 2000, 1<<30),
	}
	step(t, c, t0)

	// Two seconds later: uid-a used 1s of CPU (500m), uid-b's first measured
	// sample has no rate yet.
	t1 := t0.Add(2 * time.Second)
	sampler.samples = []*ateworkerpb.WorkloadStatsSample{
		measured("uid-a", 2_000_000, 300<<20, t1),
		measured("uid-b", 5_000_000, 128<<20, t1),
	}
	step(t, c, t1)

	// One second later: uid-b used 1.5s of CPU (1500m); uid-a's counter
	// dropped, which starts a new epoch and yields no rate.
	t2 := t1.Add(time.Second)
	sampler.samples = []*ateworkerpb.WorkloadStatsSample{
		measured("uid-a", 10, 300<<20, t2),
		measured("uid-b", 6_500_000, 128<<20, t2),
	}
	step(t, c, t2)

	if len(strategy.snapshots) != 3 {
		t.Fatalf("predictor saw %d snapshots, want 3", len(strategy.snapshots))
	}
	type got struct {
		HasCPURate bool
		Usage      *ateapi.Resources
	}
	summarize := func(s predictor.GroupUsageSnapshot) map[string]got {
		out := map[string]got{}
		for uid, sample := range s.Actors {
			out[uid] = got{HasCPURate: sample.HasCPURate, Usage: sample.Usage}
		}
		return out
	}
	want := []map[string]got{
		{
			"uid-a": {Usage: resources.CPUMemory(0, 256<<20)},
			"uid-b": {},
		},
		{
			"uid-a": {HasCPURate: true, Usage: resources.CPUMemory(500, 300<<20)},
			"uid-b": {Usage: resources.CPUMemory(0, 128<<20)},
		},
		{
			"uid-a": {Usage: resources.CPUMemory(0, 300<<20)},
			"uid-b": {HasCPURate: true, Usage: resources.CPUMemory(1500, 128<<20)},
		},
	}
	for i, snapshot := range strategy.snapshots {
		if diff := cmp.Diff(want[i], summarize(snapshot), protocmp.Transform()); diff != "" {
			t.Errorf("snapshot %d mismatch (-want +got):\n%s", i, diff)
		}
	}

	// Every tracked actor carries its declared limits.
	if diff := cmp.Diff(resources.CPUMemory(2000, 1<<30), strategy.snapshots[0].Actors["uid-b"].Limits, protocmp.Transform()); diff != "" {
		t.Errorf("limits mismatch (-want +got):\n%s", diff)
	}
}

func TestCPURate(t *testing.T) {
	base := &cpuReading{usageUsec: 1_000_000, observedAt: 1_000_000_000}
	for _, tc := range []struct {
		name     string
		previous *cpuReading
		current  *cpuReading
		want     int64
		wantOK   bool
	}{
		{"no previous", nil, base, 0, false},
		{"one core", base, &cpuReading{usageUsec: 2_000_000, observedAt: 2_000_000_000}, 1000, true},
		{"quarter core", base, &cpuReading{usageUsec: 1_500_000, observedAt: 3_000_000_000}, 250, true},
		{"idle", base, &cpuReading{usageUsec: 1_000_000, observedAt: 2_000_000_000}, 0, true},
		{"time stood still", base, &cpuReading{usageUsec: 2_000_000, observedAt: 1_000_000_000}, 0, false},
		{"new epoch", base, &cpuReading{usageUsec: 10, observedAt: 2_000_000_000}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := cpuRate(tc.previous, tc.current)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("cpuRate() = %d, %t; want %d, %t", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestRunStopsWithContext(t *testing.T) {
	sampler := &fakeSampler{}
	c := NewController(sampler.sample, &scriptedStrategy{}, &fakeAdjuster{}, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context ended")
	}
}
