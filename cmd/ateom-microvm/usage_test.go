//go:build linux

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
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/ateomstats/ateomstatstest"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func withUsageRecorder(s *AteomService) *ateomstatstest.Recorder {
	e, rec := ateomstatstest.NewEmitter()
	s.usage = e
	return rec
}

// running is a after its initial reading, so the sweep samples it.
func running(a *ateomstats.Activation) *ateomstats.Activation {
	a.Initial(nil, nil)
	return a
}

// setActivation replaces the test actor's activation.
func setActivation(s *AteomService, a *ateomstats.Activation) *hostedActor {
	h := s.lookupActor(testActor.UID)
	s.actorsMu.Lock()
	h.usage = a
	s.actorsMu.Unlock()
	return h
}

func TestSweepStampsEpochAndDiscoveryServesIt(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	s := newStatsService(agent, "app_ovl")
	epoch := time.Unix(1700, 0)
	setActivation(s, running(ateomstats.NewActivation(epoch, false)))

	before, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := before.GetSamples()[0]; got.GetSource() != ateompb.StatsSource_STATS_SOURCE_UNSPECIFIED || got.GetEpochUnixNano() != epoch.UnixNano() {
		t.Errorf("before a sweep: source %v epoch %d, want pending with epoch %d", got.GetSource(), got.GetEpochUnixNano(), epoch.UnixNano())
	}
	if len(agent.calls) != 0 {
		t.Errorf("discovery read called the guest %d times, want 0", len(agent.calls))
	}

	s.sweepUsage(context.Background())
	after, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := after.GetSamples()[0]; got.GetEpochUnixNano() != epoch.UnixNano() || got.GetCpuUsageUsec() != 5000 {
		t.Errorf("served epoch %d cpu %d, want epoch %d cpu 5000", got.GetEpochUnixNano(), got.GetCpuUsageUsec(), epoch.UnixNano())
	}
}

// TestResumedGuestCPUIsActivationRelative: a restored guest brings its CPU
// counter back, so the first reading is the baseline.
func TestResumedGuestCPUIsActivationRelative(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 9_000_000)}}
	s := newStatsService(agent, "app_ovl")
	h := setActivation(s, running(ateomstats.NewActivation(time.Now(), true)))

	s.sweepUsage(context.Background())
	if got := h.usage.Latest().GetCpuUsageUsec(); got != 0 {
		t.Errorf("first reading of a resumed guest = %d, want 0", got)
	}
	agent.mu.Lock()
	agent.stats["app_ovl"] = containerStats(1000, 2000, 100, 9_400_000)
	agent.mu.Unlock()
	s.sweepUsage(context.Background())
	if got := h.usage.Latest().GetCpuUsageUsec(); got != 400 {
		t.Errorf("second reading = %d, want 400", got)
	}
}

func TestRecordInitialAndFinal(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	s := newStatsService(agent, "app_ovl")
	rec := withUsageRecorder(s)
	h := setActivation(s, ateomstats.NewActivation(time.Now(), false))

	s.recordInitial(context.Background(), h)
	if h.usage.Latest() == nil {
		t.Fatal("initial sample was not stored for the discovery read")
	}
	calls := len(agent.calls)
	s.recordFinal(context.Background(), h)
	if len(agent.calls) != calls {
		t.Error("final record read the guest; it must reuse the latest sample")
	}

	want := []string{ateattr.StatsKindInitial, ateattr.StatsKindFinal}
	if len(rec.Kinds()) != 2 || rec.Kinds()[0] != want[0] || rec.Kinds()[1] != want[1] {
		t.Errorf("record kinds = %v, want %v", rec.Kinds(), want)
	}
}

// TestRecordFinalIfEnded pins that the final record waits for the actor to be
// unhosted, as a teardown does even when a later step fails.
func TestRecordFinalIfEnded(t *testing.T) {
	s := newStatsService(&fakeAgent{}, "app_ovl")
	rec := withUsageRecorder(s)
	h := setActivation(s, ateomstats.NewActivation(time.Now(), false))

	s.recordFinalIfEnded(context.Background(), h)
	if got := rec.Kinds(); len(got) != 0 {
		t.Fatalf("records while hosted = %v, want none", got)
	}
	unhostTestActor(s, testActor.UID)
	s.recordFinalIfEnded(context.Background(), h)
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Errorf("records after unhosting = %v, want one final", got)
	}
}

// TestReadFinal pins that the final record carries a fresh guest reading, and
// the newest measured sample when that reading fails.
func TestReadFinal(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	s := newStatsService(agent, "app_ovl")
	h := setActivation(s, ateomstats.NewActivation(time.Now(), false))
	s.recordInitial(context.Background(), h)

	agent.stats["app_ovl"] = containerStats(1000, 2000, 100, 9_000_000)
	s.readFinal(context.Background(), h)
	if got := h.usage.Latest().GetCpuUsageUsec(); got != 9000 {
		t.Errorf("CPU after the final read = %d, want 9000", got)
	}

	agent.errs = map[string]error{"app_ovl": errors.New("guest gone")}
	s.readFinal(context.Background(), h)
	if got := h.usage.Latest().GetCpuUsageUsec(); got != 9000 {
		t.Errorf("CPU after a failed final read = %d, want the last measured 9000", got)
	}
}

func TestRecordFinalWithNoSampleIsPending(t *testing.T) {
	s := newStatsService(&fakeAgent{}, "app_ovl")
	rec := withUsageRecorder(s)
	h := setActivation(s, ateomstats.NewActivation(time.Now(), false))
	s.recordFinal(context.Background(), h)
	if len(rec.Sources()) != 1 || rec.Sources()[0] != ateattr.StatsSourceUnspecified {
		t.Errorf("final record sources = %v, want one pending", rec.Sources())
	}
}

// TestRecordInitialAfterFinal pins that an initial read still in flight when
// a checkpoint or terminate writes the final record writes nothing.
func TestRecordInitialAfterFinal(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	s := newStatsService(agent, "app_ovl")
	rec := withUsageRecorder(s)
	h := setActivation(s, ateomstats.NewActivation(time.Now(), false))
	agent.onCall = func() { s.recordFinal(context.Background(), h) }

	s.recordInitial(context.Background(), h)
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Errorf("records = %v, want only the final one", got)
	}
}

// TestRecordInitialRecoversFromPanic pins that a panic in the initial read is
// recovered and still opens the sampling window.
func TestRecordInitialRecoversFromPanic(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	s := newStatsService(agent, "app_ovl")
	rec := withUsageRecorder(s)
	h := setActivation(s, ateomstats.NewActivation(time.Now(), false))
	agent.onCall = func() { panic("guest read") }

	s.recordInitial(context.Background(), h)
	if got := rec.Kinds(); len(got) != 0 {
		t.Errorf("records = %v, want none", got)
	}
	if !h.usage.Sampling() {
		t.Error("activation is not sampling after the panic")
	}
}

// TestSweepRecoversFromPanic pins that a panic in one guest read is recovered
// on its fan-out goroutine, where the sampler's recover does not reach.
func TestSweepRecoversFromPanic(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	s := newStatsService(agent, "app_ovl")
	rec := withUsageRecorder(s)
	setActivation(s, running(ateomstats.NewActivation(time.Now(), false)))
	agent.onCall = func() { panic("guest read") }

	if got := s.sweepUsage(context.Background()); len(got) != 0 {
		t.Errorf("sweep returned %v, want nothing", got)
	}
	if got := rec.Kinds(); len(got) != 0 {
		t.Errorf("records = %v, want none", got)
	}
}

// TestRecordInitialTakesASlot pins that an initial reading waits for a guest
// slot, so it adds no guest reads beyond statsFanOut.
func TestRecordInitialTakesASlot(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	s := newStatsService(agent, "app_ovl")
	withUsageRecorder(s)
	h := setActivation(s, ateomstats.NewActivation(time.Now(), false))
	for range statsFanOut {
		s.guestSlots <- struct{}{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	s.recordInitial(ctx, h)
	if len(agent.calls) != 0 {
		t.Errorf("initial reading read the guest %d times with every slot taken, want 0", len(agent.calls))
	}
}

// TestRecordInitialAfterRehost pins that a re-host during the read leaves the
// new activation untouched: the reading belongs to the old one.
func TestRecordInitialAfterRehost(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	s := newStatsService(agent, "app_ovl")
	withUsageRecorder(s)
	old := setActivation(s, ateomstats.NewActivation(time.Now(), false))
	target := s.guestOf(old)
	agent.onCall = func() { hostTestActor(s, testActor, target) }

	s.recordInitial(context.Background(), old)
	if got := old.usage.Latest(); got == nil || got.GetEpochUnixNano() != old.usage.Epoch() {
		t.Errorf("old activation holds %v, want its own initial reading", got)
	}
	if s.lookupActor(testActor.UID).usage.Latest() != nil {
		t.Error("new activation holds the old activation's sample")
	}
}

// TestPartialGuestReadDoesNotDoubleCharge fails one container's read for a
// sweep: its CPU must neither drop out of the total nor be charged again when
// it is read once more.
func TestPartialGuestReadDoesNotDoubleCharge(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
		"a_ovl": containerStats(1000, 2000, 100, 600_000_000),
		"b_ovl": containerStats(1000, 2000, 100, 400_000_000),
	}}
	s := newStatsService(agent, "a_ovl", "b_ovl")
	h := setActivation(s, running(ateomstats.NewActivation(time.Now(), true)))

	s.sweepUsage(context.Background())
	agent.mu.Lock()
	agent.stats["a_ovl"] = containerStats(1000, 2000, 100, 605_000_000)
	agent.errs = map[string]error{"b_ovl": errors.New("deadline exceeded")}
	agent.mu.Unlock()
	s.sweepUsage(context.Background())
	if got := h.usage.Latest().GetCpuUsageUsec(); got != 5000 {
		t.Fatalf("cpu with b unreadable = %d, want 5000", got)
	}

	agent.mu.Lock()
	agent.stats["a_ovl"] = containerStats(1000, 2000, 100, 606_000_000)
	agent.stats["b_ovl"] = containerStats(1000, 2000, 100, 404_000_000)
	agent.errs = nil
	agent.mu.Unlock()
	s.sweepUsage(context.Background())
	if got := h.usage.Latest().GetCpuUsageUsec(); got != 10000 {
		t.Errorf("cpu once b reads again = %d, want 10000 (b's 400s charged once)", got)
	}
}

// TestSweepWaitsForInitialAndStopsAtFinal pins that the sweep reads no actor
// before its initial reading or after its final record.
func TestSweepWaitsForInitialAndStopsAtFinal(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	s := newStatsService(agent, "app_ovl")
	rec := withUsageRecorder(s)
	h := setActivation(s, ateomstats.NewActivation(time.Now(), false))

	if got := s.sweepUsage(context.Background()); len(got) != 0 || len(agent.calls) != 0 {
		t.Fatalf("sweep before the initial reading returned %v and read the guest %d times, want neither", got, len(agent.calls))
	}
	s.recordInitial(context.Background(), h)
	s.sweepUsage(context.Background())
	s.recordFinal(context.Background(), h)
	calls := len(agent.calls)
	s.sweepUsage(context.Background())
	if len(agent.calls) != calls {
		t.Error("sweep read the guest after the final record")
	}
	want := []string{ateattr.StatsKindInitial, ateattr.StatsKindPeriodic, ateattr.StatsKindFinal}
	if got := rec.Kinds(); !slices.Equal(got, want) {
		t.Errorf("records = %v, want %v", got, want)
	}
}

// TestRecordInitialReadsItsOwnGuest re-hosts the actor with another guest
// before the old activation's initial read: the read still uses the old
// activation's guest, and the new activation is untouched.
func TestRecordInitialReadsItsOwnGuest(t *testing.T) {
	oldAgent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	newAgent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 9_000_000)}}
	s := newStatsService(oldAgent, "app_ovl")
	withUsageRecorder(s)
	old := setActivation(s, ateomstats.NewActivation(time.Now(), false))
	fresh := hostTestActor(s, testActor, &guestStatsTarget{actorUID: testActor.UID, agent: newAgent, workloadIDs: []string{"app_ovl"}})
	fresh.usage = ateomstats.NewActivation(time.Now(), false)

	s.recordInitial(context.Background(), old)
	if got := old.usage.Latest().GetCpuUsageUsec(); got != 5000 {
		t.Errorf("old activation cpu = %d, want 5000 from its own guest", got)
	}
	if len(newAgent.calls) != 0 {
		t.Error("the old activation's initial read asked the new guest")
	}
	if fresh.usage.Sampling() || fresh.usage.Latest() != nil {
		t.Error("the old activation's initial read touched the new activation")
	}
}

// TestLateInitialLeavesNewActivationAlone starts the old activation's
// initial read only after its final record and a re-host: it writes nothing,
// and the new activation still waits for its own initial reading.
func TestLateInitialLeavesNewActivationAlone(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5_000_000)}}
	s := newStatsService(agent, "app_ovl")
	rec := withUsageRecorder(s)
	old := setActivation(s, ateomstats.NewActivation(time.Now(), false))
	s.recordFinal(context.Background(), old)
	fresh := hostTestActor(s, testActor, nil)
	fresh.usage = ateomstats.NewActivation(time.Now(), false)

	s.recordInitial(context.Background(), old)
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Errorf("records = %v, want only the old activation's final", got)
	}
	if fresh.usage.Sampling() {
		t.Error("the late read opened the new activation's sampling window")
	}
}
