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
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/ateomstats/ateomstatstest"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func withUsageRecorder(s *AteomService) *ateomstatstest.Recorder {
	e, rec := ateomstatstest.NewEmitter()
	s.usage = e
	return rec
}

// hostWithEpoch hosts testActor; running means its initial reading is done.
func hostWithEpoch(s *AteomService, epoch time.Time, running bool) *hostedActor {
	h := &hostedActor{attribution: testActor, usage: ateomstats.NewActivation(epoch, false)}
	if running {
		h.usage.Initial(nil, nil)
	}
	s.actorsMu.Lock()
	s.actors = map[string]*hostedActor{testActor.UID: h}
	s.actorsMu.Unlock()
	return h
}

func TestSweepStampsEpochAndDiscoveryServesIt(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	epoch := time.Unix(1700, 0)
	h := hostWithEpoch(s, epoch, true)

	before, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := before.GetSamples()[0]; got.GetSource() != ateompb.StatsSource_STATS_SOURCE_UNSPECIFIED || got.GetEpochUnixNano() != epoch.UnixNano() {
		t.Errorf("before a sweep: source %v epoch %d, want pending with epoch %d", got.GetSource(), got.GetEpochUnixNano(), epoch.UnixNano())
	}

	s.sweepUsage(context.Background())
	after, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got := after.GetSamples()[0]
	if got != h.usage.Latest() {
		t.Error("discovery read did not serve the swept sample")
	}
	if got.GetEpochUnixNano() != epoch.UnixNano() || got.GetCpuUsageUsec() != healthyCPUUsec {
		t.Errorf("served epoch %d cpu %d, want epoch %d cpu %d", got.GetEpochUnixNano(), got.GetCpuUsageUsec(), epoch.UnixNano(), healthyCPUUsec)
	}
}

func TestGetWorkloadStatsStampsEpoch(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	epoch := time.Unix(1700, 0)
	hostWithEpoch(s, epoch, true)
	got, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: testActor.UID})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetSample().GetEpochUnixNano() != epoch.UnixNano() {
		t.Errorf("epoch = %d, want %d", got.GetSample().GetEpochUnixNano(), epoch.UnixNano())
	}
}

func TestRecordInitialAndFinal(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), false)

	s.recordInitial(context.Background(), h)
	if h.usage.Latest() == nil {
		t.Error("initial sample was not stored for the discovery read")
	}
	s.recordFinal(context.Background(), h)

	want := []string{ateattr.StatsKindInitial, ateattr.StatsKindFinal}
	if len(rec.Kinds()) != 2 || rec.Kinds()[0] != want[0] || rec.Kinds()[1] != want[1] {
		t.Errorf("record kinds = %v, want %v", rec.Kinds(), want)
	}
	for _, src := range rec.Sources() {
		if src != ateattr.StatsSourceCgroup {
			t.Errorf("record source = %q, want measured", src)
		}
	}
}

func TestFinalCPUReachesCounterAfterUnhosting(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	cpu, reader := ateomstatstest.NewCPUCounter(t, "ateom-gvisor")
	s.cpu = cpu
	h := hostWithEpoch(s, time.Now(), false)
	s.recordInitial(context.Background(), h)
	if got := ateomstatstest.CPUSeconds(t, reader); got != float64(healthyCPUUsec)/1e6 {
		t.Fatalf("CPU after initial = %v, want %v", got, float64(healthyCPUUsec)/1e6)
	}

	path := filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(testActor.UID, sandboxCgroupContainer), "cpu.stat")
	if err := os.WriteFile(path, []byte("usage_usec 1534567\nuser_usec 1000000\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.readFinal(context.Background(), h)
	setHostedActor(s, nil)
	s.recordFinalIfEnded(context.Background(), h)
	if got := ateomstatstest.CPUSeconds(t, reader); math.Abs(got-1.534567) > 1e-12 {
		t.Errorf("CPU after final = %v, want 1.534567", got)
	}
	s.recordFinalIfEnded(context.Background(), h)
	if got := ateomstatstest.CPUSeconds(t, reader); math.Abs(got-1.534567) > 1e-12 {
		t.Errorf("CPU after duplicate final = %v, want 1.534567", got)
	}
}

func TestLateInitialCPUReachesCounterWithoutLateRecord(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	rec := withUsageRecorder(s)
	cpu, reader := ateomstatstest.NewCPUCounter(t, "ateom-gvisor")
	s.cpu = cpu
	h := hostWithEpoch(s, time.Now(), false)

	s.recordFinal(context.Background(), h)
	s.recordInitial(context.Background(), h)
	if got := ateomstatstest.CPUSeconds(t, reader); math.Abs(got-float64(healthyCPUUsec)/1e6) > 1e-12 {
		t.Errorf("CPU after late initial = %v, want %v", got, float64(healthyCPUUsec)/1e6)
	}
	if got := rec.Kinds(); !slices.Equal(got, []string{ateattr.StatsKindFinal}) {
		t.Errorf("records = %v, want only final", got)
	}
}

// TestRecordFinalIfEnded pins that the final record waits for the actor to be
// unhosted, as a teardown does even when a later step fails.
func TestRecordFinalIfEnded(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), true)

	s.recordFinalIfEnded(context.Background(), h)
	if got := rec.Kinds(); len(got) != 0 {
		t.Fatalf("records while hosted = %v, want none", got)
	}
	setHostedActor(s, nil)
	s.recordFinalIfEnded(context.Background(), h)
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Errorf("records after unhosting = %v, want one final", got)
	}
}

// TestGracefulShutdownWritesFinal pins that the drain ends each hosted
// activation with a final record, read before its containers are killed.
func TestGracefulShutdownWritesFinal(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	s.inFlight = actorlock.NewInFlight()
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), true)
	s.actorsMu.Lock()
	h.session = &workloadSession{}
	s.actorsMu.Unlock()

	s.gracefulShutdown(context.Background())
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Fatalf("records = %v, want one final", got)
	}
	if got := rec.Sources(); got[0] != ateattr.StatsSourceCgroup {
		t.Errorf("final record source = %q, want measured", got[0])
	}
}

// TestRecordFinalAfterSandboxGone pins that a final read that finds the cgroup
// gone falls back to the latest sample, and to a pending record when there is
// none.
func TestRecordFinalAfterSandboxGone(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), true)
	s.sweepUsage(context.Background())
	if err := os.RemoveAll(filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(testActor.UID, sandboxCgroupContainer))); err != nil {
		t.Fatal(err)
	}
	s.readFinal(context.Background(), h)
	s.recordFinal(context.Background(), h)

	fresh := hostWithEpoch(s, time.Now(), true)
	s.readFinal(context.Background(), fresh)
	s.recordFinal(context.Background(), fresh)

	var finals []string
	for i, kind := range rec.Kinds() {
		if kind == ateattr.StatsKindFinal {
			finals = append(finals, rec.Sources()[i])
		}
	}
	if want := []string{ateattr.StatsSourceCgroup, ateattr.StatsSourceUnspecified}; !slices.Equal(finals, want) {
		t.Errorf("final record sources = %v, want %v", finals, want)
	}
}

// TestReadFinalWritesNothingUntilRecorded pins the checkpoint's order: the
// reading taken before the snapshot writes no record on its own, and the final
// record, written once the checkpoint succeeds, carries that reading.
func TestReadFinalWritesNothingUntilRecorded(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), true)

	s.readFinal(context.Background(), h)
	if got := rec.Kinds(); len(got) != 0 {
		t.Fatalf("records after the pre-snapshot read = %v, want none", got)
	}
	read := h.usage.Latest()
	if read == nil || read.GetSource() != ateompb.StatsSource_STATS_SOURCE_CGROUP {
		t.Fatalf("pre-snapshot read stored %v, want a measured sample", read)
	}
	if err := os.RemoveAll(filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(testActor.UID, sandboxCgroupContainer))); err != nil {
		t.Fatal(err)
	}

	s.recordFinal(context.Background(), h)
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Fatalf("records after the checkpoint = %v, want one final", got)
	}
	if got := rec.Sources(); got[0] != ateattr.StatsSourceCgroup {
		t.Errorf("final record source = %q, want the pre-snapshot reading", got[0])
	}
}

// TestSweepWaitsForInitialAndStopsAtFinal pins that the sweep reads no actor
// before its initial reading or after its final record.
func TestSweepWaitsForInitialAndStopsAtFinal(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), false)

	s.sweepUsage(context.Background())
	s.recordInitial(context.Background(), h)
	s.sweepUsage(context.Background())
	s.recordFinal(context.Background(), h)
	s.sweepUsage(context.Background())
	want := []string{ateattr.StatsKindInitial, ateattr.StatsKindPeriodic, ateattr.StatsKindFinal}
	if got := rec.Kinds(); !slices.Equal(got, want) {
		t.Errorf("records = %v, want %v", got, want)
	}
}

// TestGetWorkloadStatsStoresItsSample pins that the keyed read, which advances
// the activation's CPU, also updates the sample the cache serves.
func TestGetWorkloadStatsStoresItsSample(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	h := hostWithEpoch(s, time.Now(), true)
	got, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: testActor.UID})
	if err != nil {
		t.Fatal(err)
	}
	if h.usage.Latest() != got.GetSample() {
		t.Error("the keyed read's sample is not the one the cache serves")
	}
}
