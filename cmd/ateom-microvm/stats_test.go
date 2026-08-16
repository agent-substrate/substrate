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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// TestActorBootParamsAttribution pins the mapping from actorBootParams onto the
// attribution the service retains. The three loose fields are the ones most
// likely to get crossed, since actorBootParams names them differently than
// ActorAttribution does; the distinct placeholders below make a swap visible.
func TestActorBootParamsAttribution(t *testing.T) {
	tests := []struct {
		name string
		p    actorBootParams
		want ateomstats.ActorAttribution
	}{
		{
			name: "fully populated",
			p: actorBootParams{
				actorRef:     resources.ActorRef{Atespace: "atespace-a", Name: "actor-b"},
				actorUID:     "uid-c",
				templateNS:   "template-ns-d",
				templateName: "template-name-e",
			},
			want: ateomstats.ActorAttribution{
				Ref:               resources.ActorRef{Atespace: "atespace-a", Name: "actor-b"},
				UID:               "uid-c",
				TemplateNamespace: "template-ns-d",
				TemplateName:      "template-name-e",
			},
		},
		{
			name: "zero params",
			p:    actorBootParams{},
			want: ateomstats.ActorAttribution{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, tc.p.actorAttribution()); diff != "" {
				t.Errorf("actorBootParams.actorAttribution() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestActorBootParamsAttributionMatchesRequest checks the two hops the
// attribution makes — request to actorBootParams (in RunWorkload /
// RestoreWorkload) and actorBootParams to ActorAttribution — compose back into
// what the caller sent. The two hops are written in different files, so this is
// the assertion that catches them drifting apart.
func TestActorBootParamsAttributionMatchesRequest(t *testing.T) {
	req := &ateompb.RunWorkloadRequest{
		Atespace:               "atespace-a",
		ActorName:              "actor-b",
		ActorUid:               "uid-c",
		ActorTemplateNamespace: "template-ns-d",
		ActorTemplateName:      "template-name-e",
	}

	// Mirrors the actorBootParams literal in RunWorkload and RestoreWorkload.
	p := actorBootParams{
		actorRef:     resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
		actorUID:     req.GetActorUid(),
		templateNS:   req.GetActorTemplateNamespace(),
		templateName: req.GetActorTemplateName(),
	}

	if diff := cmp.Diff(ateomstats.ActorAttributionFromRequest(req), p.actorAttribution()); diff != "" {
		t.Errorf("attribution via actorBootParams differs from attribution via request (-request +params):\n%s", diff)
	}
}

// The lifecycle transitions that maintain s.activeActor and s.guestStats — set
// by RunWorkload and RestoreWorkload, cleared by CheckpointWorkload's teardown —
// have no unit test, because those three RPCs each reach for netlink,
// cloud-hypervisor, and the worker pod's netns within a few lines of entry and
// cannot be driven from `go test`. The mapping they use is covered above and in
// internal/ateomstats; the transitions are verified end to end. What is testable
// here is everything GetWorkloadStats does with the result, which is where the
// polling loop will actually live.

var testActor = ateomstats.ActorAttribution{
	Ref:               resources.ActorRef{Atespace: "space-a", Name: "actor-a"},
	UID:               "uid-a",
	TemplateNamespace: "ns-a",
	TemplateName:      "template-a",
}

// fakeAgent stands in for the kata-agent client: a canned reply per container
// id, or a canned error for the ones that are meant to fail.
type fakeAgent struct {
	stats map[string]*agentpb.CgroupStats
	errs  map[string]error

	// onCall, when set, runs at the top of every StatsContainer. It is how a
	// test interleaves a lifecycle transition with the handlers' lock-free
	// read: the handler has loaded activeActor by the time the agent is asked,
	// so flipping it here lands in the window the re-check guards.
	onCall func()

	// calls records the container ids asked for, in order, so a test can tell
	// "summed two containers" from "read one twice".
	calls []string
	// deadlines records whether each call's context carried one, which is how
	// the per-call timeout is pinned.
	deadlines []bool
}

func (f *fakeAgent) StatsContainer(ctx context.Context, containerID string) (*agentpb.CgroupStats, error) {
	if f.onCall != nil {
		f.onCall()
	}
	f.calls = append(f.calls, containerID)
	_, ok := ctx.Deadline()
	f.deadlines = append(f.deadlines, ok)
	if err, ok := f.errs[containerID]; ok {
		return nil, err
	}
	return f.stats[containerID], nil
}

// containerStats is one container's guest reading: usage bytes, peak bytes,
// reclaimable page cache, and cumulative CPU nanoseconds.
func containerStats(usage, peak, inactiveFile, cpuNanos uint64) *agentpb.CgroupStats {
	return &agentpb.CgroupStats{
		MemoryStats: &agentpb.MemoryStats{
			Usage: &agentpb.MemoryData{Usage: usage, MaxUsage: peak},
			Stats: map[string]uint64{"inactive_file": inactiveFile},
		},
		CpuStats: &agentpb.CpuStats{CpuUsage: &agentpb.CpuUsage{TotalUsage: cpuNanos}},
	}
}

// newStatsService builds a service executing testActor with the given guest
// containers published to GetWorkloadStats.
func newStatsService(agent containerStatsReader, workloadIDs ...string) *AteomService {
	s := &AteomService{}
	s.activeActor.Store(&testActor)
	s.guestStats.Store(&guestStatsTarget{actorUID: testActor.UID, agent: agent, workloadIDs: workloadIDs})
	return s
}

func TestGetWorkloadStats(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
		"app_ovl": containerStats(157286400, 209715200, 20971520, 1234567000),
	}}
	s := newStatsService(agent, "app_ovl")

	before := time.Now().UnixNano()
	got, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	after := time.Now().UnixNano()
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}

	if got.GetSample().GetObservedAtUnixNano() < before || got.GetSample().GetObservedAtUnixNano() > after {
		t.Errorf("GetWorkloadStats() observed_at_unix_nano = %d, want within [%d, %d]", got.GetSample().GetObservedAtUnixNano(), before, after)
	}
	// Checked above; zeroed so the rest can be compared as a whole.
	got.GetSample().ObservedAtUnixNano = 0

	want := &ateompb.GetWorkloadStatsResponse{Sample: &ateompb.WorkloadStatsSample{
		Atespace:               "space-a",
		ActorName:              "actor-a",
		ActorUid:               "uid-a",
		ActorTemplateNamespace: "ns-a",
		ActorTemplateName:      "template-a",
		SandboxClass:           ateompb.SandboxClass_SANDBOX_CLASS_MICROVM,
		Source:                 ateompb.StatsSource_STATS_SOURCE_GUEST_AGENT,
		MemoryCurrentBytes:     157286400,
		MemoryPeakBytes:        209715200,
		MemoryWorkingSetBytes:  136314880,
		CpuUsageUsec:           1234567,
	}}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetWorkloadStats() mismatch (-want +got):\n%s", diff)
	}

	// A poll must not be able to hang on a guest that has stopped answering,
	// which is only true if the handler bounds each call rather than passing the
	// caller's context straight through.
	if want := []bool{true}; !cmp.Equal(want, agent.deadlines) {
		t.Errorf("StatsContainer call contexts had deadlines %v, want %v", agent.deadlines, want)
	}
}

// TestGetWorkloadStatsSumsContainers covers the multi-container actor: the
// guest gives one cgroup per container (see StartOverlayWorkload), and the
// proto reports one figure for the actor.
func TestGetWorkloadStatsSumsContainers(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
		"app_ovl":     containerStats(1000, 4000, 100, 7000),
		"sidecar_ovl": containerStats(500, 800, 200, 3000),
	}}
	s := newStatsService(agent, "app_ovl", "sidecar_ovl")

	got, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}

	if want := uint64(1500); got.GetSample().GetMemoryCurrentBytes() != want {
		t.Errorf("memory_current_bytes = %d, want %d", got.GetSample().GetMemoryCurrentBytes(), want)
	}
	// The sum of the peaks, which is an upper bound on the peak of the sum: the
	// two containers need not have peaked at the same moment.
	if want := uint64(4800); got.GetSample().GetMemoryPeakBytes() != want {
		t.Errorf("memory_peak_bytes = %d, want %d", got.GetSample().GetMemoryPeakBytes(), want)
	}
	if want := uint64(1200); got.GetSample().GetMemoryWorkingSetBytes() != want {
		t.Errorf("memory_working_set_bytes = %d, want %d", got.GetSample().GetMemoryWorkingSetBytes(), want)
	}
	if want := uint64(10); got.GetSample().GetCpuUsageUsec() != want {
		t.Errorf("cpu_usage_usec = %d, want %d", got.GetSample().GetCpuUsageUsec(), want)
	}

	if want := []string{"app_ovl", "sidecar_ovl"}; !cmp.Equal(want, agent.calls) {
		t.Errorf("StatsContainer called for %v, want %v", agent.calls, want)
	}
}

// TestGetWorkloadStatsSkipsUnreadableContainer pins the partial-reading trade:
// one container the agent cannot report must not silence the actor's telemetry.
// The usual way in is a container that has exited, which took its guest cgroup
// with it and consumes nothing from here on — so contributing zero is the
// correct answer for it, not merely a tolerable one.
func TestGetWorkloadStatsSkipsUnreadableContainer(t *testing.T) {
	agent := &fakeAgent{
		stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5000)},
		errs:  map[string]error{"exited_ovl": errors.New("no such container")},
	}
	s := newStatsService(agent, "app_ovl", "exited_ovl")

	got, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}
	if want := uint64(1000); got.GetSample().GetMemoryCurrentBytes() != want {
		t.Errorf("memory_current_bytes = %d, want %d", got.GetSample().GetMemoryCurrentBytes(), want)
	}
	if want := uint64(5); got.GetSample().GetCpuUsageUsec() != want {
		t.Errorf("cpu_usage_usec = %d, want %d", got.GetSample().GetCpuUsageUsec(), want)
	}
}

// TestGetWorkloadStatsCountsAnsweredContainer covers the boundary of the rule
// above: a container the agent answers for without cgroup stats is a reading of
// zero, not a failure, so the sample stands even when it is the only container.
func TestGetWorkloadStatsCountsAnsweredContainer(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": nil}}
	s := newStatsService(agent, "app_ovl")

	got, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}
	if got.GetSample().GetMemoryCurrentBytes() != 0 || got.GetSample().GetCpuUsageUsec() != 0 {
		t.Errorf("GetWorkloadStats() = %v, want an all-zero measurement", got)
	}
}

func TestGetWorkloadStatsErrors(t *testing.T) {
	healthy := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5000)}}

	for _, tc := range []struct {
		name string
		// service builds the service under test, so each case can put the two
		// atomics in exactly the state it means to exercise.
		service  func() *AteomService
		actorUID string
		want     codes.Code
	}{
		{
			// A required field the caller left off: a client bug, distinct from
			// the races below, so it gets a distinct code.
			name:     "empty actor_uid",
			service:  func() *AteomService { return newStatsService(healthy, "app_ovl") },
			actorUID: "",
			want:     codes.InvalidArgument,
		},
		{
			// Not here at all. NOT_FOUND rather than FAILED_PRECONDITION, because
			// what the caller should do about it is re-resolve, not retry.
			name:     "ateom is available",
			service:  func() *AteomService { return &AteomService{} },
			actorUID: "uid-a",
			want:     codes.NotFound,
		},
		{
			// The worker was recycled between the caller's view of the world and
			// this call. Reporting anyway would file one actor's numbers under
			// another's name, and it is the same "not here" as the case above.
			name:     "actor_uid does not match the executing workload",
			service:  func() *AteomService { return newStatsService(healthy, "app_ovl") },
			actorUID: "uid-b",
			want:     codes.NotFound,
		},
		{
			// The requested actor is the one here, but the guest is not up yet —
			// a poll landing in the boot or the restore, or one landing after
			// teardownActor cleared the target. The transient case.
			name: "no guest agent connection yet",
			service: func() *AteomService {
				s := &AteomService{}
				s.activeActor.Store(&testActor)
				return s
			},
			actorUID: "uid-a",
			want:     codes.FailedPrecondition,
		},
		{
			// Should be unreachable — the two atomics are written together under
			// lock — so a disagreement is an invariant violation, not a routine
			// state: Internal, unlike every other way sampleGuest declines.
			name: "guest agent connection belongs to another actor",
			service: func() *AteomService {
				s := newStatsService(healthy, "app_ovl")
				s.guestStats.Store(&guestStatsTarget{actorUID: "uid-b", agent: healthy, workloadIDs: []string{"app_ovl"}})
				return s
			},
			actorUID: "uid-a",
			want:     codes.Internal,
		},
		{
			// Not one container gone but the guest as a whole not answering: the
			// sandbox is going away, which the next CheckpointWorkload turns into
			// the NOT_FOUND above, or the agent is briefly unreachable. Either way
			// it is "no numbers right now" rather than Internal — the guest not
			// answering is a routine state here, not a bug in this ateom.
			name: "no container answers",
			service: func() *AteomService {
				agent := &fakeAgent{errs: map[string]error{
					"app_ovl":     errors.New("ttrpc: closed"),
					"sidecar_ovl": errors.New("ttrpc: closed"),
				}}
				return newStatsService(agent, "app_ovl", "sidecar_ovl")
			},
			actorUID: "uid-a",
			want:     codes.FailedPrecondition,
		},
		{
			// The boot path does not produce an actor with no containers, so a
			// confident zero would be reporting a state we do not understand.
			name:     "no containers to measure",
			service:  func() *AteomService { return newStatsService(healthy) },
			actorUID: "uid-a",
			want:     codes.FailedPrecondition,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := tc.service().GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: tc.actorUID})
			if resp != nil {
				t.Errorf("GetWorkloadStats() returned response %v, want nil", resp)
			}
			if got := status.Code(err); got != tc.want {
				t.Errorf("GetWorkloadStats() error code = %v, want %v (err: %v)", got, tc.want, err)
			}
		})
	}
}

// TestGetWorkloadStatsDoesNotTakeLock is the regression test for the property
// the design turns on: a stats poll must not queue behind a lifecycle RPC.
// s.lock is held for the duration of the call here, so a handler that reached
// for it — or that looked up the agent client in s.running, which lock guards —
// would deadlock and fail this test by timing out rather than by assertion.
func TestGetWorkloadStatsDoesNotTakeLock(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5000)}}
	s := newStatsService(agent, "app_ovl")

	// Stands in for a RunWorkload or CheckpointWorkload in flight, which hold
	// the lock across their entire bodies.
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"}); err != nil {
		t.Errorf("GetWorkloadStats() error = %v, want nil", err)
	}
}

// TestAteomServiceStartsAvailable checks that a freshly constructed service
// retains no attribution and offers no guest, mirroring the gVisor ateom's test
// of the same name. GetWorkloadStats's NOT_FOUND-when-available behavior is
// built on the first: a non-nil zero value would make an idle ateom report an
// empty actor's usage instead of refusing.
func TestAteomServiceStartsAvailable(t *testing.T) {
	s := &AteomService{}
	if got := s.activeActor.Load(); got != nil {
		t.Errorf("new AteomService.activeActor = %v, want nil", got)
	}
	if got := s.guestStats.Load(); got != nil {
		t.Errorf("new AteomService.guestStats = %v, want nil", got)
	}
}

func TestGetActiveWorkloadStats(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
		"app_ovl": containerStats(157286400, 209715200, 20971520, 1234567000),
	}}
	s := newStatsService(agent, "app_ovl")

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() error = %v, want nil", err)
	}
	if got.GetSample() == nil {
		t.Fatalf("GetActiveWorkloadStats() = %v, want a sample", got)
	}

	// The keyed read against the same fake is the reference: the discovery read
	// must produce the identical sample, since both are the same measurement
	// with a different addressing mode.
	want, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}
	sample := got.GetSample()
	sample.ObservedAtUnixNano = 0
	want.GetSample().ObservedAtUnixNano = 0
	if diff := cmp.Diff(want.GetSample(), sample, protocmp.Transform()); diff != "" {
		t.Errorf("discovery sample differs from keyed sample (-keyed +discovery):\n%s", diff)
	}
}

// TestGetActiveWorkloadStatsAvailable pins the contract that makes the
// discovery read scrapeable: an idle ateom is a reason, never an error.
func TestGetActiveWorkloadStatsAvailable(t *testing.T) {
	s := &AteomService{}

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() on an available ateom: error = %v, want nil", err)
	}
	if got.GetNoSampleReason() != ateompb.NoSampleReason_NO_SAMPLE_REASON_NO_WORKLOAD {
		t.Errorf("GetActiveWorkloadStats() = %v, want NO_WORKLOAD reason", got)
	}
}

// TestGetActiveWorkloadStatsBooting: executing but no guest target yet is a
// NOT_MEASURABLE_YET reason, not an error, unlike the keyed read's
// FAILED_PRECONDITION. A blind caller finds boots as routinely as idle
// workers.
func TestGetActiveWorkloadStatsBooting(t *testing.T) {
	s := &AteomService{}
	s.activeActor.Store(&testActor) // attribution retained, target not published

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() mid-boot: error = %v, want nil", err)
	}
	if got.GetNoSampleReason() != ateompb.NoSampleReason_NO_SAMPLE_REASON_NOT_MEASURABLE_YET {
		t.Errorf("GetActiveWorkloadStats() mid-boot = %v, want NOT_MEASURABLE_YET reason", got)
	}
}

// The transition tests cover the re-check that runs after the lock-free
// measurement: the fake agent's onCall hook flips activeActor while the
// handler is mid-read, which is exactly the window a checkpoint plus a fresh
// run (or a checkpoint alone) can land in.

func TestGetActiveWorkloadStatsTransition(t *testing.T) {
	otherActor := testActor
	otherActor.UID = "uid-b"

	tests := []struct {
		name string
		to   *ateomstats.ActorAttribution
		want ateompb.NoSampleReason
	}{
		// A new actor took the slot: there is a workload, its numbers are just
		// not attributable this tick.
		{name: "to another actor", to: &otherActor, want: ateompb.NoSampleReason_NO_SAMPLE_REASON_NOT_MEASURABLE_YET},
		// A checkpoint emptied the slot: report what is true now.
		{name: "to available", to: nil, want: ateompb.NoSampleReason_NO_SAMPLE_REASON_NO_WORKLOAD},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
				"app_ovl": containerStats(1000, 2000, 100, 5000),
			}}
			s := newStatsService(agent, "app_ovl")
			agent.onCall = func() { s.activeActor.Store(tc.to) }

			got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
			if err != nil {
				t.Fatalf("GetActiveWorkloadStats() during transition: error = %v, want nil", err)
			}
			if got.GetSample() != nil {
				t.Errorf("GetActiveWorkloadStats() during transition returned sample %v, want none", got.GetSample())
			}
			if got.GetNoSampleReason() != tc.want {
				t.Errorf("GetActiveWorkloadStats() during transition = %v, want %v reason", got, tc.want)
			}
		})
	}
}

// TestGetWorkloadStatsTransition pins the keyed read's side of the same
// window: the caller asserted an actor that is gone by the time the sample
// exists, so the answer is NOT_FOUND -- its mapping wants re-resolving.
func TestGetWorkloadStatsTransition(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
		"app_ovl": containerStats(1000, 2000, 100, 5000),
	}}
	s := newStatsService(agent, "app_ovl")
	agent.onCall = func() { s.activeActor.Store(nil) }

	_, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("GetWorkloadStats() during transition: code = %v, want %v (err: %v)", got, codes.NotFound, err)
	}
}

// TestGetActiveWorkloadStatsStaleTarget pins that the one bug-shaped failure
// stays an error on the discovery read too: a target/attribution disagreement
// is an invariant violation, not a NOT_MEASURABLE_YET to skip past silently.
func TestGetActiveWorkloadStatsStaleTarget(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
		"app_ovl": containerStats(1000, 2000, 100, 5000),
	}}
	s := newStatsService(agent, "app_ovl")
	s.guestStats.Store(&guestStatsTarget{actorUID: "uid-b", agent: agent, workloadIDs: []string{"app_ovl"}})

	_, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if got := status.Code(err); got != codes.Internal {
		t.Errorf("GetActiveWorkloadStats() with stale target: code = %v, want %v (err: %v)", got, codes.Internal, err)
	}
}
