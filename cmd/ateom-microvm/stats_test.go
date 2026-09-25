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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/actorlock"
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
		want resources.ActorAttribution
	}{
		{
			name: "fully populated",
			p: actorBootParams{
				actorRef:         resources.ActorRef{Atespace: "atespace-a", Name: "actor-b"},
				actorUID:         "uid-c",
				templateAtespace: "template-ns-d",
				templateName:     "template-name-e",
			},
			want: resources.ActorAttribution{
				Ref:              resources.ActorRef{Atespace: "atespace-a", Name: "actor-b"},
				UID:              "uid-c",
				TemplateAtespace: "template-ns-d",
				TemplateName:     "template-name-e",
			},
		},
		{
			name: "zero params",
			p:    actorBootParams{},
			want: resources.ActorAttribution{},
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
		Atespace:              "atespace-a",
		ActorName:             "actor-b",
		ActorUid:              "uid-c",
		ActorTemplateAtespace: "template-ns-d",
		ActorTemplateName:     "template-name-e",
	}

	// Mirrors the actorBootParams literal in RunWorkload and RestoreWorkload.
	p := actorBootParams{
		actorRef:         resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
		actorUID:         req.GetActorUid(),
		templateAtespace: req.GetActorTemplateAtespace(),
		templateName:     req.GetActorTemplateName(),
	}

	if diff := cmp.Diff(ateomstats.ActorAttributionFromRequest(req), p.actorAttribution()); diff != "" {
		t.Errorf("attribution via actorBootParams differs from attribution via request (-request +params):\n%s", diff)
	}
}

// The lifecycle RPCs that host and unhost actors need netlink,
// cloud-hypervisor, and network namespaces, so they are covered end to end
// rather than here. These tests drive the stats handlers against a hosted set
// built directly.

var testActor = resources.ActorAttribution{
	Ref:              resources.ActorRef{Atespace: "space-a", Name: "actor-a"},
	UID:              "uid-a",
	TemplateAtespace: "ns-a",
	TemplateName:     "template-a",
}

// fakeAgent stands in for the kata-agent client: a canned reply per container
// id, or a canned error for the ones that are meant to fail.
type fakeAgent struct {
	mu    sync.Mutex
	stats map[string]*agentpb.CgroupStats
	errs  map[string]error

	// onCall, when set, runs at the top of every StatsContainer, so a test can
	// change the hosted set inside the handler's lock-free read.
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
	_, ok := ctx.Deadline()
	f.mu.Lock()
	f.calls = append(f.calls, containerID)
	f.deadlines = append(f.deadlines, ok)
	f.mu.Unlock()
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
// containers published to GetWorkloadStats. lock is constructed like NewService
// does, since it is a pointer with no usable zero value and
// TestGetWorkloadStatsDoesNotTakeLock holds it.
func newStatsService(agent containerStatsReader, workloadIDs ...string) *AteomService {
	s := &AteomService{locks: actorlock.New(), actors: map[string]*hostedActor{}}
	hostTestActor(s, testActor, &guestStatsTarget{actorUID: testActor.UID, agent: agent, workloadIDs: workloadIDs})
	return s
}

// hostTestActor registers an actor; a nil target represents an actor still booting.
func hostTestActor(s *AteomService, attribution resources.ActorAttribution, target *guestStatsTarget) *hostedActor {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	if s.actors == nil {
		s.actors = map[string]*hostedActor{}
	}
	hosted := &hostedActor{attribution: attribution, guest: target}
	s.actors[attribution.UID] = hosted
	return hosted
}

// unhostTestActor removes one actor, standing in for CheckpointWorkload.
func unhostTestActor(s *AteomService, actorUID string) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	delete(s.actors, actorUID)
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
		Atespace:              "space-a",
		ActorName:             "actor-a",
		ActorUid:              "uid-a",
		ActorTemplateAtespace: "ns-a",
		ActorTemplateName:     "template-a",
		SandboxClass:          ateompb.SandboxClass_SANDBOX_CLASS_MICROVM,
		Source:                ateompb.StatsSource_STATS_SOURCE_GUEST_AGENT,
		MemoryCurrentBytes:    157286400,
		MemoryPeakBytes:       209715200,
		MemoryWorkingSetBytes: 136314880,
		CpuUsageUsec:          1234567,
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
// guest gives one cgroup per container (see StartRootfsContainer), and the
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
				hostTestActor(s, testActor, nil)
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
				hostTestActor(s, testActor, &guestStatsTarget{actorUID: "uid-b", agent: healthy, workloadIDs: []string{"app_ovl"}})
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

// A stats poll must not queue behind a lifecycle RPC. The actor's lock is held
// throughout, so a handler that took it would time out here.
func TestGetWorkloadStatsDoesNotTakeLock(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5000)}}
	s := newStatsService(agent, "app_ovl")

	// Stands in for a RunWorkload or CheckpointWorkload in flight against this
	// actor, which holds its lock across the entire body.
	if !s.locks.Lock(context.Background(), testActor.UID) {
		t.Fatal("could not take the actor lock")
	}
	defer s.locks.Unlock(testActor.UID)

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
	if got := s.hostedActors(); len(got) != 0 {
		t.Errorf("new AteomService hosts %d actors, want none", len(got))
	}
	if got := s.lookupActor(testActor.UID); got != nil {
		t.Errorf("new AteomService.lookupActor(%q) = %v, want nil", testActor.UID, got)
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
	if len(got.GetSamples()) != 1 {
		t.Fatalf("GetActiveWorkloadStats() = %v, want one sample", got)
	}

	// The keyed read against the same fake is the reference: the discovery read
	// must produce the identical sample, since both are the same measurement
	// with a different addressing mode.
	want, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}
	sample := got.GetSamples()[0]
	sample.ObservedAtUnixNano = 0
	want.GetSample().ObservedAtUnixNano = 0
	if diff := cmp.Diff(want.GetSample(), sample, protocmp.Transform()); diff != "" {
		t.Errorf("discovery sample differs from keyed sample (-keyed +discovery):\n%s", diff)
	}
}

// TestGetActiveWorkloadStatsAvailable pins the contract that makes the
// discovery read scrapeable: an idle ateom is an empty list, never an error.
func TestGetActiveWorkloadStatsAvailable(t *testing.T) {
	s := &AteomService{}

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() on an available ateom: error = %v, want nil", err)
	}
	if n := len(got.GetSamples()); n != 0 {
		t.Errorf("GetActiveWorkloadStats() on an available ateom = %v, want no samples", got)
	}
}

// pendingFor is the entry the discovery read answers for a workload with no
// numbers yet: attribution and the runtime family, source UNSPECIFIED,
// measurements absent. observed_at is zeroed by the caller before comparing.
func pendingFor(attr resources.ActorAttribution) *ateompb.WorkloadStatsSample {
	return &ateompb.WorkloadStatsSample{
		Atespace:              attr.Ref.Atespace,
		ActorName:             attr.Ref.Name,
		ActorUid:              attr.UID,
		ActorTemplateAtespace: attr.TemplateAtespace,
		ActorTemplateName:     attr.TemplateName,
		SandboxClass:          ateompb.SandboxClass_SANDBOX_CLASS_MICROVM,
	}
}

// TestGetActiveWorkloadStatsBooting: executing but no guest target yet is a
// pending entry -- attribution without measurements -- not an error, unlike
// the keyed read's FAILED_PRECONDITION. A blind caller finds boots as
// routinely as idle workers, and the entry keeps a workload that dies during
// boot attributable.
func TestGetActiveWorkloadStatsBooting(t *testing.T) {
	s := &AteomService{}
	hostTestActor(s, testActor, nil) // attribution retained, target not published

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() mid-boot: error = %v, want nil", err)
	}
	if len(got.GetSamples()) != 1 {
		t.Fatalf("GetActiveWorkloadStats() mid-boot = %v, want one pending entry", got)
	}
	entry := got.GetSamples()[0]
	if entry.GetObservedAtUnixNano() == 0 {
		t.Error("pending entry observed_at_unix_nano = 0, want set")
	}
	entry.ObservedAtUnixNano = 0
	if diff := cmp.Diff(pendingFor(testActor), entry, protocmp.Transform()); diff != "" {
		t.Errorf("pending entry mismatch (-want +got):\n%s", diff)
	}
}

func TestGetActiveWorkloadStatsSeveralActors(t *testing.T) {
	second := testActor
	second.UID = "uid-b"
	second.Ref.Name = "actor-b"

	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
		"app_ovl": containerStats(1000, 2000, 100, 5000),
		"b_ovl":   containerStats(3000, 4000, 200, 7000),
	}}
	s := newStatsService(agent, "app_ovl")
	hostTestActor(s, second, &guestStatsTarget{actorUID: second.UID, agent: agent, workloadIDs: []string{"b_ovl"}})

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() error = %v, want nil", err)
	}
	if len(got.GetSamples()) != 2 {
		t.Fatalf("GetActiveWorkloadStats() returned %d samples, want 2: %v", len(got.GetSamples()), got)
	}
	byUID := map[string]*ateompb.WorkloadStatsSample{}
	for _, sample := range got.GetSamples() {
		byUID[sample.GetActorUid()] = sample
	}
	for _, want := range []string{testActor.UID, second.UID} {
		if byUID[want] == nil {
			t.Errorf("no sample for actor %q; got %v", want, byUID)
		}
	}
	if got := byUID[second.UID].GetMemoryCurrentBytes(); got != 3000 {
		t.Errorf("actor %q memory_current_bytes = %d, want 3000", second.UID, got)
	}
}

// One actor booting does not stop the rest being measured: it answers as a
// pending entry alongside their samples.
func TestGetActiveWorkloadStatsOneBooting(t *testing.T) {
	booting := testActor
	booting.UID = "uid-b"

	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
		"app_ovl": containerStats(1000, 2000, 100, 5000),
	}}
	s := newStatsService(agent, "app_ovl")
	hostTestActor(s, booting, nil) // accepted, no guest to ask yet

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() error = %v, want nil", err)
	}
	if len(got.GetSamples()) != 2 {
		t.Fatalf("GetActiveWorkloadStats() returned %d samples, want 2: %v", len(got.GetSamples()), got)
	}
	for _, sample := range got.GetSamples() {
		measured := sample.GetSource() != ateompb.StatsSource_STATS_SOURCE_UNSPECIFIED
		if want := sample.GetActorUid() == testActor.UID; measured != want {
			t.Errorf("actor %q measured = %v, want %v", sample.GetActorUid(), measured, want)
		}
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
	agent.onCall = func() { unhostTestActor(s, testActor.UID) }

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
	hostTestActor(s, testActor, &guestStatsTarget{actorUID: "uid-b", agent: agent, workloadIDs: []string{"app_ovl"}})

	_, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if got := status.Code(err); got != codes.Internal {
		t.Errorf("GetActiveWorkloadStats() with stale target: code = %v, want %v (err: %v)", got, codes.Internal, err)
	}
}

// The discovery read asks the guests concurrently, so a worker full of slow
// guests still answers within the caller's deadline.
func TestGetActiveWorkloadStatsSamplesGuestsConcurrently(t *testing.T) {
	const actors = 4
	var inFlight, peak atomic.Int32
	entered := make(chan struct{}, actors)
	all := make(chan struct{})
	var once sync.Once
	agent := &fakeAgent{
		stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5000)},
		onCall: func() {
			n := inFlight.Add(1)
			defer inFlight.Add(-1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			entered <- struct{}{}
			if len(entered) == actors {
				once.Do(func() { close(all) })
			}
			select {
			case <-all:
			case <-time.After(time.Second):
			}
		},
	}
	s := &AteomService{locks: actorlock.New(), actors: map[string]*hostedActor{}}
	for i := range actors {
		attr := testActor
		attr.UID = fmt.Sprintf("uid-%d", i)
		hostTestActor(s, attr, &guestStatsTarget{actorUID: attr.UID, agent: agent, workloadIDs: []string{"app_ovl"}})
	}

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() error = %v, want nil", err)
	}
	if len(got.GetSamples()) != actors {
		t.Fatalf("GetActiveWorkloadStats() returned %d samples, want %d", len(got.GetSamples()), actors)
	}
	if p := peak.Load(); p != actors {
		t.Errorf("at most %d guests were asked at once, want all %d", p, actors)
	}
}

// The guest is read with no lock held and found by UID alone, so the actor can
// be re-hosted underneath the read. Numbers are only published under the
// activation they were read for.
func TestGetActiveWorkloadStatsTransition(t *testing.T) {
	otherActor := testActor
	otherActor.UID = "uid-b"
	otherTemplate := testActor
	otherTemplate.TemplateName = "template-b"

	tests := []struct {
		name string
		// during changes the hosted set inside the guest read.
		during func(s *AteomService)
		want   []*ateompb.WorkloadStatsSample
	}{
		{
			name:   "re-hosted on another template",
			during: func(s *AteomService) { hostTestActor(s, otherTemplate, nil) },
			want:   []*ateompb.WorkloadStatsSample{pendingFor(otherTemplate)},
		},
		{
			name: "to another actor",
			during: func(s *AteomService) {
				unhostTestActor(s, testActor.UID)
				hostTestActor(s, otherActor, nil)
			},
		},
		{
			name:   "to available",
			during: func(s *AteomService) { unhostTestActor(s, testActor.UID) },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5000)}}
			s := newStatsService(agent, "app_ovl")
			var once sync.Once
			agent.onCall = func() { once.Do(func() { tc.during(s) }) }

			got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
			if err != nil {
				t.Fatalf("GetActiveWorkloadStats() during transition: error = %v, want nil", err)
			}
			var samples []*ateompb.WorkloadStatsSample
			for _, sample := range got.GetSamples() {
				// otherActor was not snapshotted by this read.
				if sample.GetActorUid() == otherActor.UID {
					continue
				}
				sample.ObservedAtUnixNano = 0
				samples = append(samples, sample)
			}
			if diff := cmp.Diff(tc.want, samples, protocmp.Transform(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("GetActiveWorkloadStats() during transition mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
