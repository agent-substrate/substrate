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
	"github.com/agent-substrate/substrate/internal/actorlock"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateom-gvisor/internal/cgroupstats"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The lifecycle RPCs that host and unhost actors need netlink, runsc, and
// network namespaces, so they are covered end to end rather than here. These
// tests drive the stats handlers against a hosted set built directly.

var testActor = resources.ActorAttribution{
	Ref:              resources.ActorRef{Atespace: "space-a", Name: "actor-a"},
	UID:              "uid-a",
	TemplateAtespace: "ns-a",
	TemplateName:     "template-a",
}

var healthyCgroup = map[string]string{
	"memory.current": "157286400\n",
	"memory.peak":    "209715200\n",
	"memory.stat":    "anon 104857600\ninactive_file 20971520\nactive_file 31457280\n",
	"cpu.stat":       "usage_usec 1234567\nuser_usec 1000000\n",
}

// newStatsService builds a service whose cgroup root is a fixture tree. A nil
// files map leaves the sandbox cgroup directory absent entirely, which is what
// a torn-down sandbox looks like.
func newStatsService(t *testing.T, files map[string]string) *AteomService {
	t.Helper()
	root := t.TempDir()
	if files != nil {
		writeCgroupFixture(t, root, ocispec.GVisorCgroupLeaf(testActor.UID, sandboxCgroupContainer), files)
	}
	return &AteomService{
		locks:      actorlock.New(),
		actors:     map[string]*hostedActor{},
		maxActors:  1000,
		cgroupRoot: root,
	}
}

// writeCgroupFixture lays down one actor's cgroup leaf under root.
func writeCgroupFixture(t *testing.T, root, leaf string, files map[string]string) {
	t.Helper()
	dir := filepath.Join(root, leaf)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("creating fixture cgroup dir: %v", err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("writing fixture %q: %v", name, err)
		}
	}
}

// setHostedActor makes the ateom host exactly this actor, the way RunWorkload
// would, or nothing when attribution is nil.
func setHostedActor(s *AteomService, attribution *resources.ActorAttribution) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	s.actors = map[string]*hostedActor{}
	if attribution != nil {
		s.actors[attribution.UID] = &hostedActor{attribution: *attribution}
	}
}

func TestGetWorkloadStats(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	setHostedActor(s, &testActor)

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
		SandboxClass:          ateompb.SandboxClass_SANDBOX_CLASS_GVISOR,
		Source:                ateompb.StatsSource_STATS_SOURCE_CGROUP,
		MemoryCurrentBytes:    157286400,
		MemoryPeakBytes:       209715200,
		MemoryWorkingSetBytes: 136314880,
		CpuUsageUsec:          1234567,
	}}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetWorkloadStats() mismatch (-want +got):\n%s", diff)
	}
}

func TestGetWorkloadStatsErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		// files is the fixture sandbox cgroup; nil means the directory is absent.
		files map[string]string
		// active is hosted when non-nil; nil leaves the ateom "available".
		active   *resources.ActorAttribution
		actorUID string
		want     codes.Code
	}{
		{
			// A required field the caller left off: a client bug, distinct from the
			// races below, so it gets a distinct code.
			name:     "empty actor_uid",
			files:    healthyCgroup,
			active:   &testActor,
			actorUID: "",
			want:     codes.InvalidArgument,
		},
		{
			// Not here at all. NOT_FOUND rather than FAILED_PRECONDITION, because
			// what the caller should do about it is re-resolve, not retry.
			name:     "ateom is available",
			files:    healthyCgroup,
			active:   nil,
			actorUID: "uid-a",
			want:     codes.NotFound,
		},
		{
			// The worker was recycled between the caller's view of the world and
			// this call. Reporting anyway would file one actor's numbers under
			// another's name, and it is the same "not here" as the case above.
			name:     "actor_uid does not match the executing workload",
			files:    healthyCgroup,
			active:   &testActor,
			actorUID: "uid-b",
			want:     codes.NotFound,
		},
		{
			// The requested actor is the one here, but there is nothing to read yet:
			// a poll landing in the boot, or a sandbox torn down between the
			// attribution check and the read. The one transient case, so the one
			// FAILED_PRECONDITION.
			name:     "no sandbox cgroup to measure",
			files:    nil,
			active:   &testActor,
			actorUID: "uid-a",
			want:     codes.FailedPrecondition,
		},
		{
			// The cgroup is there but does not parse: not a routine race, so it
			// must not be reported as one.
			name:     "sandbox cgroup is malformed",
			files:    map[string]string{"memory.current": "max\n"},
			active:   &testActor,
			actorUID: "uid-a",
			want:     codes.Internal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newStatsService(t, tc.files)
			if tc.active != nil {
				setHostedActor(s, tc.active)
			}

			resp, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: tc.actorUID})
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
	s := newStatsService(t, healthyCgroup)
	setHostedActor(s, &testActor)

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
// retains no attribution. GetWorkloadStats's NOT_FOUND-when-available behavior
// is built on this: a non-nil zero value here would make an idle ateom report
// an empty actor's usage instead of refusing.
func TestAteomServiceStartsAvailable(t *testing.T) {
	if got := (&AteomService{}).hostedActors(); len(got) != 0 {
		t.Errorf("new AteomService hosts %v, want nothing", got)
	}
}

func TestGetActiveWorkloadStats(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	setHostedActor(s, &testActor)

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() error = %v, want nil", err)
	}
	if len(got.GetSamples()) != 1 {
		t.Fatalf("GetActiveWorkloadStats() = %v, want one sample", got)
	}

	// The keyed read against the same fixture is the reference: the discovery
	// read must produce the identical sample, since both are the same
	// measurement with a different addressing mode.
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
	s := newStatsService(t, healthyCgroup)

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
		SandboxClass:          ateompb.SandboxClass_SANDBOX_CLASS_GVISOR,
	}
}

// TestGetActiveWorkloadStatsBooting: executing but nothing to measure yet is
// a pending entry -- attribution without measurements -- not an error, unlike
// the keyed read's FAILED_PRECONDITION. A blind caller finds boots as
// routinely as idle workers, and the entry keeps a workload that dies during
// boot attributable.
func TestGetActiveWorkloadStatsBooting(t *testing.T) {
	s := newStatsService(t, nil) // no cgroup directory: a poll landing mid-boot
	setHostedActor(s, &testActor)

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

// The transition tests change the hosted set inside the lock-free measurement,
// via the readSandboxCgroup seam: that is exactly the window a checkpoint plus
// a fresh run (or a checkpoint alone) can land in.

// The cgroup is read with no lock held and found by UID alone, so the actor
// can be re-hosted underneath the read. Numbers are only published under the
// activation they were read for.
func TestGetActiveWorkloadStatsTransition(t *testing.T) {
	otherActor := testActor
	otherActor.UID = "uid-b"
	otherTemplate := testActor
	otherTemplate.TemplateName = "template-b"

	tests := []struct {
		name string
		to   *resources.ActorAttribution
		// want is the expected samples, observed_at zeroed.
		want []*ateompb.WorkloadStatsSample
	}{
		// Resumed on another template under the same UID: the numbers belong
		// to the new activation, so they are withheld and it is pending.
		{name: "re-hosted on another template", to: &otherTemplate, want: []*ateompb.WorkloadStatsSample{pendingFor(otherTemplate)}},
		// Gone, or replaced by an actor this read did not snapshot.
		{name: "to another actor", to: &otherActor, want: nil},
		{name: "to available", to: nil, want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newStatsService(t, healthyCgroup)
			setHostedActor(s, &testActor)
			s.readSandboxCgroup = func(dir string) (cgroupstats.Sample, error) {
				setHostedActor(s, tc.to)
				return cgroupstats.Read(dir)
			}

			got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
			if err != nil {
				t.Fatalf("GetActiveWorkloadStats() during transition: error = %v, want nil", err)
			}
			for _, sample := range got.GetSamples() {
				sample.ObservedAtUnixNano = 0
			}
			if diff := cmp.Diff(tc.want, got.GetSamples(), protocmp.Transform(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("GetActiveWorkloadStats() during transition mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestGetWorkloadStatsTransition pins the keyed read's side of the same
// window: the caller asserted an actor that is gone by the time the sample
// exists, so the answer is NOT_FOUND -- its mapping wants re-resolving.
func TestGetWorkloadStatsTransition(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	setHostedActor(s, &testActor)
	s.readSandboxCgroup = func(dir string) (cgroupstats.Sample, error) {
		setHostedActor(s, nil)
		return cgroupstats.Read(dir)
	}

	_, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("GetWorkloadStats() during transition: code = %v, want %v (err: %v)", got, codes.NotFound, err)
	}
}

func TestGetActiveWorkloadStatsSeveralActors(t *testing.T) {
	second := testActor
	second.UID = "uid-b"

	s := newStatsService(t, healthyCgroup)
	// Give the second actor its own cgroup fixture.
	writeCgroupFixture(t, s.cgroupRoot, ocispec.GVisorCgroupLeaf(second.UID, sandboxCgroupContainer), healthyCgroup)

	s.actorsMu.Lock()
	s.actors = map[string]*hostedActor{
		testActor.UID: {attribution: testActor},
		second.UID:    {attribution: second},
	}
	s.actorsMu.Unlock()

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() error = %v, want nil", err)
	}
	if len(got.GetSamples()) != 2 {
		t.Fatalf("GetActiveWorkloadStats() returned %d samples, want 2: %v", len(got.GetSamples()), got)
	}
	seen := map[string]bool{}
	for _, sample := range got.GetSamples() {
		seen[sample.GetActorUid()] = true
	}
	for _, uid := range []string{testActor.UID, second.UID} {
		if !seen[uid] {
			t.Errorf("no sample for actor %q; got %v", uid, seen)
		}
	}
}

// One actor booting does not stop the rest being measured: it answers as a
// pending entry alongside their samples.
func TestGetActiveWorkloadStatsOneBooting(t *testing.T) {
	booting := testActor
	booting.UID = "uid-booting"

	s := newStatsService(t, healthyCgroup)
	s.actorsMu.Lock()
	s.actors = map[string]*hostedActor{
		testActor.UID: {attribution: testActor},
		// No cgroup leaf: accepted, but runsc has not created it yet.
		booting.UID: {attribution: booting},
	}
	s.actorsMu.Unlock()

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

// One actor whose cgroup will not parse is reported as pending rather than
// failing the read for the whole worker.
func TestGetActiveWorkloadStatsOneUnreadable(t *testing.T) {
	broken := testActor
	broken.UID = "uid-broken"

	s := newStatsService(t, healthyCgroup)
	dir := filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(broken.UID, sandboxCgroupContainer))
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("max\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.actorsMu.Lock()
	s.actors = map[string]*hostedActor{
		testActor.UID: {attribution: testActor},
		broken.UID:    {attribution: broken},
	}
	s.actorsMu.Unlock()

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
