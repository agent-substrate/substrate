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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The lifecycle transitions that maintain s.activeActor — set by RunWorkload and
// RestoreWorkload, cleared by CheckpointWorkload — have no unit test, because
// those three RPCs each reach for netlink, runsc, and the worker pod's netns
// within a few lines of entry and cannot be driven from `go test`. The mapping
// they use is covered in internal/ateomstats; the transitions are verified end
// to end. What is testable here is everything GetWorkloadStats does with the
// result, which is where the polling loop will actually live.

var testActor = ateomstats.ActorAttribution{
	Ref:               resources.ActorRef{Atespace: "space-a", Name: "actor-a"},
	UID:               "uid-a",
	TemplateNamespace: "ns-a",
	TemplateName:      "template-a",
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
		dir := filepath.Join(root, sandboxCgroupContainer)
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatalf("creating fixture cgroup dir: %v", err)
		}
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
				t.Fatalf("writing fixture %q: %v", name, err)
			}
		}
	}
	return &AteomService{cgroupRoot: root}
}

func TestGetWorkloadStats(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	s.activeActor.Store(&testActor)

	before := time.Now().UnixNano()
	got, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	after := time.Now().UnixNano()
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}

	if got.GetObservedAtUnixNano() < before || got.GetObservedAtUnixNano() > after {
		t.Errorf("GetWorkloadStats() observed_at_unix_nano = %d, want within [%d, %d]", got.GetObservedAtUnixNano(), before, after)
	}
	// Checked above; zeroed so the rest can be compared as a whole.
	got.ObservedAtUnixNano = 0

	want := &ateompb.GetWorkloadStatsResponse{
		Atespace:               "space-a",
		ActorName:              "actor-a",
		ActorUid:               "uid-a",
		ActorTemplateNamespace: "ns-a",
		ActorTemplateName:      "template-a",
		SandboxClass:           ateompb.SandboxClass_SANDBOX_CLASS_GVISOR,
		Source:                 ateompb.StatsSource_STATS_SOURCE_CGROUP,
		MemoryCurrentBytes:     157286400,
		MemoryPeakBytes:        209715200,
		MemoryWorkingSetBytes:  136314880,
		CpuUsageUsec:           1234567,
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetWorkloadStats() mismatch (-want +got):\n%s", diff)
	}
}

func TestGetWorkloadStatsErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		// files is the fixture sandbox cgroup; nil means the directory is absent.
		files map[string]string
		// active is stored into activeActor when non-nil; nil leaves the ateom
		// "available".
		active   *ateomstats.ActorAttribution
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
				s.activeActor.Store(tc.active)
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

// TestGetWorkloadStatsDoesNotTakeLock is the regression test for the property
// the design turns on: a stats poll must not queue behind a lifecycle RPC.
// s.lock is held for the duration of the call here, so a handler that reached
// for it would deadlock and fail this test by timing out rather than by
// assertion.
func TestGetWorkloadStatsDoesNotTakeLock(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	s.activeActor.Store(&testActor)

	// Stands in for a RunWorkload or CheckpointWorkload in flight, which hold the
	// lock across their entire bodies.
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"}); err != nil {
		t.Errorf("GetWorkloadStats() error = %v, want nil", err)
	}
}

// TestAteomServiceStartsAvailable checks that a freshly constructed service
// retains no attribution. GetWorkloadStats's NOT_FOUND-when-available behavior
// is built on this: a non-nil zero value here would make an idle ateom report
// an empty actor's usage instead of refusing.
func TestAteomServiceStartsAvailable(t *testing.T) {
	if got := (&AteomService{}).activeActor.Load(); got != nil {
		t.Errorf("new AteomService.activeActor = %v, want nil", got)
	}
}
