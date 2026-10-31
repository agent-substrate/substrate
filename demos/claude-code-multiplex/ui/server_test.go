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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
)

type fakeControlClient struct {
	ateapi.ControlClient
	mu             sync.Mutex
	actors         []*ateapi.Actor
	workers        []*ateapi.Worker
	lastAtespace   string
	resumedActors  []string
	suspendedNames []string
}

func (f *fakeControlClient) ListActors(_ context.Context, req *ateapi.ListActorsRequest, _ ...grpc.CallOption) (*ateapi.ListActorsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastAtespace = req.GetAtespace()
	return &ateapi.ListActorsResponse{Actors: f.actors}, nil
}

func (f *fakeControlClient) ListWorkers(_ context.Context, _ *ateapi.ListWorkersRequest, _ ...grpc.CallOption) (*ateapi.ListWorkersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &ateapi.ListWorkersResponse{Workers: f.workers}, nil
}

func (f *fakeControlClient) ResumeActor(_ context.Context, req *ateapi.ResumeActorRequest, _ ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumedActors = append(f.resumedActors, req.GetActor().GetName())
	return &ateapi.ResumeActorResponse{}, nil
}

func (f *fakeControlClient) SuspendActor(_ context.Context, req *ateapi.SuspendActorRequest, _ ...grpc.CallOption) (*ateapi.SuspendActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.suspendedNames = append(f.suspendedNames, req.GetActor().GetName())
	return &ateapi.SuspendActorResponse{}, nil
}

func TestScopingAndUUIDFiltering(t *testing.T) {
	fake := &fakeControlClient{
		actors: []*ateapi.Actor{
			{Metadata: &ateapi.ResourceMetadata{Atespace: defaultNamespace, Name: "luna"}, ActorTemplate: &ateapi.ObjectRef{Atespace: defaultNamespace, Name: "agent-luna"}},
			{Metadata: &ateapi.ResourceMetadata{Atespace: defaultNamespace, Name: "01234567-89ab-cdef-0123-456789abcdef"}, ActorTemplate: &ateapi.ObjectRef{Atespace: defaultNamespace, Name: "agent-luna"}},
			{Metadata: &ateapi.ResourceMetadata{Atespace: "ate-demo-counter", Name: "counter-0"}, ActorTemplate: &ateapi.ObjectRef{Atespace: "ate-demo-counter", Name: "counter"}},
		},
		workers: []*ateapi.Worker{
			{WorkerPod: "claude-workerpool-abc", WorkerPool: "claude-workerpool", WorkerNamespace: defaultNamespace, Status: &ateapi.WorkerStatus{Allocated: &ateapi.WorkerResources{Actors: 1}}},
			{WorkerPod: "counter-xyz", WorkerPool: "counter", WorkerNamespace: "ate-demo-counter"},
		},
	}
	ateClient = fake
	namespace = defaultNamespace
	t.Cleanup(func() { ateClient = nil })

	if got := listActorNames(context.Background()); !slices.Equal(got, []string{"luna"}) {
		t.Errorf("listActorNames() = %v, want [luna]", got)
	}
	if fake.lastAtespace != defaultNamespace {
		t.Errorf("ListActorsRequest.Atespace = %q, want %q", fake.lastAtespace, defaultNamespace)
	}

	rec := httptest.NewRecorder()
	handlePods(rec, httptest.NewRequest(http.MethodGet, "/api/pods", nil))
	var podResp struct {
		Pods []podSummary `json:"pods"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &podResp); err != nil {
		t.Fatalf("unmarshal pods: %v", err)
	}
	if len(podResp.Pods) != 1 || podResp.Pods[0].Name != "claude-workerpool-abc" {
		t.Errorf("handlePods = %+v, want [claude-workerpool-abc]", podResp.Pods)
	}
}

func TestFormatPodLogsFiltersNoise(t *testing.T) {
	raw := strings.Join([]string{
		`{"time":"2026-10-30T17:00:00Z","message":"Handle RPC","rpc.method":"/ateworker.Worker/GetActiveWorkloadStats"}`,
		`{"time":"2026-10-30T17:00:01Z","message":"I1030 17:00:01.000000 1 cli.go:282] **************** gVisor ****************","labels":{"ate.atespace":"claude-multiplex-demo","ate.actor.name":"luna","ate.actor.container.name":"claude"}}`,
		`{"time":"2026-10-30T17:00:01Z","message":"Args: [runsc create _sandbox]","labels":{"ate.atespace":"claude-multiplex-demo","ate.actor.name":"luna","ate.actor.container.name":"_sandbox"}}`,
		`{"time":"2026-10-30T17:00:01Z","message":"[demo-actor:golden] starting","labels":{"ate.atespace":"claude-multiplex-demo","ate.actor.name":"01234567-89ab-cdef-0123-456789abcdef","ate.actor.container.name":"claude"}}`,
		`{"time":"2026-10-30T17:00:02Z","message":"Actor restored","logging.googleapis.com/labels":{"ate.atespace":"claude-multiplex-demo","ate.actor.name":"luna"}}`,
		`{"time":"2026-10-30T17:00:03Z","message":"[demo-actor:luna] === tick 1 at 17:00:03Z ===","logging.googleapis.com/labels":{"ate.atespace":"claude-multiplex-demo","ate.actor.name":"luna","ate.actor.container.name":"claude"}}`,
		`waiting on container "claude": connecting to control server at PID 217: connection refused`,
		`{"ociVersion":"1.2.1"}`,
		`{"time":"2026-10-30T17:00:08Z","message":"Actor checkpointed","logging.googleapis.com/labels":{"ate.atespace":"claude-multiplex-demo","ate.actor.name":"luna"}}`,
	}, "\n")

	got := formatPodLogs(raw, defaultNamespace, 25)
	want := "[substrate:luna] Actor restored\n[demo-actor:luna] === tick 1 at 17:00:03Z ===\n[substrate:luna] Actor checkpointed"
	if got != want {
		t.Errorf("formatPodLogs() =\n%s\nwant:\n%s", got, want)
	}
}

func TestGiveTaskDrivesActorLifecycleAndMultiplexesPool(t *testing.T) {
	fake := &fakeControlClient{
		actors: []*ateapi.Actor{
			{Metadata: &ateapi.ResourceMetadata{Atespace: defaultNamespace, Name: "luna"}, Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}},
			{Metadata: &ateapi.ResourceMetadata{Atespace: defaultNamespace, Name: "mars"}, Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}},
			{Metadata: &ateapi.ResourceMetadata{Atespace: defaultNamespace, Name: "orion"}, Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}},
		},
		workers: []*ateapi.Worker{
			{WorkerPod: "pod-1", WorkerNamespace: defaultNamespace},
			{WorkerPod: "pod-2", WorkerNamespace: defaultNamespace},
		},
	}
	ateClient = fake
	namespace = defaultNamespace
	taskRunDuration = 20 * time.Millisecond
	mu.Lock()
	assignments = nil
	runningAgents = make(map[string]bool)
	mu.Unlock()
	t.Cleanup(func() {
		ateClient = nil
		taskRunDuration = 10 * time.Second
	})

	for range 3 {
		if asg, errMsg := giveTask(context.Background()); errMsg != "" || asg == nil {
			t.Fatalf("giveTask() = (%v, %q), want assignment", asg, errMsg)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(assignments) == 3 && assignments[0].State == stateCompleted && assignments[1].State == stateCompleted && assignments[2].State == stateCompleted
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.resumedActors) != 3 || len(fake.suspendedNames) != 3 {
		t.Errorf("resumed=%v suspended=%v, want 3 of each", fake.resumedActors, fake.suspendedNames)
	}
}
