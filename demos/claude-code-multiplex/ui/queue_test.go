// Copyright 2026 The Agent Substrate Authors
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
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
)

type taskTestClient struct {
	ateapi.ControlClient
}

func (taskTestClient) ListActors(context.Context, *ateapi.ListActorsRequest, ...grpc.CallOption) (*ateapi.ListActorsResponse, error) {
	return &ateapi.ListActorsResponse{Actors: []*ateapi.Actor{
		{
			Metadata: &ateapi.ResourceMetadata{Name: "agent-orion"},
			Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING},
		},
		{
			Metadata: &ateapi.ResourceMetadata{Name: "agent-luna"},
			Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
		},
	}}, nil
}

func (taskTestClient) ListWorkers(context.Context, *ateapi.ListWorkersRequest, ...grpc.CallOption) (*ateapi.ListWorkersResponse, error) {
	return &ateapi.ListWorkersResponse{Workers: []*ateapi.Worker{{WorkerNamespace: namespace}}}, nil
}

func TestGiveTaskAtCapacity(t *testing.T) {
	originalClient, originalAssignments := ateClient, assignments
	t.Cleanup(func() {
		ateClient, assignments = originalClient, originalAssignments
	})
	ateClient = taskTestClient{}
	assignments = make([]*assignment, 0, maxAssignments)

	submit := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handleGiveTask(w, httptest.NewRequest(http.MethodPost, "/api/give-task", nil))
		return w
	}
	for i := range maxAssignments {
		if w := submit(); w.Code != http.StatusOK {
			t.Fatalf("submission %d returned %d: %s", i+1, w.Code, w.Body)
		}
	}
	accepted := slices.Clone(assignments)
	oldestQueued := accepted[maxAssignments-1]
	oldestQueued.Agent = "agent-orion"
	oldestQueued.CreatedAt = 0
	running := accepted[maxAssignments-2]
	running.Agent = "agent-orion"
	started := nowSec()
	running.StartedAt = &started
	running.RunFor = math.MaxFloat64
	running.State = stateRunning
	nextQueued := accepted[maxAssignments-4]
	nextQueued.Agent = "agent-orion"
	nextQueued.CreatedAt = 0

	w := submit()
	var failure struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &failure); err != nil {
		t.Fatalf("decoding rejection: %v", err)
	}
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(failure.Error, "task queue is full") {
		t.Fatalf("full queue returned %d: %s", w.Code, w.Body)
	}
	if !slices.Equal(assignments, accepted) {
		t.Fatal("rejection changed the accepted assignments")
	}

	// Tasks on other actors can finish while older tasks are still active.
	finished := 0.0
	oldestCompleted := maxAssignments - 3
	for _, i := range []int{1, oldestCompleted} {
		accepted[i].Agent = "agent-luna"
		accepted[i].StartedAt = &finished
	}

	if w := submit(); w.Code != http.StatusOK {
		t.Fatalf("submission after completion returned %d: %s", w.Code, w.Body)
	}
	wantRetained := slices.Concat(accepted[:oldestCompleted], accepted[oldestCompleted+1:])
	if len(assignments) != maxAssignments || !slices.Equal(assignments[1:], wantRetained) {
		t.Fatal("submission should replace only the oldest completed assignment and preserve task order")
	}

	running.StartedAt = &finished
	running.RunFor = 0
	for _, task := range assignments {
		if task.StartedAt == nil && task != oldestQueued && task != nextQueued {
			task.QueueFor = math.MaxFloat64
		}
	}
	reconcileAgents(t.Context())
	if oldestQueued.StartedAt == nil || oldestQueued.State != stateRunning {
		t.Fatal("reconciliation did not start the oldest queued assignment")
	}
	if nextQueued.StartedAt != nil {
		t.Fatal("reconciliation also started the next assignment on the busy actor")
	}
}
