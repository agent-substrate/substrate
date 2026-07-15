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

package controlapi

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestFinalizePausedStep_WorkerGone reproduces the scenario where the worker pod
// disappears from the DB during pause finalization, so the node it ran on is
// unknown.
//
// Old behavior: NodeVmsWithLocalSnapshots = []string{""}, which made
// findFreeWorker search for a worker with node name "", never found, a
// permanent "no free workers available" on resume.
//
// Current behavior: NodeVmsWithLocalSnapshots is left nil, and the actor is
// crashed instead of left PAUSED, since a local snapshot with an unknown node
// can never be safely resumed.
func TestFinalizePausedStep_WorkerGone(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	const atespace, actorName = "team-a", "actor-1"

	actor := &ateapipb.Actor{
		Metadata:           &ateapipb.ResourceMetadata{Atespace: atespace, Name: actorName},
		Status:             ateapipb.Actor_STATUS_PAUSING,
		AteomPodNamespace:  "default",
		AteomPodName:       "worker-pod-1",
		WorkerPoolName:     "pool1",
		InProgressSnapshot: "snap-prefix",
	}
	if _, err := st.CreateActor(ctx, actor); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	// Intentionally NOT creating the worker in store, simulates worker already gone.

	step := &FinalizePausedStep{store: st}
	input := &PauseInput{Atespace: atespace, ActorName: actorName}
	state := &PauseState{}
	if err := step.Execute(ctx, input, state); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got, err := st.GetActor(ctx, atespace, actorName)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}

	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("status = %v, want CRASHED (node name unknown, cannot resume safely)", got.GetStatus())
	}
	for _, n := range got.GetLatestSnapshotInfo().GetLocal().GetNodeVmsWithLocalSnapshots() {
		if n == "" {
			t.Errorf("BUG: empty string in NodeVmsWithLocalSnapshots, findFreeWorker would never match")
		}
	}

	state.Actor = got
	done, err := step.IsComplete(ctx, input, state)
	if err != nil {
		t.Fatalf("IsComplete: %v", err)
	}
	if !done {
		t.Error("IsComplete = false, want true once the actor is CRASHED and the worker is freed")
	}
}

// TestFindFreeWorker_EmptyNodeRestriction shows the root symptom the fix
// avoids: old code wrote []string{""} into NodeVmsWithLocalSnapshots when the
// node name was unknown, and findFreeWorker required worker.NodeName == "",
// which never matches a real worker.
func TestFindFreeWorker_EmptyNodeRestriction(t *testing.T) {
	workers := []*ateapipb.Worker{
		{WorkerNamespace: "default", WorkerPool: "pool1", WorkerPod: "w1", NodeName: "node1"},
		{WorkerNamespace: "default", WorkerPool: "pool1", WorkerPod: "w2", NodeName: "node2"},
	}

	s := &AssignWorkerStep{}

	// Old behavior: []string{""}, no worker has NodeName == "", returns nil.
	got, err := s.findFreeWorker(workers, "", nil, nil, []string{""})
	if err != nil {
		t.Fatalf("findFreeWorker: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil with old buggy input, got %v", got)
	}

	// Fixed behavior: nil restrictions, any free worker matches.
	got, err = s.findFreeWorker(workers, "", nil, nil, nil)
	if err != nil {
		t.Fatalf("findFreeWorker: %v", err)
	}
	if got == nil {
		t.Error("expected a worker with nil restrictions, got nil")
	}
}
