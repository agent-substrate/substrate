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

package functionaltest

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSuspendCheckpointFailurePreservesLastGoodSnapshot(t *testing.T) {
	ns := namespaceForTest("ns-checkpoint-recovery")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	worker := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ctx := context.Background()
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "checkpoint-recovery"}
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	suspended, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	if err != nil {
		t.Fatal(err)
	}
	waitForWorkerAvailable(t, tc, worker)
	uri := durableSnapshotURI(suspended.GetActor().GetStatus())
	objects := snapshotObjectNames(t, tc, uri)
	if uri == "" || len(objects) == 0 {
		t.Fatalf("successful suspend produced no stored snapshot: %v", suspended.GetActor().GetStatus())
	}
	slices.Sort(objects)
	assertKept := func(actor *ateapipb.Actor, wantState ateapipb.ActorState) {
		t.Helper()
		if got := actor.GetStatus().GetState(); got != wantState {
			t.Fatalf("actor state = %v, want %v", got, wantState)
		}
		if got := durableSnapshotURI(actor.GetStatus()); got != uri {
			t.Fatalf("external snapshot = %q, want last good snapshot %q", got, uri)
		}
		got := snapshotObjectNames(t, tc, uri)
		slices.Sort(got)
		if !slices.Equal(got, objects) {
			t.Fatalf("last good snapshot objects = %v, want %v", got, objects)
		}
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	tc.fakeAtelet.Lock.Lock()
	tc.fakeAtelet.FailCheckpoint = fmt.Errorf("while calling ateom.CheckpointWorkload: %w",
		status.Error(codes.FailedPrecondition, `application container "app" exited before checkpoint (exit code 1)`))
	tc.fakeAtelet.Lock.Unlock()
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err == nil {
		t.Fatal("suspend accepted an exited application")
	}
	crashed, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatal(err)
	}
	assertKept(crashed, ateapipb.ActorState_ACTOR_STATE_CRASHED)
	if crashed.GetStatus().GetWorkerAssignment() != nil {
		t.Fatal("crashed actor still has a worker assignment")
	}
	tc.fakeAtelet.Reset()
	reverted, err := tc.client.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: ref})
	if err != nil {
		t.Fatal(err)
	}
	assertKept(reverted.GetActor(), ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	waitForWorkerAvailable(t, tc, worker)
	resumed, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("resume from the preserved snapshot: %v", err)
	}
	assertKept(resumed.GetActor(), ateapipb.ActorState_ACTOR_STATE_RUNNING)
}
