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

	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// Hooks for the controlapi_test package, whose tests call WorkerService, which
// imports controlapi.

var (
	NewTestPersistence     = newTestPersistence
	SeedAssignment         = seedAssignment
	WireTestAssignment     = wireTestAssignment
	SeedSuspendTemplate    = seedSuspendTemplate
	SuspendTestTemplateRef = suspendTestTemplateRef
)

// TestStorageLocation is the storage location ReleaseWindowWorkflows'
// durable snapshots are under.
const TestStorageLocation = testStorageLocation

// ConflictingUpdateStore fails every actor update with a version conflict.
type ConflictingUpdateStore = conflictingUpdateStore

// ReleaseWindowWorkflow is a workflow step that releases an actor's worker
// before it records the actor's new state.
type ReleaseWindowWorkflow struct {
	Name string
	// State is the actor's state while the worker holds it.
	State ateapipb.ActorState
	// Snapshot, when set, is the actor's latest snapshot.
	Snapshot func(t *testing.T) *ateapipb.Snapshot
	// Finalize runs the workflow or step that releases the worker and then
	// records the actor's new state against st. Pause and suspend reach the
	// fake atelet newWireCaptureWorkflow serves for the worker
	// WireTestAssignment names, so the actor must be assigned that worker
	// and run from the template SeedSuspendTemplate stores.
	Finalize func(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, actor *ateapipb.Actor) error
	// WantRetry is the code of Finalize retried against the real store.
	WantRetry codes.Code
	// WantFinalState is the actor's state once the retry commits.
	WantFinalState ateapipb.ActorState
}

// ReleaseWindowWorkflows returns pause, suspend, crash, and a resume onto an
// ineligible worker: the workflows that release a worker before rewriting the
// actor's status while the actor is in a state HostingNode resolves through
// the worker assignment. Revert and delete release theirs in REVERTING and
// DELETING, which HostingNode never resolves to a node.
func ReleaseWindowWorkflows() []ReleaseWindowWorkflow {
	// The workers these tests create carry no labels, so a template
	// selecting a pool makes any of them ineligible.
	ineligibleTemplate := &ateapipb.ActorTemplate{
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"pool": "other"}},
	}
	return []ReleaseWindowWorkflow{
		{
			Name:  "pause",
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			Snapshot: func(t *testing.T) *ateapipb.Snapshot {
				// A pause in progress: a PENDING durable copy, not yet captured.
				return newInProgressSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "local-snap-1")
			},
			// The actor is seeded SUSPENDING, so the pause skips its mark step
			// and its first actor update is the finalize commit.
			Finalize: func(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, _ *ateapipb.Actor) error {
				w, _ := newWireCaptureWorkflow(t, st)
				_, err := w.SuspendActor(ctx, actorRef, suspendAsync)
				return err
			},
			WantRetry:      codes.OK,
			WantFinalState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
		{
			Name:  "suspend",
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			Snapshot: func(t *testing.T) *ateapipb.Snapshot {
				return newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snapshot-1", someActorSnapshotURI(t, testStorageLocation, "team-a", "snapshot-1"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
			},
			// As for the pause, the finalize commit is the first actor update.
			Finalize: func(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, _ *ateapipb.Actor) error {
				w, _ := newWireCaptureWorkflow(t, st)
				_, err := w.SuspendActor(ctx, actorRef, suspendSync)
				return err
			},
			WantRetry:      codes.OK,
			WantFinalState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
		{
			Name:  "crash",
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			Finalize: func(_ *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, _ *ateapipb.Actor) error {
				return crashActor(ctx, st, actorRef, ateattr.OperationResume, "test crash")
			},
			WantRetry:      codes.OK,
			WantFinalState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		},
		{
			// A resume that finds its worker ineligible releases it, then
			// crashes the actor. The retry finds the worker no longer hosts
			// the actor and crashes it.
			Name:  "resume onto an ineligible worker",
			State: ateapipb.ActorState_ACTOR_STATE_RESUMING,
			Finalize: func(_ *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, actor *ateapipb.Actor) error {
				_, err := (&ActorWorkflow{store: st, scheduler: scheduling.New(nil)}).validateAssignedWorker(ctx, actorRef, actor, ineligibleTemplate)
				return err
			},
			WantRetry:      codes.Aborted,
			WantFinalState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		},
	}
}
