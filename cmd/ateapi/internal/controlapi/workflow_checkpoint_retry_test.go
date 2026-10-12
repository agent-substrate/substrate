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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ateomCheckpointGone is the error atelet returns for a checkpoint whose
// worker's ateom went away with its pod.
var ateomCheckpointGone = status.Error(codes.Unknown, `while calling ateom.CheckpointWorkload: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial unix /var/lib/ate/ateoms/w-1/ateom.sock: connect: no such file or directory"`)

// checkpointDiskFull is a checkpoint failing on a worker that keeps serving.
var checkpointDiskFull = status.Error(codes.Unknown, "while calling ateom.CheckpointWorkload: rpc error: code = Internal desc = writing memory file: no space left on device")

// checkpointCutCrash is the crash a suspend records when a worker drain
// removes the worker under its checkpoint.
var checkpointCutCrash = "suspend failed: " + ateletCrashMessage("Checkpoint", ateomCheckpointGone)

func durableSnapshot(uri string, storageStatus ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	return newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, "", uri, storageStatus)
}

func TestTransientCheckpointCrash(t *testing.T) {
	crashed := ateapipb.ActorState_ACTOR_STATE_CRASHED
	tests := []struct {
		name     string
		state    ateapipb.ActorState
		crash    string
		snapshot bool
		want     bool
	}{
		{"suspend cut by the drain", crashed, checkpointCutCrash, true, true},
		{"suspend whose worker pod went away", crashed, "suspend failed: " + crashMessageWorkerPodGone, true, true},
		{"suspend on a draining worker", crashed, "suspend failed: " + ateletCrashMessage("Checkpoint", checkpointDiskFull) + " (" + crashMessageWorkerDraining + ")", true, true},
		{"pause cut by the drain", crashed, "pause failed: " + ateletCrashMessage("Checkpoint", ateomCheckpointGone), true, true},
		{"no durable snapshot to return to", crashed, checkpointCutCrash, false, false},
		{"checkpoint failing on a live worker", crashed, "suspend failed: " + ateletCrashMessage("Checkpoint", checkpointDiskFull), true, false},
		// The pause snapshot is on its node, not with a worker.
		{"paused snapshot upload", crashed, "suspend failed: " + ateletCrashMessage("UploadPausedCheckpoint", ateomCheckpointGone), true, false},
		{"resume crash", crashed, "resume failed: " + crashMessageWorkerDraining, true, false},
		{"crash without an operation", crashed, crashMessageWorkerPodGone, true, false},
		{"recovered since", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, checkpointCutCrash, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actor := &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: tt.state, Crash: &ateapipb.ActorCrash{Message: tt.crash}}}
			if tt.snapshot {
				actor.Status.Snapshots = []*ateapipb.Snapshot{durableSnapshot("gs://snapshots/team-a/a/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)}
			}
			if got := transientCheckpointCrash(actor); got != tt.want {
				t.Errorf("transientCheckpointCrash(%v, %q, snapshot %v) = %v, want %v", tt.state, tt.crash, tt.snapshot, got, tt.want)
			}
		})
	}
}

// seedCheckpointCrash stores team-a/id1 CRASHED with crash, holding
// a completed durable snapshot (unless noSnapshot), the durable snapshot its
// checkpoint was writing and a local snapshot of an earlier pause. It returns
// the completed and the in-progress snapshot URIs.
func seedCheckpointCrash(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, crash string, noSnapshot bool) (string, string) {
	t.Helper()
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_CRASHED, func(a *ateapipb.Actor) {
		a.Status.Crash = &ateapipb.ActorCrash{Message: crash}
	})
	actor, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	tmpl, err := st.GetActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "ns", Name: "tmpl1"})
	if err != nil {
		t.Fatalf("GetActorTemplate: %v", err)
	}
	completed := mustActorSnapshotURI(t, tmpl, actor, "snap-1").String()
	inProgress := mustActorSnapshotURI(t, tmpl, actor, "snap-2").String()
	mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapipb.ActorStatus) {
		taking := durableSnapshot(inProgress, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS)
		taking.Generation = 3
		local := newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, "", "local-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		s.Snapshots = []*ateapipb.Snapshot{local, taking}
		if !noSnapshot {
			s.Snapshots = append(s.Snapshots, durableSnapshot(completed, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED))
		}
	})
	return completed, inProgress
}

// assertBackAtSnapshot checks that the actor is SUSPENDED and unplaced on its
// completed durable snapshot, its crash cleared, with no local snapshot and no
// snapshot in progress.
func assertBackAtSnapshot(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, completed string) {
	t.Helper()
	stored, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	s := stored.GetStatus()
	if s.GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || s.GetCrash() != nil || s.GetAssignedNode() != "" {
		t.Errorf("status = %v, want SUSPENDED, unplaced and no crash", s)
	}
	if _, durable := findLatestSnapshotStorage(s, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); durable.GetObject().GetSnapshotUri() != completed {
		t.Errorf("durable snapshot = %v, want the last completed one %q", durable, completed)
	}
	if _, local := findLatestSnapshotStorage(s, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); local != nil {
		t.Errorf("local snapshot = %v, want it dropped", local)
	}
	if _, taking := findLatestSnapshotStorage(s, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS); taking != nil {
		t.Errorf("in-progress snapshot = %v, want it discarded", taking)
	}
}

// TestRecoverTransientCrash_Checkpoint returns an actor whose suspend a
// worker drain cut to its last completed durable snapshot, as a revert does,
// and deletes the objects of the snapshot it was writing.
func TestRecoverTransientCrash_Checkpoint(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")
	objects := objectstoretest.New()
	w.snapshotPlugin = objectstoreplugintest.ControlClient(objects)
	storetest.MustCreateAtespace(t, ctx, st, "team-a")
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	completed, inProgress := seedCheckpointCrash(t, ctx, st, actorRef, checkpointCutCrash, false)
	if err := objects.WriteSnapshot(completed, "manifest.json", "memory.img"); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if err := objects.WriteSnapshot(inProgress, "memory.img"); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	crash, recovered, err := w.recoverTransientCrash(ctx, actorRef)
	if err != nil || !recovered || crash != checkpointCutCrash {
		t.Fatalf("recoverTransientCrash = (%q, %v, %v), want the crash recovered", crash, recovered, err)
	}
	assertBackAtSnapshot(t, ctx, st, actorRef, completed)
	for _, object := range objects.Objects() {
		if strings.Contains(object, "snap-2") {
			t.Errorf("object %q of the half-written snapshot kept, want it deleted", object)
		}
	}
	if got := len(objects.Objects()); got != 2 {
		t.Errorf("objects = %v, want the completed snapshot's two", objects.Objects())
	}
}

// TestSuspendActor_CheckpointCrashAnswersUnavailable suspends an actor a
// drain-cut checkpoint left CRASHED: the call answers Unavailable naming the
// crash and leaves the actor back at its last durable snapshot, so a retried
// suspend succeeds. A pause behaves the same.
func TestSuspendActor_CheckpointCrashAnswersUnavailable(t *testing.T) {
	for _, op := range []string{"suspend", "pause"} {
		t.Run(op, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			storetest.MustCreateAtespace(t, ctx, st, "team-a")
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			completed, _ := seedCheckpointCrash(t, ctx, st, actorRef, op+" failed: "+ateletCrashMessage("Checkpoint", ateomCheckpointGone), false)

			call := w.SuspendActor
			if op == "pause" {
				call = w.PauseActor
			}
			_, err := call(ctx, actorRef)
			if got := apierror.Code(err); got != codes.Unavailable {
				t.Fatalf("%s error = %v, want Unavailable", op, err)
			}
			if msg := err.Error(); !strings.Contains(msg, "ateom.sock") {
				t.Errorf("%s error message = %q, want the crash's cause named", op, msg)
			}
			assertBackAtSnapshot(t, ctx, st, actorRef, completed)

			if actor, err := w.SuspendActor(ctx, actorRef); err != nil || actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
				t.Errorf("retried SuspendActor = (%v, %v), want SUSPENDED", actor.GetStatus().GetState(), err)
			}
		})
	}
}

// TestSuspendActor_CheckpointCrashStaysCrashed keeps the crashes no snapshot
// undoes: a checkpoint that failed on a worker that keeps serving, and one cut
// by the drain on an actor with no durable snapshot to return to.
func TestSuspendActor_CheckpointCrashStaysCrashed(t *testing.T) {
	tests := []struct {
		name       string
		crash      string
		noSnapshot bool
	}{
		{name: "checkpoint failing on a live worker", crash: "suspend failed: " + ateletCrashMessage("Checkpoint", checkpointDiskFull)},
		{name: "no durable snapshot", crash: checkpointCutCrash, noSnapshot: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			storetest.MustCreateAtespace(t, ctx, st, "team-a")
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			seedCheckpointCrash(t, ctx, st, actorRef, tt.crash, tt.noSnapshot)

			_, err := w.SuspendActor(ctx, actorRef)
			if err == nil || apierror.Code(err) == codes.Unavailable {
				t.Fatalf("SuspendActor error = %v, want the refusal, not Unavailable", err)
			}
			stored, err := st.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED || stored.GetStatus().GetCrash().GetMessage() != tt.crash {
				t.Errorf("status = %v, want CRASHED with the crash kept", stored.GetStatus())
			}
		})
	}
}
