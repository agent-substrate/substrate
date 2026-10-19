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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/client-go/tools/cache"
)

// TestSuspendActor_CheckpointError exercises checkpoint error policy through
// the suspend workflow, including retrying the persisted snapshot.
func TestSuspendActor_CheckpointError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		cancel    bool
		wantCode  codes.Code
		wantState ateapipb.ActorState
	}{
		{"snapshot conflict", apierror.Aborted("checkpoint rejected"), false, codes.Aborted, ateapipb.ActorState_ACTOR_STATE_SUSPENDING},
		{"failed precondition", apierror.FailedPrecondition("checkpoint rejected"), false, codes.Internal, ateapipb.ActorState_ACTOR_STATE_CRASHED},
		{"caller cancellation", apierror.Aborted("checkpoint rejected"), true, codes.Canceled, ateapipb.ActorState_ACTOR_STATE_SUSPENDING},
	}
	for _, tt := range tests {
		for _, mode := range suspendModes {
			t.Run(tt.name+"/"+mode.name, func(t *testing.T) {
				ctx := context.Background()
				persistence := newTestPersistence(t)
				seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY})
				w, atelet := newWireCaptureWorkflow(t, persistence)
				actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
				actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
					Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
					ActorTemplate: suspendTestTemplateRef(),
					Status: &ateapipb.ActorStatus{
						State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
						AssignedNode:     wireTestAssignment().GetNodeName(),
						WorkerAssignment: wireTestAssignment(),
					},
				})
				seedWireWorker(t, ctx, persistence, assignmentOf(actor))
				callCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				atelet.onCheckpoint = func(context.Context) error {
					if tt.cancel {
						cancel()
					}
					return tt.err
				}
				_, err := w.SuspendActor(callCtx, actorRef, mode.suspendMode)
				if got := apierror.Code(err); got != tt.wantCode {
					t.Fatalf("SuspendActor error = %v (code %v), want %v", err, got, tt.wantCode)
				}
				if tt.cancel && !errors.Is(err, context.Canceled) {
					t.Errorf("SuspendActor error does not wrap context.Canceled: %v", err)
				}
				if checkpoints, _ := atelet.calls(); checkpoints != 1 {
					t.Fatalf("Checkpoint calls = %d, want 1", checkpoints)
				}
				stored := mustReadActor(t, ctx, persistence, actorRef)
				st := stored.GetStatus()
				if st.GetState() != tt.wantState {
					t.Fatalf("actor state = %v, want %v", st.GetState(), tt.wantState)
				}
				worker, err := persistence.GetWorker(ctx, wireTestAssignment().GetWorker().GetName())
				if err != nil {
					t.Fatalf("GetWorker: %v", err)
				}
				if tt.wantState == ateapipb.ActorState_ACTOR_STATE_CRASHED {
					if st.GetCrash() == nil || st.GetWorkerAssignment() != nil || worker.GetStatus().GetAllocated().GetActors() != 0 {
						t.Errorf("checkpoint failure did not crash the actor and release its worker: actor %v, worker %v", st, worker.GetStatus())
					}
					return
				}
				if st.GetCrash() != nil || !proto.Equal(st.GetWorkerAssignment(), actor.GetStatus().GetWorkerAssignment()) {
					t.Errorf("checkpoint error changed the worker assignment or crashed the actor: %v", st)
				}
				if got := worker.GetStatus().GetAllocated().GetActors(); got != 1 {
					t.Errorf("worker allocated actors = %d, want 1", got)
				}
				pending := snapshotAtLatestGeneration(st)
				if pending.GetUuid() == "" || pending.GetDurableSnapshot().GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING {
					t.Fatalf("snapshot = %v, want a persisted pending snapshot", pending)
				}
				first := atelet.checkpointRequest().GetSnapshot()
				if first.GetSnapshotUuid() != pending.GetUuid() || first.GetGeneration() != pending.GetGeneration() {
					t.Fatalf("Checkpoint snapshot = %v, want pending snapshot %v", first, pending)
				}
				atelet.mu.Lock()
				atelet.onCheckpoint = nil
				atelet.mu.Unlock()
				finished, err := w.SuspendActor(ctx, actorRef, mode.suspendMode)
				if err != nil {
					t.Fatalf("SuspendActor retry: %v", err)
				}
				if checkpoints, _ := atelet.calls(); checkpoints != 2 {
					t.Errorf("Checkpoint calls after retry = %d, want 2", checkpoints)
				}
				if diff := cmp.Diff(first, atelet.checkpointRequest().GetSnapshot(), protocmp.Transform()); diff != "" {
					t.Errorf("retry changed the snapshot (-first +retry):\n%s", diff)
				}
				if finished.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || finished.GetStatus().GetWorkerAssignment() != nil {
					t.Errorf("retry did not suspend the actor and clear its assignment: %v", finished.GetStatus())
				}
				latest := snapshotAtLatestGeneration(finished.GetStatus())
				if latest.GetUuid() != pending.GetUuid() || latest.GetGeneration() != pending.GetGeneration() {
					t.Errorf("retry finalized a different snapshot: %v, want %v", latest, pending)
				}
			})
		}
	}
}

func TestSuspendActor_MarkedSuspending_SnapshotURI(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: "gs://bucket/root/"})
	wantRuntime := &ateapipb.SandboxRuntime{
		SandboxClass: "gvisor",
		Version: &ateapipb.VersionedSandboxCompat{
			SchemaVersion: "v1",
			Attributes: []*ateapipb.AttributeEntry{
				{Key: "architecture", Value: "amd64"},
			},
		},
	}
	assignment := wireTestAssignment()
	assignment.SandboxRuntime = wantRuntime
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: assignment,
		},
	})
	w := newMarkOnlyWorkflow(persistence)
	_, err := w.SuspendActor(ctx, actorRef, suspendSync)
	assertStoppedAtDial(t, err)
	marked := mustReadActor(t, ctx, persistence, actorRef)

	inProgressSnap := findLatestDurableSnapshot(marked.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	uri := mustParseSnapshotURI(t, inProgressSnap.GetDurableSnapshot().GetObject().GetSnapshotUri())
	if !resources.IsValidResourceName(uri.Name()) {
		t.Errorf("in-progress snapshot name = %q, want a valid resource name", uri.Name())
	}
	if got := inProgressSnap.GetUuid(); got != uri.Name() {
		t.Errorf("Snapshot.Uuid = %q, want the snapshot name in its URI, %q", got, uri.Name())
	}
	want := "gs://bucket/root/atespaces/team-a/actors/" + marked.GetMetadata().GetUid() + "/snapshots/" + uri.Name()
	if uri.String() != want {
		t.Errorf("snapshot URI = %q, want %q", uri, want)
	}
	if diff := cmp.Diff(wantRuntime, inProgressSnap.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("Snapshot.SandboxRuntime mismatch (-want +got):\n%s", diff)
	}
}

// TestSuspendActor_MarkedSuspending_ReentryKeepsPersistedSnapshotLocation verifies a
// re-entered workflow, in either mode, does not mint a second snapshot
// location: the location persisted by the first attempt stays authoritative,
// both on the wire and in the snapshot the suspend commits. Both modes mark
// the same shape, so the retry's mode may differ from the first attempt's.
func TestSuspendActor_MarkedSuspending_ReentryKeepsPersistedSnapshotLocation(t *testing.T) {
	const firstAttempt = "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/first-attempt"
	pending := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
	completed := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED

	tests := []struct {
		name           string
		mode           suspendMode
		wantUploadMode ateletpb.UploadMode
		wantLocality   string
		wantDurable    ateapipb.SnapshotStorageStatus
		wantNode       string
	}{
		{"sync", suspendSync, ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE, "", completed, ""},
		{"async", suspendAsync, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED, "node-1", pending, "node-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
			w, atelet := newWireCaptureWorkflow(t, persistence)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: suspendTestTemplateRef(),
				Status: &ateapipb.ActorStatus{
					State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
					AssignedNode:           "node-1",
					WorkerAssignment:       wireTestAssignment(),
					LastAssignedGeneration: 1,
					Snapshots: []*ateapipb.Snapshot{
						newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "first-attempt", firstAttempt, pending),
					},
				},
			})
			suspended, err := w.SuspendActor(ctx, actorRef, tt.mode)
			if err != nil {
				t.Fatalf("SuspendActor: %v", err)
			}
			req := atelet.checkpointRequest()
			if got := req.GetSnapshot().GetObject().GetSnapshotUri(); got != firstAttempt {
				t.Errorf("CheckpointRequest.Snapshot.Object.SnapshotUri = %q, want the first attempt's location", got)
			}
			if got := req.GetUploadConfig().GetMode(); got != tt.wantUploadMode {
				t.Errorf("CheckpointRequest.UploadConfig.Mode = %v, want %v", got, tt.wantUploadMode)
			}
			if got := suspended.GetStatus().GetLastAssignedGeneration(); got != 1 {
				t.Errorf("last_assigned_generation = %d, want 1 (no new snapshot minted on re-entry)", got)
			}
			gotSnap := snapshotAtLatestGeneration(suspended.GetStatus())
			if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != firstAttempt {
				t.Errorf("committed durable snapshot URI = %q, want the first attempt's location", got)
			}
			if got := gotSnap.GetDurableSnapshot().GetStatus(); got != tt.wantDurable {
				t.Errorf("durable copy status = %v, want %v", got, tt.wantDurable)
			}
			if got := gotSnap.GetLocality(); got != tt.wantLocality {
				t.Errorf("Snapshot.Locality = %q, want %q", got, tt.wantLocality)
			}
			if got := suspended.GetStatus().GetAssignedNode(); got != tt.wantNode {
				t.Errorf("AssignedNode = %q, want %q", got, tt.wantNode)
			}
		})
	}
}

// TestSuspendActorWorkflow_SuspendedReturnsAsIs verifies that SuspendActor, in
// either mode, returns a SUSPENDED actor as it is, wherever its snapshot is.
// It never promotes a node-local snapshot: the stored record, durable copies,
// locality, and assigned node included, is left exactly as seeded.
func TestSuspendActorWorkflow_SuspendedReturnsAsIs(t *testing.T) {
	durableURI := someActorSnapshotURI(t, "gs://snapshots", "team-a", "durable-snap")
	withDurable := func(a *ateapipb.Actor) {
		a.Status.LastAssignedGeneration = 1
		a.Status.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "durable-snap", durableURI, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
	}

	tests := []struct {
		name     string
		seedOpts []func(*ateapipb.Actor)
	}{
		{name: "newly created, no snapshot"},
		{name: "durable snapshot", seedOpts: []func(*ateapipb.Actor){withDurable}},
		{name: "paused, upload pending", seedOpts: []func(*ateapipb.Actor){withPausedShape(t, "node-1")}},
		{name: "paused, uploaded", seedOpts: []func(*ateapipb.Actor){withUploadedPausedShape(t, "node-1")}},
	}
	for _, tc := range tests {
		for _, mode := range suspendModes {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				ctx := context.Background()
				st, cleanup := storetest.SetupTestStore(t)
				defer cleanup()
				w := newTestActorWorkflow(t, st, "ns", "tmpl1")
				actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

				seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, tc.seedOpts...)
				seeded := mustReadActor(t, ctx, st, actorRef)

				returned, err := w.SuspendActor(ctx, actorRef, mode.suspendMode)
				if err != nil {
					t.Fatalf("SuspendActor: %v", err)
				}
				if diff := cmp.Diff(seeded, returned, protocmp.Transform()); diff != "" {
					t.Errorf("returned actor mismatch (-seeded +returned):\n%s", diff)
				}
				if diff := cmp.Diff(seeded, mustReadActor(t, ctx, st, actorRef), protocmp.Transform()); diff != "" {
					t.Errorf("stored actor changed (-seeded +stored):\n%s", diff)
				}
			})
		}
	}
}

// TestSuspendActor_MarkedSuspending_StateMatrix verifies the suspend edge's
// state gating against every actor state, in either mode: RUNNING takes the
// edge and marks a snapshot with a PENDING durable copy, SUSPENDING skips the
// mark (a previous attempt already marked the actor) and goes on to atelet,
// SUSPENDED is returned as it is (see TestSuspendActorWorkflow_SuspendedReturnsAsIs),
// and everything else is rejected with FailedPrecondition and left untouched.
func TestSuspendActor_MarkedSuspending_StateMatrix(t *testing.T) {
	for _, mode := range suspendModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: "gs://snapshots"})
			w := newMarkOnlyWorkflow(persistence)

			for _, seedState := range allActorStates {
				actorRef := resources.ActorRef{Atespace: "team-a", Name: fmt.Sprintf("id%d", seedState)}
				seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", seedState, func(a *ateapipb.Actor) {
					a.Status.WorkerAssignment = wireTestAssignment()
				})

				_, err := w.SuspendActor(ctx, actorRef, mode.suspendMode)
				stored := mustReadActor(t, ctx, persistence, actorRef)
				switch seedState {
				case ateapipb.ActorState_ACTOR_STATE_RUNNING, ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
					assertStoppedAtDial(t, err)
					if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
						t.Errorf("state %v: stored state = %v, want SUSPENDING", seedState, got)
					}
					if seedState == ateapipb.ActorState_ACTOR_STATE_RUNNING {
						latest := snapshotAtLatestGeneration(stored.GetStatus())
						if latest.GetDurableSnapshot().GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING || latest.GetLocality() != "" {
							t.Errorf("state %v: latest snapshot = %v, want a PENDING durable copy and no locality", seedState, latest)
						}
					}
				case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
					if err != nil {
						t.Errorf("state %v: SuspendActor = %v, want it returned as it is", seedState, err)
					}
					if got := stored.GetStatus().GetState(); got != seedState {
						t.Errorf("state %v: stored state = %v, want it untouched", seedState, got)
					}
				default:
					if got := apierror.Code(err); got != codes.FailedPrecondition {
						t.Errorf("state %v: code = %v, want FailedPrecondition (err: %v)", seedState, got, err)
					}
					if got := stored.GetStatus().GetState(); got != seedState {
						t.Errorf("state %v: stored state = %v, want it untouched", seedState, got)
					}
				}
			}
		})
	}
}

// TestSuspendActor_CrashesWhenSuspendingActorMissingWorkerPod verifies that a
// SUSPENDING actor with no worker pod recorded is moved to CRASHED by the
// corrupted-assignment check before atelet is called, and the suspend fails
// with FailedPrecondition.
func TestSuspendActor_CrashesWhenSuspendingActorMissingWorkerPod(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDING)

	_, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, suspendSync)
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
	}

	got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("stored state = %v, want %v", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_CRASHED)
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "suspend failed: "+crashMessageWorkerAssignmentMissing; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
	}
}

// newTestPersistence returns an isolated PostgreSQL-backed store.
func newTestPersistence(t *testing.T) store.Interface {
	persistence, _ := storetest.SetupTestStore(t)
	storetest.MustCreateAtespace(t, context.Background(), persistence, "team-a")
	return persistence
}

// newDanglingDialer returns a dialer whose informer cache has no pods, so
// DialForAteletOnNode always returns ErrNoAteletOnNode.
func newDanglingDialer() *AteletDialer {
	empty := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNode: func(obj any) ([]string, error) { return nil, nil },
	})
	return NewAteletDialer(empty, installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "")
}

func TestSuspendActor_AteletSuspended_DialFailureLeavesActorRetryable(t *testing.T) {
	neverWritten := someActorSnapshotURI(t, testStorageLocation, "team-a", "never-written")

	tests := []struct {
		name         string
		prevSnapshot string
	}{
		{
			name:         "keeps previous external snapshot",
			prevSnapshot: someActorSnapshotURI(t, testStorageLocation, "team-a", "prev"),
		},
		{
			name:         "stays empty without previous external snapshot",
			prevSnapshot: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
			var snapshots []*ateapipb.Snapshot
			if tt.prevSnapshot != "" {
				snapshots = append(snapshots, newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "prev-snapshot", tt.prevSnapshot, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED))
			}
			snapshots = append(snapshots, newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "never-written", neverWritten, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING))

			actor := &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				ActorTemplate: suspendTestTemplateRef(),
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerNamespace: "worker-ns",
						WorkerPool:      "pool",
						WorkerPod:       "pod-gone",
						NodeName:        "node-gone",
					},
					LastAssignedGeneration: 2,
					Snapshots:              snapshots,
				},
			}
			storetest.MustCreateActor(t, ctx, persistence, actor)

			w := newMarkOnlyWorkflow(persistence)
			_, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, suspendSync)
			assertStoppedAtDial(t, err)

			// A dial failure is transient from this workflow's point of view: it
			// must not crash the actor or touch its snapshot state. A worker that
			// is genuinely gone is handled by the DeleteWorker workflow instead.
			stored, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
				t.Errorf("state = %v, want unchanged SUSPENDING", stored.GetStatus().GetState())
			}
			if gotSnap := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != neverWritten {
				t.Errorf("in-progress durable snapshot URI = %q, want preserved for debugging", gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri())
			}
			gotSnap := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != tt.prevSnapshot {
				t.Errorf("SnapshotUri = %q, want %q", got, tt.prevSnapshot)
			}
		})
	}
}

// TestSuspendActor_AteletSuspended_WireRequest verifies a running-origin suspend checkpoints
// to the actor's in-progress durable snapshot with an immediate upload, since
// the suspend's finalize then marks that snapshot COMPLETED.
func TestSuspendActor_AteletSuspended_WireRequest(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY})
	w, atelet := newWireCaptureWorkflow(t, persistence)
	inst, reader := newTestInstruments(t)
	w.instruments = inst

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	snapshotName := "2026-01-01t00-00-00z-abc"
	snapshotURI := someActorSnapshotURI(t, testStorageLocation, "team-a", snapshotName)
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:           "node-1",
			WorkerAssignment:       wireTestAssignment(),
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-uid-1", snapshotName, snapshotURI, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		},
	})

	if _, err := w.SuspendActor(ctx, actorRef, suspendSync); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	req := atelet.checkpointRequest()
	if req == nil {
		t.Fatal("expected Checkpoint RPC to be called")
	}
	if got := req.GetSnapshot().GetSnapshotUuid(); got != snapshotName {
		t.Errorf("CheckpointRequest.Snapshot.SnapshotUuid = %q, want %q", got, snapshotName)
	}
	if got := req.GetSnapshot().GetGeneration(); got != 1 {
		t.Errorf("CheckpointRequest.Snapshot.Generation = %d, want 1, the in-progress snapshot's", got)
	}
	if got := req.GetSnapshot().GetObject().GetSnapshotUri(); got != snapshotURI {
		t.Errorf("CheckpointRequest.Snapshot.Object.SnapshotUri = %q, want %q", got, snapshotURI)
	}
	wantConfig := &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE}
	if got := req.GetUploadConfig(); !proto.Equal(got, wantConfig) {
		t.Errorf("CheckpointRequest.UploadConfig = %v, want %v", got, wantConfig)
	}
	// The fidelity sent to atelet labels the suspend's lifecycle-op metric.
	assertLifecycleFidelity(t, reader, ateattr.SnapshotFidelityMemory)
}

// TestSuspendActor_Finalized_ReleasesReplacedSnapshot verifies which external
// snapshot a suspend collects. The actor's previous one goes away, because
// nothing else can name it — unless the actor was only borrowing it from a tag,
// in which case the tag owns those objects and the suspend must leave them be.
func TestSuspendActor_Finalized_ReleasesReplacedSnapshot(t *testing.T) {
	// The name of the snapshot the suspend has just written, in place of the
	// one the actor was running from.
	const snapshotName = "2026-01-01t00-00-00z-new"

	tests := []struct {
		name string
		// tagOwnedSnapshot makes the snapshot the actor is running from a tag's rather
		// than one it took itself, which is how an actor created from a tag
		// starts out.
		tagOwnedSnapshot bool
		wantReleased     bool
	}{
		{
			name:         "releases the external snapshot the actor owned",
			wantReleased: true,
		},
		{
			name:             "leaves an external snapshot borrowed from a tag in place",
			tagOwnedSnapshot: true,
			wantReleased:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			w, _, objects := newWireFinalizeWorkflow(t, persistence)

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status: &ateapipb.ActorStatus{
					State:            ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
					AssignedNode:     "node-1",
					WorkerAssignment: wireTestAssignment(),
				},
			})

			previous := mustActorSnapshotURI(t, template, actor, "old")
			prevType := ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR
			if tt.tagOwnedSnapshot {
				previous = mustTagSnapshotURI(t, template, "team-a", "v1-snapshot")
				prevType = ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG
			}
			fresh := mustActorSnapshotURI(t, template, actor, snapshotName)
			objects.PutSnapshot(t, previous, "manifest.json")
			objects.PutSnapshot(t, fresh, "manifest.json")

			mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.LastAssignedGeneration = 2
				s.Snapshots = []*ateapipb.Snapshot{
					newDurableSnapshot(1, prevType, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", previous.Name(), previous.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", fresh.Name(), fresh.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
				}
			})

			stored, err := w.SuspendActor(ctx, actorRef, suspendSync)
			if err != nil {
				t.Fatalf("SuspendActor: %v", err)
			}
			// Whichever way the previous snapshot went, the actor now owns the
			// one it just wrote and is no longer borrowing.
			gotSnap := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != fresh.String() {
				t.Errorf("external snapshot = %q, want the one this suspend wrote, %q", got, fresh)
			}
			if released := len(objects.Snapshot(t, previous)) == 0; released != tt.wantReleased {
				t.Errorf("previous external snapshot released = %v, want %v", released, tt.wantReleased)
			}
			if len(objects.Snapshot(t, fresh)) == 0 {
				t.Error("the external snapshot this suspend wrote was collected")
			}
		})
	}
}

// TestSuspendActor_Finalized_PauseReleasesReplacedSnapshot verifies that a
// pause keeping the previous pause's upload collects the external snapshot
// that upload replaced, and keeps the upload. The upload is recorded from the
// pause's Checkpoint state, or before the pause from atelet's report.
func TestSuspendActor_Finalized_PauseReleasesReplacedSnapshot(t *testing.T) {
	const node = "node-1"
	memory := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	completed := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	pending := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
	for _, tc := range []struct {
		name string
		// reported records the upload before the pause, as atelet's report
		// does. Otherwise only the pause's Checkpoint state carries it.
		reported bool
	}{
		{name: "recorded from the pause's Checkpoint state"},
		{name: "recorded from atelet's report before the pause", reported: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			w, atelet, objects := newWireFinalizeWorkflow(t, persistence)

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status: &ateapipb.ActorStatus{
					State:            ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
					AssignedNode:     node,
					WorkerAssignment: wireTestAssignment(),
				},
			})

			// The actor suspended to old, paused to snap-2 and resumed from
			// it, and is now pausing to snap-3. atelet has uploaded snap-2.
			old := mustActorSnapshotURI(t, template, actor, "old")
			paused := mustActorSnapshotURI(t, template, actor, "snap-2")
			pausing := mustActorSnapshotURI(t, template, actor, "snap-3")
			objects.PutSnapshot(t, old, "manifest.json")
			objects.PutSnapshot(t, paused, "manifest.json")
			mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				previousStatus := pending
				if tc.reported {
					previousStatus = completed
				}
				previous := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, memory, "", paused.Name(), paused.String(), previousStatus)
				previous.Locality = node
				s.LastAssignedGeneration = 3
				s.Snapshots = []*ateapipb.Snapshot{
					newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, memory, "", old.Name(), old.String(), completed),
					previous,
					newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, memory, "", pausing.Name(), pausing.String(), pending),
				}
			})
			atelet.checkpointState = &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: &ateletpb.Snapshot{
				SnapshotUuid: "snap-2",
				Generation:   2,
				Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				Object:       &ateletpb.ObjectStorage{SnapshotUri: paused.String()},
			}}}

			stored, err := w.SuspendActor(ctx, actorRef, suspendAsync)
			if err != nil {
				t.Fatalf("SuspendActor: %v", err)
			}
			want := []snapshotOutcome{{generation: 2, fidelity: memory, durable: completed}, {generation: 3, fidelity: memory, locality: node, durable: pending}}
			if diff := cmp.Diff(want, describeSnapshots(stored.GetStatus()), cmp.AllowUnexported(snapshotOutcome{})); diff != "" {
				t.Errorf("snapshots mismatch (-want +got):\n%s", diff)
			}
			if got := findLatestDurableSnapshot(stored.GetStatus(), completed).GetDurableSnapshot().GetObject().GetSnapshotUri(); got != paused.String() {
				t.Errorf("external snapshot = %q, want the previous pause's upload, %q", got, paused)
			}
			if len(objects.Snapshot(t, old)) != 0 {
				t.Error("the external snapshot the recorded upload replaced was not released")
			}
			if len(objects.Snapshot(t, paused)) == 0 {
				t.Error("the recorded upload was released")
			}
		})
	}
}

// TestSnapshotLocationChange_LeavesNothingBehind verifies that moving an
// actor's snapshot location, by changing --snapshot-location, toggling
// the PerTemplateSnapshotLocation preview gate, or repointing the actor, strands
// nothing: the next suspend releases the replaced snapshot under its own
// location, and deleting the actor then collects the new one.
func TestSnapshotLocationChange_LeavesNothingBehind(t *testing.T) {
	const (
		oldLocation = "gs://old-bucket/root"
		newLocation = "gs://new-bucket/root"
	)
	ctx := context.Background()
	persistence := newTestPersistence(t)
	w, objects := newFinalizeWorkflow(persistence)

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING},
	})
	previous, err := resources.NewActorSnapshotURI(oldLocation, actorRef.Atespace, actor.GetMetadata().GetUid(), "old")
	if err != nil {
		t.Fatalf("NewActorSnapshotURI(old): %v", err)
	}
	fresh, err := resources.NewActorSnapshotURI(newLocation, actorRef.Atespace, actor.GetMetadata().GetUid(), "2026-01-01t00-00-00z-new")
	if err != nil {
		t.Fatalf("NewActorSnapshotURI(new): %v", err)
	}
	objects.PutSnapshot(t, previous, "manifest.json")
	objects.PutSnapshot(t, fresh, "manifest.json")
	mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.LastAssignedGeneration = 2
		s.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", previous.Name(), previous.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", fresh.Name(), fresh.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		}
	})

	stored, err := w.ensureSuspendedFinalized(ctx, actorRef, suspendSync, nil)
	if err != nil {
		t.Fatalf("ensureSuspendedFinalized: %v", err)
	}
	if got := objects.Snapshot(t, previous); len(got) != 0 {
		t.Errorf("snapshot under the old location after the suspend = %v, want released", got)
	}
	if len(objects.Snapshot(t, fresh)) == 0 {
		t.Fatal("the snapshot this suspend wrote under the new location was collected")
	}

	if err := w.ensureExternalSnapshotsReleased(ctx, stored); err != nil {
		t.Fatalf("ensureExternalSnapshotsReleased: %v", err)
	}
	if got := objects.Snapshot(t, fresh); len(got) != 0 {
		t.Errorf("snapshot under the new location after the delete = %v, want released", got)
	}
}

// errObjectStore stands in for object storage being unreachable.
var errObjectStore = errors.New("object storage is unavailable")

// isObjectStoreErr reports whether err came from errObjectStore. The object
// store sits behind the object-store plugin's gRPC boundary, which carries the
// message but not the error value, so errors.Is cannot match it.
func isObjectStoreErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), errObjectStore.Error())
}

// TestSuspendActor_Finalized_CommitsDespiteObjectStoreFailure verifies that
// failing to collect the snapshot a suspend replaced does not fail the
// suspend. The worker is already released by then, so aborting would leave the
// actor SUSPENDING with no way to resume; it commits SUSPENDED on the new
// snapshot instead and leaves the replaced one in storage.
func TestSuspendActor_Finalized_CommitsDespiteObjectStoreFailure(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, _, objects := newWireFinalizeWorkflow(t, persistence)

	const snapshotName = "2026-01-01t00-00-00z-new"
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:     "node-1",
			WorkerAssignment: wireTestAssignment(),
		},
	})

	previous := mustActorSnapshotURI(t, template, actor, "old")
	fresh := mustActorSnapshotURI(t, template, actor, snapshotName)
	objects.PutSnapshot(t, previous, "manifest.json")
	objects.PutSnapshot(t, fresh, "manifest.json")

	mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.LastAssignedGeneration = 2
		s.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", previous.Name(), previous.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", fresh.Name(), fresh.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		}
	})

	objects.OnDelete = func(string, string) error { return errObjectStore }
	stored, err := w.SuspendActor(ctx, actorRef, suspendSync)
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	gotSnap := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != fresh.String() {
		t.Errorf("actor snapshot uri = %q, want the one this suspend wrote, %q", got, fresh)
	}
	if got := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); got != nil {
		t.Errorf("in-progress durable snapshot = %v, want cleared", got)
	}
	if len(objects.Snapshot(t, previous)) == 0 {
		t.Error("replaced external snapshot was collected despite every delete failing")
	}
	if len(objects.Snapshot(t, fresh)) == 0 {
		t.Error("the external snapshot this suspend wrote was collected")
	}
}

// TestSuspendActor_Finalized_KeepsReplacedSnapshotOnConflict verifies that a
// commit lost to a concurrent update releases nothing: the actor record still
// names its old snapshot, so collecting it would leave the actor pointing at
// deleted objects.
func TestSuspendActor_Finalized_KeepsReplacedSnapshotOnConflict(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	// The actor is seeded SUSPENDING, so the mark step skips and the first
	// actor update the suspend makes is its finalize commit.
	w, atelet, objects := newWireFinalizeWorkflow(t, &conflictingUpdateStore{Interface: persistence})

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:     "node-1",
			WorkerAssignment: wireTestAssignment(),
		},
	})

	previous := mustActorSnapshotURI(t, template, actor, "old")
	fresh := mustActorSnapshotURI(t, template, actor, "2026-01-01t00-00-00z-new")
	objects.PutSnapshot(t, previous, "manifest.json")
	objects.PutSnapshot(t, fresh, "manifest.json")
	mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.LastAssignedGeneration = 2
		s.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", previous.Name(), previous.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", fresh.Name(), fresh.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		}
	})

	if _, err := w.SuspendActor(ctx, actorRef, suspendSync); apierror.Code(err) != codes.Aborted {
		t.Fatalf("SuspendActor = %v, want code Aborted", err)
	}
	// Every actor update conflicts, so pin that the Aborted came from finalize
	// and not from an earlier step that never reached atelet.
	if checkpoints, _ := atelet.calls(); checkpoints != 1 {
		t.Errorf("Checkpoint calls = %d, want 1 before finalize", checkpoints)
	}
	if len(objects.Snapshot(t, previous)) == 0 {
		t.Error("replaced external snapshot was collected though the commit lost")
	}
}

// conflictingUpdateStore fails every actor update with a version conflict, as
// if another writer always got there first.
type conflictingUpdateStore struct {
	store.Interface
}

func (conflictingUpdateStore) UpdateActor(context.Context, resources.ActorRef, store.Precondition, func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	return nil, store.ErrVersionConflict
}

// TestSuspendActor_Finalized_KeepsCrashFromWorkerDelete verifies that a crash the
// DeleteWorker workflow commits while the suspend's finalize is freeing the
// worker is kept: the finalize update is pinned to the actor version it
// checked, so it fails Aborted instead of committing SUSPENDED over the crash.
func TestSuspendActor_Finalized_KeepsCrashFromWorkerDelete(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

	created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:           "node-1",
			WorkerAssignment:       wireTestAssignment(),
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snapshot-1", someActorSnapshotURI(t, testStorageLocation, "team-a", "snapshot-1"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		},
	})
	seedWireWorker(t, ctx, persistence, assignmentOf(created))

	w, _ := newWireCaptureWorkflow(t, workerDeletedDuringReleaseStore{Interface: persistence})
	_, err := w.SuspendActor(ctx, actorRef, suspendSync)
	assertConcurrentCrashKept(t, ctx, persistence, actorRef, err, "suspend failed: "+crashMessageWorkerPodGone)
}

func TestSuspendActor_Finalized_ReleasesOnlyOwnWorker(t *testing.T) {
	tests := []struct {
		name               string
		assignmentAtespace string
		mismatchedUID      bool
		wantReleased       bool
	}{
		{
			name:               "frees worker assigned to this actor",
			assignmentAtespace: "team-a",
			wantReleased:       true,
		},
		{
			name:               "keeps worker assigned to same-named actor in another atespace",
			assignmentAtespace: "team-b",
			wantReleased:       false,
		},
		{
			name:               "keeps worker assigned to previous incarnation of same actor",
			assignmentAtespace: "team-a",
			mismatchedUID:      true,
			wantReleased:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
			w, _ := newWireCaptureWorkflow(t, persistence)
			actor := &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
				ActorTemplate: suspendTestTemplateRef(),
				Status: &ateapipb.ActorStatus{
					State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
					AssignedNode:           "node-1",
					WorkerAssignment:       wireTestAssignment(),
					LastAssignedGeneration: 1,
					Snapshots: []*ateapipb.Snapshot{
						newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snapshot-1", someActorSnapshotURI(t, testStorageLocation, "team-a", "snapshot-1"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
					},
				},
			}
			created := storetest.MustCreateActor(t, ctx, persistence, actor)

			uid := created.GetMetadata().GetUid()
			if tt.assignmentAtespace != "team-a" || tt.mismatchedUID {
				uid = "other-actor-uid-b"
			}
			seedWireWorker(t, ctx, persistence, &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: tt.assignmentAtespace, Name: "shared"},
				ActorUid: uid,
			})

			if _, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}, suspendSync); err != nil {
				t.Fatalf("SuspendActor: %v", err)
			}

			stored := firstAssignment(t, persistence, wireTestAssignment().GetWorker().GetName())
			if released := stored == nil; released != tt.wantReleased {
				t.Errorf("worker released = %t, want %t (assignment: %v)", released, tt.wantReleased, stored)
			}
		})
	}
}

// TestSuspendActor_Finalized_MissingDurableSnapshotClearsAssignment verifies that if the
// latest generation snapshot entry is missing when the suspend finalizes, the
// actor transitions to CRASHED and clears WorkerAssignment after releasing the
// worker rather than getting stuck in SUSPENDING, and the suspend fails.
func TestSuspendActor_Finalized_MissingDurableSnapshotClearsAssignment(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, st, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
	w, atelet := newWireCaptureWorkflow(t, st)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	records := crashRecords(t)

	created := storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:           "node-1",
			WorkerAssignment:       wireTestAssignment(),
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snapshot-1", someActorSnapshotURI(t, testStorageLocation, "team-a", "snapshot-1"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		},
	})
	seedWireWorker(t, ctx, st, assignmentOf(created))
	updateDuringCheckpoint(t, atelet, st, actorRef, func(s *ateapipb.ActorStatus) {
		s.Snapshots = nil
	})

	if _, err := w.SuspendActor(ctx, actorRef, suspendSync); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SuspendActor error = %v, want FailedPrecondition for the actor it crashed", err)
	}
	got, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("state = %v, want CRASHED", got.GetStatus().GetState())
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "suspend failed: "+crashMessageDurableSnapshotMissing; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("WorkerAssignment = %v, want nil", got.GetStatus().GetWorkerAssignment())
	}
	if len(*records) != 1 {
		t.Fatalf("got %d crash records, want 1", len(*records))
	}
}

// suspendModes runs a table test against both suspend workflows: sync, whose
// snapshot is durable before it returns, and async, whose snapshot stays
// node-local.
var suspendModes = []struct {
	name        string
	suspendMode suspendMode
}{
	{"sync", suspendSync},
	{"async", suspendAsync},
}

func TestSuspendModeFromOptions(t *testing.T) {
	tests := []struct {
		name string
		opts *ateapipb.SuspendOptions
		want suspendMode
	}{
		{"no options", nil, suspendAsync},
		{"mode unspecified", &ateapipb.SuspendOptions{}, suspendAsync},
		{"synchronous", &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS}, suspendSync},
		{"asynchronous", &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS}, suspendAsync},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := suspendModeFromOptions(tt.opts); got != tt.want {
				t.Errorf("suspendModeFromOptions(%v) = %v, want %v", tt.opts, got, tt.want)
			}
		})
	}
}

// suspendOutcome is what one suspend workflow does to a seeded actor. Rows
// omit zero-valued fields, which the matrix still pins.
type suspendOutcome struct {
	code         codes.Code
	atelet       ateletCall
	state        ateapipb.ActorState
	assignedNode string
	// workerHeld is whether the worker wireTestAssignment names still holds
	// the actor's assignment.
	workerHeld bool
	generation int32
	snapshots  []snapshotOutcome
}

// ateletCall is the suspend RPC atelet received; the zero value is none.
type ateletCall struct {
	checkpoints, promotes int
	// fidelity and object describe the request sent; uploadMode is set only
	// on a Checkpoint.
	fidelity   ateletpb.SnapshotFidelity
	uploadMode ateletpb.UploadMode
	object     bool
}

// snapshotOutcome is the part of a stored snapshot the matrix pins.
type snapshotOutcome struct {
	generation int32
	fidelity   ateapipb.SnapshotFidelity
	locality   string
	// durable is the durable copy's status, UNSPECIFIED when there is none.
	durable ateapipb.SnapshotStorageStatus
}

// TestSuspendAndPause_CharacteristicMatrix pins, for each actor shape either
// mode can be called on, what a sync and an async SuspendActor do: the code
// returned, the atelet RPC sent, the record left behind, and whether the
// actor's worker was released.
func TestSuspendAndPause_CharacteristicMatrix(t *testing.T) {
	const node = "node-1"
	volumes := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	durable := func(gen int32, status ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
		name := fmt.Sprintf("snap-%d", gen)
		return newDurableSnapshot(gen, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, volumes, "", name, someActorSnapshotURI(t, testStorageLocation, "team-a", name), status)
	}
	local := func(gen int32, locality string) *ateapipb.Snapshot {
		return newPausedSnapshot(t, gen, volumes, "", fmt.Sprintf("snap-%d", gen), locality)
	}
	completed := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	pending := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING

	ateletVolumes := ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	immediate := ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE
	checkpointSync := ateletCall{checkpoints: 1, fidelity: ateletVolumes, uploadMode: immediate, object: true}
	checkpointAsync := ateletCall{checkpoints: 1, fidelity: ateletVolumes, uploadMode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED, object: true}
	// rejected is a FailedPrecondition outcome: no atelet call, and the
	// record left as want describes it.
	rejected := func(want suspendOutcome) suspendOutcome {
		want.code = codes.FailedPrecondition
		return want
	}
	suspended := ateapipb.ActorState_ACTOR_STATE_SUSPENDED

	tests := []struct {
		name string
		// atespace defaults to team-a.
		atespace    string
		status      *ateapipb.ActorStatus
		sync, async suspendOutcome
	}{
		{
			name:   "RUNNING",
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, AssignedNode: node, WorkerAssignment: wireTestAssignment()},
			sync:   suspendOutcome{code: codes.OK, atelet: checkpointSync, state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", completed}}},
			async:  suspendOutcome{code: codes.OK, atelet: checkpointAsync, state: suspended, assignedNode: node, generation: 1, snapshots: []snapshotOutcome{{1, volumes, node, pending}}},
		},
		{
			name: "RUNNING resumed from pause",
			status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_RUNNING, AssignedNode: node, WorkerAssignment: wireTestAssignment(),
				LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{local(1, node)},
			},
			sync:  suspendOutcome{code: codes.OK, atelet: checkpointSync, state: suspended, generation: 2, snapshots: []snapshotOutcome{{2, volumes, "", completed}}},
			async: suspendOutcome{code: codes.OK, atelet: checkpointAsync, state: suspended, assignedNode: node, generation: 2, snapshots: []snapshotOutcome{{2, volumes, node, pending}}},
		},
		{
			name: "RUNNING resumed from a durable snapshot",
			status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_RUNNING, AssignedNode: node, WorkerAssignment: wireTestAssignment(),
				LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, completed)},
			},
			sync:  suspendOutcome{code: codes.OK, atelet: checkpointSync, state: suspended, generation: 2, snapshots: []snapshotOutcome{{2, volumes, "", completed}}},
			async: suspendOutcome{code: codes.OK, atelet: checkpointAsync, state: suspended, assignedNode: node, generation: 2, snapshots: []snapshotOutcome{{1, volumes, "", completed}, {2, volumes, node, pending}}},
		},
		{
			name:   "SUSPENDED with a durable snapshot",
			status: &ateapipb.ActorStatus{State: suspended, LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, completed)}},
			sync:   suspendOutcome{code: codes.OK, state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", completed}}},
			async:  suspendOutcome{code: codes.OK, state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", completed}}},
		},
		{
			// Neither mode promotes the node-local snapshot.
			name:   "paused",
			status: &ateapipb.ActorStatus{State: suspended, AssignedNode: node, LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{local(1, node)}},
			sync:   suspendOutcome{code: codes.OK, state: suspended, assignedNode: node, generation: 1, snapshots: []snapshotOutcome{{1, volumes, node, pending}}},
			async:  suspendOutcome{code: codes.OK, state: suspended, assignedNode: node, generation: 1, snapshots: []snapshotOutcome{{1, volumes, node, pending}}},
		},
		{
			// Both modes mark this shape, and a retry finishes in its own mode.
			name: "suspend in progress",
			status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, AssignedNode: node, WorkerAssignment: wireTestAssignment(),
				LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, pending)},
			},
			sync:  suspendOutcome{code: codes.OK, atelet: checkpointSync, state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", completed}}},
			async: suspendOutcome{code: codes.OK, atelet: checkpointAsync, state: suspended, assignedNode: node, generation: 1, snapshots: []snapshotOutcome{{1, volumes, node, pending}}},
		},
		{
			name:   "RESUMING",
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING},
			sync:   rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_RESUMING}),
			async:  rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_RESUMING}),
		},
		{
			name:   "CRASHED",
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
			sync:   rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_CRASHED}),
			async:  rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_CRASHED}),
		},
		{
			name:   "DELETING",
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
			sync:   rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_DELETING}),
			async:  rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_DELETING}),
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range suspendModes {
				t.Run(mode.name, func(t *testing.T) {
					want := tc.sync
					if mode.name == "async" {
						want = tc.async
					}
					atespace := tc.atespace
					if atespace == "" {
						atespace = "team-a"
					}
					// Each run gets its own store, so it can seed the one worker
					// wireTestAssignment names.
					ctx := context.Background()
					persistence := newTestPersistence(t)
					tmpl := seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation, PreferredFidelity: volumes})
					actorRef := resources.ActorRef{Atespace: atespace, Name: fmt.Sprintf("row%d-%s", i, mode.name)}
					created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
						Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
						ActorTemplate: suspendTestTemplateRef(),
						Status:        proto.CloneOf(tc.status),
					})
					var assignment *ateapipb.ActorAssignment
					if tc.status.GetWorkerAssignment() != nil {
						assignment = assignmentOf(created)
					}
					seedWireWorker(t, ctx, persistence, assignment)
					w, atelet := newWireCaptureWorkflow(t, persistence)

					_, err := w.SuspendActor(ctx, actorRef, mode.suspendMode)
					stored := mustReadActor(t, ctx, persistence, actorRef)
					got := suspendOutcome{
						code:         apierror.Code(err),
						atelet:       describeAteletCall(atelet),
						state:        stored.GetStatus().GetState(),
						assignedNode: stored.GetStatus().GetAssignedNode(),
						workerHeld:   firstAssignment(t, persistence, wireTestAssignment().GetWorker().GetName()) != nil,
						generation:   stored.GetStatus().GetLastAssignedGeneration(),
						snapshots:    describeSnapshots(stored.GetStatus()),
					}
					if diff := cmp.Diff(want, got, cmp.AllowUnexported(suspendOutcome{}, ateletCall{}, snapshotOutcome{})); diff != "" {
						t.Errorf("outcome mismatch (-want +got):\n%s (err: %v)", diff, err)
					}
					if got.state == ateapipb.ActorState_ACTOR_STATE_SUSPENDED && stored.GetStatus().GetWorkerAssignment() != nil {
						t.Errorf("WorkerAssignment = %v, want cleared on SUSPENDED", stored.GetStatus().GetWorkerAssignment())
					}
					assertRequestNamesLatestSnapshot(t, atelet, tmpl, stored)
				})
			}
		})
	}
}

// TestSuspendAndPause_FinalizeWithoutAssignment verifies that both workflows
// preserve an out-of-band crash and return FailedPrecondition when the crash
// clears the worker assignment between the atelet call and finalize's read.
func TestSuspendAndPause_FinalizeWithoutAssignment(t *testing.T) {
	for _, mode := range suspendModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
			w, atelet := newWireCaptureWorkflow(t, persistence)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: suspendTestTemplateRef(),
				Status: &ateapipb.ActorStatus{
					State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
					AssignedNode:     "node-1",
					WorkerAssignment: wireTestAssignment(),
				},
			})
			// What a worker delete or the syncer commits: CRASHED with the
			// crash recorded and the assignment cleared, landing after the
			// atelet call and before finalize re-reads the actor.
			originalCrash := newActorCrash(ateattr.OperationDelete, "original crash reason")
			updateDuringCheckpoint(t, atelet, persistence, actorRef, func(s *ateapipb.ActorStatus) {
				s.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
				s.Crash = originalCrash
				s.WorkerAssignment = nil
			})

			returned, err := w.SuspendActor(ctx, actorRef, mode.suspendMode)
			if got := apierror.Code(err); got != codes.FailedPrecondition {
				t.Fatalf("%s: code = %v (err %v), want FailedPrecondition", mode.name, got, err)
			}
			if returned != nil {
				t.Errorf("%s: returned actor = %v, want nil", mode.name, returned)
			}
			stored := mustReadActor(t, ctx, persistence, actorRef)
			if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
				t.Errorf("%s: stored state = %v, want CRASHED", mode.name, got)
			}
			if got, want := stored.GetStatus().GetCrash().GetMessage(), originalCrash.GetMessage(); got != want {
				t.Errorf("%s: crash message = %q, want %q kept", mode.name, got, want)
			}
		})
	}
}

// TestSuspendAndPause_InvalidStorageLocationRejected verifies that both
// workflows validate the template's storage location in MarkSuspending before
// writing SUSPENDING, leaving the actor RUNNING untouched.
func TestSuspendAndPause_InvalidStorageLocationRejected(t *testing.T) {
	for _, mode := range suspendModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: "not-a-uri"})
			w := newMarkOnlyWorkflow(persistence)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: suspendTestTemplateRef(),
				Status: &ateapipb.ActorStatus{
					State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
					AssignedNode:     "node-1",
					WorkerAssignment: wireTestAssignment(),
				},
			})

			if _, err := w.SuspendActor(ctx, actorRef, mode.suspendMode); err == nil {
				t.Fatalf("%s: expected error for invalid storage location, got nil", mode.name)
			}
			stored := mustReadActor(t, ctx, persistence, actorRef)
			if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
				t.Errorf("%s: stored state = %v, want RUNNING untouched", mode.name, got)
			}
			if got := stored.GetStatus().GetLastAssignedGeneration(); got != 0 {
				t.Errorf("%s: last_assigned_generation = %d, want 0 untouched", mode.name, got)
			}
			if got := stored.GetStatus().GetSnapshots(); len(got) != 0 {
				t.Errorf("%s: snapshots = %v, want empty", mode.name, got)
			}
		})
	}
}

// describeAteletCall summarizes the suspend RPCs atelet received: how many
// of each, and the fidelity, upload mode, and object destination requested.
func describeAteletCall(atelet *capturingAtelet) ateletCall {
	var call ateletCall
	call.checkpoints, call.promotes = atelet.calls()
	var snap *ateletpb.Snapshot
	if req := atelet.checkpointRequest(); req != nil {
		snap = req.GetSnapshot()
		call.uploadMode = req.GetUploadConfig().GetMode()
	} else if req := atelet.promoteRequest(); req != nil {
		snap = req.GetSnapshot()
	}
	call.fidelity = snap.GetFidelity()
	call.object = snap.GetObject() != nil
	return call
}

// describeSnapshots summarizes each stored snapshot as its generation,
// fidelity, locality, and durable copy's status.
func describeSnapshots(status *ateapipb.ActorStatus) []snapshotOutcome {
	var out []snapshotOutcome
	for _, snap := range status.GetSnapshots() {
		out = append(out, snapshotOutcome{
			generation: snap.GetGeneration(),
			fidelity:   snap.GetFidelity(),
			locality:   snap.GetLocality(),
			durable:    snap.GetDurableSnapshot().GetStatus(),
		})
	}
	return out
}

// assertRequestNamesLatestSnapshot verifies the suspend RPC atelet received,
// if any, names the snapshot at the stored actor's latest generation and its
// object storage URI.
func assertRequestNamesLatestSnapshot(t *testing.T, atelet *capturingAtelet, tmpl *ateapipb.ActorTemplate, stored *ateapipb.Actor) {
	t.Helper()
	var snap *ateletpb.Snapshot
	if req := atelet.checkpointRequest(); req != nil {
		snap = req.GetSnapshot()
	} else if req := atelet.promoteRequest(); req != nil {
		snap = req.GetSnapshot()
	} else {
		return
	}
	latest := snapshotAtLatestGeneration(stored.GetStatus())
	if got, want := snap.GetSnapshotUuid(), latest.GetUuid(); got != want {
		t.Errorf("request Snapshot.SnapshotUuid = %q, want the latest generation's %q", got, want)
	}
	wantURI := latest.GetDurableSnapshot().GetObject().GetSnapshotUri()
	if wantURI == "" {
		wantURI = mustActorSnapshotURI(t, tmpl, stored, latest.GetUuid()).String()
	}
	if got := snap.GetObject().GetSnapshotUri(); got != wantURI {
		t.Errorf("request Snapshot.Object.SnapshotUri = %q, want %q", got, wantURI)
	}
}

// TestSuspendActor_AppliesCheckpointState verifies finalize applies the
// snapshot state atelet returns from Checkpoint, with the report rules. A state
// that conflicts with the record crashes the actor.
func TestSuspendActor_AppliesCheckpointState(t *testing.T) {
	const node = "node-1"
	memory := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	pending := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
	completed := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	durable := func(gen int32, status ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
		name := fmt.Sprintf("snap-%d", gen)
		return newDurableSnapshot(gen, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, memory, "", name, someActorSnapshotURI(t, testStorageLocation, "team-a", name), status)
	}
	report := func(gen int32, uri string, fidelity ateletpb.SnapshotFidelity) *ateletpb.SnapshotReport {
		return &ateletpb.SnapshotReport{Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: fmt.Sprintf("snap-%d", gen),
			Generation:   gen,
			Fidelity:     fidelity,
			Object:       &ateletpb.ObjectStorage{SnapshotUri: uri},
		}}
	}
	uriOf := func(gen int32) string {
		return someActorSnapshotURI(t, testStorageLocation, "team-a", fmt.Sprintf("snap-%d", gen))
	}

	tests := []struct {
		name string
		mode suspendMode
		// status is the RUNNING actor's status before the suspend.
		status *ateapipb.ActorStatus
		// state builds what Checkpoint returns, from the request it got.
		state         func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState
		wantSnapshots []snapshotOutcome
		// wantNoNode is set when atelet keeps no local copy of an async
		// suspend's durable snapshot, so AssignedNode is cleared.
		wantNoNode bool
		// wantCode, when set, is the error the suspend fails with.
		wantCode codes.Code
	}{{
		// Async suspend can't reach this shape. It starts only from RUNNING, and
		// a sync suspend in progress must be reverted before the actor runs
		// again. The case pins that finalize applies the state before it prunes.
		name:   "async: an older upload atelet finished is kept as COMPLETED",
		mode:   suspendAsync,
		status: &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, pending)}},
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return &ateletpb.ActorSnapshotState{
				LatestLocal:   &ateletpb.SnapshotReport{Snapshot: req.GetSnapshot()},
				LatestDurable: report(1, uriOf(1), ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
			}
		},
		wantSnapshots: []snapshotOutcome{{generation: 1, fidelity: memory, durable: completed}, {generation: 2, fidelity: memory, locality: node, durable: pending}},
	}, {
		// A RUNNING actor resumed from a pause keeps that pause's snapshot,
		// with its PENDING durable copy. atelet uploads it in the background,
		// and the next pause records that.
		name:   "async: the previous pause's upload atelet finished is recorded as COMPLETED",
		mode:   suspendAsync,
		status: &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{newPausedSnapshot(t, 1, memory, "", "snap-1", node)}},
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return &ateletpb.ActorSnapshotState{
				LatestLocal:   &ateletpb.SnapshotReport{Snapshot: req.GetSnapshot()},
				LatestDurable: report(1, uriOf(1), ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
			}
		},
		wantSnapshots: []snapshotOutcome{{generation: 1, fidelity: memory, durable: completed}, {generation: 2, fidelity: memory, locality: node, durable: pending}},
	}, {
		name:   "async: without a state, the PENDING upload is pruned",
		mode:   suspendAsync,
		status: &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, pending)}},
		state: func(*ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return nil
		},
		wantSnapshots: []snapshotOutcome{{generation: 2, fidelity: memory, locality: node, durable: pending}},
	}, {
		// A retried async suspend can get a state where atelet already
		// uploaded and detached the snapshot it committed on the first
		// attempt. The snapshot is recorded as durable with no node
		// locality, and AssignedNode is cleared so the actor can resume
		// on any node.
		name:   "async: a snapshot atelet already detached is recorded without locality",
		mode:   suspendAsync,
		status: &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, completed)}},
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return &ateletpb.ActorSnapshotState{
				LatestDurable: &ateletpb.SnapshotReport{Snapshot: req.GetSnapshot()},
			}
		},
		wantSnapshots: []snapshotOutcome{{generation: 2, fidelity: memory, durable: completed}},
		wantNoNode:    true,
	}, {
		// The suspend's first attempt was sync and kept no local copy, and
		// the retry is async. An older snapshot's upload is still running.
		name:   "async: a durable snapshot atelet kept no copy of is recorded without locality",
		mode:   suspendAsync,
		status: &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, pending)}},
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return &ateletpb.ActorSnapshotState{
				Uploading:     report(1, uriOf(1), ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
				LatestDurable: &ateletpb.SnapshotReport{Snapshot: req.GetSnapshot()},
			}
		},
		wantSnapshots: []snapshotOutcome{{generation: 2, fidelity: memory, durable: completed}},
		wantNoNode:    true,
	}, {
		name: "sync: the fidelity atelet reports is recorded",
		mode: suspendSync,
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			snap := proto.CloneOf(req.GetSnapshot())
			snap.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
			return &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: snap}}
		},
		wantSnapshots: []snapshotOutcome{{generation: 1, fidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, durable: completed}},
	}, {
		name: "sync: a state that conflicts with the record crashes the actor",
		mode: suspendSync,
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			snap := proto.CloneOf(req.GetSnapshot())
			snap.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
			snap.Object = &ateletpb.ObjectStorage{SnapshotUri: uriOf(9)}
			return &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: snap}}
		},
		wantCode: codes.FailedPrecondition,
	}, {
		name:   "async: a state that conflicts with the record crashes the actor",
		mode:   suspendAsync,
		status: &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, pending)}},
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return &ateletpb.ActorSnapshotState{
				LatestLocal:   &ateletpb.SnapshotReport{Snapshot: req.GetSnapshot()},
				LatestDurable: report(1, uriOf(9), ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
			}
		},
		wantCode: codes.FailedPrecondition,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation, PreferredFidelity: memory})
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			status := proto.CloneOf(tt.status)
			if status == nil {
				status = &ateapipb.ActorStatus{}
			}
			status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
			status.AssignedNode = node
			status.WorkerAssignment = wireTestAssignment()
			created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: suspendTestTemplateRef(),
				Status:        status,
			})
			seedWireWorker(t, ctx, persistence, assignmentOf(created))
			w, atelet := newWireCaptureWorkflow(t, persistence)
			atelet.onCheckpoint = func(context.Context) error {
				st := tt.state(atelet.checkpointRequest())
				atelet.mu.Lock()
				atelet.checkpointState = st
				atelet.mu.Unlock()
				return nil
			}

			_, err := w.SuspendActor(ctx, actorRef, tt.mode)
			stored := mustReadActor(t, ctx, persistence, actorRef)
			if tt.wantCode != codes.OK {
				if got := apierror.Code(err); got != tt.wantCode {
					t.Fatalf("suspend = %v, want code %v", err, tt.wantCode)
				}
				if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
					t.Errorf("state = %v, want CRASHED", got)
				}
				if got, want := stored.GetStatus().GetCrash().GetMessage(), "suspend failed: "+crashMessageSnapshotStateConflict; got != want {
					t.Errorf("crash message = %q, want %q", got, want)
				}
				return
			}
			if err != nil {
				t.Fatalf("suspend: %v", err)
			}
			if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
				t.Errorf("state = %v, want SUSPENDED", got)
			}
			wantNode := node
			if tt.mode == suspendSync || tt.wantNoNode {
				wantNode = ""
			}
			if got := stored.GetStatus().GetAssignedNode(); got != wantNode {
				t.Errorf("assigned_node = %q, want %q", got, wantNode)
			}
			if diff := cmp.Diff(tt.wantSnapshots, describeSnapshots(stored.GetStatus()), cmp.AllowUnexported(snapshotOutcome{})); diff != "" {
				t.Errorf("snapshots mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSuspendActor_Finalized_RecordsLocalCopyWhenKept(t *testing.T) {
	const node = "node-1"
	memory := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	completed := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	localReport := func(snap *ateletpb.Snapshot) *ateletpb.ActorSnapshotState {
		return &ateletpb.ActorSnapshotState{LatestLocal: &ateletpb.SnapshotReport{Snapshot: snap}}
	}
	variant := func(req *ateletpb.CheckpointRequest, mutate func(*ateletpb.Snapshot)) *ateletpb.Snapshot {
		snap := proto.CloneOf(req.GetSnapshot())
		mutate(snap)
		return snap
	}
	tests := []struct {
		name         string
		state        func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState
		wantLocality string
	}{{
		name: "kept",
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return localReport(req.GetSnapshot())
		},
		wantLocality: node,
	}, {
		name: "not kept",
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: req.GetSnapshot()}}
		},
	}, {
		name: "uuid mismatch",
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return localReport(variant(req, func(snap *ateletpb.Snapshot) { snap.SnapshotUuid = "another-snapshot" }))
		},
	}, {
		name: "generation mismatch",
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return localReport(variant(req, func(snap *ateletpb.Snapshot) { snap.Generation = req.GetSnapshot().GetGeneration() - 1 }))
		},
	}, {
		name: "uri mismatch",
		state: func(req *ateletpb.CheckpointRequest) *ateletpb.ActorSnapshotState {
			return localReport(variant(req, func(snap *ateletpb.Snapshot) {
				snap.Object = &ateletpb.ObjectStorage{SnapshotUri: "gs://bucket/root/another-snapshot"}
			}))
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation, PreferredFidelity: memory})
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: suspendTestTemplateRef(),
				Status: &ateapipb.ActorStatus{
					State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
					AssignedNode:     node,
					WorkerAssignment: wireTestAssignment(),
				},
			})
			seedWireWorker(t, ctx, persistence, assignmentOf(created))
			w, atelet := newWireCaptureWorkflow(t, persistence)
			atelet.onCheckpoint = func(context.Context) error {
				st := tt.state(atelet.checkpointRequest())
				atelet.mu.Lock()
				atelet.checkpointState = st
				atelet.mu.Unlock()
				return nil
			}

			if _, err := w.SuspendActor(ctx, actorRef, suspendSync); err != nil {
				t.Fatalf("suspend: %v", err)
			}
			stored := mustReadActor(t, ctx, persistence, actorRef)
			if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
				t.Errorf("state = %v, want SUSPENDED", got)
			}
			if got := stored.GetStatus().GetAssignedNode(); got != tt.wantLocality {
				t.Errorf("AssignedNode = %q, want %q", got, tt.wantLocality)
			}
			if got := stored.GetStatus().GetWorkerAssignment(); got != nil {
				t.Errorf("WorkerAssignment = %v, want cleared", got)
			}
			want := []snapshotOutcome{{generation: 1, fidelity: memory, locality: tt.wantLocality, durable: completed}}
			if diff := cmp.Diff(want, describeSnapshots(stored.GetStatus()), cmp.AllowUnexported(snapshotOutcome{})); diff != "" {
				t.Errorf("snapshots mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
