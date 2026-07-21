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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/ateredis"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/cache"
)

// TestSuspendActorWorkflow_RejectedAndIdempotentPaths covers the two
// short-circuit paths of the suspend workflow: rejection by
// MarkSuspendingStep's CheckPrerequisite and the IsComplete idempotent
// fast-forward.
func TestSuspendActorWorkflow_RejectedAndIdempotentPaths(t *testing.T) {
	tests := []struct {
		name       string
		seedStatus ateapipb.Actor_Status
		// wantErr true means SuspendActor must fail with FailedPrecondition.
		wantErr bool
		// wantStatus is the stored status after the call.
		wantStatus ateapipb.Actor_Status
	}{
		{
			// The state machine's PAUSED->SUSPENDED commit edge is rejected
			// (suspending needs a live worker to checkpoint from) and the
			// actor's status is left untouched.
			name:       "paused rejected",
			seedStatus: ateapipb.Actor_STATUS_PAUSED,
			wantErr:    true,
			wantStatus: ateapipb.Actor_STATUS_PAUSED,
		},
		{
			// Suspending a SUSPENDED actor succeeds idempotently via
			// IsComplete fast-forward without calling atelet.
			name:       "already suspended succeeds",
			seedStatus: ateapipb.Actor_STATUS_SUSPENDED,
			wantStatus: ateapipb.Actor_STATUS_SUSPENDED,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			seedWorkflowActor(t, ctx, st, "team-a", "id1", "ns", "tmpl1", tc.seedStatus)

			actor, err := w.SuspendActor(ctx, "team-a", "id1")
			if tc.wantErr {
				if got := status.Code(err); got != codes.FailedPrecondition {
					t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
				}
			} else {
				if err != nil {
					t.Fatalf("SuspendActor failed: %v", err)
				}
				if actor.GetStatus() != tc.wantStatus {
					t.Errorf("returned status = %v, want %v", actor.GetStatus(), tc.wantStatus)
				}
			}

			got, err := st.GetActor(ctx, "team-a", "id1")
			if err != nil {
				t.Fatalf("GetActor failed: %v", err)
			}
			if got.GetStatus() != tc.wantStatus {
				t.Errorf("stored status = %v, want %v", got.GetStatus(), tc.wantStatus)
			}
		})
	}
}

// TestSuspendSteps_CheckPrerequisite verifies each suspend step's
// CheckPrerequisite against every actor status: nil for the step's allowed
// statuses, FailedPrecondition for all others.
func TestSuspendSteps_CheckPrerequisite(t *testing.T) {
	tests := []struct {
		name string
		step WorkflowStep[*SuspendInput, *SuspendState]
		// allowed lists the statuses CheckPrerequisite accepts; nil means
		// every status is accepted.
		allowed map[ateapipb.Actor_Status]bool
	}{
		{
			// Loading has no prerequisite: it is allowed from every status.
			name:    "LoadActorForSuspendStep",
			step:    &LoadActorForSuspendStep{},
			allowed: nil,
		},
		{
			// Suspending is allowed only from RUNNING.
			name: "MarkSuspendingStep",
			step: &MarkSuspendingStep{},
			allowed: map[ateapipb.Actor_Status]bool{
				ateapipb.Actor_STATUS_RUNNING: true,
			},
		},
		{
			// The checkpoint call is allowed only from SUSPENDING (SUSPENDED
			// is fast-forwarded by IsComplete).
			name: "CallAteletSuspendStep",
			step: &CallAteletSuspendStep{},
			allowed: map[ateapipb.Actor_Status]bool{
				ateapipb.Actor_STATUS_SUSPENDING: true,
			},
		},
		{
			// Finalizing is allowed only from SUSPENDING: a persisted
			// SUSPENDED actor always has its worker pod fields cleared and is
			// fast-forwarded by IsComplete.
			name: "FinalizeSuspendedStep",
			step: &FinalizeSuspendedStep{},
			allowed: map[ateapipb.Actor_Status]bool{
				ateapipb.Actor_STATUS_SUSPENDING: true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			for _, st := range allActorStatuses {
				// Worker pod fields are populated so CallAteletSuspendStep's
				// missing-worker crash branch is not taken; this test only
				// verifies status gating.
				err := tc.step.CheckPrerequisite(ctx, &SuspendInput{ActorName: "id1"}, &SuspendState{Actor: &ateapipb.Actor{Status: st, AteomPodNamespace: "ns", AteomPodName: "worker-1"}})
				assertPrerequisiteResult(t, st, err, tc.allowed == nil || tc.allowed[st])
			}
		})
	}
}

// TestSuspendActor_CrashesWhenSuspendingActorMissingWorkerPod verifies that a
// SUSPENDING actor with no worker pod recorded is moved to CRASHED by
// CallAteletSuspendStep's prerequisite check and the suspend fails.
func TestSuspendActor_CrashesWhenSuspendingActorMissingWorkerPod(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, "team-a", "id1", "ns", "tmpl1", ateapipb.Actor_STATUS_SUSPENDING)

	if _, err := w.SuspendActor(ctx, "team-a", "id1"); err == nil {
		t.Fatal("SuspendActor succeeded, want error for SUSPENDING actor with no worker pod")
	}

	got, err := st.GetActor(ctx, "team-a", "id1")
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("stored status = %v, want %v", got.GetStatus(), ateapipb.Actor_STATUS_CRASHED)
	}
}

// newTestPersistence returns a store backed by a throwaway miniredis.
func newTestPersistence(t *testing.T) store.Interface {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{mr.Addr()}})
	t.Cleanup(func() { rdb.Close() }) //nolint:errcheck // test cleanup
	return ateredis.NewPersistence(rdb)
}

// newDanglingDialer returns a dialer whose informer cache has no pods, so
// DialForWorker returns ErrWorkerPodNotFound.
func newDanglingDialer() *AteletDialer {
	empty := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNamespaceAndName: func(obj any) ([]string, error) { return nil, nil },
	})
	return NewAteletDialer(empty, empty)
}

func TestCallAteletSuspendStep_DanglingWorkerDoesNotRecordPhantomSnapshot(t *testing.T) {
	tests := []struct {
		name         string
		prevSnapshot *ateapipb.SnapshotInfo
	}{
		{
			name: "keeps previous snapshot",
			prevSnapshot: &ateapipb.SnapshotInfo{
				Data: &ateapipb.SnapshotInfo_External{
					External: &ateapipb.ExternalSnapshotInfo{SnapshotUriPrefix: "gs://snapshots/actor-1/prev"},
				},
			},
		},
		{
			name:         "stays nil without previous snapshot",
			prevSnapshot: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			actor := &ateapipb.Actor{
				Metadata:           &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				Status:             ateapipb.Actor_STATUS_SUSPENDING,
				AteomPodNamespace:  "worker-ns",
				AteomPodName:       "pod-gone",
				WorkerPoolName:     "pool",
				InProgressSnapshot: "gs://snapshots/actor-1/never-written",
				LatestSnapshotInfo: tt.prevSnapshot,
			}
			created, err := persistence.CreateActor(ctx, actor)
			if err != nil {
				t.Fatalf("CreateActor: %v", err)
			}

			step := &CallAteletSuspendStep{store: persistence, dialer: newDanglingDialer()}
			input := &SuspendInput{ActorName: "actor-1", Atespace: "team-a"}
			if err := step.Execute(ctx, input, &SuspendState{Actor: created}); err == nil {
				t.Fatal("Execute: want error for dangling worker, got nil")
			}

			stored, err := persistence.GetActor(ctx, "team-a", "actor-1")
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if stored.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
				t.Errorf("status = %v, want CRASHED", stored.GetStatus())
			}
			if got := stored.GetInProgressSnapshot(); got != "gs://snapshots/actor-1/never-written" {
				t.Errorf("InProgressSnapshot = %q, want preserved for debugging", got)
			}
			if tt.prevSnapshot == nil {
				if stored.GetLatestSnapshotInfo() != nil {
					t.Errorf("LatestSnapshotInfo = %v, want nil", stored.GetLatestSnapshotInfo())
				}
			} else if got, want := stored.GetLatestSnapshotInfo().GetExternal().GetSnapshotUriPrefix(), tt.prevSnapshot.GetExternal().GetSnapshotUriPrefix(); got != want {
				t.Errorf("LatestSnapshotInfo uri = %q, want %q", got, want)
			}
		})
	}
}

func TestFinalizeSuspendedStep_ReleasesOnlyOwnWorker(t *testing.T) {
	tests := []struct {
		name               string
		assignmentAtespace string
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			worker := &ateapipb.Worker{
				WorkerNamespace: "worker-ns",
				WorkerPool:      "pool",
				WorkerPod:       "pod-1",
				Assignment: &ateapipb.Assignment{
					Actor: &ateapipb.ObjectRef{Atespace: tt.assignmentAtespace, Name: "shared"},
				},
			}
			if err := persistence.CreateWorker(ctx, worker); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}

			actor := &ateapipb.Actor{
				Metadata:           &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
				Status:             ateapipb.Actor_STATUS_SUSPENDING,
				AteomPodNamespace:  "worker-ns",
				AteomPodName:       "pod-1",
				WorkerPoolName:     "pool",
				InProgressSnapshot: "gs://snapshots/shared/1",
			}
			if _, err := persistence.CreateActor(ctx, actor); err != nil {
				t.Fatalf("CreateActor: %v", err)
			}

			step := &FinalizeSuspendedStep{store: persistence}
			input := &SuspendInput{ActorName: "shared", Atespace: "team-a"}
			if err := step.Execute(ctx, input, &SuspendState{}); err != nil {
				t.Fatalf("Execute: %v", err)
			}

			stored, err := persistence.GetWorker(ctx, "worker-ns", "pool", "pod-1")
			if err != nil {
				t.Fatalf("GetWorker: %v", err)
			}
			if released := stored.GetAssignment() == nil; released != tt.wantReleased {
				t.Errorf("worker released = %t, want %t (assignment: %v)", released, tt.wantReleased, stored.GetAssignment())
			}
		})
	}
}
