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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestSuspendActorWithLease_StaleTupleFailsClosed(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-a"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	issued, err := persistence.IssueActorRuntimeLease(ctx, actor.GetMetadata().GetUid(), resources.ActorRefFromActor(actor))
	if err != nil {
		t.Fatalf("IssueActorRuntimeLease: %v", err)
	}
	w := &ActorWorkflow{store: persistence}
	_, err = w.SuspendActorWithLease(ctx, resources.ActorRefFromActor(actor), &ateapipb.ActorLease{
		Token:      issued.Token,
		Generation: issued.Generation + 1,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SuspendActorWithLease error = %v, want FailedPrecondition", err)
	}
	stored, err := persistence.GetActor(ctx, resources.ActorRefFromActor(actor))
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state after stale suspend = %v, want RUNNING", stored.GetStatus().GetState())
	}
	if _, err := persistence.GetActorRuntimeLease(ctx, actor.GetMetadata().GetUid()); err != nil {
		t.Fatalf("runtime lease after stale suspend: %v", err)
	}
}

type idempotentRuntimeLeaseStore struct {
	store.Interface
	actor       *ateapipb.Actor
	runtime     *store.ActorRuntimeLease
	deleteCalls int
}

func (s *idempotentRuntimeLeaseStore) GetActor(_ context.Context, _ resources.ActorRef) (*ateapipb.Actor, error) {
	return proto.Clone(s.actor).(*ateapipb.Actor), nil
}

func (s *idempotentRuntimeLeaseStore) GetActorRuntimeLease(_ context.Context, _ string) (*store.ActorRuntimeLease, error) {
	if s.runtime == nil {
		return nil, store.ErrNotFound
	}
	lease := *s.runtime
	return &lease, nil
}

func (s *idempotentRuntimeLeaseStore) DeleteActorRuntimeLease(_ context.Context, actorUID, token string, generation int64) error {
	s.deleteCalls++
	if s.runtime == nil || s.runtime.ActorUID != actorUID || s.runtime.Token != token || s.runtime.Generation != generation {
		return store.ErrRuntimeLeaseInvalid
	}
	s.runtime = nil
	return nil
}

func (s *idempotentRuntimeLeaseStore) ClaimExpiredActorRuntimeLease(_ context.Context, actorUID, token string, generation int64) error {
	if s.runtime == nil || s.runtime.ActorUID != actorUID || s.runtime.Token != token || s.runtime.Generation != generation {
		return store.ErrRuntimeLeaseInvalid
	}
	return nil
}

func TestRuntimeLeaseReconciler_IdempotentAndDoesNotMutateSnapshot(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-a", Uid: "actor-uid"},
		Status: &ateapipb.ActorStatus{
			State:                   ateapipb.ActorState_ACTOR_STATE_CRASHED,
			ExternalSnapshot:        &ateapipb.ExternalSnapshot{SnapshotUri: "gs://bucket/last"},
			InProgressSnapshotName:  "in-flight",
			CurrentActorTemplateUid: "template-uid",
		},
	}
	ref := resources.ActorRefFromActor(actor)
	lease := &store.ActorRuntimeLease{
		ActorUID:   actor.GetMetadata().GetUid(),
		ActorRef:   ref,
		Token:      "expired-token",
		Generation: 7,
		ExpiresAt:  time.Now().Add(-time.Minute),
	}
	fake := &idempotentRuntimeLeaseStore{Interface: persistence, actor: actor, runtime: lease}
	terminateCalls := 0
	r := newRuntimeLeaseReconciler(fake, func(context.Context, *ateapipb.Actor) error {
		terminateCalls++
		return errors.New("must not terminate an already crashed actor")
	})
	if err := r.reconcileOne(ctx, *lease); err != nil {
		t.Fatalf("first reconcileOne: %v", err)
	}
	if err := r.reconcileOne(ctx, *lease); err != nil {
		t.Fatalf("second reconcileOne: %v", err)
	}
	if terminateCalls != 0 {
		t.Errorf("terminate calls = %d, want 0", terminateCalls)
	}
	if fake.deleteCalls != 1 {
		t.Errorf("delete calls = %d, want 1", fake.deleteCalls)
	}
	if got := fake.actor.GetStatus().GetExternalSnapshot().GetSnapshotUri(); got != "gs://bucket/last" {
		t.Errorf("durable snapshot = %q, want last snapshot", got)
	}
	if got := fake.actor.GetStatus().GetInProgressSnapshotName(); got != "in-flight" {
		t.Errorf("in-progress snapshot = %q, want unchanged", got)
	}
}

func TestCleanupFailedResume_TerminatesBeforeReleaseAndClearsLease(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	workerName := testWorkerUID("failed-resume-pod")
	if _, err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: workerName},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "failed-resume-pod",
		WorkerPodUid:    workerName,
		Status:          &ateapipb.WorkerStatus{},
	}); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-a"},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_RESUMING,
			WorkerAssignment:       &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: workerName}, WorkerNamespace: "worker-ns", WorkerPod: "failed-resume-pod", WorkerPodUid: workerName},
			ExternalSnapshot:       &ateapipb.ExternalSnapshot{SnapshotUri: "gs://bucket/last"},
			InProgressSnapshotName: "in-flight",
		},
	})
	seedAssignment(t, persistence, workerName, &ateapipb.ActorAssignment{Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-a"}, ActorUid: actor.GetMetadata().GetUid()})
	issued, err := persistence.IssueActorRuntimeLease(ctx, actor.GetMetadata().GetUid(), resources.ActorRefFromActor(actor))
	if err != nil {
		t.Fatalf("IssueActorRuntimeLease: %v", err)
	}
	terminateCalls := 0
	w := &ActorWorkflow{
		store: persistence,
		terminateWorkload: func(context.Context, *ateapipb.Actor) error {
			terminateCalls++
			return nil
		},
	}
	if err := w.cleanupFailedResume(ctx, resources.ActorRefFromActor(actor), actor, actorRuntimeLeaseProto(issued)); err != nil {
		t.Fatalf("cleanupFailedResume: %v", err)
	}
	if terminateCalls != 1 {
		t.Errorf("terminate calls = %d, want 1", terminateCalls)
	}
	stored, err := persistence.GetActor(ctx, resources.ActorRefFromActor(actor))
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED || stored.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("actor after cleanup = %v assignment %v, want CRASHED with no assignment", stored.GetStatus().GetState(), stored.GetStatus().GetWorkerAssignment())
	}
	if stored.GetStatus().GetExternalSnapshot().GetSnapshotUri() != "gs://bucket/last" || stored.GetStatus().GetInProgressSnapshotName() != "in-flight" {
		t.Errorf("snapshot state changed during cleanup: %v", stored.GetStatus())
	}
	if _, err := persistence.GetActorRuntimeLease(ctx, actor.GetMetadata().GetUid()); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("runtime lease after cleanup = %v, want not found", err)
	}
	if assignment := firstAssignment(t, persistence, workerName); assignment != nil {
		t.Errorf("worker assignment after cleanup = %v, want nil", assignment)
	}
}
