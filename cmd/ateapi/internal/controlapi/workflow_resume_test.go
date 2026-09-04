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
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestSchedulerRecordable guards the retry-dedup rule: the assignment loop
// re-runs attempts on store.ErrVersionConflict, and those attempts (raw or
// wrapped) must not be recorded, while the terminal success or real error
// must be.
func TestSchedulerRecordable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "success is recorded", err: nil, want: true},
		{name: "version conflict is skipped", err: store.ErrVersionConflict, want: false},
		{name: "wrapped version conflict is skipped", err: fmt.Errorf("update worker: %w", store.ErrVersionConflict), want: false},
		{name: "real error is recorded", err: status.Error(codes.Internal, "boom"), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := schedulerRecordable(tt.err); got != tt.want {
				t.Errorf("schedulerRecordable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

type leaseCountingStore struct {
	store.Interface
	acquireCalls int
}

func (s *leaseCountingStore) AcquireLease(ctx context.Context, key string) (*store.Lease, error) {
	s.acquireCalls++
	return s.Interface.AcquireLease(ctx, key)
}

func TestResumeActor_RunningFastPathDoesNotAcquireLease(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	st := &leaseCountingStore{Interface: persistence}
	w := &ActorWorkflow{store: st}

	got, resumed, err := w.ResumeActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, false)
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	if resumed {
		t.Error("ResumeActor resumed = true, want false")
	}
	if !proto.Equal(got, created) {
		t.Errorf("ResumeActor actor = %v, want %v", got, created)
	}
	if st.acquireCalls != 0 {
		t.Errorf("AcquireLease calls = %d, want 0", st.acquireCalls)
	}
}

// TestFinalizeRunning_RecordsSprintTemplate verifies committing RUNNING stamps
// the template the sprint booted with, overwriting the previous sprint's
// record, so the next resume can detect a repointed template by UID.
func TestFinalizeRunning_RecordsSprintTemplate(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "tmpl-2"},
		Status: &ateapipb.ActorStatus{
			State:                   ateapipb.ActorState_ACTOR_STATE_RESUMING,
			CurrentActorTemplateUid: "tmpl-uid-1",
		},
	})
	w := &ActorWorkflow{store: persistence}

	got, err := w.finalizeRunning(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "tmpl-2", Uid: "tmpl-uid-2"},
	})
	if err != nil {
		t.Fatalf("finalizeRunning: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("state = %v, want RUNNING", got.GetStatus().GetState())
	}
	if uid := got.GetStatus().GetCurrentActorTemplateUid(); uid != "tmpl-uid-2" {
		t.Errorf("CurrentActorTemplateUid = %q, want %q", uid, "tmpl-uid-2")
	}
}

// bindErrorStore fails every claim, standing in for a worker that moved or
// vanished between the pick and the write.
type bindErrorStore struct {
	store.Interface
	err error
}

func (s *bindErrorStore) BindActorToWorker(context.Context, string, *ateapipb.ActorAssignment, func(*ateapipb.Worker) error) error {
	return s.err
}

func TestAssignWorkerAttempt_MissingSelectedWorkerIsRetried(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor, wc := seedAssignFixture(t, ctx, persistence)
	st := &bindErrorStore{Interface: persistence, err: store.ErrNotFound}
	w := &ActorWorkflow{store: st, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &ateapipb.ActorTemplate{SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR}}

	_, _, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("assignWorkerAttempt error = %v, want ErrVersionConflict", err)
	}
	workers, err := wc.Workers()
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if len(workers) != 0 {
		t.Errorf("cached workers after missing claim = %d, want 0", len(workers))
	}
}

func TestEnsureWorkerAssigned_ConflictExhaustionIsRetryable(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor, wc := seedAssignFixture(t, ctx, persistence)
	st := &bindErrorStore{Interface: persistence, err: store.ErrVersionConflict}
	w := &ActorWorkflow{store: st, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &ateapipb.ActorTemplate{SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR}}

	_, _, err := w.ensureWorkerAssigned(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("ensureWorkerAssigned error = %v, want ErrVersionConflict", err)
	}
}

// TestAssignWorkerAttempt_StampsSubstrateTemplateRef verifies a ref-mode
// actor's worker claim names the substrate template via actor_template_ref
// and leaves the legacy kube reference unset.
func TestAssignWorkerAttempt_StampsSubstrateTemplateRef(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-free")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-free",
		WorkerPodUid:    testWorkerUID("pod-free"),
		SandboxClass:    "gvisor",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
	}
	if _, err := persistence.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &ateapipb.ActorTemplate{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "sub-tmpl"},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
	}
	_, assigned, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt: %v", err)
	}

	assignment, err := persistence.GetWorkerAssignment(ctx, assigned.GetMetadata().GetName(), actor.GetMetadata().GetUid())
	if err != nil {
		t.Fatalf("GetWorkerAssignment: %v", err)
	}
	if assignment.GetActorTemplateRef().GetAtespace() != "team-a" || assignment.GetActorTemplateRef().GetName() != "sub-tmpl" {
		t.Errorf("assignment ActorTemplateRef = %v, want team-a/sub-tmpl", assignment.GetActorTemplateRef())
	}
}

func TestAssignWorkerAttempt_SkipsWorkerAssignedInOtherAtespace(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	// The only worker is held by a same-named actor in another atespace. It is
	// eligible for the template, so a name-only match would adopt it.
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-1")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		WorkerPodUid:    testWorkerUID("pod-1"),
		SandboxClass:    "gvisor",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
	}
	if _, err := persistence.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	seedAssignment(t, persistence, testWorkerUID("pod-1"), &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-b", Name: "shared"},
		ActorUid: "team-b-actor-uid",
	})

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared", Uid: "actor-uid"},
	}
	tmpl := &ateapipb.ActorTemplate{
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
	}
	_, _, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}, actor, tmpl)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("assignWorkerAttempt() error = %v, want ResourceExhausted (no free workers)", err)
	}

	stored := firstAssignment(t, persistence, testWorkerUID("pod-1"))
	if got := stored.GetActorUid(); got != "team-b-actor-uid" {
		t.Errorf("worker assignment uid = %q, want %q (assignment: %v)", got, "team-b-actor-uid", stored)
	}
	if got := stored.GetActor().GetAtespace(); got != "team-b" {
		t.Errorf("worker assignment atespace = %q, want %q (assignment: %v)", got, "team-b", stored)
	}
}

// TestAssignWorkerAttempt_ReleasesIneligibleStaleWorker verifies that a worker
// claimed by a previous failed attempt whose pool is no longer eligible is
// released back to the free pool, without failing the resume, while a fresh
// eligible worker is assigned.
func TestAssignWorkerAttempt_ReleasesIneligibleStaleWorker(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})

	// stale-pod is claimed by this actor from a failed attempt but its sandbox
	// class no longer matches the template; free-pod is eligible and free.
	stale := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("stale-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-a",
		WorkerPod:       "stale-pod",
		WorkerPodUid:    testWorkerUID("stale-pod"),
		SandboxClass:    "microvm",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
	}
	free := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("free-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-b",
		WorkerPod:       "free-pod",
		WorkerPodUid:    testWorkerUID("free-pod"),
		SandboxClass:    "gvisor",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
	}
	for _, w := range []*ateapipb.Worker{stale, free} {
		if _, err := persistence.CreateWorker(ctx, w); err != nil {
			t.Fatalf("CreateWorker(%s): %v", w.GetWorkerPod(), err)
		}
	}
	seedAssignment(t, persistence, testWorkerUID("stale-pod"), &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "id1"},
		ActorUid: actor.GetMetadata().GetUid(),
	})

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &ateapipb.ActorTemplate{
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
	}
	_, worker, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt() error = %v, want nil (release must not fail the resume)", err)
	}

	if got := worker.GetWorkerPod(); got != "free-pod" {
		t.Errorf("assigned worker = %q, want %q", got, "free-pod")
	}

	// The stale worker must already be released: the actor could not have been
	// placed on another worker otherwise.
	if stored := firstAssignment(t, persistence, testWorkerUID("stale-pod")); stored != nil {
		t.Errorf("stale worker still assigned: %v", stored)
	}
}

// TestAssignWorkerAttempt_RetryAfterConflictPicksFreshWorker verifies an
// assignment attempt carries no state from a conflicted predecessor: when a
// concurrent resume wins the picked worker, the loser's retry re-selects from
// the cache instead of re-submitting the same stale version until the backoff
// is exhausted.
func TestAssignWorkerAttempt_RetryAfterConflictPicksFreshWorker(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	contested := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("contested-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "contested-pod",
		WorkerPodUid:    testWorkerUID("contested-pod"),
		SandboxClass:    "gvisor",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
	}
	fallback := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("fallback-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "fallback-pod",
		WorkerPodUid:    testWorkerUID("fallback-pod"),
		SandboxClass:    "gvisor",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
	}
	for _, w := range []*ateapipb.Worker{contested, fallback} {
		if _, err := persistence.CreateWorker(ctx, w); err != nil {
			t.Fatalf("CreateWorker(%s): %v", w.GetWorkerPod(), err)
		}
	}

	// A concurrent resume of another actor wins the contested worker, bumping
	// its stored version past the failed attempt's snapshot.
	seedAssignment(t, persistence, testWorkerUID("contested-pod"), &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "other"},
		ActorUid: "other-actor-uid",
	})

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &ateapipb.ActorTemplate{
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
	}
	_, worker, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt() on retry = %v, want nil (must re-pick a free worker)", err)
	}
	if got := worker.GetWorkerPod(); got != "fallback-pod" {
		t.Errorf("assigned worker = %q, want %q", got, "fallback-pod")
	}

	storedContested := firstAssignment(t, persistence, testWorkerUID("contested-pod"))
	if got := storedContested.GetActorUid(); got != "other-actor-uid" {
		t.Errorf("contested worker assignment = %v, want to remain with actor %q", storedContested, "other-actor-uid")
	}
	storedFallback := firstAssignment(t, persistence, testWorkerUID("fallback-pod"))
	if got := storedFallback.GetActorUid(); got != actor.GetMetadata().GetUid() {
		t.Errorf("fallback worker assignment = %v, want actor uid %q", storedFallback, actor.GetMetadata().GetUid())
	}

	storedActor, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if storedActor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RESUMING {
		t.Errorf("stored actor state = %v, want %v", storedActor.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_RESUMING)
	}
	if got := storedActor.GetStatus().GetWorkerAssignment().GetWorkerPod(); got != "fallback-pod" {
		t.Errorf("stored actor WorkerAssignment.WorkerPod = %q, want %q", got, "fallback-pod")
	}
}

// conflictInjectingStore wraps a store and runs inject exactly once,
// immediately before the first update, simulating a concurrent writer racing
// the step's read-modify-write window.
type conflictInjectingStore struct {
	store.Interface
	once   sync.Once
	inject func()
}

func (c *conflictInjectingStore) UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	c.once.Do(c.inject)
	return c.Interface.UpdateActor(ctx, actorRef, precondition, mutate)
}

func (c *conflictInjectingStore) UpdateActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef, precondition store.Precondition, mutate func(*ateapipb.ActorSnapshotTag) error) (*ateapipb.ActorSnapshotTag, error) {
	c.once.Do(c.inject)
	return c.Interface.UpdateActorSnapshotTag(ctx, tagRef, precondition, mutate)
}

// seedAssignFixture stores one free gvisor worker and a SUSPENDED actor and
// returns the actor plus a started worker cache.
func seedAssignFixture(t *testing.T, ctx context.Context, persistence store.Interface) (*ateapipb.Actor, *workercache.Cache) {
	t.Helper()
	if _, err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-1")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		WorkerPodUid:    testWorkerUID("pod-1"),
		SandboxClass:    "gvisor",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
	}); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	cacheCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}
	return actor, wc
}

// TestAssignWorkerAttempt_ConflictRefreshesActor verifies the actor write's
// conflict handling within a single attempt: a concurrent spec write leaves
// ErrVersionConflict with the refreshed actor returned for the retry, while
// a concurrent transition out of a resumable state aborts the resume.
func TestAssignWorkerAttempt_ConflictRefreshesActor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// mutate is the racing concurrent write applied to the fresh actor.
		mutate func(fresh *ateapipb.Actor)
		// wantRetry means the attempt surfaces ErrVersionConflict with the
		// refreshed actor returned; otherwise Aborted.
		wantRetry bool
		// wantStoredState is the persisted state after Execute.
		wantStoredState ateapipb.ActorState
	}{
		{
			name: "another writer refreshes state.Actor - can recover",
			mutate: func(fresh *ateapipb.Actor) {
				fresh.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"team": "blue"}}
			},
			wantRetry:       true,
			wantStoredState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
		{
			name: "another writer crash the Actor",
			mutate: func(fresh *ateapipb.Actor) {
				fresh.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
			},
			wantRetry:       false,
			wantStoredState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			persistence := newTestPersistence(t)
			actor, wc := seedAssignFixture(t, ctx, persistence)

			var injected *ateapipb.Actor
			st := &conflictInjectingStore{Interface: persistence, inject: func() {
				fresh, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
				if err != nil {
					t.Errorf("inject GetActor: %v", err)
					return
				}
				// Guards on the uid and version just read, so the racing
				// write lands and the attempt under test is the one that loses.
				injected, err = persistence.UpdateActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, store.PreconditionFrom(fresh), func(toUpdate *ateapipb.Actor) error {
					tc.mutate(toUpdate)
					return nil
				})
				if err != nil {
					t.Errorf("inject UpdateActor: %v", err)
				}
			}}

			w := &ActorWorkflow{store: st, workerCache: wc, scheduler: scheduling.New(wc)}
			tmpl := &ateapipb.ActorTemplate{
				SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
			}
			refreshed, _, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)

			if tc.wantRetry {
				if !errors.Is(err, store.ErrVersionConflict) {
					t.Fatalf("assignWorkerAttempt: %v, want ErrVersionConflict", err)
				}
				if got := refreshed.GetMetadata().GetVersion(); got != injected.GetMetadata().GetVersion() {
					t.Errorf("refreshed actor version = %d, want %d (refreshed for the retry)", got, injected.GetMetadata().GetVersion())
				}
				if !proto.Equal(refreshed.GetWorkerSelector(), injected.GetWorkerSelector()) {
					t.Errorf("refreshed actor WorkerSelector = %v, want %v (concurrent write must survive)", refreshed.GetWorkerSelector(), injected.GetWorkerSelector())
				}
			} else {
				if got := status.Code(err); got != codes.Aborted {
					t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.Aborted, err)
				}
			}

			stored, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if stored.GetStatus().GetState() != tc.wantStoredState {
				t.Errorf("stored state = %v, want %v", stored.GetStatus().GetState(), tc.wantStoredState)
			}
		})
	}
}

// TestResumeActorWorkflow_RejectedAndIdempotentPaths covers the two
// short-circuit paths of the resume workflow: rejection of the resume edge
// for a non-resumable actor and the idempotent fast-forward for a RUNNING one.
func TestResumeActorWorkflow_RejectedAndIdempotentPaths(t *testing.T) {
	tests := []struct {
		name      string
		seedState ateapipb.ActorState
		// wantErr true means ResumeActor must fail with FailedPrecondition.
		wantErr bool
		// wantState is the stored state after the call.
		wantState ateapipb.ActorState
	}{
		{
			// The resume edge only exists from SUSPENDED, PAUSED, and
			// RESUMING; a CRASHED actor is rejected by ensureWorkerAssigned
			// and its state is left untouched.
			name:      "crashed rejected",
			seedState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantErr:   true,
			wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		},
		{
			// Resuming a RUNNING actor succeeds idempotently: every step
			// fast-forwards via IsComplete.
			name:      "already running succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", tc.seedState, func(a *ateapipb.Actor) {
				a.Status.WorkerAssignment = &ateapipb.WorkerAssignment{
					Worker:          &ateapipb.ObjectRef{Name: "uid"},
					WorkerNamespace: "wns",
					WorkerPool:      "pool1",
					WorkerPod:       "wpod",
					WorkerPodUid:    "uid",
					WorkerPodIp:     "1.2.3.4",
				}
			})

			actor, resumed, err := w.ResumeActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, false)
			if tc.wantErr {
				if got := status.Code(err); got != codes.FailedPrecondition {
					t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
				}
			} else {
				if err != nil {
					t.Fatalf("ResumeActor failed: %v", err)
				}
				if actor.GetStatus().GetState() != tc.wantState {
					t.Errorf("returned state = %v, want %v", actor.GetStatus().GetState(), tc.wantState)
				}
				if tc.seedState == ateapipb.ActorState_ACTOR_STATE_RUNNING {
					if resumed {
						t.Errorf("expected resumed = false for already running actor, got true")
					}
				} else {
					if !resumed {
						t.Errorf("expected resumed = true for cold activation, got false")
					}
				}
			}

			got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
			if err != nil {
				t.Fatalf("GetActor failed: %v", err)
			}
			if got.GetStatus().GetState() != tc.wantState {
				t.Errorf("stored state = %v, want %v", got.GetStatus().GetState(), tc.wantState)
			}
		})
	}
}

// TestEnsureWorkerAssigned_RejectsNonResumableStates verifies the resume
// edge's state gating: every state outside SUSPENDED, PAUSED, and RESUMING
// is rejected with FailedPrecondition before any dependency is touched.
// (SUSPENDED/PAUSED assignment and RESUMING recovery are exercised by the
// assignment-attempt and worker-validation tests; RUNNING never reaches this
// step because the orchestrator early-returns.)
func TestEnsureWorkerAssigned_RejectsNonResumableStates(t *testing.T) {
	ctx := context.Background()
	w := &ActorWorkflow{}
	for _, st := range allActorStates {
		switch st {
		case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_PAUSED, ateapipb.ActorState_ACTOR_STATE_RESUMING:
			continue
		}
		actor := &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: st}, Metadata: &ateapipb.ResourceMetadata{Name: "id1", Uid: "actor-uid-1"}}
		_, _, err := w.ensureWorkerAssigned(ctx, resources.ActorRef{Name: "id1"}, actor, &ateapipb.ActorTemplate{})
		assertPrerequisiteResult(t, st, err, false)
	}
}

// TestResumeActor_MetricSkipsAlreadyRunningNoop guards the recording rule: the
// router resumes per routed request, so a clean already-running no-op must not
// be recorded, while failures must be.
func TestResumeActor_MetricSkipsAlreadyRunningNoop(t *testing.T) {
	tests := []struct {
		name       string
		seedState  ateapipb.ActorState
		wantRecord bool
	}{
		{name: "already running no-op is skipped", seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING, wantRecord: false},
		{name: "failed resume is recorded", seedState: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantRecord: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			inst, reader := newTestInstruments(t)
			w.instruments = inst

			seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", tt.seedState, func(a *ateapipb.Actor) {
				a.Status.WorkerAssignment = &ateapipb.WorkerAssignment{
					Worker:          &ateapipb.ObjectRef{Name: "uid"},
					WorkerNamespace: "wns",
					WorkerPool:      "pool1",
					WorkerPod:       "wpod",
					WorkerPodUid:    "uid",
				}
			})

			_, _, err := w.ResumeActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, false)
			if tt.wantRecord && err == nil {
				t.Fatal("expected resume to fail, got nil error")
			}
			if !tt.wantRecord && err != nil {
				t.Fatalf("ResumeActor failed: %v", err)
			}

			_, recorded := collectMetric(t, reader, lifecycleOpDurationMetric)
			if recorded != tt.wantRecord {
				t.Errorf("lifecycle datapoint recorded = %v, want %v", recorded, tt.wantRecord)
			}
		})
	}
}

// TestResumeActor_CrashesOnMissingWorkerAssignment verifies that a RESUMING
// actor with no worker assignment is moved to CRASHED by
// ensureWorkerAssigned's recovery validation and the resume fails with
// Aborted. A RESUMING actor always has a worker assigned, so reaching this
// state means the record is corrupt and the actor cannot be recovered.
func TestResumeActor_CrashesOnMissingWorkerAssignment(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_RESUMING, func(a *ateapipb.Actor) {
		a.Status.WorkerAssignment = nil // RESUMING without a worker: corrupt record
	})

	_, _, err := w.ResumeActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, false)
	if got := status.Code(err); got != codes.Aborted {
		t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.Aborted, err)
	}

	got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("stored state = %v, want %v", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_CRASHED)
	}
}

// TestValidateAssignedWorker_WorkerOwnership verifies that RESUMING recovery
// only proceeds on a worker whose assignment still names this actor: the
// recovery path loads the worker by pod name only, so the assignment may have
// been cleared and the worker re-claimed by another actor in the meantime. On
// a mismatch the actor is crashed and the worker — which is not ours — must
// not be written.
func TestValidateAssignedWorker_WorkerOwnership(t *testing.T) {
	ownAssignment := &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "shared"},
		ActorUid: "own-actor-uid",
	}
	otherAssignment := &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-b", Name: "shared"},
		ActorUid: "other-actor-uid",
	}
	staleIncarnationAssignment := &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "shared"},
		ActorUid: "stale-incarnation-uid",
	}

	tests := []struct {
		name         string
		sandboxClass string
		assignment   *ateapipb.ActorAssignment
		// wantCode is codes.OK when validateAssignedWorker must return nil.
		wantCode       codes.Code
		wantActorState ateapipb.ActorState
		// wantAssignment is the assignment expected on the stored worker
		// afterwards; wantWorkerWrite false additionally asserts the worker
		// version did not move (no write at all).
		wantAssignment  *ateapipb.ActorAssignment
		wantWorkerWrite bool
	}{
		{
			name:           "crashes actor and leaves worker untouched when assigned to another actor",
			sandboxClass:   "gvisor",
			assignment:     otherAssignment,
			wantCode:       codes.Aborted,
			wantActorState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantAssignment: otherAssignment,
		},
		{
			name:           "crashes actor and leaves worker untouched when assigned to previous incarnation of same actor",
			sandboxClass:   "gvisor",
			assignment:     staleIncarnationAssignment,
			wantCode:       codes.Aborted,
			wantActorState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantAssignment: staleIncarnationAssignment,
		},
		{
			name:           "crashes actor and leaves worker untouched when assignment is cleared",
			sandboxClass:   "gvisor",
			assignment:     nil,
			wantCode:       codes.Aborted,
			wantActorState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantAssignment: nil,
		},
		{
			name:           "passes for own eligible worker",
			sandboxClass:   "gvisor",
			assignment:     ownAssignment,
			wantCode:       codes.OK,
			wantActorState: ateapipb.ActorState_ACTOR_STATE_RESUMING,
			wantAssignment: ownAssignment,
		},
		{
			name:            "releases own ineligible worker and crashes actor",
			sandboxClass:    "microvm",
			assignment:      ownAssignment,
			wantCode:        codes.Aborted,
			wantActorState:  ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantAssignment:  nil,
			wantWorkerWrite: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			if _, err := persistence.CreateWorker(ctx, &ateapipb.Worker{
				Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-1")},
				WorkerNamespace: "worker-ns",
				WorkerPool:      "pool",
				WorkerPod:       "pod-1",
				WorkerPodUid:    testWorkerUID("pod-1"),
				SandboxClass:    tt.sandboxClass,
				Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
			}); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}
			seedAssignment(t, persistence, testWorkerUID("pod-1"), tt.assignment)
			// Fetch the stored version so the no-write assertion below can
			// detect any optimistic update.
			seeded, err := persistence.GetWorker(ctx, testWorkerUID("pod-1"))
			if err != nil {
				t.Fatalf("GetWorker: %v", err)
			}

			seedWorkflowActor(t, ctx, persistence, resources.ActorRef{Atespace: "team-a", Name: "shared"}, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_RESUMING)

			w := &ActorWorkflow{store: persistence, scheduler: scheduling.New(nil)}
			resumingActor := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared", Uid: "own-actor-uid"},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_RESUMING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker:          &ateapipb.ObjectRef{Name: testWorkerUID("pod-1")},
						WorkerNamespace: "worker-ns",
						WorkerPool:      "pool",
						WorkerPod:       "pod-1",
						WorkerPodUid:    testWorkerUID("pod-1"),
					},
				},
			}
			tmpl := &ateapipb.ActorTemplate{SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR}}
			_, err = w.validateAssignedWorker(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}, resumingActor, tmpl)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}

			actor, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"})
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if actor.GetStatus().GetState() != tt.wantActorState {
				t.Errorf("stored actor state = %v, want %v", actor.GetStatus().GetState(), tt.wantActorState)
			}

			stored, err := persistence.GetWorker(ctx, testWorkerUID("pod-1"))
			if err != nil {
				t.Fatalf("GetWorker: %v", err)
			}
			if got := firstAssignment(t, persistence, testWorkerUID("pod-1")); !proto.Equal(got, tt.wantAssignment) {
				t.Errorf("stored worker assignment = %v, want %v", got, tt.wantAssignment)
			}
			if !tt.wantWorkerWrite && stored.GetMetadata().GetVersion() != seeded.GetMetadata().GetVersion() {
				t.Errorf("worker version moved %d -> %d, want no write", seeded.GetMetadata().GetVersion(), stored.GetMetadata().GetVersion())
			}
		})
	}
}

// TestLoadActorForResume_OnGoldenDataResume verifies the golden-location
// plumbing and the resolved wire scope: when the template's onResume.fromData
// is Golden, a pending data-only restore (a Data durable snapshot, or a
// paused actor whose onPause is Data) additionally resolves the template's
// golden snapshot and rides it as DATA_ON_GOLDEN; otherwise the captured
// scope of the snapshot the restore will use rides out, the local pause
// checkpoint taking precedence over the durable snapshot.
func TestLoadActorForResume_OnGoldenDataResume(t *testing.T) {
	goldenSnapshotURI := someActorSnapshotURI(t, "gs://bucket/golden-root", "ate-golden", "golden-1")
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	tests := []struct {
		name     string
		fromData ateapipb.ResumeSource
		// paused seeds the actor with LocalSnapshotInfo (a pause checkpoint)
		// instead of a durable snapshot (in addition to one when alsoDurable
		// is set); onPause is the template's pause scope, contentScope the
		// durable snapshot's recorded content.
		paused       bool
		alsoDurable  bool
		onPause      ateapipb.SnapshotContentScope
		contentScope ateapipb.SnapshotContentScope
		// builtOnTemplateUID seeds the template UID the actor's guest state
		// was built on; a non-empty value differing from the created
		// template's own UID marks the actor repointed. Empty leaves the
		// field unset.
		builtOnTemplateUID string
		// goldenURI and goldenScope are the template's recorded golden
		// external snapshot; an empty URI means the template has none. A zero
		// scope is treated as Full, the scope a golden snapshot must hold.
		goldenURI     string
		goldenScope   ateapipb.SnapshotContentScope
		wantCode      codes.Code
		wantGoldenURI string
		wantWireScope ateletpb.SnapshotScope
	}{
		{
			name:          "resolves golden location for Data durable snapshot",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			contentScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: goldenSnapshotURI,
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			name:          "resolves golden location for paused actor with Data onPause",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			paused:        true,
			onPause:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: goldenSnapshotURI,
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			// A repointed actor's data resume resolves the replacement
			// template's golden through the repoint path, which takes
			// precedence over the onResume policy but lands on the same
			// golden location.
			name:               "resolves golden location for repointed Data durable snapshot",
			fromData:           ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			builtOnTemplateUID: "replaced-uid",
			goldenURI:          goldenSnapshotURI,
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.OK,
			wantGoldenURI:      goldenSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			// A Full pause snapshot restores from its own content; the policy
			// only governs data-only restores.
			name:          "leaves golden location empty for paused actor with Full onPause",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			paused:        true,
			onPause:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: "",
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			// The restore uses the local pause checkpoint, so its Full onPause
			// scope decides — the stale durable Data snapshot neither pulls in
			// the golden nor narrows the wire scope.
			name:          "local pause scope wins over a stale durable Data snapshot",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			paused:        true,
			alsoDurable:   true,
			onPause:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			contentScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: "",
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			// Legacy snapshots recorded before content_scope existed read as
			// Full on the wire.
			name:          "unspecified captured scope reads as Full",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			contentScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED,
			wantCode:      codes.OK,
			wantGoldenURI: "",
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			name:         "fails when golden snapshot is not Full",
			fromData:     ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			contentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:    goldenSnapshotURI,
			goldenScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			wantCode:     codes.FailedPrecondition,
		},
		{
			name:         "fails when template has no golden snapshot",
			fromData:     ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			contentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			wantCode:     codes.FailedPrecondition,
		},
		{
			name:         "fails when the golden snapshot uri is malformed",
			fromData:     ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			contentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:    "golden-1",
			goldenScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:     codes.DataLoss,
		},
		{
			// A Full snapshot restores from its own content even under
			// Golden fromData (e.g. taken before the template switched).
			name:          "leaves golden location empty for Full snapshot",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			contentScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: "",
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			name:          "leaves golden location empty under ColdBoot fromData",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT,
			contentScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: "",
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			var seedOpts []func(*ateapipb.Actor)
			if tt.paused {
				seedOpts = append(seedOpts, func(a *ateapipb.Actor) {
					a.Status.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{SnapshotName: "pause-1"}
				})
			}
			if !tt.paused || tt.alsoDurable {
				seedOpts = append(seedOpts, func(a *ateapipb.Actor) {
					a.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{
						SnapshotUri:  someActorSnapshotURI(t, testStorageLocation, actorRef.Atespace, "snap-1"),
						ContentScope: tt.contentScope,
					}
					a.Status.CurrentActorTemplateUid = tt.builtOnTemplateUID
				})
			}
			actorState := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
			if tt.paused {
				actorState = ateapipb.ActorState_ACTOR_STATE_PAUSED
			}
			seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", actorState, seedOpts...)

			storetest.MustCreateAtespace(t, ctx, persistence, "ns")
			tmpl := &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
				SnapshotsConfig: &ateapipb.SnapshotsConfig{
					OnPause:  tt.onPause,
					OnResume: &ateapipb.OnResumeConfig{FromData: tt.fromData},
				},
			}
			if tt.goldenURI != "" {
				tmpl.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
					GoldenSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: tt.goldenURI, ContentScope: tt.goldenScope},
				}}
			}
			if _, err := persistence.CreateActorTemplate(ctx, tmpl); err != nil {
				t.Fatalf("create template: %v", err)
			}

			w := &ActorWorkflow{store: persistence}
			_, _, src, err := w.loadActorForResume(ctx, actorRef, false)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if err != nil {
				return
			}
			if got := src.GoldenForDataSnapshotURI.String(); got != tt.wantGoldenURI {
				t.Errorf("src.GoldenSnapshotURI = %q, want %q", got, tt.wantGoldenURI)
			}
			if src.WireScope != tt.wantWireScope {
				t.Errorf("src.WireScope = %v, want %v", src.WireScope, tt.wantWireScope)
			}
		})
	}
}

// TestLoadActorForResume_GoldenFallbackRejectsNonFullGolden covers the
// golden-fallback branch (actor with no snapshot of its own): a golden
// snapshot recorded with a non-Full scope holds no guest state, so the resume
// must fail with a clear error instead of forwarding its scope to atelet
// with no golden location (which atelet rejects with a confusing
// "missing bucket" validation error).
func TestLoadActorForResume_GoldenFallbackRejectsNonFullGolden(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	storetest.MustCreateAtespace(t, ctx, persistence, "ns")
	if _, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
		Status: &ateapipb.ActorTemplateStatus{
			GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
				GoldenSnapshot: &ateapipb.ExternalSnapshot{
					SnapshotUri:  someActorSnapshotURI(t, "gs://bucket/golden-root", "ate-golden", "golden-1"),
					ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				},
			},
		},
	}); err != nil {
		t.Fatalf("create template: %v", err)
	}

	w := &ActorWorkflow{store: persistence}
	_, _, _, err := w.loadActorForResume(ctx, actorRef, false)
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status.Code(err) = %v, want FailedPrecondition (err: %v)", got, err)
	}
}

// TestLoadActorForResume_Repointed covers the resume of a repointed actor:
// when the guest state the restore will boot from was captured under a
// template other than the actor's current one, it belongs to the replaced
// template and the restore must ride the replacement template's golden
// snapshot — for Full and Data snapshots alike — and fail when that golden
// is not usable. A new template whose onResume policy selects ColdBoot, or
// an explicit boot request, instead resumes data-only, with no golden
// required. The actor's template stamp (Status.CurrentActorTemplateUid) is
// the detection signal; actors without one (never committed RUNNING) restore
// themselves.
func TestLoadActorForResume_Repointed(t *testing.T) {
	goldenSnapshotURI := someActorSnapshotURI(t, "gs://bucket/golden-root", "ate-golden", "golden-1")
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	durableSnapshotURI := someActorSnapshotURI(t, testStorageLocation, actorRef.Atespace, "snap-1")

	tests := []struct {
		name string
		// builtOnTemplateUID seeds the actor's template stamp
		// (Status.CurrentActorTemplateUid), the template UID its guest state
		// was built on; "current" stands for the created template's own UID,
		// "" leaves the stamp unset (an actor that never committed RUNNING:
		// created from scratch or from a snapshot clone).
		builtOnTemplateUID string
		contentScope       ateapipb.SnapshotContentScope
		noSnapshot         bool
		// paused seeds the actor PAUSED with a local pause snapshot, the
		// boot source that takes precedence over any durable snapshot.
		paused bool
		// goldenURI and goldenScope are the template's recorded golden
		// external snapshot; an empty URI means the template has none.
		goldenURI   string
		goldenScope ateapipb.SnapshotContentScope
		// fromData seeds the template's onResume policy; UNSPECIFIED leaves
		// the config unset.
		fromData ateapipb.ResumeSource
		// boot is the caller's explicit cold-boot request.
		boot     bool
		wantCode codes.Code
		// wantSnapshotURI is the boot snapshot the resume must resolve: the
		// actor's own durable snapshot, the golden restored as the actor's
		// own content, or empty for a cold boot from the spec.
		wantSnapshotURI string
		wantGoldenURI   string
		wantWireScope   ateletpb.SnapshotScope
	}{
		{
			// The headline fix: a repointed Full snapshot no longer restores
			// (or cold-boots) its own guest state — it rides the new golden.
			name:               "repointed Full snapshot rides the new template's golden",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:          goldenSnapshotURI,
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.OK,
			wantSnapshotURI:    durableSnapshotURI,
			wantGoldenURI:      goldenSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			// The onResume policy defaults to ColdBoot here; the repoint
			// path resolves the golden anyway.
			name:               "repointed Data snapshot rides the golden without the Golden policy",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:          goldenSnapshotURI,
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.OK,
			wantSnapshotURI:    durableSnapshotURI,
			wantGoldenURI:      goldenSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			// An explicit Golden policy resolves the golden the same way the
			// unset default does.
			name:               "repointed Full snapshot rides the golden under an explicit Golden policy",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			fromData:           ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			goldenURI:          goldenSnapshotURI,
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.OK,
			wantSnapshotURI:    durableSnapshotURI,
			wantGoldenURI:      goldenSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			// A ColdBoot policy resumes data-only: the snapshot's guest state
			// is discarded, and no golden is required (none is seeded here).
			name:               "repointed Full snapshot resumes data-only under a ColdBoot policy",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			fromData:           ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT,
			wantCode:           codes.OK,
			wantSnapshotURI:    durableSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
		{
			name:               "repointed Data snapshot resumes data-only under a ColdBoot policy",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			fromData:           ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT,
			wantCode:           codes.OK,
			wantSnapshotURI:    durableSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
		{
			name:               "fails when the new template has no golden snapshot",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.FailedPrecondition,
		},
		{
			// The escape hatch for the case right above: an explicit boot
			// request resumes data-only, so no golden is required.
			name:               "explicit boot resumes a repointed actor data-only without a golden",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			boot:               true,
			wantCode:           codes.OK,
			wantSnapshotURI:    durableSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
		{
			name:               "explicit boot skips the golden even when one is available",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			boot:               true,
			goldenURI:          goldenSnapshotURI,
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.OK,
			wantSnapshotURI:    durableSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
		{
			name:               "fails when the golden snapshot uri is malformed",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:          "golden-1",
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.DataLoss,
		},
		{
			name:               "fails when the golden snapshot is not Full",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:          goldenSnapshotURI,
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			wantCode:           codes.FailedPrecondition,
		},
		{
			name:               "snapshot taken under the current template restores itself",
			builtOnTemplateUID: "current",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.OK,
			wantSnapshotURI:    durableSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			name:               "snapshot without a recorded template UID restores itself",
			builtOnTemplateUID: "",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.OK,
			wantSnapshotURI:    durableSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			// A cold boot goes out as a RunRequest, which carries no scope;
			// the resolved Full is inert.
			name:          "no durable snapshot resolves no golden",
			noSnapshot:    true,
			wantCode:      codes.OK,
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			// The repointed no-snapshot case: nothing captured under the
			// replaced template carries over, so the new template's golden
			// boots as the actor's own Full content.
			name:               "repointed actor with no snapshot boots from the new template's golden",
			builtOnTemplateUID: "some-other-uid",
			noSnapshot:         true,
			goldenURI:          goldenSnapshotURI,
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.OK,
			wantSnapshotURI:    goldenSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			name:               "repointed actor with no snapshot cold boots when the template has no golden",
			builtOnTemplateUID: "some-other-uid",
			noSnapshot:         true,
			wantCode:           codes.OK,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			name:               "repointed actor with no snapshot fails when the golden uri is malformed",
			builtOnTemplateUID: "some-other-uid",
			noSnapshot:         true,
			goldenURI:          "golden-1",
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.DataLoss,
		},
		{
			name:               "repointed actor with no snapshot rejects a non-Full golden",
			builtOnTemplateUID: "some-other-uid",
			noSnapshot:         true,
			goldenURI:          goldenSnapshotURI,
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			wantCode:           codes.FailedPrecondition,
		},
		{
			// The local pause snapshot was captured under the current
			// template (a repoint requires SUSPENDED, and suspending clears
			// the local snapshot). It restores itself — no golden required,
			// none seeded here.
			name:               "local pause snapshot under the current template restores itself",
			paused:             true,
			builtOnTemplateUID: "current",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.OK,
			wantSnapshotURI:    durableSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			// The one way a paused actor's template can change: deleted and
			// recreated under the same name. The local snapshot's guest state
			// belongs to the deleted template, so the restore rides the golden.
			name:               "local pause snapshot under a recreated template rides the golden",
			paused:             true,
			builtOnTemplateUID: "some-other-uid",
			noSnapshot:         true,
			goldenURI:          goldenSnapshotURI,
			goldenScope:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:           codes.OK,
			wantGoldenURI:      goldenSnapshotURI,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			name:               "fails when the recreated template has no golden snapshot",
			paused:             true,
			builtOnTemplateUID: "some-other-uid",
			noSnapshot:         true,
			wantCode:           codes.FailedPrecondition,
		},
		{
			name:               "local pause snapshot under a recreated ColdBoot template resumes data-only",
			paused:             true,
			builtOnTemplateUID: "some-other-uid",
			noSnapshot:         true,
			fromData:           ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT,
			wantCode:           codes.OK,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
		{
			// Actors paused before the template stamp existed cannot have
			// been repointed while paused; they restore themselves.
			name:               "local pause snapshot without a template stamp restores itself",
			paused:             true,
			builtOnTemplateUID: "",
			noSnapshot:         true,
			wantCode:           codes.OK,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			storetest.MustCreateAtespace(t, ctx, persistence, "ns")
			toCreate := &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
			}
			if tt.fromData != ateapipb.ResumeSource_RESUME_SOURCE_UNSPECIFIED {
				toCreate.SnapshotsConfig = &ateapipb.SnapshotsConfig{
					OnResume: &ateapipb.OnResumeConfig{FromData: tt.fromData},
				}
			}
			if tt.goldenURI != "" {
				toCreate.Status = &ateapipb.ActorTemplateStatus{
					GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
						GoldenSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: tt.goldenURI, ContentScope: tt.goldenScope},
					},
				}
			}
			tmpl, err := persistence.CreateActorTemplate(ctx, toCreate)
			if err != nil {
				t.Fatalf("create template: %v", err)
			}
			if tmpl.GetMetadata().GetUid() == "" {
				t.Fatal("created template has no UID; the matching case would be vacuous")
			}

			var seedOpts []func(*ateapipb.Actor)
			if !tt.noSnapshot {
				seedOpts = append(seedOpts, func(a *ateapipb.Actor) {
					a.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{
						SnapshotUri:  someActorSnapshotURI(t, testStorageLocation, actorRef.Atespace, "snap-1"),
						ContentScope: tt.contentScope,
					}
				})
			}
			actorState := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
			if tt.paused {
				actorState = ateapipb.ActorState_ACTOR_STATE_PAUSED
				seedOpts = append(seedOpts, func(a *ateapipb.Actor) {
					a.Status.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{SnapshotName: "pause-1"}
				})
			}
			if tt.builtOnTemplateUID != "" {
				uid := tt.builtOnTemplateUID
				if uid == "current" {
					uid = tmpl.GetMetadata().GetUid()
				}
				seedOpts = append(seedOpts, func(a *ateapipb.Actor) {
					a.Status.CurrentActorTemplateUid = uid
				})
			}
			seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", actorState, seedOpts...)

			w := &ActorWorkflow{store: persistence}
			_, _, src, err := w.loadActorForResume(ctx, actorRef, tt.boot)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if err != nil {
				return
			}
			if got := src.SnapshotURI.String(); got != tt.wantSnapshotURI {
				t.Errorf("src.SnapshotURI = %q, want %q", got, tt.wantSnapshotURI)
			}
			if got := src.GoldenForDataSnapshotURI.String(); got != tt.wantGoldenURI {
				t.Errorf("src.GoldenSnapshotURI = %q, want %q", got, tt.wantGoldenURI)
			}
			if src.WireScope != tt.wantWireScope {
				t.Errorf("src.WireScope = %v, want %v", src.WireScope, tt.wantWireScope)
			}
		})
	}
}

// TestResolveResumeScopeAfterUpdatedTemplate covers the repointed-actor scope
// decision in isolation, with the durable snapshot record handed in directly:
// an actor with no snapshot at all boots the new template's golden as its own
// content; a Data-scoped capture follows the onResume policy (data-only by
// default, riding the golden when the policy selects it); any other capture
// holds the replaced template's guest state and must ride the golden. A
// paused actor's capture scope is the template's onPause policy, not the
// stale durable record's.
func TestResolveResumeScopeAfterUpdatedTemplate(t *testing.T) {
	goldenSnapshotURI := someActorSnapshotURI(t, "gs://bucket/golden-root", "ate-golden", "golden-1")
	durableSnapshotURI := someActorSnapshotURI(t, testStorageLocation, "team-a", "snap-1")

	tests := []struct {
		name string
		// hasDurable hands in a durable snapshot captured with durableScope;
		// without it the zero durableSnapshot stands for "none recorded".
		hasDurable   bool
		durableScope ateapipb.SnapshotContentScope
		// paused gives the actor a local pause snapshot; onPause is the
		// template's pause scope, which then stands in as the capture scope.
		paused  bool
		onPause ateapipb.SnapshotContentScope
		// fromData seeds the template's onResume policy; UNSPECIFIED leaves
		// it unset (ColdBoot semantics).
		fromData ateapipb.ResumeSource
		// goldenURI and goldenScope are the template's recorded golden
		// external snapshot; an empty URI means the template has none.
		goldenURI   string
		goldenScope ateapipb.SnapshotContentScope
		wantCode    codes.Code
		// wantSnapshotURI is the resolved boot snapshot: the actor's own
		// durable snapshot, the golden restored as the actor's own content,
		// or empty for a cold boot from the spec.
		wantSnapshotURI string
		wantGoldenURI   string
		wantWireScope   ateletpb.SnapshotScope
	}{
		{
			// Nothing captured under the replaced template carries over, so
			// the new template's golden boots as the actor's own Full content.
			name:            "no snapshot boots the golden as the actor's own content",
			goldenURI:       goldenSnapshotURI,
			goldenScope:     ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:        codes.OK,
			wantSnapshotURI: goldenSnapshotURI,
			wantWireScope:   ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			// No snapshot and no golden: cold boot from the spec. The Full
			// scope is inert — a cold boot goes out as a RunRequest, which
			// carries no scope.
			name:          "no snapshot and no golden cold boots from the spec",
			wantCode:      codes.OK,
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			name:        "no snapshot fails when the golden uri is malformed",
			goldenURI:   "golden-1",
			goldenScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:    codes.DataLoss,
		},
		{
			// A non-Full golden holds no guest state to boot from.
			name:        "no snapshot rejects a non-Full golden",
			goldenURI:   goldenSnapshotURI,
			goldenScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			wantCode:    codes.FailedPrecondition,
		},
		{
			// A Data capture follows the onResume policy like any data
			// resume; the unset default is ColdBoot, so no golden is needed
			// (none is seeded here).
			name:            "Data snapshot resumes data-only under the default ColdBoot policy",
			hasDurable:      true,
			durableScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			wantCode:        codes.OK,
			wantSnapshotURI: durableSnapshotURI,
			wantWireScope:   ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
		{
			name:            "Data snapshot rides the golden under the Golden policy",
			hasDurable:      true,
			durableScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			fromData:        ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			goldenURI:       goldenSnapshotURI,
			goldenScope:     ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:        codes.OK,
			wantSnapshotURI: durableSnapshotURI,
			wantGoldenURI:   goldenSnapshotURI,
			wantWireScope:   ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			// A Full capture holds the replaced template's guest state, so
			// only its durable data survives: the restore must ride the new
			// template's golden even without the Golden policy.
			name:            "Full snapshot rides the golden regardless of policy",
			hasDurable:      true,
			durableScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:       goldenSnapshotURI,
			goldenScope:     ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:        codes.OK,
			wantSnapshotURI: durableSnapshotURI,
			wantGoldenURI:   goldenSnapshotURI,
			wantWireScope:   ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			// Legacy snapshots recorded before content_scope existed read as
			// Full, so they ride the golden the same way.
			name:            "legacy Unspecified capture scope rides the golden like Full",
			hasDurable:      true,
			durableScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED,
			goldenURI:       goldenSnapshotURI,
			goldenScope:     ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:        codes.OK,
			wantSnapshotURI: durableSnapshotURI,
			wantGoldenURI:   goldenSnapshotURI,
			wantWireScope:   ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			name:         "Full snapshot fails when the template has no golden",
			hasDurable:   true,
			durableScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:     codes.FailedPrecondition,
		},
		{
			name:         "Full snapshot fails when the golden uri is malformed",
			hasDurable:   true,
			durableScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:    "golden-1",
			goldenScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:     codes.DataLoss,
		},
		{
			name:         "Full snapshot rejects a non-Full golden",
			hasDurable:   true,
			durableScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:    goldenSnapshotURI,
			goldenScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			wantCode:     codes.FailedPrecondition,
		},
		{
			// The restore will use the local pause checkpoint, so the
			// template's Data onPause decides — not the stale Full durable
			// record — and the resume stays data-only with no golden needed.
			name:            "paused actor reads its scope from onPause, not the stale durable record",
			paused:          true,
			onPause:         ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			hasDurable:      true,
			durableScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:        codes.OK,
			wantSnapshotURI: durableSnapshotURI,
			wantWireScope:   ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
		{
			name:          "paused Data onPause rides the golden under the Golden policy",
			paused:        true,
			onPause:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: goldenSnapshotURI,
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			// A Full pause checkpoint carries the replaced template's guest
			// state like any Full capture: it must ride the golden.
			name:          "paused Full onPause rides the golden",
			paused:        true,
			onPause:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: goldenSnapshotURI,
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actor := &ateapipb.Actor{Status: &ateapipb.ActorStatus{}}
			if tt.paused {
				actor.Status.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{SnapshotName: "pause-1"}
			}

			tmpl := &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1", Uid: "new-template-uid"},
				SnapshotsConfig: &ateapipb.SnapshotsConfig{
					OnPause:  tt.onPause,
					OnResume: &ateapipb.OnResumeConfig{FromData: tt.fromData},
				},
			}
			if tt.goldenURI != "" {
				tmpl.Status = &ateapipb.ActorTemplateStatus{
					GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
						GoldenSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: tt.goldenURI, ContentScope: tt.goldenScope},
					},
				}
			}

			var durable durableSnapshot
			if tt.hasDurable {
				uri, err := resources.ParseSnapshotURI(durableSnapshotURI)
				if err != nil {
					t.Fatalf("ParseSnapshotURI(%q): %v", durableSnapshotURI, err)
				}
				durable = durableSnapshot{exists: true, uri: uri, capturedScope: tt.durableScope}
			}

			_, _, src, err := resolveResumeScopeAfterUpdatedTemplate(actor, tmpl, durable)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if err != nil {
				return
			}
			if got := src.SnapshotURI.String(); got != tt.wantSnapshotURI {
				t.Errorf("src.SnapshotURI = %q, want %q", got, tt.wantSnapshotURI)
			}
			if got := src.GoldenForDataSnapshotURI.String(); got != tt.wantGoldenURI {
				t.Errorf("src.GoldenForDataSnapshotURI = %q, want %q", got, tt.wantGoldenURI)
			}
			if src.WireScope != tt.wantWireScope {
				t.Errorf("src.WireScope = %v, want %v", src.WireScope, tt.wantWireScope)
			}
		})
	}
}

func TestLoadActorForResume_RunningActorShortCircuits(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	// Seed the actor as RUNNING. Note: No snapshot or template is seeded in the
	// store, proving that loadActorForResume short-circuits before attempting
	// to fetch either.
	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "missing-tmpl", ateapipb.ActorState_ACTOR_STATE_RUNNING)

	w := &ActorWorkflow{store: persistence}

	actor, tmpl, src, err := w.loadActorForResume(ctx, actorRef, false)
	if err != nil {
		t.Fatalf("loadActorForResume() unexpected error = %v", err)
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want %v", actor.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_RUNNING)
	}
	if tmpl != nil {
		t.Errorf("expected nil template, got %v", tmpl)
	}
	if !src.SnapshotURI.IsZero() || !src.GoldenForDataSnapshotURI.IsZero() {
		t.Errorf("expected empty snapshot source, got %+v", src)
	}
}
