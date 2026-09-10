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
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/ateattr"
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
// record, so the next resume can detect an updated template by UID.
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

func (c *conflictInjectingStore) UpdateTag(ctx context.Context, tagRef resources.TagRef, precondition store.Precondition, mutate func(*ateapipb.Tag) error) (*ateapipb.Tag, error) {
	c.once.Do(c.inject)
	return c.Interface.UpdateTag(ctx, tagRef, precondition, mutate)
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

// resumeSeed describes the stored state a loadActorForResume test builds: the
// actor's snapshot state, and the ns/tmpl1 template's snapshots config and
// recorded golden snapshot.
type resumeSeed struct {
	// paused stores the actor PAUSED with a local pause checkpoint; noSnapshot
	// leaves the actor without any snapshot; otherwise a durable snapshot
	// captured at contentScope is seeded.
	paused       bool
	noSnapshot   bool
	contentScope ateapipb.SnapshotContentScope
	// pausedScope is the content scope recorded on the pause checkpoint; the
	// zero value seeds a legacy checkpoint with no recorded scope.
	pausedScope ateapipb.SnapshotContentScope
	// onPause and fromData seed the template's SnapshotsConfig.
	onPause  ateapipb.SnapshotContentScope
	fromData ateapipb.ResumeSource
	// goldenURI and goldenScope are the template's recorded golden external
	// snapshot; an empty URI means the template has none.
	goldenURI   string
	goldenScope ateapipb.SnapshotContentScope
}

// seedResumeTemplate creates the ns atespace and the ns/tmpl1 template the
// seed describes, returning the stored template (for its assigned UID).
func seedResumeTemplate(t *testing.T, ctx context.Context, persistence store.Interface, seed resumeSeed) *ateapipb.ActorTemplate {
	t.Helper()
	storetest.MustCreateAtespace(t, ctx, persistence, "ns")
	tmpl := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			OnPause:  seed.onPause,
			OnResume: &ateapipb.OnResumeConfig{FromData: seed.fromData},
		},
	}
	if seed.goldenURI != "" {
		tmpl.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
			GoldenSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: seed.goldenURI, ContentScope: seed.goldenScope},
		}}
	}
	stored, err := persistence.CreateActorTemplate(ctx, tmpl)
	if err != nil {
		t.Fatalf("create template: %v", err)
	}
	return stored
}

// seedResumeActor stores the actor the seed describes, bound to ns/tmpl1, and
// returns the durable snapshot URI it was seeded with ("" when none was).
// opts mutate the actor after the seed's snapshot state is applied.
func seedResumeActor(t *testing.T, ctx context.Context, persistence store.Interface, actorRef resources.ActorRef, seed resumeSeed, opts ...func(*ateapipb.Actor)) string {
	t.Helper()
	durableSnapshotURI := ""
	var seedOpts []func(*ateapipb.Actor)
	actorState := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	switch {
	case seed.paused:
		actorState = ateapipb.ActorState_ACTOR_STATE_PAUSED
		seedOpts = append(seedOpts, func(a *ateapipb.Actor) {
			a.Status.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{SnapshotName: "pause-1", ContentScope: seed.pausedScope}
		})
	case !seed.noSnapshot:
		durableSnapshotURI = someActorSnapshotURI(t, testStorageLocation, actorRef.Atespace, "snap-1")
		seedOpts = append(seedOpts, func(a *ateapipb.Actor) {
			a.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{
				SnapshotUri:  durableSnapshotURI,
				ContentScope: seed.contentScope,
			}
		})
	}
	seedOpts = append(seedOpts, opts...)
	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", actorState, seedOpts...)
	return durableSnapshotURI
}

// TestLoadActorForResume_OnGoldenDataResume verifies the golden-location
// plumbing: when the template's onResume.fromData is Golden, a pending
// data-only restore (a Data durable snapshot, or a paused actor whose
// onPause is Data) additionally resolves the template's golden snapshot
func TestLoadActorForResume_OnGoldenDataResume(t *testing.T) {
	goldenSnapshotURI := someActorSnapshotURI(t, "gs://bucket/golden-root", "ate-golden", "golden-1")
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	tests := []struct {
		name     string
		fromData ateapipb.ResumeSource
		// paused seeds the actor with LocalSnapshotInfo (a pause checkpoint)
		// instead of a durable snapshot; pausedScope is the scope recorded on
		// that checkpoint (zero for a legacy checkpoint without one); onPause
		// is the template's pause scope, contentScope the durable snapshot's
		// recorded content.
		paused       bool
		pausedScope  ateapipb.SnapshotContentScope
		onPause      ateapipb.SnapshotContentScope
		contentScope ateapipb.SnapshotContentScope
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
			// A legacy checkpoint records no scope, so the template's onPause
			// stands in for it.
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
			// The scope recorded on the checkpoint is authoritative: a Data
			// capture rides the golden even when the template's onPause says
			// Full.
			name:          "recorded Data pause scope outranks Full onPause",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			paused:        true,
			pausedScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			onPause:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: goldenSnapshotURI,
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			// The reverse direction: a Full capture restores from its own
			// content even when the template's onPause says Data.
			name:          "recorded Full pause scope outranks Data onPause",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			paused:        true,
			pausedScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			onPause:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: "",
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
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
			// A Data pause checkpoint under a non-Golden policy restores its
			// data alone: no golden ride, plain Data wire scope.
			name:          "leaves golden location empty for paused actor with Data onPause under ColdBoot fromData",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT,
			paused:        true,
			onPause:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: "",
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
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

			seed := resumeSeed{
				paused:       tt.paused,
				pausedScope:  tt.pausedScope,
				contentScope: tt.contentScope,
				onPause:      tt.onPause,
				fromData:     tt.fromData,
				goldenURI:    tt.goldenURI,
				goldenScope:  tt.goldenScope,
			}
			seedResumeTemplate(t, ctx, persistence, seed)
			seedResumeActor(t, ctx, persistence, actorRef, seed)

			w := &ActorWorkflow{store: persistence}
			_, _, src, err := w.loadActorForResume(ctx, actorRef, false)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if err != nil {
				return
			}
			if got := src.GoldenForDataSnapshotURI.String(); got != tt.wantGoldenURI {
				t.Errorf("src.GoldenForDataSnapshotURI = %q, want %q", got, tt.wantGoldenURI)
			}
			if src.WireScope != tt.wantWireScope {
				t.Errorf("src.WireScope = %v, want %v", src.WireScope, tt.wantWireScope)
			}
			wantKind := restoreDurableSnapshot
			if tt.paused {
				wantKind = restoreLocalSnapshot
			}
			if src.Kind != wantKind {
				t.Errorf("src.Kind = %v, want %v", src.Kind, wantKind)
			}
		})
	}
}

// TestLoadActorForResume_UnparseableDurableSnapshotURI pins that a recorded
// durable snapshot URI that fails to parse fails the resume with DataLoss
// only when that snapshot is the boot source. A paused actor's local
// checkpoint outranks the durable snapshot it shadows, so the shadowed URI
// is never parsed and cannot fail a restore that does not consume it.
func TestLoadActorForResume_UnparseableDurableSnapshotURI(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	tests := []struct {
		name     string
		paused   bool
		wantCode codes.Code
	}{
		{name: "paused actor ignores an unparseable shadowed durable snapshot", paused: true, wantCode: codes.OK},
		{name: "suspended actor with an unparseable durable snapshot", wantCode: codes.DataLoss},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			seed := resumeSeed{paused: tt.paused, noSnapshot: true}
			seedResumeTemplate(t, ctx, persistence, seed)
			seedResumeActor(t, ctx, persistence, actorRef, seed, func(a *ateapipb.Actor) {
				a.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: "not-a-snapshot-uri"}
			})

			w := &ActorWorkflow{store: persistence}
			_, _, src, err := w.loadActorForResume(ctx, actorRef, false)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if err != nil {
				return
			}
			if src.Kind != restoreLocalSnapshot {
				t.Errorf("src.Kind = %v, want %v", src.Kind, restoreLocalSnapshot)
			}
			if !src.SnapshotURI.IsZero() {
				t.Errorf("src.SnapshotURI = %q, want zero: the shadowed durable snapshot is not the boot source", src.SnapshotURI.String())
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

	seed := resumeSeed{
		noSnapshot:  true,
		goldenURI:   someActorSnapshotURI(t, "gs://bucket/golden-root", "ate-golden", "golden-1"),
		goldenScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
	}
	seedResumeTemplate(t, ctx, persistence, seed)
	seedResumeActor(t, ctx, persistence, actorRef, seed)

	w := &ActorWorkflow{store: persistence}
	_, _, _, err := w.loadActorForResume(ctx, actorRef, false)
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status.Code(err) = %v, want FailedPrecondition (err: %v)", got, err)
	}
	if !strings.Contains(err.Error(), "regenerate the golden snapshot") {
		t.Errorf("error %q does not tell the operator to regenerate the golden snapshot", err)
	}
}

// TestLoadActorForResume_GoldenFallbackRestoresGoldenAsFull covers the
// golden-fallback success path: an actor with no snapshot of its own boots
// from the template's golden snapshot as its own content, always at FULL
// wire scope (a golden snapshot is FULL by construction; an Unspecified
// recorded scope just predates the scope field).
func TestLoadActorForResume_GoldenFallbackRestoresGoldenAsFull(t *testing.T) {
	goldenSnapshotURI := someActorSnapshotURI(t, "gs://bucket/golden-root", "ate-golden", "golden-1")
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	tests := []struct {
		name        string
		goldenScope ateapipb.SnapshotContentScope
	}{
		{name: "Full golden", goldenScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
		{name: "legacy golden without a recorded scope", goldenScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			seed := resumeSeed{
				noSnapshot:  true,
				goldenURI:   goldenSnapshotURI,
				goldenScope: tt.goldenScope,
			}
			seedResumeTemplate(t, ctx, persistence, seed)
			seedResumeActor(t, ctx, persistence, actorRef, seed)

			w := &ActorWorkflow{store: persistence}
			_, _, src, err := w.loadActorForResume(ctx, actorRef, false)
			if err != nil {
				t.Fatalf("loadActorForResume: %v", err)
			}
			if got := src.SnapshotURI.String(); got != goldenSnapshotURI {
				t.Errorf("src.SnapshotURI = %q, want the golden %q", got, goldenSnapshotURI)
			}
			if !src.GoldenForDataSnapshotURI.IsZero() {
				t.Errorf("src.GoldenForDataSnapshotURI = %q, want empty", src.GoldenForDataSnapshotURI)
			}
			if src.WireScope != ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL {
				t.Errorf("src.WireScope = %v, want %v", src.WireScope, ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL)
			}
			if src.Kind != restoreGoldenSnapshot {
				t.Errorf("src.Kind = %v, want %v", src.Kind, restoreGoldenSnapshot)
			}
		})
	}
}

// TestLoadActorForResume_TemplateReplaced covers the detection of an actor
// whose ActorTemplate was updated: the actor records the template UID its
// guest state was built on, and a mismatch with its current template means
// the snapshot's guest state must not be restored, forcing the restore
// operation to data-only. Such a restore never rides the golden snapshot, so
// the onResume policy must not be consulted: a Golden policy with no usable
// golden must not block the resume.
func TestLoadActorForResume_TemplateReplaced(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	tests := []struct {
		name string
		// builtOnTemplateUID seeds the template UID the actor's guest state
		// was built on; "current" stands for the created template's own UID,
		// "" leaves the field unset (an actor from before it was recorded).
		builtOnTemplateUID string
		noSnapshot         bool
		// paused seeds a PAUSED actor holding only a local pause checkpoint
		// instead of a durable snapshot; onPause is the template's pause
		// scope, contentScope the durable snapshot's recorded content.
		paused       bool
		onPause      ateapipb.SnapshotContentScope
		contentScope ateapipb.SnapshotContentScope
		// fromData seeds the template's onResume policy. No case seeds a
		// golden snapshot, so a case that consulted the Golden policy would
		// fail with FailedPrecondition instead of resolving its scope.
		fromData ateapipb.ResumeSource
		// wantWireScope is the resolved restore scope: DATA for the snapshot
		// of an actor whose template was updated, FULL for a snapshot
		// restored as-is, and unspecified (inert) for a cold boot with no
		// snapshot or golden.
		wantWireScope ateletpb.SnapshotScope
	}{
		{name: "snapshot taken under the current template", builtOnTemplateUID: "current", contentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL},
		{name: "snapshot taken under a replaced template", builtOnTemplateUID: "some-other-uid", contentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA},
		{name: "snapshot without a recorded template UID", builtOnTemplateUID: "", contentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL},
		{name: "no durable snapshot", noSnapshot: true, wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED},
		{name: "pause checkpoint taken under the current template", paused: true, builtOnTemplateUID: "current", wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL},
		{name: "pause checkpoint taken under a replaced template", paused: true, builtOnTemplateUID: "some-other-uid", wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA},
		{
			name:               "Data snapshot from an updated template under the Golden policy skips the absent golden",
			builtOnTemplateUID: "some-other-uid",
			contentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			fromData:           ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
		{
			name:               "Data pause checkpoint from an updated template under the Golden policy skips the absent golden",
			paused:             true,
			builtOnTemplateUID: "some-other-uid",
			onPause:            ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			fromData:           ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			wantWireScope:      ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			seed := resumeSeed{
				paused:       tt.paused,
				noSnapshot:   tt.noSnapshot,
				contentScope: tt.contentScope,
				onPause:      tt.onPause,
				fromData:     tt.fromData,
			}
			tmpl := seedResumeTemplate(t, ctx, persistence, seed)
			if tmpl.GetMetadata().GetUid() == "" {
				t.Fatal("created template has no UID; the matching case would be vacuous")
			}

			var seedOpts []func(*ateapipb.Actor)
			if !tt.noSnapshot {
				uid := tt.builtOnTemplateUID
				if uid == "current" {
					uid = tmpl.GetMetadata().GetUid()
				}
				seedOpts = append(seedOpts, func(a *ateapipb.Actor) {
					a.Status.CurrentActorTemplateUid = uid
				})
			}
			seedResumeActor(t, ctx, persistence, actorRef, seed, seedOpts...)

			w := &ActorWorkflow{store: persistence}
			_, _, src, err := w.loadActorForResume(ctx, actorRef, false)
			if err != nil {
				t.Fatalf("loadActorForResume: %v", err)
			}
			if src.WireScope != tt.wantWireScope {
				t.Errorf("src.WireScope = %v, want %v", src.WireScope, tt.wantWireScope)
			}
			// No case rides the golden, whatever the onResume policy says.
			if !src.GoldenForDataSnapshotURI.IsZero() {
				t.Errorf("src.GoldenForDataSnapshotURI = %v, want zero", src.GoldenForDataSnapshotURI)
			}
			wantKind := restoreDurableSnapshot
			switch {
			case tt.paused:
				wantKind = restoreLocalSnapshot
			case tt.noSnapshot:
				wantKind = restoreColdBoot
			}
			if src.Kind != wantKind {
				t.Errorf("src.Kind = %v, want %v", src.Kind, wantKind)
			}
		})
	}
}

// TestLoadActorForResume_BootSkipsGoldenFallback covers the explicit boot
// request: it only suppresses the golden-snapshot fallback for an actor with
// no snapshot of its own, without resolving the golden (an unusable golden
// must not block a boot). An actor's own local or durable snapshot restores
// exactly as it would without the flag.
func TestLoadActorForResume_BootSkipsGoldenFallback(t *testing.T) {
	goldenSnapshotURI := someActorSnapshotURI(t, "gs://bucket/golden-root", "ate-golden", "golden-1")
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	tests := []struct {
		name string
		// paused seeds a local pause checkpoint; noSnapshot leaves the actor
		// without a durable snapshot; otherwise a durable snapshot captured
		// with contentScope is seeded.
		paused       bool
		noSnapshot   bool
		contentScope ateapipb.SnapshotContentScope
		// fromData seeds the template's onResume policy; goldenURI and
		// goldenScope the template's recorded golden snapshot.
		fromData    ateapipb.ResumeSource
		goldenURI   string
		goldenScope ateapipb.SnapshotContentScope
		// wantSnapshotURI reports whether the actor's own durable snapshot
		// must be the boot source; a boot never resolves the golden as one.
		wantSnapshotURI bool
		// wantGoldenForDataURI is the golden location of a pending
		// data-on-golden restore, empty for every other restore.
		wantGoldenForDataURI string
		wantWireScope        ateletpb.SnapshotScope
	}{
		{
			name:            "Full durable snapshot restores as-is",
			contentScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenURI:       goldenSnapshotURI,
			goldenScope:     ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantSnapshotURI: true,
			wantWireScope:   ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			name:                 "Data durable snapshot still rides the golden under the Golden policy",
			contentScope:         ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			fromData:             ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			goldenURI:            goldenSnapshotURI,
			goldenScope:          ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantSnapshotURI:      true,
			wantGoldenForDataURI: goldenSnapshotURI,
			wantWireScope:        ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		},
		{
			name:          "no snapshot skips the golden fallback and cold boots",
			noSnapshot:    true,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED,
		},
		{
			name:          "no snapshot with an unusable golden still cold boots",
			noSnapshot:    true,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED,
		},
		{
			name:          "paused actor restores its checkpoint at the pause scope",
			paused:        true,
			wantWireScope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			seed := resumeSeed{
				paused:       tt.paused,
				noSnapshot:   tt.noSnapshot,
				contentScope: tt.contentScope,
				fromData:     tt.fromData,
				goldenURI:    tt.goldenURI,
				goldenScope:  tt.goldenScope,
			}
			seedResumeTemplate(t, ctx, persistence, seed)
			durableSnapshotURI := seedResumeActor(t, ctx, persistence, actorRef, seed)

			w := &ActorWorkflow{store: persistence}
			_, _, src, err := w.loadActorForResume(ctx, actorRef, true)
			if err != nil {
				t.Fatalf("loadActorForResume: %v", err)
			}
			wantSnapshotURI := ""
			if tt.wantSnapshotURI {
				wantSnapshotURI = durableSnapshotURI
			}
			if got := src.SnapshotURI.String(); got != wantSnapshotURI {
				t.Errorf("src.SnapshotURI = %q, want %q", got, wantSnapshotURI)
			}
			if got := src.GoldenForDataSnapshotURI.String(); got != tt.wantGoldenForDataURI {
				t.Errorf("src.GoldenForDataSnapshotURI = %q, want %q", got, tt.wantGoldenForDataURI)
			}
			if src.WireScope != tt.wantWireScope {
				t.Errorf("src.WireScope = %v, want %v", src.WireScope, tt.wantWireScope)
			}
			wantKind := restoreDurableSnapshot
			switch {
			case tt.paused:
				wantKind = restoreLocalSnapshot
			case tt.noSnapshot:
				wantKind = restoreColdBoot
			}
			if src.Kind != wantKind {
				t.Errorf("src.Kind = %v, want %v", src.Kind, wantKind)
			}
		})
	}
}

// TestRestoreKindTelemetrySnapshotKind pins the restore-kind to metric-label
// mapping: these are the recorded values of the resume lifecycle metric's
// snapshot-kind attribute.
func TestRestoreKindTelemetrySnapshotKind(t *testing.T) {
	tests := []struct {
		kind restoreKind
		want string
	}{
		{restoreColdBoot, ateattr.SnapshotKindBoot},
		{restoreLocalSnapshot, ateattr.SnapshotKindLocal},
		{restoreDurableSnapshot, ateattr.SnapshotKindLatest},
		{restoreGoldenSnapshot, ateattr.SnapshotKindGolden},
	}
	for _, tt := range tests {
		if got := tt.kind.telemetrySnapshotKind(); got != tt.want {
			t.Errorf("restoreKind(%d).telemetrySnapshotKind() = %q, want %q", tt.kind, got, tt.want)
		}
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
