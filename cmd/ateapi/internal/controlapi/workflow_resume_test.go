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
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		// instead of a durable snapshot; onPause is the template's pause
		// scope, contentScope the durable snapshot's recorded content.
		paused       bool
		onPause      ateapipb.SnapshotContentScope
		contentScope ateapipb.SnapshotContentScope
		// goldenURI and goldenScope are the template's recorded golden
		// external snapshot; an empty URI means the template has none. A zero
		// scope is treated as Full, the scope a golden snapshot must hold.
		goldenURI     string
		goldenScope   ateapipb.SnapshotContentScope
		wantCode      codes.Code
		wantGoldenURI string
	}{
		{
			name:          "resolves golden location for Data durable snapshot",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
			contentScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: goldenSnapshotURI,
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
		},
		{
			name:          "leaves golden location empty under ColdBoot fromData",
			fromData:      ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT,
			contentScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenURI:     goldenSnapshotURI,
			goldenScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:      codes.OK,
			wantGoldenURI: "",
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
			} else {
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
			if got := src.GoldenSnapshotURI.String(); got != tt.wantGoldenURI {
				t.Errorf("src.GoldenSnapshotURI = %q, want %q", got, tt.wantGoldenURI)
			}
			if !tt.paused && src.Scope != tt.contentScope {
				t.Errorf("src.Scope = %v, want %v", src.Scope, tt.contentScope)
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
	if !strings.Contains(err.Error(), "regenerate the golden snapshot") {
		t.Errorf("error %q does not tell the operator to regenerate the golden snapshot", err)
	}
}

// TestLoadActorForResume_TemplateReplaced covers the detection of a repointed
// actor: the actor records the template UID its guest state was built on, and
// a mismatch with its current template marks the source TemplateReplaced,
// forcing the restore to data-only.
func TestLoadActorForResume_TemplateReplaced(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	tests := []struct {
		name string
		// builtOnTemplateUID seeds the template UID the actor's guest state
		// was built on; "current" stands for the created template's own UID,
		// "" leaves the field unset (an actor from before it was recorded).
		builtOnTemplateUID string
		noSnapshot         bool
		want               bool
	}{
		{name: "snapshot taken under the current template", builtOnTemplateUID: "current", want: false},
		{name: "snapshot taken under a replaced template", builtOnTemplateUID: "some-other-uid", want: true},
		{name: "snapshot without a recorded template UID", builtOnTemplateUID: "", want: false},
		{name: "no durable snapshot", noSnapshot: true, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			storetest.MustCreateAtespace(t, ctx, persistence, "ns")
			tmpl, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
			})
			if err != nil {
				t.Fatalf("create template: %v", err)
			}
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
					a.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{
						SnapshotUri:  someActorSnapshotURI(t, testStorageLocation, actorRef.Atespace, "snap-1"),
						ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
					}
					a.Status.CurrentActorTemplateUid = uid
				})
			}
			seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, seedOpts...)

			w := &ActorWorkflow{store: persistence}
			_, _, src, err := w.loadActorForResume(ctx, actorRef, false)
			if err != nil {
				t.Fatalf("loadActorForResume: %v", err)
			}
			if src.TemplateReplaced != tt.want {
				t.Errorf("src.TemplateReplaced = %v, want %v", src.TemplateReplaced, tt.want)
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
	if !src.SnapshotURI.IsZero() || !src.GoldenSnapshotURI.IsZero() {
		t.Errorf("expected empty snapshot source, got %+v", src)
	}
}

// capturingAtelet records the last Restore and Run request it receives, so a
// test can assert on the exact wire request the resume workflow sends.
type capturingAtelet struct {
	ateletpb.UnimplementedAteomHerderServer

	mu      sync.Mutex
	restore *ateletpb.RestoreRequest
	run     *ateletpb.RunRequest
}

func (f *capturingAtelet) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restore = proto.Clone(req).(*ateletpb.RestoreRequest)
	return &ateletpb.RestoreResponse{}, nil
}

func (f *capturingAtelet) Run(ctx context.Context, req *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.run = proto.Clone(req).(*ateletpb.RunRequest)
	return &ateletpb.RunResponse{}, nil
}

// requests returns the recorded Restore and Run requests, nil for an RPC that
// was never called.
func (f *capturingAtelet) requests() (*ateletpb.RestoreRequest, *ateletpb.RunRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var restore *ateletpb.RestoreRequest
	var run *ateletpb.RunRequest
	if f.restore != nil {
		restore = proto.Clone(f.restore).(*ateletpb.RestoreRequest)
	}
	if f.run != nil {
		run = proto.Clone(f.run).(*ateletpb.RunRequest)
	}
	return restore, run
}

// wireTestAssignment is the worker assignment matching the pods
// newWireCaptureWorkflow seeds in the dialer's informer caches.
func wireTestAssignment() *ateapipb.WorkerAssignment {
	return &ateapipb.WorkerAssignment{
		Worker:          &ateapipb.ObjectRef{Name: "worker-1"},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		WorkerPodUid:    "worker-pod-uid",
	}
}

// newWireCaptureWorkflow builds an ActorWorkflow whose atelet dialer resolves
// to an in-process capturing fake. The dialer's conn cache is pre-warmed with
// a bufconn-backed connection keyed by the atelet pod's UID, so DialForWorker
// returns it without dialing the pod IP.
func newWireCaptureWorkflow(t *testing.T, persistence store.Interface) (*ActorWorkflow, *capturingAtelet) {
	t.Helper()

	fake := &capturingAtelet{}
	srv := grpc.NewServer()
	ateletpb.RegisterAteomHerderServer(srv, fake)
	lis := bufconn.Listen(1 << 20)
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("fake atelet server exited: %v", err)
		}
	}()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("connecting to the fake atelet: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})

	workerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "worker-ns", Name: "pod-1", UID: "worker-pod-uid"},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	ateletPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ateletNamespace, Name: "atelet-1", UID: "atelet-uid"},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	dialer := newDialerForPods(t, workerPod, ateletPod)
	dialer.ateletConns.Add("atelet-uid", conn)

	lister := sandboxConfigListerFor(t, []*atev1alpha1.SandboxConfig{{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: atev1alpha1.SandboxClassGvisor,
			PauseImage:   "pause@sha256:abc",
			Assets:       testAssets(),
		},
	}})

	return &ActorWorkflow{store: persistence, dialer: dialer, sandboxConfigLister: lister}, fake
}

// TestResumeActor_AteletWireRequest is the characteristic test for the
// loadActorForResume + ensureAteletRestored seam: for every combination of
// boot-source inputs it pins the exact request atelet receives — which RPC,
// req.Scope, req.GoldenSnapshotUri, and the snapshot the config names — and
// that a source-resolution error never produces an atelet RPC.
//
// The rows are ordered strictly by input columns (local → external → tmplUID →
// golden → fromData → boot) so a missing permutation is visible by scanning.
func TestResumeActor_AteletWireRequest(t *testing.T) {
	const localSnapshotName = "pause-snap-1"
	const malformedURI = "not-a-valid-snapshot-uri"

	actorURI := someActorSnapshotURI(t, testStorageLocation, "team-a", "snap-1")
	goldenURI := someActorSnapshotURI(t, "gs://bucket/golden-root", "ate-golden", "golden-1")

	fullScope := ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	dataScope := ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
	unspecScope := ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED
	fromGolden := ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN

	// actorSeed is the actor status a row persists before resuming.
	type actorSeed struct {
		// localSnapshot seeds Status.LocalSnapshotInfo (the pause checkpoint);
		// a non-nil value also parks the actor PAUSED instead of SUSPENDED.
		localSnapshot *ateapipb.LocalSnapshotInfo
		// externalSnapshot seeds Status.ExternalSnapshot (the durable snapshot).
		externalSnapshot *ateapipb.ExternalSnapshot
		// tmplUID seeds Status.CurrentActorTemplateUid, the template UID the
		// guest state was built on: "current" stands for the created template's
		// store-assigned UID (unknown until runtime), "" leaves the field unset
		// (an actor from before it was recorded), anything else mismatches (a
		// repointed actor).
		tmplUID string
	}
	// templateSeed is the ActorTemplate configuration a row persists.
	type templateSeed struct {
		// onPause is the template's pause scope.
		onPause ateapipb.SnapshotContentScope
		// golden seeds Status.GoldenSnapshotStatus.GoldenSnapshot.
		golden *ateapipb.ExternalSnapshot
		// fromData is the template's onResume boot-source policy.
		fromData ateapipb.ResumeSource
	}
	// restoreWant pins the request atelet receives. On a non-OK code neither
	// Restore nor Run may reach atelet; with run set the Run RPC (cold boot)
	// must fire instead of Restore; otherwise exactly one Restore carrying
	// these wire values.
	type restoreWant struct {
		code           codes.Code
		run            bool
		checkpointType ateletpb.CheckpointType
		// snapshotName is LocalConfig.SnapshotName; snapshotURI is
		// ExternalConfig.SnapshotUri. Both are asserted on every Restore, so
		// a row also pins that the other config is absent.
		snapshotName string
		snapshotURI  string
		scope        ateletpb.SnapshotScope
		goldenURI    string
	}

	tests := []struct {
		name  string
		actor actorSeed
		tmpl  templateSeed
		boot  bool
		want  restoreWant
	}{
		{
			name: "01 nothing to restore cold-boots from the spec",
			want: restoreWant{run: true},
		},
		{
			// fromData only governs data-only snapshots; with no snapshot at
			// all the golden policy never engages and the actor cold-boots.
			name: "02 Golden fromData with nothing to restore still cold-boots",
			tmpl: templateSeed{fromData: fromGolden},
			want: restoreWant{run: true},
		},
		{
			name: "03 golden fallback restores the golden snapshot in Full",
			tmpl: templateSeed{golden: &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope}},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    goldenURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
		},
		{
			name: "04 boot skips the golden fallback and cold-boots from the spec",
			tmpl: templateSeed{golden: &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope}},
			boot: true,
			want: restoreWant{run: true},
		},
		{
			// With no actor snapshot the restore is not data-only, so the
			// golden rides in ExternalConfig and GoldenSnapshotUri stays empty.
			name: "05 golden fallback under Golden fromData is a plain Full restore",
			tmpl: templateSeed{
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope},
				fromData: fromGolden,
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    goldenURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
		},
		{
			name: "06 golden fallback rejects a non-Full golden",
			tmpl: templateSeed{golden: &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: dataScope}},
			want: restoreWant{code: codes.FailedPrecondition},
		},
		{
			name: "07 golden fallback rejects a malformed golden URI",
			tmpl: templateSeed{golden: &ateapipb.ExternalSnapshot{SnapshotUri: malformedURI, ContentScope: fullScope}},
			want: restoreWant{code: codes.DataLoss},
		},
		{
			// TemplateReplaced is derived from the actor's own durable
			// snapshot; without one, a repoint cannot downgrade the golden
			// fallback.
			name:  "08 template repoint does not affect the golden fallback",
			actor: actorSeed{tmplUID: "mismatch"},
			tmpl:  templateSeed{golden: &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope}},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    goldenURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
		},
		{
			name:  "09 Full durable snapshot restores itself in Full",
			actor: actorSeed{externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: fullScope}},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    actorURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
		},
		{
			name:  "10 Full durable snapshot ignores Golden fromData",
			actor: actorSeed{externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: fullScope}},
			tmpl: templateSeed{
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope},
				fromData: fromGolden,
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    actorURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
		},
		{
			name: "11 durable snapshot built on the current template stays Full",
			actor: actorSeed{
				externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: fullScope},
				tmplUID:          "current",
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    actorURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
		},
		{
			// The snapshot's guest state was built on another template; the
			// repointed actor must drop to Data so the new template's image
			// boots fresh and only the volume data carries over.
			name: "12 repointed actor's Full durable snapshot drops to Data",
			actor: actorSeed{
				externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: fullScope},
				tmplUID:          "mismatch",
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    actorURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
			},
		},
		{
			// TemplateReplaced leads the scope switch: a repointed actor
			// restores plain Data even when the golden policy is configured,
			// with no golden overlay.
			name: "13 template repoint beats the golden policy",
			actor: actorSeed{
				externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: fullScope},
				tmplUID:          "mismatch",
			},
			tmpl: templateSeed{
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope},
				fromData: fromGolden,
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    actorURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
			},
		},
		{
			name:  "14 Data durable snapshot restores as Data",
			actor: actorSeed{externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: dataScope}},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    actorURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
			},
		},
		{
			name:  "15 Data durable snapshot under Golden fromData restores on the golden",
			actor: actorSeed{externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: dataScope}},
			tmpl: templateSeed{
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope},
				fromData: fromGolden,
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    actorURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
				goldenURI:      goldenURI,
			},
		},
		{
			name:  "16 Golden data resume rejects a non-Full golden",
			actor: actorSeed{externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: dataScope}},
			tmpl: templateSeed{
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: dataScope},
				fromData: fromGolden,
			},
			want: restoreWant{code: codes.FailedPrecondition},
		},
		{
			// Even when the golden policy would produce DATA_ON_GOLDEN, a
			// repointed actor restores plain Data with no golden overlay.
			name: "17 template repoint beats a Golden data resume",
			actor: actorSeed{
				externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: dataScope},
				tmplUID:          "mismatch",
			},
			tmpl: templateSeed{
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope},
				fromData: fromGolden,
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    actorURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
			},
		},
		{
			// Snapshots recorded before content_scope existed carry
			// UNSPECIFIED; the conversion sends them out as Full.
			name:  "18 unspecified durable scope goes out as Full",
			actor: actorSeed{externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: unspecScope}},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
				snapshotURI:    actorURI,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
		},
		{
			name:  "19 malformed durable snapshot URI fails with DataLoss",
			actor: actorSeed{externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: malformedURI, ContentScope: fullScope}},
			want:  restoreWant{code: codes.DataLoss},
		},
		{
			name: "20 Full pause snapshot restores locally as Full",
			actor: actorSeed{
				localSnapshot: &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
			},
			tmpl: templateSeed{onPause: fullScope},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
				snapshotName:   localSnapshotName,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
		},
		{
			name: "21 Full pause snapshot ignores Golden fromData",
			actor: actorSeed{
				localSnapshot: &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
			},
			tmpl: templateSeed{
				onPause:  fullScope,
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope},
				fromData: fromGolden,
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
				snapshotName:   localSnapshotName,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
		},
		{
			// The repoint permutation without a durable snapshot is
			// deliberately not pinned: its wire scope is in flux while the
			// resume-source resolution is being reworked.
			name: "22 local snapshot built on the current template stays Full",
			actor: actorSeed{
				localSnapshot: &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
				tmplUID:       "current",
			},
			tmpl: templateSeed{onPause: fullScope},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
				snapshotName:   localSnapshotName,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
		},
		{
			name: "23 Data pause snapshot restores locally as Data",
			actor: actorSeed{
				localSnapshot: &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
			},
			tmpl: templateSeed{onPause: dataScope},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
				snapshotName:   localSnapshotName,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
			},
		},
		{
			name: "24 Golden data resume requires a golden snapshot",
			actor: actorSeed{
				localSnapshot: &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
			},
			tmpl: templateSeed{onPause: dataScope, fromData: fromGolden},
			want: restoreWant{code: codes.FailedPrecondition},
		},
		{
			name: "25 Data pause snapshot under Golden fromData restores on the golden",
			actor: actorSeed{
				localSnapshot: &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
			},
			tmpl: templateSeed{
				onPause:  dataScope,
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope},
				fromData: fromGolden,
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
				snapshotName:   localSnapshotName,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
				goldenURI:      goldenURI,
			},
		},
		{
			name: "26 Golden data resume rejects a non-Full golden, local path",
			actor: actorSeed{
				localSnapshot: &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
			},
			tmpl: templateSeed{
				onPause:  dataScope,
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: dataScope},
				fromData: fromGolden,
			},
			want: restoreWant{code: codes.FailedPrecondition},
		},
		{
			// Golden snapshots recorded before content_scope existed carry
			// UNSPECIFIED, which counts as Full.
			name: "27 unspecified golden scope is accepted for a Golden data resume",
			actor: actorSeed{
				localSnapshot: &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
			},
			tmpl: templateSeed{
				onPause:  dataScope,
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: unspecScope},
				fromData: fromGolden,
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
				snapshotName:   localSnapshotName,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
				goldenURI:      goldenURI,
			},
		},
		{
			name: "28 Golden data resume rejects a malformed golden URI",
			actor: actorSeed{
				localSnapshot: &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
			},
			tmpl: templateSeed{
				onPause:  dataScope,
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: malformedURI, ContentScope: fullScope},
				fromData: fromGolden,
			},
			want: restoreWant{code: codes.DataLoss},
		},
		{
			// The local snapshot takes precedence at restore, and dataOnly
			// comes from the pause scope, not the durable snapshot's.
			name: "29 local snapshot wins over a Full durable snapshot",
			actor: actorSeed{
				localSnapshot:    &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
				externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: fullScope},
			},
			tmpl: templateSeed{
				onPause:  dataScope,
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope},
				fromData: fromGolden,
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
				snapshotName:   localSnapshotName,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
				goldenURI:      goldenURI,
			},
		},
		{
			// TemplateReplaced wins in the local branch too: the pause
			// checkpoint restores as plain Data, the golden overlay dropped.
			name: "30 template repoint beats the golden policy on the local path",
			actor: actorSeed{
				localSnapshot:    &ateapipb.LocalSnapshotInfo{SnapshotName: localSnapshotName, NodeVmsWithLocalSnapshots: []string{"node-1"}},
				externalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actorURI, ContentScope: fullScope},
				tmplUID:          "mismatch",
			},
			tmpl: templateSeed{
				onPause:  dataScope,
				golden:   &ateapipb.ExternalSnapshot{SnapshotUri: goldenURI, ContentScope: fullScope},
				fromData: fromGolden,
			},
			want: restoreWant{
				checkpointType: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
				snapshotName:   localSnapshotName,
				scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			w, atelet := newWireCaptureWorkflow(t, persistence)

			storetest.MustCreateAtespace(t, ctx, persistence, "ns")
			tmpl := &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
				SnapshotsConfig: &ateapipb.SnapshotsConfig{
					StorageLocation: testStorageLocation,
					OnPause:         tt.tmpl.onPause,
					OnResume:        &ateapipb.OnResumeConfig{FromData: tt.tmpl.fromData},
				},
				SandboxConfig: &ateapipb.SandboxConfig{
					SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
					ConfigName:   "gvisor",
				},
			}
			if tt.tmpl.golden != nil {
				tmpl.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
					GoldenSnapshot: tt.tmpl.golden,
				}}
			}
			createdTmpl, err := persistence.CreateActorTemplate(ctx, tmpl)
			if err != nil {
				t.Fatalf("create template: %v", err)
			}
			if createdTmpl.GetMetadata().GetUid() == "" {
				t.Fatal("created template has no UID; the matching tmplUID case would be vacuous")
			}

			// A pause checkpoint is what parks an actor PAUSED; without one a
			// non-running actor resumes from SUSPENDED.
			actorState := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
			if tt.actor.localSnapshot != nil {
				actorState = ateapipb.ActorState_ACTOR_STATE_PAUSED
			}
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", actorState, func(a *ateapipb.Actor) {
				a.Status.WorkerAssignment = wireTestAssignment()
				a.Status.LocalSnapshotInfo = tt.actor.localSnapshot
				a.Status.ExternalSnapshot = tt.actor.externalSnapshot
				uid := tt.actor.tmplUID
				if uid == "current" {
					uid = createdTmpl.GetMetadata().GetUid()
				}
				a.Status.CurrentActorTemplateUid = uid
			})

			actor, loadedTmpl, src, err := w.loadActorForResume(ctx, actorRef, tt.boot)
			if err == nil {
				_, err = w.ensureAteletRestored(ctx, actorRef, actor, loadedTmpl, src)
			}
			if got := status.Code(err); got != tt.want.code {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tt.want.code, err)
			}

			restore, run := atelet.requests()
			if tt.want.code != codes.OK {
				if restore != nil || run != nil {
					t.Fatalf("atelet received a request after a resolution error: restore=%v run=%v", restore, run)
				}
				return
			}

			if tt.want.run {
				if run == nil || restore != nil {
					t.Fatalf("atelet requests = (restore=%v, run=%v), want exactly one Run", restore, run)
				}
				return
			}
			if restore == nil || run != nil {
				t.Fatalf("atelet requests = (restore=%v, run=%v), want exactly one Restore", restore, run)
			}
			if got := restore.GetType(); got != tt.want.checkpointType {
				t.Errorf("restore type = %v, want %v", got, tt.want.checkpointType)
			}
			if got := restore.GetLocalConfig().GetSnapshotName(); got != tt.want.snapshotName {
				t.Errorf("LocalConfig.SnapshotName = %q, want %q", got, tt.want.snapshotName)
			}
			if got := restore.GetExternalConfig().GetSnapshotUri(); got != tt.want.snapshotURI {
				t.Errorf("ExternalConfig.SnapshotUri = %q, want %q", got, tt.want.snapshotURI)
			}
			if got := restore.GetScope(); got != tt.want.scope {
				t.Errorf("restore scope = %v, want %v", got, tt.want.scope)
			}
			if got := restore.GetGoldenSnapshotUri(); got != tt.want.goldenURI {
				t.Errorf("GoldenSnapshotUri = %q, want %q", got, tt.want.goldenURI)
			}
		})
	}
}
