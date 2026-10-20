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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
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
		{name: "filled up worker is skipped", err: errWorkerFilledUp, want: false},
		{name: "wrapped filled up worker is skipped", err: fmt.Errorf("bind actor: %w", errWorkerFilledUp), want: false},
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

	got, resumed, err := w.ResumeActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
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

// TestFinalizeRunning_CommitsRunning verifies the last step of a resume moves
// the actor out of RESUMING against a freshly read version, and keeps the
// locality of the local snapshot it restored from: resume does not delete the
// snapshot's files from the node.
func TestFinalizeRunning_CommitsRunning(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "tmpl-2"},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_RESUMING,
			AssignedNode:           "node-1",
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-1", "node-1"),
			},
		},
	})
	w := &ActorWorkflow{store: persistence}

	got, err := w.finalizeRunning(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("finalizeRunning: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("state = %v, want RUNNING", got.GetStatus().GetState())
	}
	if locality := findLatestLocalSnapshot(got.GetStatus()).GetLocality(); locality != "node-1" {
		t.Errorf("local snapshot locality = %q, want %q kept after resume", locality, "node-1")
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
	tmpl := &ateapipb.ActorTemplate{}

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
	tmpl := &ateapipb.ActorTemplate{}

	_, _, err := w.ensureWorkerAssigned(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("ensureWorkerAssigned error = %v, want ErrVersionConflict", err)
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
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(testSandboxRuntime()),
		},
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
	tmpl := &ateapipb.ActorTemplate{}
	_, _, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}, actor, tmpl)
	if apierror.Code(err) != codes.ResourceExhausted {
		t.Fatalf("assignWorkerAttempt() error = %v, want ResourceExhausted (no worker has room)", err)
	}

	stored := firstAssignment(t, persistence, testWorkerUID("pod-1"))
	if got := stored.GetActorUid(); got != "team-b-actor-uid" {
		t.Errorf("worker assignment uid = %q, want %q (assignment: %v)", got, "team-b-actor-uid", stored)
	}
	if got := stored.GetActor().GetAtespace(); got != "team-b" {
		t.Errorf("worker assignment atespace = %q, want %q (assignment: %v)", got, "team-b", stored)
	}
}

// TestEnsureWorkerAssigned_MalformedSnapshotURIClaimsNoWorker verifies that a
// snapshot URI that does not parse fails the resume before a worker is claimed,
// so the actor stays SUSPENDED and the worker stays free.
func TestEnsureWorkerAssigned_MalformedSnapshotURIClaimsNoWorker(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-1")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		WorkerPodUid:    testWorkerUID("pod-1"),
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(testSandboxRuntime()),
		},
	}
	if _, err := persistence.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, func(a *ateapipb.Actor) {
		a.Status.LastAssignedGeneration = 1
		a.Status.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap-1", "not-a-valid-snapshot-uri", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
	})
	actor, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	if _, _, err := w.ensureWorkerAssigned(ctx, actorRef, actor, &ateapipb.ActorTemplate{}); apierror.Code(err) != codes.DataLoss {
		t.Fatalf("ensureWorkerAssigned() error = %v, want DataLoss", err)
	}

	if got := firstAssignment(t, persistence, testWorkerUID("pod-1")); got != nil {
		t.Errorf("worker assignment = %v, want none", got)
	}
	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("stored state = %v, want SUSPENDED", got)
	}
	if got := stored.GetStatus().GetWorkerAssignment(); got != nil {
		t.Errorf("stored WorkerAssignment = %v, want none", got)
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

	// stale-pod is claimed by this actor from a failed attempt but its labels
	// do not match the template's selector; free-pod is eligible and free.
	stale := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("stale-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-a",
		WorkerPod:       "stale-pod",
		WorkerPodUid:    testWorkerUID("stale-pod"),
		NodeName:        "node-1",
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(testSandboxRuntime()),
		},
	}
	free := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("free-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-b",
		WorkerPod:       "free-pod",
		WorkerPodUid:    testWorkerUID("free-pod"),
		NodeName:        "node-1",
		Labels:          map[string]string{"pool": "b"},
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(testSandboxRuntime()),
		},
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
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"pool": "b"}},
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

	wantRuntime := testSandboxRuntime()
	contested := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("contested-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "contested-pod",
		WorkerPodUid:    testWorkerUID("contested-pod"),
		NodeName:        "node-1",
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(wantRuntime),
		},
	}
	fallback := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("fallback-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "fallback-pod",
		WorkerPodUid:    testWorkerUID("fallback-pod"),
		NodeName:        "node-1",
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(wantRuntime),
		},
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
	tmpl := &ateapipb.ActorTemplate{}
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
	if gotRt := storedActor.GetStatus().GetWorkerAssignment().GetSandboxRuntime(); !proto.Equal(gotRt, wantRuntime) {
		t.Errorf("stored actor WorkerAssignment.SandboxRuntime = %v, want %v", gotRt, wantRuntime)
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
		NodeName:        "node-1",
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(testSandboxRuntime()),
		},
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
			tmpl := &ateapipb.ActorTemplate{}
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
				if got := apierror.Code(err); got != codes.Aborted {
					t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.Aborted, err)
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
			// The resume edge only exists from SUSPENDED and RESUMING; a
			// CRASHED actor is rejected by ensureWorkerAssigned
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
					WorkerPodIps:    []string{"1.2.3.4"},
				}
			})

			actor, resumed, err := w.ResumeActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
			if tc.wantErr {
				if got := apierror.Code(err); got != codes.FailedPrecondition {
					t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
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
// edge's state gating: every state outside SUSPENDED and RESUMING is
// rejected with FailedPrecondition before any dependency is touched.
// (SUSPENDED assignment and RESUMING recovery are exercised by the
// assignment-attempt and worker-validation tests; RUNNING never reaches this
// step because the orchestrator early-returns.)
func TestEnsureWorkerAssigned_RejectsNonResumableStates(t *testing.T) {
	ctx := context.Background()
	w := &ActorWorkflow{}
	for _, st := range allActorStates {
		switch st {
		case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_RESUMING:
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

			_, _, err := w.ResumeActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
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

	_, _, err := w.ResumeActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if got := apierror.Code(err); got != codes.Aborted {
		t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.Aborted, err)
	}

	got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("stored state = %v, want %v", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_CRASHED)
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "resume failed: "+crashMessageWorkerAssignmentMissing; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
	}
}

// TestValidateAssignedWorker verifies that RESUMING recovery only proceeds on
// a live, non-draining worker whose assignment still names this actor: the
// recovery path loads the worker by pod name only, so the assignment may have
// been cleared and the worker re-claimed by another actor in the meantime. On
// a mismatch the actor is crashed and the worker — which is not ours — must
// not be written.
func TestValidateAssignedWorker(t *testing.T) {
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
	activeStatus := &ateapipb.WorkerStatus{
		State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
		Capacity:       &ateapipb.WorkerResources{Actors: 1},
		SandboxClasses: workerSandboxClasses(testSandboxRuntime()),
	}
	drainingStatus := &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_DRAINING, Capacity: &ateapipb.WorkerResources{Actors: 1}, SandboxClasses: workerSandboxClasses(testSandboxRuntime())}
	unavailableStatus := &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE, Capacity: &ateapipb.WorkerResources{Actors: 1}, SandboxClasses: workerSandboxClasses(testSandboxRuntime())}

	tests := []struct {
		name string
		// workerStatus is the stored worker's status, nil for a worker that
		// is gone.
		workerStatus *ateapipb.WorkerStatus
		tmplSelector map[string]string
		assignment   *ateapipb.ActorAssignment
		// wantCode is codes.OK when validateAssignedWorker must return nil.
		wantCode       codes.Code
		wantActorState ateapipb.ActorState
		// wantCrashMessage is the crash recorded when the actor is crashed.
		wantCrashMessage string
		// wantAssignment is the assignment expected on the stored worker
		// afterwards; wantWorkerWrite false additionally asserts the worker
		// version did not move (no write at all).
		wantAssignment  *ateapipb.ActorAssignment
		wantWorkerWrite bool
	}{
		{
			name:             "crashes actor when worker is gone",
			wantCode:         codes.Aborted,
			wantActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantCrashMessage: "resume failed: " + crashMessageWorkerGone,
		},
		{
			name:             "crashes actor and leaves worker untouched when worker is draining",
			workerStatus:     drainingStatus,
			assignment:       ownAssignment,
			wantCode:         codes.Aborted,
			wantActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantCrashMessage: "resume failed: " + crashMessageWorkerDraining,
			wantAssignment:   ownAssignment,
		},
		{
			name:           "leaves actor and worker untouched when worker is unavailable",
			workerStatus:   unavailableStatus,
			assignment:     ownAssignment,
			wantCode:       codes.Unavailable,
			wantActorState: ateapipb.ActorState_ACTOR_STATE_RESUMING,
			wantAssignment: ownAssignment,
		},
		{
			name:             "crashes actor and leaves worker untouched when assigned to another actor",
			workerStatus:     activeStatus,
			assignment:       otherAssignment,
			wantCode:         codes.Aborted,
			wantActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantCrashMessage: "resume failed: " + crashMessageWorkerReassigned,
			wantAssignment:   otherAssignment,
		},
		{
			name:             "crashes actor and leaves worker untouched when assigned to previous incarnation of same actor",
			workerStatus:     activeStatus,
			assignment:       staleIncarnationAssignment,
			wantCode:         codes.Aborted,
			wantActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantCrashMessage: "resume failed: " + crashMessageWorkerReassigned,
			wantAssignment:   staleIncarnationAssignment,
		},
		{
			name:             "crashes actor and leaves worker untouched when assignment is cleared",
			workerStatus:     activeStatus,
			assignment:       nil,
			wantCode:         codes.Aborted,
			wantActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantCrashMessage: "resume failed: " + crashMessageWorkerReassigned,
			wantAssignment:   nil,
		},
		{
			name:           "passes for own eligible worker",
			workerStatus:   activeStatus,
			assignment:     ownAssignment,
			wantCode:       codes.OK,
			wantActorState: ateapipb.ActorState_ACTOR_STATE_RESUMING,
			wantAssignment: ownAssignment,
		},
		{
			name:             "releases own ineligible worker and crashes actor",
			workerStatus:     activeStatus,
			tmplSelector:     map[string]string{"pool": "other"},
			assignment:       ownAssignment,
			wantCode:         codes.Aborted,
			wantActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantCrashMessage: "resume failed: " + crashMessageWorkerIneligible,
			wantAssignment:   nil,
			wantWorkerWrite:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			var seeded *ateapipb.Worker
			if tt.workerStatus != nil {
				if _, err := persistence.CreateWorker(ctx, &ateapipb.Worker{
					Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-1")},
					WorkerNamespace: "worker-ns",
					WorkerPool:      "pool",
					WorkerPod:       "pod-1",
					WorkerPodUid:    testWorkerUID("pod-1"),
					Status:          tt.workerStatus,
				}); err != nil {
					t.Fatalf("CreateWorker: %v", err)
				}
				seedAssignment(t, persistence, testWorkerUID("pod-1"), tt.assignment)
				// Fetch the stored version so the no-write assertion below can
				// detect any optimistic update.
				var err error
				if seeded, err = persistence.GetWorker(ctx, testWorkerUID("pod-1")); err != nil {
					t.Fatalf("GetWorker: %v", err)
				}
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
			tmpl := &ateapipb.ActorTemplate{}
			if tt.tmplSelector != nil {
				tmpl.WorkerSelector = &ateapipb.Selector{MatchLabels: tt.tmplSelector}
			}
			_, err := w.validateAssignedWorker(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}, resumingActor, tmpl)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}

			actor, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"})
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if actor.GetStatus().GetState() != tt.wantActorState {
				t.Errorf("stored actor state = %v, want %v", actor.GetStatus().GetState(), tt.wantActorState)
			}
			if msg := actor.GetStatus().GetCrash().GetMessage(); msg != tt.wantCrashMessage {
				t.Errorf("crash message = %q, want %q", msg, tt.wantCrashMessage)
			}

			if tt.workerStatus == nil {
				return
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

func TestLoadActorForResume_RunningActorShortCircuits(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	// Seed the actor as RUNNING. Note: No snapshot or template is seeded in the
	// store, proving that loadActorForResume short-circuits before attempting
	// to fetch either.
	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "missing-tmpl", ateapipb.ActorState_ACTOR_STATE_RUNNING)

	w := &ActorWorkflow{store: persistence}

	actor, tmpl, err := w.loadActorForResume(ctx, actorRef)
	if err != nil {
		t.Fatalf("loadActorForResume() unexpected error = %v", err)
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want %v", actor.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_RUNNING)
	}
	if tmpl != nil {
		t.Errorf("expected nil template, got %v", tmpl)
	}
}

// capturingAtelet records the last request of each RPC it receives, so a test
// can assert on the exact wire request a workflow sends.
type capturingAtelet struct {
	ateletpb.UnimplementedAteletServer

	mu          sync.Mutex
	restore     *ateletpb.RestoreRequest
	run         *ateletpb.RunRequest
	checkpoint  *ateletpb.CheckpointRequest
	promote     *ateletpb.PromoteSnapshotRequest
	checkpoints int
	promotes    int

	// onCheckpoint, when set, runs before Checkpoint returns, standing in for
	// whatever else happens while the workload checkpoints. Its error fails
	// the RPC.
	onCheckpoint func(ctx context.Context) error
	// checkpointState is the snapshot state Checkpoint returns.
	checkpointState *ateletpb.ActorSnapshotState
	// promoteState is the snapshot state PromoteSnapshot returns.
	promoteState *ateletpb.ActorSnapshotState
	// promoteErr, when set, fails PromoteSnapshot after it is recorded.
	promoteErr error
	// detach is the last DetachActor request, and detachState the snapshot
	// state DetachActor returns. detachErr, when set, fails DetachActor after
	// it is recorded.
	detach      *ateletpb.DetachActorRequest
	detachState *ateletpb.ActorSnapshotState
	detachErr   error
	// onDetach, when set, runs before DetachActor returns.
	onDetach     func()
	terminateErr error
	onTerminate  func()
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

func (f *capturingAtelet) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	f.mu.Lock()
	f.checkpoint = proto.Clone(req).(*ateletpb.CheckpointRequest)
	f.checkpoints++
	hook := f.onCheckpoint
	f.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return &ateletpb.CheckpointResponse{State: proto.CloneOf(f.checkpointState)}, nil
}

func (f *capturingAtelet) PromoteSnapshot(ctx context.Context, req *ateletpb.PromoteSnapshotRequest) (*ateletpb.PromoteSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promote = proto.Clone(req).(*ateletpb.PromoteSnapshotRequest)
	f.promotes++
	if f.promoteErr != nil {
		return nil, f.promoteErr
	}
	return &ateletpb.PromoteSnapshotResponse{State: proto.CloneOf(f.promoteState)}, nil
}

func (f *capturingAtelet) DetachActor(ctx context.Context, req *ateletpb.DetachActorRequest) (*ateletpb.DetachActorResponse, error) {
	f.mu.Lock()
	hook := f.onDetach
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.detach = proto.CloneOf(req)
	if f.detachErr != nil {
		return nil, f.detachErr
	}
	return &ateletpb.DetachActorResponse{State: proto.CloneOf(f.detachState)}, nil
}

func (f *capturingAtelet) Terminate(ctx context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	f.mu.Lock()
	hook := f.onTerminate
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	f.mu.Lock()
	err := f.terminateErr
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return f.UnimplementedAteletServer.Terminate(ctx, req)
}

func (f *capturingAtelet) detachRequest() *ateletpb.DetachActorRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return proto.CloneOf(f.detach)
}

// calls returns how many Checkpoint and PromoteSnapshot RPCs were received.
func (f *capturingAtelet) calls() (checkpoints, promotes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkpoints, f.promotes
}

func (f *capturingAtelet) checkpointRequest() *ateletpb.CheckpointRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.checkpoint == nil {
		return nil
	}
	return proto.Clone(f.checkpoint).(*ateletpb.CheckpointRequest)
}

func (f *capturingAtelet) promoteRequest() *ateletpb.PromoteSnapshotRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.promote == nil {
		return nil
	}
	return proto.Clone(f.promote).(*ateletpb.PromoteSnapshotRequest)
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
		NodeName:        "node-1",
	}
}

// newWireCaptureWorkflow builds an ActorWorkflow whose atelet dialer resolves
// to an in-process capturing fake. The dialer's conn cache is pre-warmed with
// a bufconn-backed connection for the atelet pod's UID and IP, so
// DialForAteletOnNode returns it without dialing the pod IP. The fake's server
// runs atelet's interceptor, so its errors reach the workflow with the code
// atelet would send.
func newWireCaptureWorkflow(t *testing.T, persistence store.Interface) (*ActorWorkflow, *capturingAtelet) {
	t.Helper()

	fake := &capturingAtelet{}
	srv := grpc.NewServer(grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor))
	ateletpb.RegisterAteletServer(srv, fake)
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

	ateletPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: installdefaults.SystemNamespace, Name: "atelet-1", UID: "atelet-uid"},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
		Status:     corev1.PodStatus{PodIPs: []corev1.PodIP{{IP: "10.0.0.1"}}},
	}
	dialer := NewAteletDialer(newTestAteletIndexer(t, ateletPod), installdefaults.SystemNamespace, "", "")
	dialer.ateletConns.Add("atelet-uid", &ateletConn{ip: "10.0.0.1", conn: conn})

	return &ActorWorkflow{store: persistence, dialer: dialer}, fake
}

// TestResumeActor_AteletWireRequest is the characteristic test for the
// loadActorForResume + ensureAteletRestored seam: for every combination of
// boot-source inputs it pins the exact request atelet receives — which RPC,
// req.Fidelity, and the snapshot the config names — and that a source-resolution
// error never produces an atelet RPC.
//
// The rows are ordered strictly by input columns (local → external → tmplUID →
// golden) so a missing permutation is visible by scanning.
func TestResumeActor_AteletWireRequest(t *testing.T) {
	const localSnapshotName = "pause-snap-1"
	// durableSnapshotName is the uuid of every durable snapshot a row seeds.
	const durableSnapshotName = "snap-1"
	const malformedURI = "not-a-valid-snapshot-uri"

	actorURI := someActorSnapshotURI(t, testStorageLocation, "team-a", durableSnapshotName)
	goldenURI := someActorSnapshotURI(t, "gs://bucket/golden-root", "team-a", "golden-1")

	fullScope := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	rootfsScope := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	dataScope := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	unspecScope := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED

	extSnap := func(uri string, fidelity ateapipb.SnapshotFidelity) *ateapipb.Snapshot {
		return newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, fidelity, "", durableSnapshotName, uri, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	}
	localSnap := func(name string, fidelity ateapipb.SnapshotFidelity) *ateapipb.Snapshot {
		return newPausedSnapshot(t, 1, fidelity, "", name, "node-1")
	}
	stampedLocalSnap := func(name string, fidelity ateapipb.SnapshotFidelity, rt *ateapipb.ActorSandboxRuntime) *ateapipb.Snapshot {
		snap := localSnap(name, fidelity)
		snap.SandboxRuntime = rt
		return snap
	}
	// pauseUploadURI is where atelet uploads the pause snapshot.
	pauseUploadURI := someActorSnapshotURI(t, testStorageLocation, "team-a", localSnapshotName)
	uploadedLocalSnap := func(name string, fidelity ateapipb.SnapshotFidelity) *ateapipb.Snapshot {
		snap := localSnap(name, fidelity)
		snap.DurableSnapshot = &ateapipb.SnapshotStorage{
			Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
			Object: &ateapipb.ObjectSnapshot{SnapshotUri: pauseUploadURI},
		}
		return snap
	}
	// localSnapWithUpload is a local snapshot whose record names upload as
	// its durable copy, or none when upload is nil.
	localSnapWithUpload := func(name string, fidelity ateapipb.SnapshotFidelity, upload *ateapipb.SnapshotStorage) *ateapipb.Snapshot {
		snap := localSnap(name, fidelity)
		snap.DurableSnapshot = upload
		return snap
	}

	// actorSeed is the actor status a row persists before resuming.
	type actorSeed struct {
		// localSnapshot seeds a completed local snapshot (the pause checkpoint);
		// a non-nil value also pins the actor to the snapshot's node.
		localSnapshot *ateapipb.Snapshot
		// externalSnapshot seeds a completed durable snapshot.
		externalSnapshot *ateapipb.Snapshot
		// tmplUID stamps externalSnapshot. "current" means the created template's UID.
		tmplUID string
		// localTmplUID stamps localSnapshot independently.
		localTmplUID string
		// sourceTag seeds Actor.SourceTag when testing explicit tag clones.
		sourceTag *ateapipb.ObjectRef
		// assignedRuntime seeds WorkerAssignment.SandboxRuntime.
		assignedRuntime *ateapipb.ActorSandboxRuntime
		// externalNewer gives externalSnapshot a higher generation than
		// localSnapshot. By default the local snapshot is newer.
		externalNewer bool
	}
	// templateSeed is the ActorTemplate configuration a row persists.
	type templateSeed struct {
		// golden seeds the template's golden tag snapshot.
		golden *ateapipb.Snapshot
	}
	// restoreWant pins the request atelet receives. On a non-OK code neither
	// Restore nor Run may reach atelet; with run set the Run RPC (cold boot)
	// must fire instead of Restore; otherwise exactly one Restore carrying
	// these wire values.
	type restoreWant struct {
		code codes.Code
		run  bool
		// snapshotName is Snapshot.SnapshotUuid and snapshotURI is
		// Snapshot.Object.SnapshotUri. Both are asserted on every Restore, so
		// a local row also pins which durable copy, if any, is sent.
		snapshotName string
		snapshotURI  string
		scope        ateletpb.SnapshotFidelity
		// kind is the restore's snapshot kind label: local when the
		// restored snapshot is on a node, otherwise latest.
		kind string
	}

	amd64RT := testSandboxRuntime()
	arm64RT := testRuntime("arm64", "")
	wantAteletRuntime := map[*ateapipb.ActorSandboxRuntime]*ateletpb.ActorSandboxRuntime{
		amd64RT: {
			SandboxClass: "gvisor",
			Version: &ateletpb.VersionedSandboxCompat{
				SchemaVersion: "v1",
				Attributes:    []*ateletpb.AttributeEntry{{Key: "architecture", Value: "amd64"}},
			},
		},
		arm64RT: {
			SandboxClass: "gvisor",
			Version: &ateletpb.VersionedSandboxCompat{
				SchemaVersion: "v1",
				Attributes:    []*ateletpb.AttributeEntry{{Key: "architecture", Value: "arm64"}},
			},
		},
	}
	stampedExtSnap := func(owner ateapipb.SnapshotOwner, uri string, fidelity ateapipb.SnapshotFidelity, rt *ateapipb.ActorSandboxRuntime) *ateapipb.Snapshot {
		snap := newDurableSnapshot(1, owner, fidelity, "", durableSnapshotName, uri, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		snap.SandboxRuntime = rt
		return snap
	}

	tests := []struct {
		name  string
		actor actorSeed
		tmpl  templateSeed
		want  restoreWant
	}{
		{
			name:  "01 nothing to restore cold-boots from the spec",
			actor: actorSeed{assignedRuntime: amd64RT},
			want:  restoreWant{run: true},
		},
		{
			name:  "02 inherited golden snapshot restores in Full",
			actor: actorSeed{externalSnapshot: extSnap(goldenURI, fullScope), tmplUID: "current"},
			tmpl:  templateSeed{golden: extSnap(goldenURI, fullScope)},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  goldenURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			},
		},
		{
			name: "03 late non-Full golden does not change a cold boot",
			tmpl: templateSeed{golden: extSnap(goldenURI, dataScope)},
			want: restoreWant{run: true},
		},
		{
			name:  "04 inherited golden snapshot rejects a malformed URI",
			actor: actorSeed{externalSnapshot: extSnap(malformedURI, fullScope), tmplUID: "current"},
			tmpl:  templateSeed{golden: extSnap(malformedURI, fullScope)},
			want:  restoreWant{code: codes.DataLoss},
		},
		{
			name:  "05 template repoint with a late golden still cold-boots",
			actor: actorSeed{tmplUID: "old-template-uid"},
			tmpl:  templateSeed{golden: extSnap(goldenURI, fullScope)},
			want:  restoreWant{run: true},
		},
		{
			name:  "06 Full durable snapshot restores itself in Full",
			actor: actorSeed{externalSnapshot: extSnap(actorURI, fullScope)},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			},
		},
		{
			name: "07 durable snapshot built on the current template stays Full",
			actor: actorSeed{
				externalSnapshot: extSnap(actorURI, fullScope),
				tmplUID:          "current",
			},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			},
		},
		{
			// The snapshot's guest state was built on another template; the
			// repointed actor must drop to Data so the new template's image
			// boots fresh and only the volume data carries over.
			name: "08 repointed actor's Full durable snapshot drops to Data",
			actor: actorSeed{
				externalSnapshot: extSnap(actorURI, fullScope),
				tmplUID:          "mismatch",
			},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			},
		},
		{
			name:  "08a Rootfs durable snapshot restores as Rootfs",
			actor: actorSeed{externalSnapshot: extSnap(actorURI, rootfsScope)},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			},
		},
		{
			// A rootfs upper is only valid over the image it was written on,
			// so a repointed actor drops to Data like a Full snapshot does.
			name: "08b repointed actor's Rootfs durable snapshot drops to Data",
			actor: actorSeed{
				externalSnapshot: extSnap(actorURI, rootfsScope),
				tmplUID:          "mismatch",
			},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			},
		},
		{
			name:  "09 Data durable snapshot restores as Data",
			actor: actorSeed{externalSnapshot: extSnap(actorURI, dataScope)},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			},
		},
		{
			// The template's golden snapshot never supplies guest state for a
			// Data snapshot: it restores as plain Data.
			name:  "10 Data durable snapshot ignores the template's golden",
			actor: actorSeed{externalSnapshot: extSnap(actorURI, dataScope)},
			tmpl:  templateSeed{golden: extSnap(goldenURI, fullScope)},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			},
		},
		{
			// A stored snapshot always records its fidelity; one without is a
			// drifted record, and ateapi forwards it unchanged for atelet's
			// validation to reject rather than guessing MEMORY.
			name:  "11 unspecified durable fidelity goes out unspecified",
			actor: actorSeed{externalSnapshot: extSnap(actorURI, unspecScope)},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED,
			},
		},
		{
			name:  "12 malformed durable snapshot URI fails with DataLoss",
			actor: actorSeed{externalSnapshot: extSnap(malformedURI, fullScope)},
			want:  restoreWant{code: codes.DataLoss},
		},
		{
			// A pause snapshot's upload is PENDING until atelet reports it,
			// but a local restore names it anyway, so atelet can download the
			// snapshot if it detached the actor before the report landed.
			name: "13 Full pause snapshot restores locally as Full",
			actor: actorSeed{
				localSnapshot:   localSnap(localSnapshotName, fullScope),
				assignedRuntime: amd64RT,
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				kind:         ateattr.SnapshotKindLocal,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			},
		},
		{
			name: "14 local snapshot built on the current template stays Full",
			actor: actorSeed{
				localSnapshot: localSnap(localSnapshotName, fullScope),
				localTmplUID:  "current",
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				kind:         ateattr.SnapshotKindLocal,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			},
		},
		{
			name: "14a repointed actor's Full local snapshot drops to Data",
			actor: actorSeed{
				localSnapshot: localSnap(localSnapshotName, fullScope),
				localTmplUID:  "mismatch",
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			},
		},
		{
			name: "14b repointed actor's Rootfs local snapshot drops to Data",
			actor: actorSeed{
				localSnapshot: localSnap(localSnapshotName, rootfsScope),
				localTmplUID:  "mismatch",
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			},
		},
		{
			name: "15 Data pause snapshot restores locally as Data",
			actor: actorSeed{
				localSnapshot: localSnap(localSnapshotName, dataScope),
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				kind:         ateattr.SnapshotKindLocal,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			},
		},
		{
			name: "16 Data pause snapshot ignores the template's golden",
			actor: actorSeed{
				localSnapshot: localSnap(localSnapshotName, dataScope),
			},
			tmpl: templateSeed{
				golden: extSnap(goldenURI, fullScope),
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				kind:         ateattr.SnapshotKindLocal,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			},
		},
		{
			// The local snapshot takes precedence at restore, carrying its own
			// scope and upload rather than the durable snapshot's.
			name: "17 local snapshot wins over a Full durable snapshot",
			actor: actorSeed{
				localSnapshot:    localSnap(localSnapshotName, dataScope),
				externalSnapshot: extSnap(actorURI, fullScope),
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				kind:         ateattr.SnapshotKindLocal,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			},
		},
		{
			// An older external snapshot's template mismatch does not affect a
			// local pause restore captured under the current template.
			name: "18 local snapshot ignores an older external snapshot's template mismatch",
			actor: actorSeed{
				localSnapshot: func() *ateapipb.Snapshot {
					s := localSnap(localSnapshotName, fullScope)
					s.ActorTemplateUid = "current"
					return s
				}(),
				externalSnapshot: extSnap(actorURI, fullScope),
				tmplUID:          "mismatch",
				localTmplUID:     "current",
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				kind:         ateattr.SnapshotKindLocal,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			},
		},
		{
			name: "18a newer durable snapshot wins over an older local snapshot",
			actor: actorSeed{
				localSnapshot:    localSnap(localSnapshotName, fullScope),
				externalSnapshot: extSnap(actorURI, rootfsScope),
				externalNewer:    true,
			},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			},
		},
		{
			name: "19 stamped Full durable snapshot on matching worker restores in Full",
			actor: actorSeed{
				externalSnapshot: stampedExtSnap(ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, actorURI, fullScope, amd64RT),
				tmplUID:          "current",
				assignedRuntime:  amd64RT,
			},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			},
		},
		{
			name: "20 stamped actor-owned Full durable snapshot on incompatible worker drops to Rootfs",
			actor: actorSeed{
				externalSnapshot: stampedExtSnap(ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, actorURI, fullScope, amd64RT),
				tmplUID:          "current",
				assignedRuntime:  arm64RT,
			},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			},
		},
		{
			name: "21 stamped borrowed golden snapshot on incompatible worker drops to Rootfs",
			actor: actorSeed{
				externalSnapshot: stampedExtSnap(ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, goldenURI, fullScope, amd64RT),
				tmplUID:          "current",
				assignedRuntime:  arm64RT,
			},
			tmpl: templateSeed{golden: stampedExtSnap(ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, goldenURI, fullScope, amd64RT)},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  goldenURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			},
		},
		{
			name: "22 stamped explicit source_tag clone on incompatible worker drops to Rootfs",
			actor: actorSeed{
				externalSnapshot: stampedExtSnap(ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, actorURI, fullScope, amd64RT),
				tmplUID:          "current",
				sourceTag:        &ateapipb.ObjectRef{Atespace: "team-a", Name: "user-tag"},
				assignedRuntime:  arm64RT,
			},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			},
		},
		{
			// A repointed actor drops to Volumes whatever the worker's runtime.
			name: "23 stamped Full durable snapshot with replaced template on incompatible worker drops to Volumes",
			actor: actorSeed{
				externalSnapshot: stampedExtSnap(ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, actorURI, fullScope, amd64RT),
				tmplUID:          "mismatch",
				assignedRuntime:  arm64RT,
			},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			},
		},
		{
			name: "24 stamped Rootfs durable snapshot on worker with different runtime stays Rootfs",
			actor: actorSeed{
				externalSnapshot: stampedExtSnap(ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, actorURI, rootfsScope, amd64RT),
				tmplUID:          "current",
				assignedRuntime:  arm64RT,
			},
			want: restoreWant{
				snapshotName: durableSnapshotName,
				kind:         ateattr.SnapshotKindLatest,
				snapshotURI:  actorURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			},
		},
		{
			name: "24a stamped local snapshot on incompatible worker drops to Rootfs",
			actor: actorSeed{
				localSnapshot:   stampedLocalSnap(localSnapshotName, fullScope, amd64RT),
				localTmplUID:    "current",
				assignedRuntime: arm64RT,
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				kind:         ateattr.SnapshotKindLocal,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			},
		},
		{
			name: "24b stamped local snapshot on matching worker restores in Full",
			actor: actorSeed{
				localSnapshot:   stampedLocalSnap(localSnapshotName, fullScope, amd64RT),
				localTmplUID:    "current",
				assignedRuntime: amd64RT,
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				kind:         ateattr.SnapshotKindLocal,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			},
		},
		{
			// A recorded upload changes nothing: a local restore names the
			// pause snapshot's upload whether or not it is COMPLETED.
			name: "25 uploaded pause snapshot restores locally and sends its upload",
			actor: actorSeed{
				localSnapshot: uploadedLocalSnap(localSnapshotName, fullScope),
			},
			want: restoreWant{
				snapshotName: localSnapshotName,
				kind:         ateattr.SnapshotKindLocal,
				snapshotURI:  pauseUploadURI,
				scope:        ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			},
		},
		{
			// A local restore names the pause snapshot's upload, so a record
			// that cannot name it fails, as a malformed durable snapshot URI
			// does.
			name:  "26 local snapshot with no upload recorded fails with DataLoss",
			actor: actorSeed{localSnapshot: localSnapWithUpload(localSnapshotName, fullScope, nil)},
			want:  restoreWant{code: codes.DataLoss},
		},
		{
			name: "27 local snapshot with a malformed upload URI fails with DataLoss",
			actor: actorSeed{localSnapshot: localSnapWithUpload(localSnapshotName, fullScope, &ateapipb.SnapshotStorage{
				Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING,
				Object: &ateapipb.ObjectSnapshot{SnapshotUri: malformedURI},
			})},
			want: restoreWant{code: codes.DataLoss},
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
				SnapshotConfig: &ateapipb.SnapshotConfig{
					StorageLocation: testStorageLocation,
				},
			}
			if tt.tmpl.golden != nil {
				tmpl.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
					GoldenTag: &ateapipb.ObjectRef{Atespace: "ns", Name: "golden"},
				}}
			}
			createdTmpl, err := persistence.CreateActorTemplate(ctx, tmpl)
			if err != nil {
				t.Fatalf("create template: %v", err)
			}
			if tt.tmpl.golden != nil {
				if _, err := persistence.CreateTag(ctx, &ateapipb.Tag{
					Metadata:    &ateapipb.ResourceMetadata{Atespace: "ns", Name: "golden"},
					SourceActor: &ateapipb.ObjectRef{Atespace: "ns", Name: "golden"},
					Scope:       ateapipb.TagScope_TAG_SCOPE_PUBLISHED,
					Status:      &ateapipb.TagStatus{ActorTemplateUid: createdTmpl.GetMetadata().GetUid(), Snapshot: tt.tmpl.golden},
				}); err != nil {
					t.Fatalf("create golden tag: %v", err)
				}
			}
			if createdTmpl.GetMetadata().GetUid() == "" {
				t.Fatal("created template has no UID; the matching tmplUID case would be vacuous")
			}

			// Paused and suspended actors are both SUSPENDED; a pause
			// checkpoint is what makes the resume local.
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			var extGen, localGen int32
			seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, func(a *ateapipb.Actor) {
				a.SourceTag = tt.actor.sourceTag
				a.Status.WorkerAssignment = wireTestAssignment()
				a.Status.WorkerAssignment.SandboxRuntime = proto.CloneOf(tt.actor.assignedRuntime)
				resolve := func(uid string) string {
					if uid == "current" {
						return createdTmpl.GetMetadata().GetUid()
					}
					return uid
				}
				var gen int32
				seedExternal := func() {
					if tt.actor.externalSnapshot == nil {
						return
					}
					gen++
					extGen = gen
					ext := proto.CloneOf(tt.actor.externalSnapshot)
					ext.Generation = gen
					if ext.ActorTemplateUid == "" {
						ext.ActorTemplateUid = resolve(tt.actor.tmplUID)
					}
					a.Status.Snapshots = append(a.Status.Snapshots, ext)
				}
				seedLocal := func() {
					if tt.actor.localSnapshot == nil {
						return
					}
					gen++
					localGen = gen
					a.Status.AssignedNode = "node-1"
					loc := proto.CloneOf(tt.actor.localSnapshot)
					loc.Generation = gen
					if loc.ActorTemplateUid == "" {
						loc.ActorTemplateUid = resolve(tt.actor.localTmplUID)
					} else if loc.ActorTemplateUid == "current" {
						loc.ActorTemplateUid = createdTmpl.GetMetadata().GetUid()
					}
					a.Status.Snapshots = append(a.Status.Snapshots, loc)
				}
				if tt.actor.externalNewer {
					seedLocal()
					seedExternal()
				} else {
					seedExternal()
					seedLocal()
				}
				a.Status.LastAssignedGeneration = gen
			})

			actor, loadedTmpl, err := w.loadActorForResume(ctx, actorRef)
			// Assignment checks the URI before it claims a worker.
			if err == nil {
				err = checkSnapshotURI(actorRef, actor)
			}
			var tele restoreTelemetry
			if err == nil {
				tele, err = w.ensureAteletRestored(ctx, actorRef, actor, loadedTmpl, nil)
			}
			if got := apierror.Code(err); got != tt.want.code {
				t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, tt.want.code, err)
			}

			restore, run := atelet.requests()
			if tt.want.code != codes.OK {
				if restore != nil || run != nil {
					t.Fatalf("atelet received a request after a resolution error: restore=%v run=%v", restore, run)
				}
				return
			}

			wantSandboxRuntime := wantAteletRuntime[tt.actor.assignedRuntime]
			if tt.want.run {
				if run == nil || restore != nil {
					t.Fatalf("atelet requests = (restore=%v, run=%v), want exactly one Run", restore, run)
				}
				if !proto.Equal(run.GetSandboxRuntime(), wantSandboxRuntime) {
					t.Errorf("run ActorSandboxRuntime = %v, want %v", run.GetSandboxRuntime(), wantSandboxRuntime)
				}
				if tele.SnapshotKind != ateattr.SnapshotKindBoot {
					t.Errorf("SnapshotKind = %q, want %q", tele.SnapshotKind, ateattr.SnapshotKindBoot)
				}
				return
			}
			if restore == nil || run != nil {
				t.Fatalf("atelet requests = (restore=%v, run=%v), want exactly one Restore", restore, run)
			}
			if !proto.Equal(restore.GetSandboxRuntime(), wantSandboxRuntime) {
				t.Errorf("restore ActorSandboxRuntime = %v, want %v", restore.GetSandboxRuntime(), wantSandboxRuntime)
			}
			if got := restore.GetSnapshot().GetSnapshotUuid(); got != tt.want.snapshotName {
				t.Errorf("Snapshot.SnapshotUuid = %q, want %q", got, tt.want.snapshotName)
			}
			if got := restore.GetSnapshot().GetObject().GetSnapshotUri(); got != tt.want.snapshotURI {
				t.Errorf("Snapshot.Object.SnapshotUri = %q, want %q", got, tt.want.snapshotURI)
			}
			if got := restore.GetSnapshot().GetFidelity(); got != tt.want.scope {
				t.Errorf("restore scope = %v, want %v", got, tt.want.scope)
			}
			if want := ateattr.SnapshotFidelityValue(tt.want.scope); tele.WireFidelity != want {
				t.Errorf("WireFidelity = %q, want %q, the fidelity sent to atelet", tele.WireFidelity, want)
			}
			if tele.SnapshotKind != tt.want.kind {
				t.Errorf("SnapshotKind = %q, want %q", tele.SnapshotKind, tt.want.kind)
			}
			wantGen := extGen
			if tt.want.snapshotName == localSnapshotName {
				wantGen = localGen
			}
			if got := restore.GetSnapshot().GetGeneration(); got != wantGen {
				t.Errorf("Snapshot.Generation = %d, want %d, the restored snapshot's", got, wantGen)
			}
		})
	}
}

// publishContextVolumePlugin records AttachVolume requests and hands back a
// fixed publish context, standing in for a driver whose node plugin needs
// attachment metadata.
type publishContextVolumePlugin struct {
	volume.VolumePluginControlPlane
	publishContext map[string]string
	attachCalls    []volume.AttachVolumeRequest
}

func (p *publishContextVolumePlugin) AttachVolume(ctx context.Context, req volume.AttachVolumeRequest) (volume.AttachVolumeResponse, error) {
	p.attachCalls = append(p.attachCalls, req)
	return volume.AttachVolumeResponse{PublishContext: p.publishContext}, nil
}

// The attach step hands the publish context to the Restore call in memory and
// leaves the stored actor untouched.
func TestEnsureVolumesAttached_ReturnsPublishContext(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RESUMING,
			ExternalVolumes: &ateapipb.ExternalVolumes{Volumes: []*ateapipb.ExternalVolumeStatus{
				{Name: "mounted", StorageVolumeId: "storage-mounted", VolumeType: "mock"},
				{Name: "unmounted", StorageVolumeId: "storage-unmounted", VolumeType: "mock"},
			}},
		},
	})
	actor, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}

	plugin := &publishContextVolumePlugin{publishContext: map[string]string{"devicePath": "/dev/xvdba"}}
	w := &ActorWorkflow{
		store: persistence,
		pluginRegistry: &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{
			"mock": plugin,
		}},
	}
	worker := &ateapipb.Worker{NodeName: "node-1"}
	tmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{Name: "mounted", External: &ateapipb.ExternalVolumeSource{Template: &ateapipb.ExternalVolumeTemplate{StorageClassName: "sc"}}},
			{Name: "unmounted", External: &ateapipb.ExternalVolumeSource{Template: &ateapipb.ExternalVolumeTemplate{StorageClassName: "sc"}}},
		},
		Containers: []*ateapipb.Container{
			{Name: "main", Image: "img", VolumeMounts: []*ateapipb.VolumeMount{{Name: "mounted", MountPath: "/data"}}},
		},
	}

	got, err := w.ensureVolumesAttached(ctx, actor, worker, tmpl)
	if err != nil {
		t.Fatalf("ensureVolumesAttached: %v", err)
	}
	want := map[string]map[string]string{"mounted": {"devicePath": "/dev/xvdba"}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("publish contexts mismatch (-want +got):\n%s", diff)
	}
	wantCalls := []volume.AttachVolumeRequest{{VolumeID: "storage-mounted", Node: "node-1"}}
	if diff := cmp.Diff(wantCalls, plugin.attachCalls); diff != "" {
		t.Errorf("attachCalls mismatch (-want +got):\n%s", diff)
	}

	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if diff := cmp.Diff(actor, stored, protocmp.Transform()); diff != "" {
		t.Errorf("attach wrote to the stored actor (-before +after):\n%s", diff)
	}
}

// createCountingVolumePlugin provisions volumes and counts the calls.
type createCountingVolumePlugin struct {
	volume.VolumePluginControlPlane
	creates int
}

func (p *createCountingVolumePlugin) CreateVolume(ctx context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	p.creates++
	return volume.CreateVolumeResponse{VolumeID: "id-" + req.Name}, nil
}

// An actor being deleted gets no volume provisioned by a resume.
func TestEnsureVolumesCreated(t *testing.T) {
	tmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{{
			Name:     "data",
			External: &ateapipb.ExternalVolumeSource{Template: &ateapipb.ExternalVolumeTemplate{StorageClassName: "sc"}},
		}},
	}
	tests := []struct {
		name        string
		state       ateapipb.ActorState
		wantCode    codes.Code
		wantCreates int
	}{
		{"suspended actor gets its volume", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, codes.OK, 1},
		{"deleting actor gets no volume", ateapipb.ActorState_ACTOR_STATE_DELETING, codes.FailedPrecondition, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
				Status: &ateapipb.ActorStatus{
					State:           tt.state,
					ExternalVolumes: &ateapipb.ExternalVolumes{Volumes: []*ateapipb.ExternalVolumeStatus{{Name: "data", VolumeType: "mock"}}},
				},
			})
			actor, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}

			plugin := &createCountingVolumePlugin{}
			w := &ActorWorkflow{
				store:          persistence,
				pluginRegistry: &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock": plugin}},
				storageClassLister: &fakeStorageClassLister{storageClasses: map[string]*storagev1.StorageClass{
					"sc": {ObjectMeta: metav1.ObjectMeta{Name: "sc"}, Provisioner: "mock"},
				}},
			}

			_, err = w.ensureVolumesCreated(ctx, actorRef, actor, tmpl)
			if code := apierror.Code(err); code != tt.wantCode {
				t.Fatalf("ensureVolumesCreated code = %v, want %v; error = %v", code, tt.wantCode, err)
			}
			if plugin.creates != tt.wantCreates {
				t.Errorf("CreateVolume calls = %d, want %d", plugin.creates, tt.wantCreates)
			}
			stored, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if provisioned := stored.GetStatus().GetExternalVolumes().GetVolumes()[0].GetStorageVolumeId() != ""; provisioned != (tt.wantCreates > 0) {
				t.Errorf("stored volume provisioned = %t, want %t", provisioned, tt.wantCreates > 0)
			}
		})
	}
}

// filledUpScheduler hands out workers in order and refuses room on the ones
// named in full. It stands in for a stale worker cache: Schedule offers a
// worker that looked free, and the admit callback that BindActorToWorker runs
// under the Worker's row lock then refuses it with errWorkerFilledUp.
type filledUpScheduler struct {
	workers []*ateapipb.Worker
	full    map[string]bool
	calls   int
	// onSchedule runs after each Schedule call, with the number of calls made
	// so far, so a test can act between attempts.
	onSchedule func(calls int)
	// err, if set, is what every Schedule call returns.
	err error
}

// Schedule repeats the last worker, so a test that offers no free one keeps
// returning a full worker until the backoff ends.
func (s *filledUpScheduler) Schedule(context.Context, scheduling.Constraints) (*ateapipb.Worker, error) {
	i := min(s.calls, len(s.workers)-1)
	s.calls++
	if s.onSchedule != nil {
		s.onSchedule(s.calls)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.workers[i], nil
}

func (s *filledUpScheduler) Applies(*ateapipb.Worker, scheduling.Constraints) bool { return true }

func (s *filledUpScheduler) HasRoom(worker *ateapipb.Worker, _ scheduling.Constraints) bool {
	return !s.full[worker.GetWorkerPod()]
}

// contentionStore is the smallest store that an assignment attempt needs. The
// bug under test is in the metric path, so nothing here reaches a database.
type contentionStore struct {
	actorWorkflowStore
	workers map[string]*ateapipb.Worker
	actor   *ateapipb.Actor
}

func (s *contentionStore) FindWorkerHostingActor(context.Context, string) (string, error) {
	return "", store.ErrNotFound
}

// BindActorToWorker runs admit as the real store does under the Worker's row
// lock, and returns its refusal unchanged.
func (s *contentionStore) BindActorToWorker(_ context.Context, workerName string, _ *ateapipb.ActorAssignment, admit func(*ateapipb.Worker) error) error {
	worker, ok := s.workers[workerName]
	if !ok {
		return store.ErrNotFound
	}
	if admit != nil {
		return admit(worker)
	}
	return nil
}

func (s *contentionStore) UpdateActor(_ context.Context, _ resources.ActorRef, _ store.Precondition, mutate func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	updated := proto.Clone(s.actor).(*ateapipb.Actor)
	if err := mutate(updated); err != nil {
		return nil, err
	}
	s.actor = updated
	return updated, nil
}

// newAssignmentReader gives a workflow its own ManualReader-backed
// Instruments, so a test reads ate.scheduler.assignment.duration without the
// global meter provider.
func newAssignmentReader(t *testing.T) (*Instruments, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	instruments, err := NewInstruments(mp.Meter("ateapi"))
	if err != nil {
		t.Fatalf("NewInstruments: %v", err)
	}
	return instruments, reader
}

// assignmentObservations returns how many observations reached
// ate.scheduler.assignment.duration, and the outcome of each one. An outcome
// that carries error.type is reported as "outcome/error.type".
func assignmentObservations(t *testing.T, reader *sdkmetric.ManualReader) (int, []string) {
	t.Helper()
	m, ok := collectMetric(t, reader, "ate.scheduler.assignment.duration")
	if !ok {
		return 0, nil
	}
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("ate.scheduler.assignment.duration data = %T, want Histogram[float64]", m.Data)
	}
	total := 0
	var outcomes []string
	for _, dp := range hist.DataPoints {
		total += int(dp.Count)
		outcome, _ := dp.Attributes.Value(ateattr.SchedulerOutcomeKey)
		label := outcome.AsString()
		if errType, ok := dp.Attributes.Value(ateattr.ErrorTypeKey); ok {
			label += "/" + errType.AsString()
		}
		for range dp.Count {
			outcomes = append(outcomes, label)
		}
	}
	return total, outcomes
}

// assignmentSum returns the total of every observation on
// ate.scheduler.assignment.duration, in seconds.
func assignmentSum(t *testing.T, reader *sdkmetric.ManualReader) float64 {
	t.Helper()
	m, ok := collectMetric(t, reader, "ate.scheduler.assignment.duration")
	if !ok {
		return 0
	}
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("ate.scheduler.assignment.duration data = %T, want Histogram[float64]", m.Data)
	}
	var sum float64
	for _, dp := range hist.DataPoints {
		sum += dp.Sum
	}
	return sum
}

func contendedWorker(podName string) *ateapipb.Worker {
	return &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID(podName)},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       podName,
		WorkerPodUid:    testWorkerUID(podName),
		NodeName:        "node-1",
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(testSandboxRuntime()),
		},
	}
}

// staticWorkerSource feeds a worker cache a fixed fleet and a watch that
// never fires, which is the stale cache this contention arises from.
type staticWorkerSource struct {
	workers []*ateapipb.Worker
}

func (s *staticWorkerSource) WatchWorkers(context.Context) (*store.WorkerWatch, error) {
	return store.NewWorkerWatch(make(chan store.WorkerEvent), func() {}), nil
}

func (s *staticWorkerSource) ListWorkers(context.Context, store.ListOptions) (store.ListResponse[*ateapipb.Worker], error) {
	return store.ListResponse[*ateapipb.Worker]{Items: s.workers}, nil
}

// newContendedWorkflow builds a workflow around sched, with a SUSPENDED actor
// ready to resume and a cache that holds every worker sched offers.
func newContendedWorkflow(t *testing.T, sched *filledUpScheduler) (*ActorWorkflow, *ateapipb.Actor, *sdkmetric.ManualReader) {
	t.Helper()
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1", Uid: "actor-uid", Version: 1},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}
	byName := make(map[string]*ateapipb.Worker, len(sched.workers))
	for _, worker := range sched.workers {
		byName[worker.GetMetadata().GetName()] = worker
	}
	instruments, reader := newAssignmentReader(t)

	cacheCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	wc := workercache.New(&staticWorkerSource{workers: sched.workers}, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}
	w := &ActorWorkflow{
		store:       &contentionStore{workers: byName, actor: actor},
		workerCache: wc,
		scheduler:   sched,
		instruments: instruments,
	}
	return w, actor, reader
}

// TestEnsureWorkerAssigned_ContentionRecordsOneObservation holds the note on
// ate.scheduler.assignment.duration: ateapi records only the last attempt. A
// resume that loses a race for the last slot on a worker retries, and the
// retried attempt must not reach the histogram. Losing that race is a
// capacity condition, and the registry says a lack of room is not a failure.
func TestEnsureWorkerAssigned_ContentionRecordsOneObservation(t *testing.T) {
	sched := &filledUpScheduler{
		workers: []*ateapipb.Worker{contendedWorker("full-pod"), contendedWorker("free-pod")},
		full:    map[string]bool{"full-pod": true},
	}
	w, actor, reader := newContendedWorkflow(t, sched)

	_, worker, err := w.ensureWorkerAssigned(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, &ateapipb.ActorTemplate{})
	if err != nil {
		t.Fatalf("ensureWorkerAssigned: %v", err)
	}
	if got := worker.GetWorkerPod(); got != "free-pod" {
		t.Errorf("assigned worker = %q, want %q", got, "free-pod")
	}

	got, outcomes := assignmentObservations(t, reader)
	if got != 1 {
		t.Errorf("observations on ate.scheduler.assignment.duration = %d %v, want 1: the retried attempt must not be recorded", got, outcomes)
	}
	for _, outcome := range outcomes {
		if outcome != ateattr.SchedulerOutcomeAssigned {
			t.Errorf("recorded outcome %q, want only %q", outcome, ateattr.SchedulerOutcomeAssigned)
		}
	}
}

// TestEnsureWorkerAssigned_ExhaustedBackoffRecordsOneError covers the other
// end: every attempt loses the race, the retries run out, and the resume
// fails. The histogram carries one observation for the resume, and not one
// for each attempt. error.type is the code RPCService.ResumeActor returns, so
// the scheduler histogram agrees with the router. The returned error carries
// that code too, so ate.actor.lifecycle.operation.duration, which classifies
// the same error, also records Aborted. The record times the last attempt and
// not the backoff, as every other record on the histogram does.
func TestEnsureWorkerAssigned_ExhaustedBackoffRecordsOneError(t *testing.T) {
	sched := &filledUpScheduler{
		workers: []*ateapipb.Worker{contendedWorker("full-pod")},
		full:    map[string]bool{"full-pod": true},
	}
	w, actor, reader := newContendedWorkflow(t, sched)

	_, _, err := w.ensureWorkerAssigned(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, &ateapipb.ActorTemplate{})
	if err == nil {
		t.Fatal("ensureWorkerAssigned succeeded, want a failure after the backoff ended")
	}
	if got := apierror.Code(err); got != codes.Aborted {
		t.Errorf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.Aborted, err)
	}
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("ensureWorkerAssigned error = %v, want one that wraps store.ErrVersionConflict for RPCService.ResumeActor", err)
	}

	got, outcomes := assignmentObservations(t, reader)
	if got != 1 {
		t.Fatalf("observations on ate.scheduler.assignment.duration = %d %v, want 1 for the whole resume", got, outcomes)
	}
	want := ateattr.SchedulerOutcomeError + "/" + codes.Aborted.String()
	if outcomes[0] != want {
		t.Errorf("recorded outcome %q, want %q: error.type must be the code ResumeActor returns", outcomes[0], want)
	}

	// One attempt against the stubs takes microseconds, so a record that
	// includes the sleep after the last attempt goes above half of it.
	_, sleep := replayBackoff(t, assignmentBackoff)
	if recorded := assignmentSum(t, reader); recorded >= sleep.Seconds()/2 {
		t.Errorf("recorded duration = %.3fs, want less than %.3fs: the record must time the last attempt, not the backoff", recorded, sleep.Seconds()/2)
	}
}

// replayBackoff returns how many attempts wait.ExponentialBackoff makes with
// b, and the shortest sleep it makes after the last attempt before it gives
// up. It replays the steps of ExponentialBackoff without jitter.
func replayBackoff(t *testing.T, b wait.Backoff) (attempts int, lastSleep time.Duration) {
	t.Helper()
	b.Jitter = 0
	for b.Steps > 0 {
		attempts++
		if b.Steps == 1 {
			t.Fatal("the backoff ends on Steps, with no sleep after the last attempt; these tests need a backoff that ends on Cap")
		}
		lastSleep = b.Step()
	}
	return attempts, lastSleep
}

// TestEnsureWorkerAssigned_CancelledDuringBackoffRecordsOne covers the loop
// ending between attempts. The caller goes away while an attempt that lost a
// race is waiting to retry, so no attempt records, and the resume would leave
// no trace on the histogram without the one record the loop writes. That
// record times the attempt that lost the race, not the context check after it.
func TestEnsureWorkerAssigned_CancelledDuringBackoffRecordsOne(t *testing.T) {
	sched := &filledUpScheduler{
		workers: []*ateapipb.Worker{contendedWorker("full-pod")},
		full:    map[string]bool{"full-pod": true},
	}
	w, actor, reader := newContendedWorkflow(t, sched)

	// Cancel once the first attempt has lost its race, so the loop ends in the
	// pre-check of the second attempt and not before the first. The sleep
	// makes that attempt long enough to tell apart from the context check.
	const attemptTime = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	sched.onSchedule = func(calls int) {
		if calls == 1 {
			time.Sleep(attemptTime)
			cancel()
		}
	}
	defer cancel()

	_, _, err := w.ensureWorkerAssigned(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, &ateapipb.ActorTemplate{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureWorkerAssigned error = %v, want context.Canceled", err)
	}

	got, outcomes := assignmentObservations(t, reader)
	if got != 1 {
		t.Fatalf("observations on ate.scheduler.assignment.duration = %d %v, want 1 for the whole resume", got, outcomes)
	}
	want := ateattr.SchedulerOutcomeError + "/" + codes.Canceled.String()
	if outcomes[0] != want {
		t.Errorf("recorded outcome %q, want %q", outcomes[0], want)
	}
	if recorded := assignmentSum(t, reader); recorded < attemptTime.Seconds() {
		t.Errorf("recorded duration = %.3fs, want at least %.3fs: the record must time the attempt that lost the race", recorded, attemptTime.Seconds())
	}
}

// TestEnsureWorkerAssigned_CancelledBeforeFirstAttemptRecordsNothing covers a
// context that ended before the loop began. No attempt ran, so there is no
// attempt to time and the histogram gets no record.
func TestEnsureWorkerAssigned_CancelledBeforeFirstAttemptRecordsNothing(t *testing.T) {
	sched := &filledUpScheduler{
		workers: []*ateapipb.Worker{contendedWorker("free-pod")},
	}
	w, actor, reader := newContendedWorkflow(t, sched)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := w.ensureWorkerAssigned(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, &ateapipb.ActorTemplate{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureWorkerAssigned error = %v, want context.Canceled", err)
	}
	if sched.calls != 0 {
		t.Fatalf("Schedule calls = %d, want 0: no attempt may run after the context ended", sched.calls)
	}
	if got, outcomes := assignmentObservations(t, reader); got != 0 {
		t.Errorf("observations on ate.scheduler.assignment.duration = %d %v, want 0", got, outcomes)
	}
}

// TestEnsureWorkerAssigned_CancelledInLastSleepRecordsOne covers the context
// ending in the sleep after the last attempt. The backoff then gives up with
// its own timeout, but the caller went away, so the resume reports the
// context error and not that the retries ran out.
func TestEnsureWorkerAssigned_CancelledInLastSleepRecordsOne(t *testing.T) {
	sched := &filledUpScheduler{
		workers: []*ateapipb.Worker{contendedWorker("full-pod")},
		full:    map[string]bool{"full-pod": true},
	}
	w, actor, reader := newContendedWorkflow(t, sched)

	attempts, _ := replayBackoff(t, assignmentBackoff)
	ctx, cancel := context.WithCancel(context.Background())
	sched.onSchedule = func(calls int) {
		if calls == attempts {
			cancel()
		}
	}
	defer cancel()

	_, _, err := w.ensureWorkerAssigned(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, &ateapipb.ActorTemplate{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureWorkerAssigned error = %v, want context.Canceled", err)
	}
	if sched.calls != attempts {
		t.Fatalf("Schedule calls = %d, want %d: the context must end after the last attempt", sched.calls, attempts)
	}

	got, outcomes := assignmentObservations(t, reader)
	if got != 1 {
		t.Fatalf("observations on ate.scheduler.assignment.duration = %d %v, want 1 for the whole resume", got, outcomes)
	}
	want := ateattr.SchedulerOutcomeError + "/" + codes.Canceled.String()
	if outcomes[0] != want {
		t.Errorf("recorded outcome %q, want %q", outcomes[0], want)
	}
}

// TestEnsureWorkerAssigned_ContextEndsInsideAttemptRecordsOne covers an
// attempt that fails because the context ended inside it. The attempt records
// itself, so the loop must not add a second record, and the caller gets the
// context error and not Aborted.
func TestEnsureWorkerAssigned_ContextEndsInsideAttemptRecordsOne(t *testing.T) {
	sched := &filledUpScheduler{
		workers: []*ateapipb.Worker{contendedWorker("free-pod")},
		err:     fmt.Errorf("list workers: %w", context.DeadlineExceeded),
	}
	w, actor, reader := newContendedWorkflow(t, sched)

	_, _, err := w.ensureWorkerAssigned(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, &ateapipb.ActorTemplate{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ensureWorkerAssigned error = %v, want context.DeadlineExceeded", err)
	}
	if got := apierror.Code(err); got != codes.DeadlineExceeded {
		t.Errorf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.DeadlineExceeded, err)
	}

	got, outcomes := assignmentObservations(t, reader)
	if got != 1 {
		t.Fatalf("observations on ate.scheduler.assignment.duration = %d %v, want 1 for the whole resume", got, outcomes)
	}
	want := ateattr.SchedulerOutcomeError + "/" + codes.DeadlineExceeded.String()
	if outcomes[0] != want {
		t.Errorf("recorded outcome %q, want %q", outcomes[0], want)
	}
}

// testRuntime is a gvisor ActorSandboxRuntime for arch under compatibility schema
// v1. A non-empty bundleHash adds a sandbox_bundle_hash attribute, so two
// runtimes can share an architecture and still differ, as two versions of
// one SandboxConfig do.
func testRuntime(arch, bundleHash string) *ateapipb.ActorSandboxRuntime {
	attrs := []*ateapipb.AttributeEntry{{Key: "architecture", Value: arch}}
	if bundleHash != "" {
		attrs = append(attrs, &ateapipb.AttributeEntry{Key: hardware.AttrSandboxBundleHash, Value: bundleHash})
	}
	return &ateapipb.ActorSandboxRuntime{
		SandboxClass: "gvisor",
		Version:      &ateapipb.VersionedSandboxCompat{SchemaVersion: "v1", Attributes: attrs},
	}
}

// testSandboxRuntime is the amd64 gvisor runtime most tests report and stamp.
func testSandboxRuntime() *ateapipb.ActorSandboxRuntime {
	return testRuntime("amd64", "")
}

// workerSandboxClasses is the sandbox_classes a worker reports to run def as
// its default runtime and restore restorable beyond it, all of def's class.
// The arguments are the ActorSandboxRuntimes a test also stamps snapshots and
// assignments with, so both sides of a match derive from one value.
func workerSandboxClasses(def *ateapipb.ActorSandboxRuntime, restorable ...*ateapipb.ActorSandboxRuntime) []*ateapipb.WorkerSandboxClass {
	class := &ateapipb.WorkerSandboxClass{SandboxClass: def.GetSandboxClass(), DefaultRuntime: &ateapipb.WorkerSandboxRuntime{Version: def.GetVersion()}}
	for _, r := range restorable {
		class.RestorableRuntimes = append(class.RestorableRuntimes, &ateapipb.WorkerSandboxRuntime{Version: r.GetVersion()})
	}
	return []*ateapipb.WorkerSandboxClass{class}
}

// seedRuntimeWorker stores an ACTIVE gvisor worker on node with room for one
// actor, reporting def as its default runtime and restorable beyond it.
func seedRuntimeWorker(t *testing.T, ctx context.Context, st store.Interface, pod, node string, def *ateapipb.ActorSandboxRuntime, restorable ...*ateapipb.ActorSandboxRuntime) *ateapipb.Worker {
	t.Helper()
	worker, err := st.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID(pod)},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       pod,
		WorkerPodUid:    testWorkerUID(pod),
		NodeName:        node,
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(def, restorable...),
		},
	})
	if err != nil {
		t.Fatalf("CreateWorker(%s): %v", pod, err)
	}
	return worker
}

// durableMemorySnapshot is a completed, actor-owned MEMORY snapshot of
// generation 1 under team-a/id1's own prefix, stamped with runtime.
func durableMemorySnapshot(t *testing.T, runtime *ateapipb.ActorSandboxRuntime) *ateapipb.Snapshot {
	t.Helper()
	snap := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap-1", someActorSnapshotURI(t, testStorageLocation, "team-a", "snap-1"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	snap.SandboxRuntime = runtime
	return snap
}

// seedMicroVMWorker stores an ACTIVE microvm worker on node with room for one
// actor, reporting a microvm default runtime.
func seedMicroVMWorker(t *testing.T, ctx context.Context, st store.Interface, pod, node string) *ateapipb.Worker {
	t.Helper()
	worker, err := st.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID(pod)},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "vm-pool",
		WorkerPod:       pod,
		WorkerPodUid:    testWorkerUID(pod),
		NodeName:        node,
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: []*ateapipb.WorkerSandboxClass{{
				SandboxClass: "microvm",
				DefaultRuntime: &ateapipb.WorkerSandboxRuntime{Version: &ateapipb.VersionedSandboxCompat{SchemaVersion: "v1", Attributes: []*ateapipb.AttributeEntry{
					{Key: "architecture", Value: "amd64"},
				}}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("CreateWorker(%s): %v", pod, err)
	}
	return worker
}

// seedActorWithSnapshot stores actor team-a/id1 in state holding snap as its
// only snapshot. node is the AssignedNode a PAUSED actor keeps, which pins
// its resume to that node.
func seedActorWithSnapshot(t *testing.T, ctx context.Context, st store.Interface, state ateapipb.ActorState, node string, snap *ateapipb.Snapshot) *ateapipb.Actor {
	t.Helper()
	return storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status: &ateapipb.ActorStatus{
			State:                  state,
			AssignedNode:           node,
			LastAssignedGeneration: snap.GetGeneration(),
			Snapshots:              []*ateapipb.Snapshot{snap},
		},
	})
}

// newSchedulingWorkflow is a workflow that schedules over a worker cache fed
// by st. The cache is started now and stopped when the test ends.
func newSchedulingWorkflow(t *testing.T, ctx context.Context, st store.Interface) *ActorWorkflow {
	t.Helper()
	cacheCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	wc := workercache.New(st, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}
	return &ActorWorkflow{store: st, workerCache: wc, scheduler: scheduling.New(wc)}
}

// assertAssignedRuntime checks the runtime recorded on actor's assignment,
// which is what its next checkpoint will be stamped with.
func assertAssignedRuntime(t *testing.T, actor *ateapipb.Actor, want *ateapipb.ActorSandboxRuntime) {
	t.Helper()
	if diff := cmp.Diff(want, actor.GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("WorkerAssignment.SandboxRuntime mismatch (-want +got):\n%s", diff)
	}
}

// relaxingTestScheduler stands in for a fleet where no runtime-compatible
// worker has room: a strict Schedule reports no capacity and a relaxed one
// returns relaxedWorker, as the scheduler's cache would. Applies and HasRoom
// are the real ones.
type relaxingTestScheduler struct {
	scheduling.Scheduler
	relaxedWorker *ateapipb.Worker
}

func (s *relaxingTestScheduler) Schedule(_ context.Context, c scheduling.Constraints) (*ateapipb.Worker, error) {
	if c.RequiredRuntime.Runtime() != nil {
		return nil, scheduling.ErrNoCapacity
	}
	return s.relaxedWorker, nil
}

func TestAssignWorkerAttempt_PrefersRuntimeMatchingWorkerAndFallsBack(t *testing.T) {
	amd64RT := testSandboxRuntime()
	arm64RT := testRuntime("arm64", "")
	// Two runtimes of one architecture that differ only by binary: a
	// SandboxConfig whose default moved on while the old version stays
	// enabled for restores.
	newDefaultRT := testRuntime("amd64", "new-default")
	oldRestorableRT := testRuntime("amd64", "old-restorable")
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	suspended := ateapipb.ActorState_ACTOR_STATE_SUSPENDED

	t.Run("places on runtime-compatible worker when available", func(t *testing.T) {
		ctx := context.Background()
		st := newTestPersistence(t)
		seedRuntimeWorker(t, ctx, st, "arm-pod", "node-arm", arm64RT)
		seedRuntimeWorker(t, ctx, st, "amd-pod", "node-amd", amd64RT)
		actor := seedActorWithSnapshot(t, ctx, st, suspended, "", durableMemorySnapshot(t, amd64RT))

		stored, worker, err := newSchedulingWorkflow(t, ctx, st).assignWorkerAttempt(ctx, actorRef, actor, &ateapipb.ActorTemplate{})
		if err != nil {
			t.Fatalf("assignWorkerAttempt: %v", err)
		}
		if got := worker.GetWorkerPod(); got != "amd-pod" {
			t.Errorf("assigned worker = %q, want %q", got, "amd-pod")
		}
		assertAssignedRuntime(t, stored, amd64RT)
	})

	t.Run("records matched restorable_runtimes entry on WorkerAssignment", func(t *testing.T) {
		ctx := context.Background()
		st := newTestPersistence(t)
		seedRuntimeWorker(t, ctx, st, "amd-pod", "node-amd", newDefaultRT, oldRestorableRT)
		actor := seedActorWithSnapshot(t, ctx, st, suspended, "", durableMemorySnapshot(t, oldRestorableRT))

		stored, _, err := newSchedulingWorkflow(t, ctx, st).assignWorkerAttempt(ctx, actorRef, actor, &ateapipb.ActorTemplate{})
		if err != nil {
			t.Fatalf("assignWorkerAttempt: %v", err)
		}
		// The actor restores under the old version, not the worker's default,
		// and its next checkpoint must say so.
		assertAssignedRuntime(t, stored, oldRestorableRT)
	})

	t.Run("falls back to incompatible worker when no runtime-matching worker has room", func(t *testing.T) {
		ctx := context.Background()
		st := newTestPersistence(t)
		seedRuntimeWorker(t, ctx, st, "arm-pod", "node-arm", arm64RT)
		actor := seedActorWithSnapshot(t, ctx, st, suspended, "", durableMemorySnapshot(t, amd64RT))

		stored, worker, err := newSchedulingWorkflow(t, ctx, st).assignWorkerAttempt(ctx, actorRef, actor, &ateapipb.ActorTemplate{})
		if err != nil {
			t.Fatalf("assignWorkerAttempt: %v", err)
		}
		if got := worker.GetWorkerPod(); got != "arm-pod" {
			t.Errorf("assigned worker = %q, want %q", got, "arm-pod")
		}
		// The restore without memory state runs under the fallback worker's default.
		assertAssignedRuntime(t, stored, arm64RT)
	})

	t.Run("does not fall back to a worker of another sandbox class", func(t *testing.T) {
		// A snapshot's files are written by one class's worker and read back
		// by the same, so relaxing the runtime never crosses classes, not
		// even for a volumes-only restore that needs no runtime at all.
		for _, fidelity := range []ateapipb.SnapshotFidelity{
			ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		} {
			t.Run(fidelity.String(), func(t *testing.T) {
				ctx := context.Background()
				st := newTestPersistence(t)
				seedMicroVMWorker(t, ctx, st, "vm-pod", "node-vm")
				snap := durableMemorySnapshot(t, amd64RT)
				snap.Fidelity = fidelity
				actor := seedActorWithSnapshot(t, ctx, st, suspended, "", snap)

				_, _, err := newSchedulingWorkflow(t, ctx, st).assignWorkerAttempt(ctx, actorRef, actor, &ateapipb.ActorTemplate{})
				if apierror.Code(err) != codes.ResourceExhausted {
					t.Fatalf("assignWorkerAttempt with only a microvm worker = %v, want ResourceExhausted", err)
				}
			})
		}
	})

	t.Run("does not relax runtime constraint for local pause snapshot", func(t *testing.T) {
		ctx := context.Background()
		st := newTestPersistence(t)
		seedRuntimeWorker(t, ctx, st, "node1-incompatible", "node-1", arm64RT)
		// A paused actor: SUSPENDED, pinned to its node by a local snapshot.
		local := newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-1", "node-1")
		local.SandboxRuntime = amd64RT
		actor := seedActorWithSnapshot(t, ctx, st, suspended, "node-1", local)

		_, _, err := newSchedulingWorkflow(t, ctx, st).assignWorkerAttempt(ctx, actorRef, actor, &ateapipb.ActorTemplate{})
		if got := apierror.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.ResourceExhausted, err)
		}
		// Nothing was written: the actor waits for its node, still paused,
		// unassigned, and holding no claim anything would have to release.
		after, err := st.GetActor(ctx, actorRef)
		if err != nil {
			t.Fatalf("GetActor: %v", err)
		}
		if got := after.GetStatus().GetState(); got != suspended {
			t.Errorf("state = %v, want SUSPENDED", got)
		}
		if got := after.GetStatus().GetWorkerAssignment(); got != nil {
			t.Errorf("WorkerAssignment = %v, want none", got)
		}
		if _, err := st.FindWorkerHostingActor(ctx, actor.GetMetadata().GetUid()); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("FindWorkerHostingActor() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("relaxed schedule landing on restorable worker records matched restorable runtime", func(t *testing.T) {
		ctx := context.Background()
		st := newTestPersistence(t)
		// A compatible worker freed up between the strict and the relaxed
		// Schedule: the relaxed pick can restore the snapshot after all.
		worker := seedRuntimeWorker(t, ctx, st, "amd-pod", "node-amd", newDefaultRT, oldRestorableRT)
		actor := seedActorWithSnapshot(t, ctx, st, suspended, "", durableMemorySnapshot(t, oldRestorableRT))

		w := &ActorWorkflow{store: st, scheduler: &relaxingTestScheduler{Scheduler: scheduling.New(nil), relaxedWorker: worker}}
		stored, _, err := w.assignWorkerAttempt(ctx, actorRef, actor, &ateapipb.ActorTemplate{})
		if err != nil {
			t.Fatalf("assignWorkerAttempt: %v", err)
		}
		// Relaxing the constraint must not relabel the assignment: it is
		// derived from the snapshot's stamp, not from the relaxed constraints.
		assertAssignedRuntime(t, stored, oldRestorableRT)
	})

	t.Run("records runtime from row-locked worker when cached worker runtime is stale", func(t *testing.T) {
		ctx := context.Background()
		st := newTestPersistence(t)
		// The stored row no longer lists the old version; the cached copy the
		// scheduler hands back still does.
		fresh := seedRuntimeWorker(t, ctx, st, "amd-pod", "node-amd", newDefaultRT)
		stale := proto.CloneOf(fresh)
		stale.Status.SandboxClasses = workerSandboxClasses(newDefaultRT, oldRestorableRT)
		actor := seedActorWithSnapshot(t, ctx, st, suspended, "", durableMemorySnapshot(t, oldRestorableRT))

		w := &ActorWorkflow{store: st, scheduler: &relaxingTestScheduler{Scheduler: scheduling.New(nil), relaxedWorker: stale}}
		stored, bound, err := w.assignWorkerAttempt(ctx, actorRef, actor, &ateapipb.ActorTemplate{})
		if err != nil {
			t.Fatalf("assignWorkerAttempt: %v", err)
		}
		assertAssignedRuntime(t, stored, newDefaultRT)
		if diff := cmp.Diff(fresh.GetStatus().GetSandboxClasses(), bound.GetStatus().GetSandboxClasses(), protocmp.Transform()); diff != "" {
			t.Errorf("returned worker SandboxClasses mismatch, want the fresh row's (-fresh +got):\n%s", diff)
		}
	})
}

func TestValidateAssignedWorker_RuntimeFallback(t *testing.T) {
	t.Run("accepts fallback worker on re-entry for durable snapshot", func(t *testing.T) {
		ctx := context.Background()
		st := newTestPersistence(t)
		arm64RT := testRuntime("arm64", "")
		seedRuntimeWorker(t, ctx, st, "arm-pod", "node-arm", arm64RT)
		seedAssignment(t, st, testWorkerUID("arm-pod"), &ateapipb.ActorAssignment{
			Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "id1"},
			ActorUid: "own-actor-uid",
		})

		// A previous attempt bound the actor relaxed, under the worker's
		// default, because no worker could restore its amd64 snapshot.
		resumingActor := &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1", Uid: "own-actor-uid"},
			Status: &ateapipb.ActorStatus{
				State:                  ateapipb.ActorState_ACTOR_STATE_RESUMING,
				AssignedNode:           "node-arm",
				LastAssignedGeneration: 1,
				Snapshots:              []*ateapipb.Snapshot{durableMemorySnapshot(t, testSandboxRuntime())},
				WorkerAssignment: &ateapipb.WorkerAssignment{
					Worker:          &ateapipb.ObjectRef{Name: testWorkerUID("arm-pod")},
					WorkerNamespace: "worker-ns",
					WorkerPool:      "pool",
					WorkerPod:       "arm-pod",
					WorkerPodUid:    testWorkerUID("arm-pod"),
					NodeName:        "node-arm",
					SandboxRuntime:  arm64RT,
				},
			},
		}

		w := &ActorWorkflow{store: st, scheduler: scheduling.New(nil)}
		worker, err := w.validateAssignedWorker(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, resumingActor, &ateapipb.ActorTemplate{})
		if err != nil {
			t.Fatalf("validateAssignedWorker: %v", err)
		}
		if got := worker.GetWorkerPod(); got != "arm-pod" {
			t.Errorf("validated worker = %q, want %q", got, "arm-pod")
		}
	})

	t.Run("accepts fallback worker on re-entry for an uploaded pause snapshot", func(t *testing.T) {
		ctx := context.Background()
		st := newTestPersistence(t)
		arm64RT := testRuntime("arm64", "")
		seedRuntimeWorker(t, ctx, st, "arm-pod", "node-arm", arm64RT)
		seedAssignment(t, st, testWorkerUID("arm-pod"), &ateapipb.ActorAssignment{
			Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "id1"},
			ActorUid: "own-actor-uid",
		})

		// Re-entry must accept a previously allowed ROOTFS assignment.
		local := newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-1", "node-arm")
		local.SandboxRuntime = testSandboxRuntime()
		local.DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
		resumingActor := &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1", Uid: "own-actor-uid"},
			Status: &ateapipb.ActorStatus{
				State:                  ateapipb.ActorState_ACTOR_STATE_RESUMING,
				AssignedNode:           "node-arm",
				LastAssignedGeneration: 1,
				Snapshots:              []*ateapipb.Snapshot{local},
				WorkerAssignment: &ateapipb.WorkerAssignment{
					Worker:          &ateapipb.ObjectRef{Name: testWorkerUID("arm-pod")},
					WorkerNamespace: "worker-ns",
					WorkerPool:      "pool",
					WorkerPod:       "arm-pod",
					WorkerPodUid:    testWorkerUID("arm-pod"),
					NodeName:        "node-arm",
					SandboxRuntime:  arm64RT,
				},
			},
		}

		w := &ActorWorkflow{store: st, scheduler: scheduling.New(nil)}
		worker, err := w.validateAssignedWorker(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, resumingActor, &ateapipb.ActorTemplate{})
		if err != nil {
			t.Fatalf("validateAssignedWorker: %v", err)
		}
		if got := worker.GetWorkerPod(); got != "arm-pod" {
			t.Errorf("validated worker = %q, want %q", got, "arm-pod")
		}
	})
}

// TestRequiredSnapshotRuntime pins which snapshot a resume takes its runtime
// constraint from, and when it takes none.
func TestRequiredSnapshotRuntime(t *testing.T) {
	const tmplUID = "tmpl-uid"
	tmpl := &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Uid: tmplUID}}
	localRT := testRuntime("amd64", "paused-under")
	durableRT := testRuntime("amd64", "suspended-under")
	memory := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	rootfs := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	volumes := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	completed := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	pending := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING

	durable := func(gen int32, fidelity ateapipb.SnapshotFidelity, capturedUnder string, status ateapipb.SnapshotStorageStatus, rt *ateapipb.ActorSandboxRuntime) *ateapipb.Snapshot {
		snap := newDurableSnapshot(gen, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, fidelity, capturedUnder, "snap", "gs://bucket/snap", status)
		snap.SandboxRuntime = rt
		return snap
	}
	local := func(gen int32, fidelity ateapipb.SnapshotFidelity, capturedUnder string, rt *ateapipb.ActorSandboxRuntime) *ateapipb.Snapshot {
		snap := newPausedSnapshot(t, gen, fidelity, capturedUnder, "local", "node-1")
		snap.SandboxRuntime = rt
		return snap
	}

	tests := []struct {
		name      string
		snapshots []*ateapipb.Snapshot
		want      *ateapipb.ActorSandboxRuntime
	}{
		{name: "no snapshot cold-boots unconstrained"},
		{
			name:      "completed MEMORY durable snapshot requires its runtime",
			snapshots: []*ateapipb.Snapshot{durable(1, memory, tmplUID, completed, durableRT)},
			want:      durableRT,
		},
		{
			name:      "completed ROOTFS durable snapshot restores no memory state",
			snapshots: []*ateapipb.Snapshot{durable(1, rootfs, tmplUID, completed, durableRT)},
		},
		{
			name:      "VOLUMES durable snapshot restores no memory state",
			snapshots: []*ateapipb.Snapshot{durable(1, volumes, tmplUID, completed, durableRT)},
		},
		{
			name:      "durable snapshot captured under a replaced template is not restored",
			snapshots: []*ateapipb.Snapshot{durable(1, memory, "old-tmpl-uid", completed, durableRT)},
		},
		{
			name:      "durable snapshot with no template uid keeps its runtime",
			snapshots: []*ateapipb.Snapshot{durable(1, memory, "", completed, durableRT)},
			want:      durableRT,
		},
		{
			name:      "unstamped durable snapshot imposes no constraint",
			snapshots: []*ateapipb.Snapshot{durable(1, memory, tmplUID, completed, nil)},
		},
		{
			name:      "PENDING durable snapshot is not a restore source",
			snapshots: []*ateapipb.Snapshot{durable(1, memory, tmplUID, pending, durableRT)},
		},
		{
			name:      "local checkpoint requires its runtime",
			snapshots: []*ateapipb.Snapshot{local(1, memory, tmplUID, localRT)},
			want:      localRT,
		},
		{
			name:      "local checkpoint with no template uid keeps its runtime",
			snapshots: []*ateapipb.Snapshot{local(1, memory, "", localRT)},
			want:      localRT,
		},
		{
			name:      "local checkpoint captured under a replaced template is not restored",
			snapshots: []*ateapipb.Snapshot{local(1, memory, "old-tmpl-uid", localRT)},
		},
		{
			// ensureAteletRestored restores a local checkpoint before any
			// durable snapshot, so the constraint follows it regardless of
			// which is newer.
			name: "local checkpoint takes precedence over a newer durable snapshot",
			snapshots: []*ateapipb.Snapshot{
				local(1, memory, tmplUID, localRT),
				durable(2, memory, tmplUID, completed, durableRT),
			},
			want: localRT,
		},
		{
			name: "ROOTFS local checkpoint imposes no constraint beside a stamped durable snapshot",
			snapshots: []*ateapipb.Snapshot{
				local(2, rootfs, tmplUID, localRT),
				durable(1, memory, tmplUID, completed, durableRT),
			},
		},
		{
			name: "VOLUMES local checkpoint imposes no constraint beside a stamped durable snapshot",
			snapshots: []*ateapipb.Snapshot{
				local(2, volumes, tmplUID, localRT),
				durable(1, memory, tmplUID, completed, durableRT),
			},
		},
		{
			// Restore uses the newer local snapshot's template.
			name: "local checkpoint ignores an older durable snapshot's replaced template",
			snapshots: []*ateapipb.Snapshot{
				durable(1, memory, "old-tmpl-uid", completed, durableRT),
				local(2, memory, tmplUID, localRT),
			},
			want: localRT,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actor := &ateapipb.Actor{Status: &ateapipb.ActorStatus{Snapshots: tc.snapshots}}
			if diff := cmp.Diff(tc.want, requiredSnapshotRuntime(actor, tmpl), protocmp.Transform()); diff != "" {
				t.Errorf("requiredSnapshotRuntime mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// mockVolume returns a volume served by the "mock" plugin.
func mockVolume(name, storageID string) *ateapipb.ExternalVolumeStatus {
	return &ateapipb.ExternalVolumeStatus{Name: name, StorageVolumeId: storageID, VolumeType: "mock"}
}

// volumeTemplate returns a template that declares the volumes named in
// declared and mounts those named in mounted.
func volumeTemplate(declared []string, mounted ...string) *ateapipb.ActorTemplate {
	tmpl := &ateapipb.ActorTemplate{
		Containers: []*ateapipb.Container{{Name: "main", Image: "img"}},
	}
	for _, name := range declared {
		tmpl.Volumes = append(tmpl.Volumes, &ateapipb.Volume{Name: name, External: &ateapipb.ExternalVolumeSource{Template: &ateapipb.ExternalVolumeTemplate{StorageClassName: "sc"}}})
	}
	for _, name := range mounted {
		tmpl.Containers[0].VolumeMounts = append(tmpl.Containers[0].VolumeMounts, &ateapipb.VolumeMount{Name: name, MountPath: "/" + name})
	}
	return tmpl
}

// seedVolumeAssignFixture stores one free gvisor worker on node and a
// SUSPENDED actor holding volumes, and returns the stored actor plus a
// started worker cache.
func seedVolumeAssignFixture(t *testing.T, ctx context.Context, persistence store.Interface, node string, volumes ...*ateapipb.ExternalVolumeStatus) (*ateapipb.Actor, *workercache.Cache) {
	t.Helper()
	if _, err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-1")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		WorkerPodUid:    testWorkerUID("pod-1"),
		NodeName:        node,
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(testSandboxRuntime()),
		},
	}); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ExternalVolumes: &ateapipb.ExternalVolumes{Volumes: volumes}},
	})
	actor, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	cacheCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}
	return actor, wc
}

// newVolumeAssignWorkflow returns a workflow that schedules from wc and
// serves "mock" volumes with plugin.
func newVolumeAssignWorkflow(st store.Interface, wc *workercache.Cache, plugin volume.VolumePluginControlPlane) *ActorWorkflow {
	return &ActorWorkflow{
		store:          st,
		workerCache:    wc,
		scheduler:      scheduling.New(wc),
		pluginRegistry: &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock": plugin}},
	}
}

// TestResume_ChecksMountedVolumesBeforeAttach runs the assignment write and
// the attach step together: the assignment write records AssignedNode on the
// actor, and ensureVolumesAttached publishes only mounted volumes to it. A
// selected volume without a storage volume ID fails the attach step with
// FailedPrecondition before any publish call, even when a valid volume
// precedes it.
func TestResume_ChecksMountedVolumesBeforeAttach(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	tests := []struct {
		name string
		// last is the second mounted volume, placed after every other volume.
		last *ateapipb.ExternalVolumeStatus
		// wantErr is a substring of the expected FailedPrecondition; empty
		// means the attach succeeds.
		wantErr string
	}{{
		name: "all provisioned",
		last: mockVolume("b", "id-b"),
	}, {
		name:    "mounted volume without storage ID",
		last:    mockVolume("b", ""),
		wantErr: `volume "b" has no storage volume ID`,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			actor, wc := seedVolumeAssignFixture(t, ctx, persistence, "node-1",
				mockVolume("a", "id-a"),
				mockVolume("u", ""),
				mockVolume("o", ""),
				tt.last,
			)
			plugin := &publishContextVolumePlugin{}
			w := newVolumeAssignWorkflow(persistence, wc, plugin)
			// u is declared but not mounted; o is not declared at all.
			tmpl := volumeTemplate([]string{"a", "u", "b"}, "a", "b")

			assigned, worker, err := w.assignWorkerAttempt(ctx, actorRef, actor, tmpl)
			if err != nil {
				t.Fatalf("assignWorkerAttempt: %v", err)
			}
			if got := assigned.GetStatus().GetAssignedNode(); got != "node-1" {
				t.Errorf("assigned actor AssignedNode = %q, want node-1", got)
			}
			_, attachErr := w.ensureVolumesAttached(ctx, assigned, worker, tmpl)

			nameByID := make(map[string]string)
			for _, vol := range assigned.GetStatus().GetExternalVolumes().GetVolumes() {
				nameByID[vol.GetStorageVolumeId()] = vol.GetName()
			}

			if tt.wantErr != "" {
				if got := apierror.Code(attachErr); got != codes.FailedPrecondition {
					t.Fatalf("ensureVolumesAttached error = %v (code %v), want FailedPrecondition", attachErr, got)
				}
				if !strings.Contains(attachErr.Error(), tt.wantErr) {
					t.Errorf("ensureVolumesAttached error = %q, want it to contain %q", attachErr, tt.wantErr)
				}
				if len(plugin.attachCalls) != 0 {
					t.Errorf("attach calls = %v, want none", plugin.attachCalls)
				}
				return
			}
			if attachErr != nil {
				t.Fatalf("ensureVolumesAttached: %v", attachErr)
			}
			var published []string
			for _, call := range plugin.attachCalls {
				if call.Node != "node-1" {
					t.Errorf("attach of %q went to node %q, want node-1", call.VolumeID, call.Node)
				}
				published = append(published, nameByID[call.VolumeID])
			}
			if diff := cmp.Diff([]string{"a", "b"}, published); diff != "" {
				t.Errorf("published volumes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestCanRelaxRuntime pins when a resume may drop its runtime constraint: only
// when it has one, and never while a local pause checkpoint pins the actor.
func TestCanRelaxRuntime(t *testing.T) {
	rt := testSandboxRuntime()
	memory := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	completed := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	pending := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
	suspended := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	resuming := ateapipb.ActorState_ACTOR_STATE_RESUMING

	durable := func(gen int32) *ateapipb.Snapshot {
		return newDurableSnapshot(gen, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, memory, "", "snap", "gs://bucket/snap", completed)
	}
	local := func(gen int32) *ateapipb.Snapshot {
		return newPausedSnapshot(t, gen, memory, "", "local", "node-1")
	}
	localDurable := func(gen int32, storageStatus ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
		snap := local(gen)
		snap.GetDurableSnapshot().Status = storageStatus
		return snap
	}
	localNoDurable := func(gen int32) *ateapipb.Snapshot {
		snap := local(gen)
		snap.DurableSnapshot = nil
		return snap
	}

	tests := []struct {
		name      string
		state     ateapipb.ActorState
		node      string
		snapshots []*ateapipb.Snapshot
		required  *ateapipb.ActorSandboxRuntime
		want      bool
	}{
		{
			name:      "nothing to relax without a runtime constraint",
			state:     suspended,
			snapshots: []*ateapipb.Snapshot{durable(1)},
		},
		{
			name:      "durable snapshot may fall back",
			state:     suspended,
			snapshots: []*ateapipb.Snapshot{durable(1)},
			required:  rt,
			want:      true,
		},
		{
			name:      "local checkpoint stays pinned",
			state:     suspended,
			node:      "node-1",
			snapshots: []*ateapipb.Snapshot{local(1)},
			required:  rt,
		},
		{
			name:      "local checkpoint stays pinned beside a durable snapshot",
			state:     suspended,
			node:      "node-1",
			snapshots: []*ateapipb.Snapshot{durable(1), local(2)},
			required:  rt,
		},
		{
			name:      "uploaded checkpoint detaches before relaxing a pinned actor",
			state:     suspended,
			node:      "node-1",
			snapshots: []*ateapipb.Snapshot{localDurable(2, completed)},
			required:  rt,
		},
		{
			name:      "unbound uploaded checkpoint may fall back",
			state:     suspended,
			snapshots: []*ateapipb.Snapshot{localDurable(2, completed)},
			required:  rt,
			want:      true,
		},
		{
			name:      "resuming uploaded checkpoint keeps its relaxed assignment",
			state:     resuming,
			node:      "node-1",
			snapshots: []*ateapipb.Snapshot{localDurable(2, completed)},
			required:  rt,
			want:      true,
		},
		{
			name:      "resuming pending checkpoint cannot relax",
			state:     resuming,
			node:      "node-1",
			snapshots: []*ateapipb.Snapshot{localDurable(2, pending)},
			required:  rt,
		},
		{
			name:      "resuming checkpoint without a durable copy cannot relax",
			state:     resuming,
			node:      "node-1",
			snapshots: []*ateapipb.Snapshot{localNoDurable(2)},
			required:  rt,
		},
		{
			name:      "older durable copy cannot authorize a resuming checkpoint",
			state:     resuming,
			node:      "node-1",
			snapshots: []*ateapipb.Snapshot{durable(1), localNoDurable(2)},
			required:  rt,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actor := &ateapipb.Actor{Status: &ateapipb.ActorStatus{
				State:        tc.state,
				AssignedNode: tc.node,
				Snapshots:    tc.snapshots,
			}}
			if got := canRelaxRuntime(actor, scheduling.Constraints{RequiredRuntime: hardware.NewRequirement(tc.required)}); got != tc.want {
				t.Errorf("canRelaxRuntime = %v, want %v", got, tc.want)
			}
		})
	}
}

// atenet-router requires the assignment's SPIFFE ID on atunnel's certificate,
// so the worker's has to reach it.
func TestWorkerAssignmentFromCopiesSPIFFEID(t *testing.T) {
	const id = "spiffe://cluster.local/ns/workers/sa/substrate-worker"
	got := workerAssignmentFrom(&ateapipb.Worker{WorkerSpiffeId: id}, hardware.Requirement{})
	if got.GetWorkerSpiffeId() != id {
		t.Errorf("worker SPIFFE ID = %q, want %q", got.GetWorkerSpiffeId(), id)
	}
}

func TestToAteletSandboxRuntime(t *testing.T) {
	tests := []struct {
		name string
		in   *ateapipb.ActorSandboxRuntime
		want *ateletpb.ActorSandboxRuntime
	}{
		{name: "nil"},
		{
			name: "class only",
			in:   &ateapipb.ActorSandboxRuntime{SandboxClass: "gvisor"},
			want: &ateletpb.ActorSandboxRuntime{SandboxClass: "gvisor"},
		},
		{
			name: "full",
			in: &ateapipb.ActorSandboxRuntime{
				SandboxClass: "gvisor",
				Version: &ateapipb.VersionedSandboxCompat{
					SchemaVersion: "v1",
					Attributes: []*ateapipb.AttributeEntry{
						{Key: "architecture", Value: "amd64"},
						{Key: "cpu_features", Value: "3f"},
					},
				},
			},
			want: &ateletpb.ActorSandboxRuntime{
				SandboxClass: "gvisor",
				Version: &ateletpb.VersionedSandboxCompat{
					SchemaVersion: "v1",
					Attributes: []*ateletpb.AttributeEntry{
						{Key: "architecture", Value: "amd64"},
						{Key: "cpu_features", Value: "3f"},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, toAteletSandboxRuntime(tc.in), protocmp.Transform()); diff != "" {
				t.Errorf("toAteletSandboxRuntime mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestAssignWorkerAttempt_WorkerWithoutNodeRejectsVolumes verifies a worker
// with no node name is not claimed: the attempt fails with FailedPrecondition
// before the bind, leaving no worker claim and no actor write.
func TestAssignWorkerAttempt_WorkerWithoutNodeRejectsVolumes(t *testing.T) {
	tests := []struct {
		name    string
		mounted []string
	}{
		{name: "mounted volume", mounted: []string{"vol"}},
		{name: "unmounted volume", mounted: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			actor, wc := seedVolumeAssignFixture(t, ctx, persistence, "",
				mockVolume("vol", "storage-vol"),
			)
			plugin := &publishContextVolumePlugin{}
			w := newVolumeAssignWorkflow(persistence, wc, plugin)
			tmpl := volumeTemplate([]string{"vol"}, tt.mounted...)

			_, _, err := w.assignWorkerAttempt(ctx, actorRef, actor, tmpl)
			stored, gerr := persistence.GetActor(ctx, actorRef)
			if gerr != nil {
				t.Fatalf("GetActor: %v", gerr)
			}
			claim := firstAssignment(t, persistence, testWorkerUID("pod-1"))
			if got := apierror.Code(err); got != codes.FailedPrecondition {
				t.Fatalf("assignWorkerAttempt error = %v (code %v), want FailedPrecondition", err, got)
			}
			if want := "has no node name"; !strings.Contains(err.Error(), want) {
				t.Errorf("assignWorkerAttempt error = %q, want it to contain %q", err, want)
			}
			if claim != nil {
				t.Errorf("worker claim = %v, want none", claim)
			}
			if diff := cmp.Diff(actor, stored, protocmp.Transform()); diff != "" {
				t.Errorf("assignment wrote to the stored actor (-before +after):\n%s", diff)
			}
			if len(plugin.attachCalls) != 0 {
				t.Errorf("attach calls = %v, want none", plugin.attachCalls)
			}
		})
	}
}
