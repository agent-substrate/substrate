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
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
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

func TestAssignWorkerAttempt_SkipsWorkerAssignedInOtherAtespace(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	// The only worker is held by a same-named actor in another atespace. It is
	// eligible for the template, so a name-only match would adopt it.
	worker := &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		SandboxClass:    "gvisor",
		State:           ateapipb.Worker_STATE_ACTIVE,
		Assignment: &ateapipb.Assignment{
			Actor:    &ateapipb.ObjectRef{Atespace: "team-b", Name: "shared"},
			ActorUid: "team-b-actor-uid",
		},
	}
	if err := persistence.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

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
	tmpl := &atev1alpha1.ActorTemplate{
		Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor},
	}
	_, _, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}, actor, tmpl)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("assignWorkerAttempt() error = %v, want FailedPrecondition (no free workers)", err)
	}

	stored, err := persistence.GetWorker(ctx, "worker-ns", "pool", "pod-1")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got := stored.GetAssignment().GetActorUid(); got != "team-b-actor-uid" {
		t.Errorf("worker assignment uid = %q, want %q (assignment: %v)", got, "team-b-actor-uid", stored.GetAssignment())
	}
	if got := stored.GetAssignment().GetActor().GetAtespace(); got != "team-b" {
		t.Errorf("worker assignment atespace = %q, want %q (assignment: %v)", got, "team-b", stored.GetAssignment())
	}
}

// TestAssignWorkerAttempt_ReleasesIneligibleStaleWorkerInBackground verifies
// that a worker claimed by a previous failed attempt whose pool is no longer
// eligible is released back to the free pool asynchronously, without failing
// the resume, while a fresh eligible worker is assigned.
func TestAssignWorkerAttempt_ReleasesIneligibleStaleWorkerInBackground(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	actor, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status:   ateapipb.Actor_STATUS_SUSPENDED,
	})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	// stale-pod is claimed by this actor from a failed attempt but its sandbox
	// class no longer matches the template; free-pod is eligible and free.
	stale := &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-a",
		WorkerPod:       "stale-pod",
		SandboxClass:    "microvm",
		State:           ateapipb.Worker_STATE_ACTIVE,
		Assignment: &ateapipb.Assignment{
			Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "id1"},
			ActorUid: actor.GetMetadata().GetUid(),
		},
	}
	free := &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-b",
		WorkerPod:       "free-pod",
		SandboxClass:    "gvisor",
		State:           ateapipb.Worker_STATE_ACTIVE,
	}
	for _, w := range []*ateapipb.Worker{stale, free} {
		if err := persistence.CreateWorker(ctx, w); err != nil {
			t.Fatalf("CreateWorker(%s): %v", w.GetWorkerPod(), err)
		}
	}

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &atev1alpha1.ActorTemplate{
		Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor},
	}
	_, worker, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt() error = %v, want nil (release must not fail the resume)", err)
	}

	if got := worker.GetWorkerPod(); got != "free-pod" {
		t.Errorf("assigned worker = %q, want %q", got, "free-pod")
	}

	// The stale worker is released in the background; poll until its
	// assignment is cleared.
	deadline := time.Now().Add(5 * time.Second)
	for {
		stored, err := persistence.GetWorker(ctx, "worker-ns", "pool-a", "stale-pod")
		if err != nil {
			t.Fatalf("GetWorker: %v", err)
		}
		if stored.GetAssignment() == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale worker still assigned after %v: %v", 5*time.Second, stored.GetAssignment())
		}
		time.Sleep(10 * time.Millisecond)
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
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "contested-pod",
		SandboxClass:    "gvisor",
		State:           ateapipb.Worker_STATE_ACTIVE,
	}
	fallback := &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "fallback-pod",
		SandboxClass:    "gvisor",
		State:           ateapipb.Worker_STATE_ACTIVE,
	}
	for _, w := range []*ateapipb.Worker{contested, fallback} {
		if err := persistence.CreateWorker(ctx, w); err != nil {
			t.Fatalf("CreateWorker(%s): %v", w.GetWorkerPod(), err)
		}
	}

	// Snapshot the contested worker at the version the failed attempt saw.
	beforeClaim, err := persistence.GetWorker(ctx, "worker-ns", "pool", "contested-pod")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}

	// A concurrent resume of another actor wins the contested worker, bumping
	// its stored version past the failed attempt's snapshot.
	claimed := proto.Clone(beforeClaim).(*ateapipb.Worker)
	claimed.Assignment = &ateapipb.Assignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "other"},
		ActorUid: "other-actor-uid",
	}
	if err := persistence.UpdateWorker(ctx, claimed, claimed.GetVersion()); err != nil {
		t.Fatalf("UpdateWorker (concurrent claim): %v", err)
	}

	actor, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status:   ateapipb.Actor_STATUS_SUSPENDED,
	})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &atev1alpha1.ActorTemplate{
		Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor},
	}
	_, worker, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt() on retry = %v, want nil (must re-pick a free worker)", err)
	}
	if got := worker.GetWorkerPod(); got != "fallback-pod" {
		t.Errorf("assigned worker = %q, want %q", got, "fallback-pod")
	}

	storedContested, err := persistence.GetWorker(ctx, "worker-ns", "pool", "contested-pod")
	if err != nil {
		t.Fatalf("GetWorker(contested-pod): %v", err)
	}
	if got := storedContested.GetAssignment().GetActorUid(); got != "other-actor-uid" {
		t.Errorf("contested worker assignment = %v, want to remain with actor %q", storedContested.GetAssignment(), "other-actor-uid")
	}
	storedFallback, err := persistence.GetWorker(ctx, "worker-ns", "pool", "fallback-pod")
	if err != nil {
		t.Fatalf("GetWorker(fallback-pod): %v", err)
	}
	if got := storedFallback.GetAssignment().GetActorUid(); got != actor.GetMetadata().GetUid() {
		t.Errorf("fallback worker assignment = %v, want actor uid %q", storedFallback.GetAssignment(), actor.GetMetadata().GetUid())
	}

	storedActor, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if storedActor.GetStatus() != ateapipb.Actor_STATUS_RESUMING {
		t.Errorf("stored actor status = %v, want %v", storedActor.GetStatus(), ateapipb.Actor_STATUS_RESUMING)
	}
	if got := storedActor.GetWorkerAssignment().GetWorkerPod(); got != "fallback-pod" {
		t.Errorf("stored actor WorkerAssignment.WorkerPod = %q, want %q", got, "fallback-pod")
	}
}

// conflictInjectingStore wraps a store and runs inject exactly once,
// immediately before the first UpdateActor, simulating a concurrent writer
// racing the step's read-modify-write window.
type conflictInjectingStore struct {
	store.Interface
	once   sync.Once
	inject func()
}

func (c *conflictInjectingStore) UpdateActor(ctx context.Context, actor *ateapipb.Actor, expectedVersion int64) (*ateapipb.Actor, error) {
	c.once.Do(c.inject)
	return c.Interface.UpdateActor(ctx, actor, expectedVersion)
}

// seedAssignFixture stores one free gvisor worker and a SUSPENDED actor and
// returns the actor plus a started worker cache.
func seedAssignFixture(t *testing.T, ctx context.Context, persistence store.Interface) (*ateapipb.Actor, *workercache.Cache) {
	t.Helper()
	if err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		SandboxClass:    "gvisor",
		State:           ateapipb.Worker_STATE_ACTIVE,
	}); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	actor, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status:   ateapipb.Actor_STATUS_SUSPENDED,
	})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
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
// a concurrent transition out of a resumable status aborts the resume.
func TestAssignWorkerAttempt_ConflictRefreshesActor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// mutate is the racing concurrent write applied to the fresh actor.
		mutate func(fresh *ateapipb.Actor)
		// wantRetry means the attempt surfaces ErrVersionConflict with the
		// refreshed actor returned; otherwise Aborted.
		wantRetry bool
		// wantStoredStatus is the persisted status after Execute.
		wantStoredStatus ateapipb.Actor_Status
	}{
		{
			name: "another writer refreshes state.Actor - can recover",
			mutate: func(fresh *ateapipb.Actor) {
				fresh.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"team": "blue"}}
			},
			wantRetry:        true,
			wantStoredStatus: ateapipb.Actor_STATUS_SUSPENDED,
		},
		{
			name: "another writer crash the Actor",
			mutate: func(fresh *ateapipb.Actor) {
				fresh.Status = ateapipb.Actor_STATUS_CRASHED
			},
			wantRetry:        false,
			wantStoredStatus: ateapipb.Actor_STATUS_CRASHED,
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
				tc.mutate(fresh)
				injected, err = persistence.UpdateActor(ctx, fresh, fresh.GetMetadata().GetVersion())
				if err != nil {
					t.Errorf("inject UpdateActor: %v", err)
				}
			}}

			w := &ActorWorkflow{store: st, workerCache: wc, scheduler: scheduling.New(wc)}
			tmpl := &atev1alpha1.ActorTemplate{
				Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor},
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
			if stored.GetStatus() != tc.wantStoredStatus {
				t.Errorf("stored status = %v, want %v", stored.GetStatus(), tc.wantStoredStatus)
			}
		})
	}
}

// TestResumeActorWorkflow_RejectedAndIdempotentPaths covers the two
// short-circuit paths of the resume workflow: rejection of the resume edge
// for a non-resumable actor and the idempotent fast-forward for a RUNNING one.
func TestResumeActorWorkflow_RejectedAndIdempotentPaths(t *testing.T) {
	tests := []struct {
		name       string
		seedStatus ateapipb.Actor_Status
		// wantErr true means ResumeActor must fail with FailedPrecondition.
		wantErr bool
		// wantStatus is the stored status after the call.
		wantStatus ateapipb.Actor_Status
	}{
		{
			// The resume edge only exists from SUSPENDED, PAUSED, and
			// RESUMING; a CRASHED actor is rejected by ensureWorkerAssigned
			// and its status is left untouched.
			name:       "crashed rejected",
			seedStatus: ateapipb.Actor_STATUS_CRASHED,
			wantErr:    true,
			wantStatus: ateapipb.Actor_STATUS_CRASHED,
		},
		{
			// Resuming a RUNNING actor succeeds idempotently: every step
			// fast-forwards via IsComplete.
			name:       "already running succeeds",
			seedStatus: ateapipb.Actor_STATUS_RUNNING,
			wantStatus: ateapipb.Actor_STATUS_RUNNING,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", tc.seedStatus, func(a *ateapipb.Actor) {
				a.WorkerAssignment = &ateapipb.WorkerAssignment{
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
				if actor.GetStatus() != tc.wantStatus {
					t.Errorf("returned status = %v, want %v", actor.GetStatus(), tc.wantStatus)
				}
				if tc.seedStatus == ateapipb.Actor_STATUS_RUNNING {
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
			if got.GetStatus() != tc.wantStatus {
				t.Errorf("stored status = %v, want %v", got.GetStatus(), tc.wantStatus)
			}
		})
	}
}

// TestEnsureWorkerAssigned_RejectsNonResumableStatuses verifies the resume
// edge's status gating: every status outside SUSPENDED, PAUSED, and RESUMING
// is rejected with FailedPrecondition before any dependency is touched.
// (SUSPENDED/PAUSED assignment and RESUMING recovery are exercised by the
// assignment-attempt and worker-validation tests; RUNNING never reaches this
// step because the orchestrator early-returns.)
func TestEnsureWorkerAssigned_RejectsNonResumableStatuses(t *testing.T) {
	ctx := context.Background()
	w := &ActorWorkflow{}
	for _, st := range allActorStatuses {
		switch st {
		case ateapipb.Actor_STATUS_SUSPENDED, ateapipb.Actor_STATUS_PAUSED, ateapipb.Actor_STATUS_RESUMING:
			continue
		}
		actor := &ateapipb.Actor{Status: st, Metadata: &ateapipb.ResourceMetadata{Name: "id1", Uid: "actor-uid-1"}}
		_, _, err := w.ensureWorkerAssigned(ctx, resources.ActorRef{Name: "id1"}, actor, &atev1alpha1.ActorTemplate{})
		assertPrerequisiteResult(t, st, err, false)
	}
}

// TestResumeActor_MetricSkipsAlreadyRunningNoop guards the recording rule: the
// router resumes per routed request, so a clean already-running no-op must not
// be recorded, while failures must be.
func TestResumeActor_MetricSkipsAlreadyRunningNoop(t *testing.T) {
	tests := []struct {
		name       string
		seedStatus ateapipb.Actor_Status
		wantRecord bool
	}{
		{name: "already running no-op is skipped", seedStatus: ateapipb.Actor_STATUS_RUNNING, wantRecord: false},
		{name: "failed resume is recorded", seedStatus: ateapipb.Actor_STATUS_CRASHED, wantRecord: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			inst, reader := newTestInstruments(t)
			w.instruments = inst

			seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", tt.seedStatus, func(a *ateapipb.Actor) {
				a.WorkerAssignment = &ateapipb.WorkerAssignment{
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

	seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", ateapipb.Actor_STATUS_RESUMING, func(a *ateapipb.Actor) {
		a.WorkerAssignment = nil // RESUMING without a worker: corrupt record
	})

	_, _, err := w.ResumeActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, false)
	if got := status.Code(err); got != codes.Aborted {
		t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.Aborted, err)
	}

	got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("stored status = %v, want %v", got.GetStatus(), ateapipb.Actor_STATUS_CRASHED)
	}
}

// TestValidateAssignedWorker_WorkerOwnership verifies that RESUMING recovery
// only proceeds on a worker whose assignment still names this actor: the
// recovery path loads the worker by pod name only, so the assignment may have
// been cleared and the worker re-claimed by another actor in the meantime. On
// a mismatch the actor is crashed and the worker — which is not ours — must
// not be written.
func TestValidateAssignedWorker_WorkerOwnership(t *testing.T) {
	ownAssignment := &ateapipb.Assignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "shared"},
		ActorUid: "own-actor-uid",
	}
	otherAssignment := &ateapipb.Assignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-b", Name: "shared"},
		ActorUid: "other-actor-uid",
	}
	staleIncarnationAssignment := &ateapipb.Assignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "shared"},
		ActorUid: "stale-incarnation-uid",
	}

	tests := []struct {
		name         string
		sandboxClass string
		assignment   *ateapipb.Assignment
		// wantCode is codes.OK when validateAssignedWorker must return nil.
		wantCode        codes.Code
		wantActorStatus ateapipb.Actor_Status
		// wantAssignment is the assignment expected on the stored worker
		// afterwards; wantWorkerWrite false additionally asserts the worker
		// version did not move (no write at all).
		wantAssignment  *ateapipb.Assignment
		wantWorkerWrite bool
	}{
		{
			name:            "crashes actor and leaves worker untouched when assigned to another actor",
			sandboxClass:    "gvisor",
			assignment:      otherAssignment,
			wantCode:        codes.Aborted,
			wantActorStatus: ateapipb.Actor_STATUS_CRASHED,
			wantAssignment:  otherAssignment,
		},
		{
			name:            "crashes actor and leaves worker untouched when assigned to previous incarnation of same actor",
			sandboxClass:    "gvisor",
			assignment:      staleIncarnationAssignment,
			wantCode:        codes.Aborted,
			wantActorStatus: ateapipb.Actor_STATUS_CRASHED,
			wantAssignment:  staleIncarnationAssignment,
		},
		{
			name:            "crashes actor and leaves worker untouched when assignment is cleared",
			sandboxClass:    "gvisor",
			assignment:      nil,
			wantCode:        codes.Aborted,
			wantActorStatus: ateapipb.Actor_STATUS_CRASHED,
			wantAssignment:  nil,
		},
		{
			name:            "passes for own eligible worker",
			sandboxClass:    "gvisor",
			assignment:      ownAssignment,
			wantCode:        codes.OK,
			wantActorStatus: ateapipb.Actor_STATUS_RESUMING,
			wantAssignment:  ownAssignment,
		},
		{
			name:            "releases own ineligible worker and crashes actor",
			sandboxClass:    "microvm",
			assignment:      ownAssignment,
			wantCode:        codes.Aborted,
			wantActorStatus: ateapipb.Actor_STATUS_CRASHED,
			wantAssignment:  nil,
			wantWorkerWrite: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			if err := persistence.CreateWorker(ctx, &ateapipb.Worker{
				WorkerNamespace: "worker-ns",
				WorkerPool:      "pool",
				WorkerPod:       "pod-1",
				SandboxClass:    tt.sandboxClass,
				State:           ateapipb.Worker_STATE_ACTIVE,
				Assignment:      tt.assignment,
			}); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}
			// Fetch the stored version so the no-write assertion below can
			// detect any optimistic update.
			seeded, err := persistence.GetWorker(ctx, "worker-ns", "pool", "pod-1")
			if err != nil {
				t.Fatalf("GetWorker: %v", err)
			}

			seedWorkflowActor(t, ctx, persistence, resources.ActorRef{Atespace: "team-a", Name: "shared"}, "ns", "tmpl1", ateapipb.Actor_STATUS_RESUMING)

			w := &ActorWorkflow{store: persistence, scheduler: scheduling.New(nil)}
			resumingActor := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared", Uid: "own-actor-uid"},
				Status:   ateapipb.Actor_STATUS_RESUMING,
				WorkerAssignment: &ateapipb.WorkerAssignment{
					WorkerNamespace: "worker-ns",
					WorkerPool:      "pool",
					WorkerPod:       "pod-1",
				},
			}
			tmpl := &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor}}
			_, err = w.validateAssignedWorker(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}, resumingActor, tmpl)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}

			actor, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"})
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if actor.GetStatus() != tt.wantActorStatus {
				t.Errorf("stored actor status = %v, want %v", actor.GetStatus(), tt.wantActorStatus)
			}

			stored, err := persistence.GetWorker(ctx, "worker-ns", "pool", "pod-1")
			if err != nil {
				t.Fatalf("GetWorker: %v", err)
			}
			if !proto.Equal(stored.GetAssignment(), tt.wantAssignment) {
				t.Errorf("stored worker assignment = %v, want %v", stored.GetAssignment(), tt.wantAssignment)
			}
			if !tt.wantWorkerWrite && stored.GetVersion() != seeded.GetVersion() {
				t.Errorf("worker version moved %d -> %d, want no write", seeded.GetVersion(), stored.GetVersion())
			}
		})
	}
}

// TestLoadActorForResume_OnGoldenDataResume verifies the golden-location
// plumbing: when the template's onResume.fromData is Golden, a pending
// data-only restore (a Data durable snapshot, or a paused actor whose
// onPause is Data) additionally resolves the template's golden snapshot
func TestLoadActorForResume_OnGoldenDataResume(t *testing.T) {
	const goldenSnapshotURI = "gs://bucket/golden-root/snapshots/ate-golden/golden-1"
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	tests := []struct {
		name     string
		fromData atev1alpha1.ResumeSource
		// paused seeds the actor with LocalSnapshotInfo (a pause checkpoint)
		// instead of a durable snapshot; onPause is the template's pause
		// scope, contentScope the durable snapshot's recorded content.
		paused       bool
		onPause      atev1alpha1.SnapshotScope
		contentScope ateapipb.SnapshotContentScope
		// goldenSnapshot is ActorTemplate.Status.GoldenSnapshot; seedGolden
		// controls whether the golden ActorSnapshot row it names exists, and
		// goldenScope the scope it records (zero value UNSPECIFIED is treated
		// as Full for legacy snapshots).
		goldenSnapshot string
		seedGolden     bool
		goldenScope    ateapipb.SnapshotContentScope
		wantCode       codes.Code
		wantGoldenURI  string
	}{
		{
			name:           "resolves golden location for Data durable snapshot",
			fromData:       atev1alpha1.ResumeSourceGolden,
			contentScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenSnapshot: "golden-1",
			seedGolden:     true,
			goldenScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:       codes.OK,
			wantGoldenURI:  goldenSnapshotURI,
		},
		{
			name:           "resolves golden location for paused actor with Data onPause",
			fromData:       atev1alpha1.ResumeSourceGolden,
			paused:         true,
			onPause:        atev1alpha1.SnapshotScopeData,
			goldenSnapshot: "golden-1",
			seedGolden:     true,
			goldenScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:       codes.OK,
			wantGoldenURI:  goldenSnapshotURI,
		},
		{
			// A Full pause snapshot restores from its own content; the policy
			// only governs data-only restores.
			name:           "leaves golden location empty for paused actor with Full onPause",
			fromData:       atev1alpha1.ResumeSourceGolden,
			paused:         true,
			onPause:        atev1alpha1.SnapshotScopeFull,
			goldenSnapshot: "golden-1",
			seedGolden:     true,
			goldenScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:       codes.OK,
			wantGoldenURI:  "",
		},
		{
			name:           "fails when golden snapshot is not Full",
			fromData:       atev1alpha1.ResumeSourceGolden,
			contentScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenSnapshot: "golden-1",
			seedGolden:     true,
			goldenScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			wantCode:       codes.FailedPrecondition,
		},
		{
			name:         "fails when template has no golden snapshot",
			fromData:     atev1alpha1.ResumeSourceGolden,
			contentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			wantCode:     codes.FailedPrecondition,
		},
		{
			name:           "fails when golden snapshot data is missing",
			fromData:       atev1alpha1.ResumeSourceGolden,
			contentScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenSnapshot: "golden-1",
			wantCode:       codes.DataLoss,
		},
		{
			// A Full snapshot restores from its own content even under
			// Golden fromData (e.g. taken before the template switched).
			name:           "leaves golden location empty for Full snapshot",
			fromData:       atev1alpha1.ResumeSourceGolden,
			contentScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			goldenSnapshot: "golden-1",
			seedGolden:     true,
			goldenScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:       codes.OK,
			wantGoldenURI:  "",
		},
		{
			name:           "leaves golden location empty under ColdBoot fromData",
			fromData:       atev1alpha1.ResumeSourceColdBoot,
			contentScope:   ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			goldenSnapshot: "golden-1",
			seedGolden:     true,
			goldenScope:    ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			wantCode:       codes.OK,
			wantGoldenURI:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			if tt.seedGolden {
				if _, err := persistence.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
					Metadata:     &ateapipb.ResourceMetadata{Atespace: resources.GoldenActorAtespace, Name: tt.goldenSnapshot},
					ContentScope: tt.goldenScope,
					SnapshotUri:  goldenSnapshotURI,
				}); err != nil {
					t.Fatalf("CreateActorSnapshot(golden): %v", err)
				}
			}

			var seedOpts []func(*ateapipb.Actor)
			if tt.paused {
				seedOpts = append(seedOpts, func(a *ateapipb.Actor) {
					a.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{SnapshotName: "pause-1"}
				})
			} else {
				snap, err := persistence.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
					Metadata:     &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: "snap-1"},
					SourceActor:  &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
					ContentScope: tt.contentScope,
					SnapshotUri:  "gs://bucket/root/snapshots/" + actorRef.Atespace + "/snap-1",
				})
				if err != nil {
					t.Fatalf("CreateActorSnapshot: %v", err)
				}
				seedOpts = append(seedOpts, func(a *ateapipb.Actor) {
					a.LatestSnapshot = &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: snap.GetMetadata().GetName()}
				})
			}
			actorStatus := ateapipb.Actor_STATUS_SUSPENDED
			if tt.paused {
				actorStatus = ateapipb.Actor_STATUS_PAUSED
			}
			seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", actorStatus, seedOpts...)

			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if err := indexer.Add(&atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "tmpl1"},
				Spec: atev1alpha1.ActorTemplateSpec{
					SnapshotsConfig: atev1alpha1.SnapshotsConfig{
						OnPause:  tt.onPause,
						OnResume: atev1alpha1.OnResumeConfig{FromData: tt.fromData},
					},
				},
				Status: atev1alpha1.ActorTemplateStatus{GoldenSnapshot: tt.goldenSnapshot},
			}); err != nil {
				t.Fatalf("add template to indexer: %v", err)
			}

			w := &ActorWorkflow{store: persistence, actorTemplateLister: listersv1alpha1.NewActorTemplateLister(indexer)}
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

	if _, err := persistence.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
		Metadata:     &ateapipb.ResourceMetadata{Atespace: resources.GoldenActorAtespace, Name: "golden-1"},
		ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
		SnapshotUri:  "gs://bucket/golden-root/snapshots/ate-golden/golden-1",
	}); err != nil {
		t.Fatalf("CreateActorSnapshot(golden): %v", err)
	}
	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", ateapipb.Actor_STATUS_SUSPENDED)

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(&atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "tmpl1"},
		Status:     atev1alpha1.ActorTemplateStatus{GoldenSnapshot: "golden-1"},
	}); err != nil {
		t.Fatalf("add template to indexer: %v", err)
	}

	w := &ActorWorkflow{store: persistence, actorTemplateLister: listersv1alpha1.NewActorTemplateLister(indexer)}
	_, _, _, err := w.loadActorForResume(ctx, actorRef, false)
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status.Code(err) = %v, want FailedPrecondition (err: %v)", got, err)
	}
	if !strings.Contains(err.Error(), "regenerate the golden snapshot") {
		t.Errorf("error %q does not tell the operator to regenerate the golden snapshot", err)
	}
}

func TestLoadActorForResume_RunningActorShortCircuits(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	// Seed the actor as RUNNING. Note: No snapshot or template is seeded in the
	// store or lister, proving that loadActorForResume short-circuits before
	// attempting to fetch either.
	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "missing-tmpl", ateapipb.Actor_STATUS_RUNNING)

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	w := &ActorWorkflow{store: persistence, actorTemplateLister: listersv1alpha1.NewActorTemplateLister(indexer)}

	actor, tmpl, src, err := w.loadActorForResume(ctx, actorRef, false)
	if err != nil {
		t.Fatalf("loadActorForResume() unexpected error = %v", err)
	}
	if actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("actor status = %v, want %v", actor.GetStatus(), ateapipb.Actor_STATUS_RUNNING)
	}
	if tmpl != nil {
		t.Errorf("expected nil template, got %v", tmpl)
	}
	if !src.SnapshotURI.IsZero() || !src.GoldenSnapshotURI.IsZero() {
		t.Errorf("expected empty snapshot source, got %+v", src)
	}
}
