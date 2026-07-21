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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIsWorkerEligibleForActor(t *testing.T) {
	tests := []struct {
		name             string
		worker           *ateapipb.Worker
		templateClass    atev1alpha1.SandboxClass
		templateSelector *metav1.LabelSelector
		actorSelector    *ateapipb.Selector
		wantEligible     bool
	}{
		{
			name: "both nil matches everything",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"foo": "bar"},
			},
			templateClass:    atev1alpha1.SandboxClassGvisor,
			templateSelector: nil,
			actorSelector:    nil,
			wantEligible:     true,
		},
		{
			name: "template selector only match",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"workload": "code-sandbox"},
			},
			templateClass: atev1alpha1.SandboxClassGvisor,
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "code-sandbox"},
			},
			actorSelector: nil,
			wantEligible:  true,
		},
		{
			name: "template selector only no match",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"workload": "browser-agent"},
			},
			templateClass: atev1alpha1.SandboxClassGvisor,
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "code-sandbox"},
			},
			actorSelector: nil,
			wantEligible:  false,
		},
		{
			name: "actor selector only match",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"tier": "paid"},
			},
			templateClass:    atev1alpha1.SandboxClassGvisor,
			templateSelector: nil,
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			wantEligible: true,
		},
		{
			name: "actor selector only no match",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"tier": "free"},
			},
			templateClass:    atev1alpha1.SandboxClassGvisor,
			templateSelector: nil,
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			wantEligible: false,
		},
		{
			name: "AND of two selectors match",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"workload": "code-sandbox", "tier": "paid"},
			},
			templateClass: atev1alpha1.SandboxClassGvisor,
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "code-sandbox"},
			},
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			wantEligible: true,
		},
		{
			name: "AND of two selectors one fails",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"workload": "code-sandbox", "tier": "free"},
			},
			templateClass: atev1alpha1.SandboxClassGvisor,
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "code-sandbox"},
			},
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			wantEligible: false,
		},
		{
			name: "microvm template matches only microvm worker",
			worker: &ateapipb.Worker{
				SandboxClass: "microvm",
			},
			templateClass: atev1alpha1.SandboxClassMicroVM,
			wantEligible:  true,
		},
		{
			name: "microvm template excludes gvisor worker",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
			},
			templateClass: atev1alpha1.SandboxClassMicroVM,
			wantEligible:  false,
		},
		{
			name: "gvisor template excludes microvm worker",
			worker: &ateapipb.Worker{
				SandboxClass: "microvm",
			},
			templateClass: atev1alpha1.SandboxClassGvisor,
			wantEligible:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isWorkerEligibleForActor(tt.worker, tt.templateClass, tt.templateSelector, tt.actorSelector)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantEligible {
				t.Errorf("got eligible=%t, want %t", got, tt.wantEligible)
			}
		})
	}
}

func TestAssignWorkerStep_SkipsWorkerAssignedInOtherAtespace(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	// The only worker is held by a same-named actor in another atespace. It is
	// eligible for the template, so a name-only match would adopt it.
	worker := &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		SandboxClass:    "gvisor",
		Assignment: &ateapipb.Assignment{
			Actor: &ateapipb.ObjectRef{Atespace: "team-b", Name: "shared"},
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

	step := &AssignWorkerStep{store: persistence, workerCache: wc}
	state := &ResumeState{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
		},
		ActorTemplate: &atev1alpha1.ActorTemplate{
			Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor},
		},
	}
	err := step.Execute(ctx, &ResumeInput{ActorName: "shared", Atespace: "team-a"}, state)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Execute() error = %v, want FailedPrecondition (no free workers)", err)
	}

	stored, err := persistence.GetWorker(ctx, "worker-ns", "pool", "pod-1")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got := stored.GetAssignment().GetActor().GetAtespace(); got != "team-b" {
		t.Errorf("worker assignment atespace = %q, want %q (assignment: %v)", got, "team-b", stored.GetAssignment())
	}
}

// TestAssignWorkerStep_ReleasesIneligibleStaleWorkerInBackground verifies
// that a worker claimed by a previous failed attempt whose pool is no longer
// eligible is released back to the free pool asynchronously, without failing
// the resume, while a fresh eligible worker is assigned.
func TestAssignWorkerStep_ReleasesIneligibleStaleWorkerInBackground(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	// stale-pod is claimed by this actor from a failed attempt but its sandbox
	// class no longer matches the template; free-pod is eligible and free.
	stale := &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-a",
		WorkerPod:       "stale-pod",
		SandboxClass:    "microvm",
		Assignment: &ateapipb.Assignment{
			Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "id1"},
		},
	}
	free := &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-b",
		WorkerPod:       "free-pod",
		SandboxClass:    "gvisor",
	}
	for _, w := range []*ateapipb.Worker{stale, free} {
		if err := persistence.CreateWorker(ctx, w); err != nil {
			t.Fatalf("CreateWorker(%s): %v", w.GetWorkerPod(), err)
		}
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

	step := &AssignWorkerStep{store: persistence, workerCache: wc}
	state := &ResumeState{
		Actor: actor,
		ActorTemplate: &atev1alpha1.ActorTemplate{
			Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor},
		},
	}
	if err := step.Execute(ctx, &ResumeInput{ActorName: "id1", Atespace: "team-a"}, state); err != nil {
		t.Fatalf("Execute() error = %v, want nil (release must not fail the resume)", err)
	}

	if got := state.Worker.GetWorkerPod(); got != "free-pod" {
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

// TestResumeActorWorkflow_RejectedAndIdempotentPaths covers the two
// short-circuit paths of the resume workflow: rejection by AssignWorkerStep's
// CheckPrerequisite and the IsComplete idempotent fast-forward.
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
			// RESUMING; a CRASHED actor is rejected by AssignWorkerStep's
			// CheckPrerequisite and its status is left untouched.
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

			seedWorkflowActor(t, ctx, st, "team-a", "id1", "ns", "tmpl1", tc.seedStatus, func(a *ateapipb.Actor) {
				a.AteomPodNamespace = "wns"
				a.AteomPodName = "wpod"
				a.AteomPodIp = "1.2.3.4"
				a.AteomPodUid = "uid"
				a.WorkerPoolName = "pool1"
			})

			actor, err := w.ResumeActor(ctx, "team-a", "id1", false)
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

// TestResumeSteps_CheckPrerequisite verifies each resume step's
// CheckPrerequisite against every actor status: nil for the step's allowed
// statuses, FailedPrecondition for all others.
func TestResumeSteps_CheckPrerequisite(t *testing.T) {
	tests := []struct {
		name string
		step WorkflowStep[*ResumeInput, *ResumeState]
		// allowed lists the statuses CheckPrerequisite accepts; nil means
		// every status is accepted.
		allowed map[ateapipb.Actor_Status]bool
	}{
		{
			// Loading has no prerequisite: it is allowed from every status.
			name:    "LoadActorForResumeStep",
			step:    &LoadActorForResumeStep{},
			allowed: nil,
		},
		{
			// Resuming is allowed from SUSPENDED and PAUSED (RESUMING and
			// RUNNING are fast-forwarded by IsComplete).
			name: "AssignWorkerStep",
			step: &AssignWorkerStep{},
			allowed: map[ateapipb.Actor_Status]bool{
				ateapipb.Actor_STATUS_SUSPENDED: true,
				ateapipb.Actor_STATUS_PAUSED:    true,
			},
		},
		{
			// The restore call is allowed only from RESUMING (RUNNING is
			// fast-forwarded by IsComplete).
			name: "CallAteletRestoreStep",
			step: &CallAteletRestoreStep{},
			allowed: map[ateapipb.Actor_Status]bool{
				ateapipb.Actor_STATUS_RESUMING: true,
			},
		},
		{
			// Finalizing transitions RESUMING -> RUNNING; RUNNING itself is
			// fast-forwarded by IsComplete before the prerequisite is checked.
			name: "FinalizeRunningStep",
			step: &FinalizeRunningStep{},
			allowed: map[ateapipb.Actor_Status]bool{
				ateapipb.Actor_STATUS_RESUMING: true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			for _, st := range allActorStatuses {
				// An eligible Worker assigned to this actor is provided so
				// CallAteletRestoreStep's worker checks pass; this test only
				// verifies status gating.
				state := &ResumeState{
					Actor: &ateapipb.Actor{Status: st},
					Worker: &ateapipb.Worker{
						SandboxClass: string(atev1alpha1.SandboxClassGvisor),
						Assignment:   &ateapipb.Assignment{Actor: &ateapipb.ObjectRef{Name: "id1"}},
					},
					ActorTemplate: &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor}},
				}
				err := tc.step.CheckPrerequisite(ctx, &ResumeInput{ActorName: "id1"}, state)
				assertPrerequisiteResult(t, st, err, tc.allowed == nil || tc.allowed[st])
			}
		})
	}
}

// TestResumeActor_CrashesOnCorruptWorkerAssignment verifies that a RESUMING
// actor with only some of its worker assignment fields populated is moved to
// CRASHED by LoadActorForResumeStep and the resume fails with Aborted.
func TestResumeActor_CrashesOnCorruptWorkerAssignment(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, "team-a", "id1", "ns", "tmpl1", ateapipb.Actor_STATUS_RESUMING, func(a *ateapipb.Actor) {
		a.AteomPodName = "worker-1" // AteomPodUid and WorkerPoolName left empty
	})

	_, err := w.ResumeActor(ctx, "team-a", "id1", false)
	if got := status.Code(err); got != codes.Aborted {
		t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.Aborted, err)
	}

	got, err := st.GetActor(ctx, "team-a", "id1")
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("stored status = %v, want %v", got.GetStatus(), ateapipb.Actor_STATUS_CRASHED)
	}
}

// TestCallAteletRestoreStep_CheckPrerequisite_WorkerOwnership verifies that
// the restore prerequisite only proceeds on a worker whose assignment still
// names this actor: the recovery path loads the worker by pod name only, so
// the assignment may have been cleared and the worker re-claimed by another
// actor in the meantime. On a mismatch the actor is crashed and the worker —
// which is not ours — must not be written.
func TestCallAteletRestoreStep_CheckPrerequisite_WorkerOwnership(t *testing.T) {
	ownAssignment := &ateapipb.Assignment{
		Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "shared"},
	}
	otherAssignment := &ateapipb.Assignment{
		Actor: &ateapipb.ObjectRef{Atespace: "team-b", Name: "shared"},
	}

	tests := []struct {
		name         string
		sandboxClass string
		assignment   *ateapipb.Assignment
		// wantCode is codes.OK when CheckPrerequisite must return nil.
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
				Assignment:      tt.assignment,
			}); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}
			// Re-fetch so state.Worker carries the stored version (needed by
			// the release path's optimistic update).
			seeded, err := persistence.GetWorker(ctx, "worker-ns", "pool", "pod-1")
			if err != nil {
				t.Fatalf("GetWorker: %v", err)
			}

			seedWorkflowActor(t, ctx, persistence, "team-a", "shared", "ns", "tmpl1", ateapipb.Actor_STATUS_RESUMING)

			step := &CallAteletRestoreStep{store: persistence}
			state := &ResumeState{
				Actor: &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
					Status:   ateapipb.Actor_STATUS_RESUMING,
				},
				Worker:        seeded,
				ActorTemplate: &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor}},
			}
			err = step.CheckPrerequisite(ctx, &ResumeInput{Atespace: "team-a", ActorName: "shared"}, state)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}

			actor, err := persistence.GetActor(ctx, "team-a", "shared")
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
