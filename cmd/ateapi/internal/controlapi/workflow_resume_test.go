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

// TestResumeActorWorkflow exercises the resume workflow end-to-end against
// seeded actor statuses, covering both the rejected and the idempotent-success
// paths. The worker cache and atelet dialer are nil, so any step that
// unexpectedly reaches them panics.
func TestResumeActorWorkflow(t *testing.T) {
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
			// Resuming is allowed from SUSPENDED, PAUSED, and RESUMING
			// (retry of this step).
			name: "AssignWorkerStep",
			step: &AssignWorkerStep{},
			allowed: map[ateapipb.Actor_Status]bool{
				ateapipb.Actor_STATUS_SUSPENDED: true,
				ateapipb.Actor_STATUS_PAUSED:    true,
				ateapipb.Actor_STATUS_RESUMING:  true,
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
				err := tc.step.CheckPrerequisite(ctx, &ResumeInput{ActorName: "id1"}, &ResumeState{Actor: &ateapipb.Actor{Status: st}})
				assertPrerequisiteResult(t, st, err, tc.allowed == nil || tc.allowed[st])
			}
		})
	}
}
