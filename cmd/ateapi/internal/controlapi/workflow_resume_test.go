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
