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
	"testing"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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
