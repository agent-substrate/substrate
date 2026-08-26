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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestActorResourceLimits covers the actor-side extraction: the CPU/memory limits
// an ActorTemplate declares become the sandbox size and the scheduling floor.
func TestActorResourceLimits(t *testing.T) {
	tests := []struct {
		name       string
		res        *corev1.ResourceRequirements
		wantCPU    int64
		wantMemory int64
	}{
		{
			name:       "nil resources yields zero",
			res:        nil,
			wantCPU:    0,
			wantMemory: 0,
		},
		{
			name: "cpu and memory limits are read",
			res: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("4Gi"),
				},
			},
			wantCPU:    2000,
			wantMemory: 4 << 30,
		},
		{
			name: "millicpu is preserved",
			res: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500m")},
			},
			wantCPU:    1500,
			wantMemory: 0,
		},
		{
			name: "requests are ignored; only limits size the actor",
			res: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
			wantCPU:    0,
			wantMemory: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpl := mustTemplateFromCRD(&atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{Resources: tc.res}})
			cpu, mem, err := actorResourceLimits(tmpl)
			if err != nil {
				t.Fatalf("actorResourceLimits() error: %v", err)
			}
			if cpu != tc.wantCPU || mem != tc.wantMemory {
				t.Fatalf("actorResourceLimits() = (%d, %d), want (%d, %d)", cpu, mem, tc.wantCPU, tc.wantMemory)
			}
		})
	}
}
