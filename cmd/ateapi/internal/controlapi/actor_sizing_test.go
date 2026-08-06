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
			tmpl := &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{Resources: tc.res}}
			cpu, mem := actorResourceLimits(tmpl)
			if cpu != tc.wantCPU || mem != tc.wantMemory {
				t.Fatalf("actorResourceLimits() = (%d, %d), want (%d, %d)", cpu, mem, tc.wantCPU, tc.wantMemory)
			}
		})
	}
}

// TestWorkerCapacity covers the worker-side extraction: capacity comes from the
// ateom container's limits, not the pod total, and other containers are ignored.
func TestWorkerCapacity(t *testing.T) {
	pod := func(ctrs ...corev1.Container) *corev1.Pod {
		return &corev1.Pod{Spec: corev1.PodSpec{Containers: ctrs}}
	}
	limited := func(name, cpu, mem string) corev1.Container {
		lim := corev1.ResourceList{}
		if cpu != "" {
			lim[corev1.ResourceCPU] = resource.MustParse(cpu)
		}
		if mem != "" {
			lim[corev1.ResourceMemory] = resource.MustParse(mem)
		}
		return corev1.Container{Name: name, Resources: corev1.ResourceRequirements{Limits: lim}}
	}

	tests := []struct {
		name       string
		pod        *corev1.Pod
		wantCPU    int64
		wantMemory int64
	}{
		{
			name:       "no ateom container yields zero",
			pod:        pod(limited("sidecar", "1", "1Gi")),
			wantCPU:    0,
			wantMemory: 0,
		},
		{
			name:       "ateom container limits become capacity",
			pod:        pod(limited(ateomContainerName, "4", "8Gi")),
			wantCPU:    4000,
			wantMemory: 8 << 30,
		},
		{
			name:       "only the ateom container counts, not the pod total",
			pod:        pod(limited("sidecar", "16", "64Gi"), limited(ateomContainerName, "2", "2Gi")),
			wantCPU:    2000,
			wantMemory: 2 << 30,
		},
		{
			name:       "unset dimension reports zero",
			pod:        pod(limited(ateomContainerName, "2", "")),
			wantCPU:    2000,
			wantMemory: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cpu, mem := workerCapacity(tc.pod)
			if cpu != tc.wantCPU || mem != tc.wantMemory {
				t.Fatalf("workerCapacity() = (%d, %d), want (%d, %d)", cpu, mem, tc.wantCPU, tc.wantMemory)
			}
		})
	}
}
