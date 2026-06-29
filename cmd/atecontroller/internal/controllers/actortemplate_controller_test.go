// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"testing"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func TestGoldenSnapshotWarmupFor(t *testing.T) {
	probe := &atev1alpha1.ContainerReadyz{
		HTTPGet: &atev1alpha1.HTTPGetAction{Port: 80},
	}

	tests := []struct {
		name       string
		containers []atev1alpha1.Container
		wantZero   bool
	}{
		{
			name:       "no containers keeps default warmup",
			containers: nil,
			wantZero:   false,
		},
		{
			name: "all containers have readyz skips warmup",
			containers: []atev1alpha1.Container{
				{Name: "a", Readyz: probe},
				{Name: "b", Readyz: probe},
			},
			wantZero: true,
		},
		{
			name: "single container with readyz skips warmup",
			containers: []atev1alpha1.Container{
				{Name: "a", Readyz: probe},
			},
			wantZero: true,
		},
		{
			name: "mixed containers keep warmup",
			containers: []atev1alpha1.Container{
				{Name: "a", Readyz: probe},
				{Name: "b"},
			},
			wantZero: false,
		},
		{
			name: "no readyz anywhere keeps warmup",
			containers: []atev1alpha1.Container{
				{Name: "a"},
				{Name: "b"},
			},
			wantZero: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at := &atev1alpha1.ActorTemplate{
				Spec: atev1alpha1.ActorTemplateSpec{Containers: tt.containers},
			}
			got := goldenSnapshotWarmupFor(at)
			if tt.wantZero && got != 0 {
				t.Errorf("goldenSnapshotWarmupFor = %v, want 0", got)
			}
			if !tt.wantZero && got != goldenSnapshotWarmup {
				t.Errorf("goldenSnapshotWarmupFor = %v, want %v", got, goldenSnapshotWarmup)
			}
		})
	}
}
