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

package v1alpha1

import "testing"

func TestResolvedSandboxConfig(t *testing.T) {
	tests := []struct {
		name string
		pool WorkerPool
		want string
	}{{
		name: "confirmed config of the pool's class",
		pool: WorkerPool{
			Spec: WorkerPoolSpec{
				SandboxClasses: []WorkerPoolSandboxClass{{Name: SandboxClassGvisor}},
			},
			Status: WorkerPoolStatus{
				SandboxClasses: []WorkerPoolSandboxClassStatus{{
					Name:      SandboxClassGvisor,
					ConfigRef: &SandboxConfigReference{Name: "gvisor-default"},
				}},
			},
		},
		want: "gvisor-default",
	}, {
		name: "not resolved yet",
		pool: WorkerPool{
			Spec: WorkerPoolSpec{
				SandboxClasses: []WorkerPoolSandboxClass{{Name: SandboxClassGvisor}},
			},
			Status: WorkerPoolStatus{
				SandboxClasses: []WorkerPoolSandboxClassStatus{{Name: SandboxClassGvisor}},
			},
		},
	}, {
		name: "only a pending config",
		pool: WorkerPool{
			Spec: WorkerPoolSpec{
				SandboxClasses: []WorkerPoolSandboxClass{{Name: SandboxClassGvisor}},
			},
			Status: WorkerPoolStatus{
				SandboxClasses: []WorkerPoolSandboxClassStatus{{
					Name:             SandboxClassGvisor,
					PendingConfigRef: &SandboxConfigReference{Name: "gvisor-next"},
				}},
			},
		},
	}, {
		name: "status resolved another class than the pool's",
		pool: WorkerPool{
			Spec: WorkerPoolSpec{
				SandboxClasses: []WorkerPoolSandboxClass{{Name: SandboxClassMicroVM}},
			},
			Status: WorkerPoolStatus{
				SandboxClasses: []WorkerPoolSandboxClassStatus{{
					Name:      SandboxClassGvisor,
					ConfigRef: &SandboxConfigReference{Name: "gvisor-default"},
				}},
			},
		},
	}, {
		name: "empty sandboxClasses in spec",
		pool: WorkerPool{},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.pool.ResolvedSandboxConfig(); got != tt.want {
				t.Errorf("ResolvedSandboxConfig() = %q, want %q", got, tt.want)
			}
		})
	}
}
