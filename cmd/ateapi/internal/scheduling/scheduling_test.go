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

package scheduling

import (
	"context"
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/labels"
)

func TestSchedule(t *testing.T) {
	tierOne := map[string]string{"tier": "1"}
	tierTwo := map[string]string{"tier": "2"}

	tests := []struct {
		name        string
		fleet       fleet
		constraints Constraints
		wantPod     string // "" means ErrNoCapacity expected
	}{
		{
			name: "picks the only eligible free worker",
			fleet: fleet{
				worker("w-busy", "gvisor", "node-a", tierTwo, assigned("demo", "other")),
				worker("w-free", "gvisor", "node-a", tierTwo),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
			wantPod:     "w-free",
		},
		{
			name: "sandbox class is a hard constraint",
			fleet: fleet{
				worker("w-vm", "microvm", "node-a", tierTwo),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
		},
		{
			name: "template selector filters",
			fleet: fleet{
				worker("w-1", "gvisor", "node-a", tierTwo),
				worker("w-2", "gvisor", "node-a", tierOne),
			},
			constraints: Constraints{SandboxClass: "gvisor",
				TemplateSelector: labels.SelectorFromSet(labels.Set{"tier": "1"})},
			wantPod: "w-2",
		},
		{
			name: "actor selector filters on top of template selector",
			fleet: fleet{
				worker("w-1", "gvisor", "node-a", map[string]string{"tier": "1", "worload": "a"}),
				worker("w-2", "gvisor", "node-b", map[string]string{"tier": "1", "workload": "b"}),
			},
			constraints: Constraints{SandboxClass: "gvisor",
				TemplateSelector: labels.SelectorFromSet(labels.Set{"tier": "1"}),
				ActorSelector:    labels.SelectorFromSet(labels.Set{"workload": "b"})},
			wantPod: "w-2",
		},
		{
			name: "node restriction is a hard constraint",
			fleet: fleet{
				worker("w-a", "gvisor", "node-a", tierTwo),
				worker("w-b", "gvisor", "node-b", tierTwo),
			},
			constraints: Constraints{SandboxClass: "gvisor", RequiredNodes: []string{"node-b"}},
			wantPod:     "w-b",
		},
		{
			name: "nil selectors match everything",
			fleet: fleet{
				worker("w-1", "gvisor", "node-a", nil),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
			wantPod:     "w-1",
		},
		{
			name: "busy workers never scheduled even if eligible",
			fleet: fleet{
				worker("w-busy", "gvisor", "node-a", tierTwo, assigned("demo", "a")),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
		},
		{
			name:        "empty fleet",
			fleet:       fleet{},
			constraints: Constraints{SandboxClass: "gvisor"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.fleet, WithIntn(firstIntn))
			got, err := s.Schedule(context.Background(), tc.constraints)

			if tc.wantPod != "" {
				if err != nil {
					t.Fatalf("Schedule() error = %v, want worker %q", err, tc.wantPod)
				}
				if got.GetWorkerPod() != tc.wantPod {
					t.Fatalf("Schedule() = %q, want %q", got.GetWorkerPod(), tc.wantPod)
				}
				return
			}

			if !errors.Is(err, ErrNoCapacity) {
				t.Fatalf("Schedule() error = %v, want ErrNoCapacity", err)
			}
		})
	}
}

func TestApplies(t *testing.T) {
	tierSel := labels.SelectorFromSet(labels.Set{"tier": "1"})
	workloadSel := labels.SelectorFromSet(labels.Set{"workload": "a"})

	tests := []struct {
		name        string
		worker      *ateapipb.Worker
		constraints Constraints
		want        bool
	}{
		{
			name:   "satisfies all constraints",
			worker: worker("w", "gvisor", "node-a", map[string]string{"tier": "1", "workload": "a"}),
			constraints: Constraints{SandboxClass: "gvisor", TemplateSelector: tierSel, RequiredNodes: []string{"node-a"},
				ActorSelector: workloadSel},
			want: true,
		},
		{
			name:        "assignment is ignored",
			worker:      worker("w", "gvisor", "node-a", nil, assigned("demo", "someone-else")),
			constraints: Constraints{SandboxClass: "gvisor"},
			want:        true,
		},
		{
			name:        "class mismatch",
			worker:      worker("w", "microvm", "node-a", nil),
			constraints: Constraints{SandboxClass: "gvisor"},
			want:        false,
		},
		{
			name:        "template selector mismatch",
			worker:      worker("w", "gvisor", "node-a", map[string]string{"tier": "2"}),
			constraints: Constraints{SandboxClass: "gvisor", TemplateSelector: tierSel},
			want:        false,
		},
		{
			name:        "actor selector mismatch",
			worker:      worker("w", "gvisor", "node-a", map[string]string{"workload": "b"}),
			constraints: Constraints{SandboxClass: "gvisor", ActorSelector: workloadSel},
			want:        false,
		},
		{
			name:        "node restriction excludes",
			worker:      worker("w", "gvisor", "node-b", nil),
			constraints: Constraints{SandboxClass: "gvisor", RequiredNodes: []string{"node-a"}},
			want:        false,
		},
		{
			name:   "AND of template and actor selectors match",
			worker: worker("w", "gvisor", "node-a", map[string]string{"tier": "1", "workload": "a"}),
			constraints: Constraints{SandboxClass: "gvisor",
				TemplateSelector: tierSel,
				ActorSelector:    workloadSel},
			want: true,
		},
		{
			name:   "AND of two selectors, one fails",
			worker: worker("w", "gvisor", "node-a", map[string]string{"tier": "1", "workload": "b"}),
			constraints: Constraints{SandboxClass: "gvisor",
				TemplateSelector: tierSel,
				ActorSelector:    workloadSel},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(fleet{})
			if got := s.Applies(tc.worker, tc.constraints); got != tc.want {
				t.Fatalf("Applies() = %v, want %v", got, tc.want)
			}
		})
	}
}

type fleet []*ateapipb.Worker

func (f fleet) Workers() ([]*ateapipb.Worker, error) { return f, nil }

func worker(pod, class, node string, lbls map[string]string, opts ...func(*ateapipb.Worker)) *ateapipb.Worker {
	w := &ateapipb.Worker{
		WorkerPod:    pod,
		SandboxClass: class,
		NodeName:     node,
		Labels:       lbls,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func assigned(atespace, name string) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) {
		w.Assignment = &ateapipb.Assignment{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
		}
	}
}

// firstIntn always picks the first candidate, making Schedule deterministic.
func firstIntn(int) int { return 0 }
