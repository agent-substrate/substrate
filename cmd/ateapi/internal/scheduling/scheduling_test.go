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

	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/labels"
)

func TestSchedule(t *testing.T) {
	tierOne := map[string]string{"tier": "1"}
	tierTwo := map[string]string{"tier": "2"}

	tests := []struct {
		name        string
		fleet       fleet
		constraints Constraints
		limits      *ateapipb.Resources
		wantPod     string // "" means ErrNoCapacity expected
	}{
		{
			// Two full workers precede w-free so pair sampling cannot reach w-free
			// unless full workers are filtered out.
			name: "picks the only eligible free worker",
			fleet: fleet{
				worker("w-busy-1", "gvisor", "node-a", tierTwo, assigned("demo", "other-1")),
				worker("w-busy-2", "gvisor", "node-a", tierTwo, assigned("demo", "other-2")),
				worker("w-free", "gvisor", "node-a", tierTwo),
			},
			constraints: Constraints{},
			wantPod:     "w-free",
		},
		{
			name: "worker with no sandbox class is never scheduled",
			fleet: fleet{
				worker("w-unclassed", "", "node-a", tierTwo),
			},
			constraints: Constraints{},
		},
		{
			name: "worker with no default runtime is never scheduled",
			fleet: fleet{
				worker("w-unregistered", "gvisor", "node-a", tierTwo, withRuntimes(nil)),
			},
			constraints: Constraints{},
		},
		{
			name: "template selector filters",
			fleet: fleet{
				worker("w-1", "gvisor", "node-a", tierTwo),
				worker("w-2", "gvisor", "node-a", tierOne),
			},
			constraints: Constraints{}.WithWorkerSelectors(labels.SelectorFromSet(labels.Set{"tier": "1"})),
			wantPod:     "w-2",
		},
		{
			name: "actor selector filters on top of template selector",
			fleet: fleet{
				worker("w-1", "gvisor", "node-a", map[string]string{"tier": "1", "worload": "a"}),
				worker("w-2", "gvisor", "node-b", map[string]string{"tier": "1", "workload": "b"}),
			},
			constraints: Constraints{}.WithWorkerSelectors(
				labels.SelectorFromSet(labels.Set{"tier": "1"}),
				labels.SelectorFromSet(labels.Set{"workload": "b"})),
			wantPod: "w-2",
		},
		{
			name: "WithNode restricts the node",
			fleet: fleet{
				worker("w-a", "gvisor", "node-a", tierTwo),
				worker("w-b", "gvisor", "node-b", tierTwo),
			},
			constraints: Constraints{}.WithNode("node-b"),
			wantPod:     "w-b",
		},
		{
			name: "nil selectors match everything",
			fleet: fleet{
				worker("w-1", "gvisor", "node-a", nil),
			},
			constraints: Constraints{},
			wantPod:     "w-1",
		},
		{
			name: "busy workers never scheduled even if eligible",
			fleet: fleet{
				worker("w-busy", "gvisor", "node-a", tierTwo, assigned("demo", "a")),
			},
			constraints: Constraints{},
		},
		{
			name: "draining workers never scheduled",
			fleet: fleet{
				worker("w-draining", "gvisor", "node-a", tierTwo, withState(ateapipb.WorkerState_WORKER_STATE_DRAINING)),
			},
			constraints: Constraints{},
		},
		{
			name: "unspecified workers never scheduled",
			fleet: fleet{
				worker("w-unspecified", "gvisor", "node-a", tierTwo, withState(ateapipb.WorkerState_WORKER_STATE_UNSPECIFIED)),
			},
			constraints: Constraints{},
		},
		{
			name: "picks active worker over draining one",
			fleet: fleet{
				worker("w-draining", "gvisor", "node-a", tierTwo, withState(ateapipb.WorkerState_WORKER_STATE_DRAINING)),
				worker("w-active", "gvisor", "node-a", tierTwo),
			},
			constraints: Constraints{},
			wantPod:     "w-active",
		},
		{
			name: "worker with too little cpu capacity is skipped",
			fleet: fleet{
				worker("w-small", "gvisor", "node-a", tierTwo, withCapacity(1000, 8<<30)),
				worker("w-big", "gvisor", "node-a", tierTwo, withCapacity(4000, 8<<30)),
			},
			constraints: Constraints{},
			limits:      resources.CPUMemory(2000, 0),
			wantPod:     "w-big",
		},
		{
			name: "worker with too little memory capacity is skipped",
			fleet: fleet{
				worker("w-small", "gvisor", "node-a", tierTwo, withCapacity(4000, 1<<30)),
				worker("w-big", "gvisor", "node-a", tierTwo, withCapacity(4000, 4<<30)),
			},
			constraints: Constraints{},
			limits:      resources.CPUMemory(0, 2<<30),
			wantPod:     "w-big",
		},
		{
			name: "no worker with enough capacity yields ErrNoCapacity",
			fleet: fleet{
				worker("w-small", "gvisor", "node-a", tierTwo, withCapacity(1000, 1<<30)),
			},
			constraints: Constraints{},
			limits:      resources.CPUMemory(2000, 0),
		},
		{
			// A Worker reports everything it has, so one that has reported no
			// compute has none: an Actor that asks for some is not placed here.
			name: "a worker that reported no compute takes no actor that needs some",
			fleet: fleet{
				worker("w-unknown", "gvisor", "node-a", tierTwo),
			},
			constraints: Constraints{},
			limits:      resources.CPUMemory(2000, 2<<30),
		},
		{
			// An Actor that declares nothing still fits: it asks for no
			// dimension, so there is none the Worker must supply.
			name: "a worker that reported no compute still takes an actor that needs none",
			fleet: fleet{
				worker("w-unknown", "gvisor", "node-a", tierTwo),
			},
			constraints: Constraints{},
			wantPod:     "w-unknown",
		},
		{
			name: "no limits ignores worker compute capacity",
			fleet: fleet{
				worker("w-tiny", "gvisor", "node-a", tierTwo, withCapacity(100, 1<<20)),
			},
			constraints: Constraints{},
			wantPod:     "w-tiny",
		},
		{
			name:        "empty fleet",
			fleet:       fleet{},
			constraints: Constraints{},
		},
		{
			// A worker that has not said it can hold more admits one, so this is
			// the behavior every worker has until a worker reports otherwise.
			name: "unset actor capacity admits one actor",
			fleet: fleet{
				worker("w-busy", "gvisor", "node-a", tierTwo, assigned("demo", "other")),
			},
			constraints: Constraints{},
		},
		{
			name: "a worker below its actor ceiling still has room",
			fleet: fleet{
				worker("w-two", "gvisor", "node-a", tierTwo, withMaxActors(2), assigned("demo", "other")),
			},
			constraints: Constraints{},
			wantPod:     "w-two",
		},
		{
			name: "a worker at its actor ceiling is full however small the actor",
			fleet: fleet{
				worker("w-two", "gvisor", "node-a", tierTwo, withMaxActors(2),
					assigned("demo", "a"), assigned("demo", "b")),
			},
			constraints: Constraints{},
		},
		{
			// Placement is against what is left, not against the whole capacity:
			// the resident actor already took half of it.
			name: "capacity already allocated is not offered twice",
			fleet: fleet{
				worker("w-half", "gvisor", "node-a", tierTwo, withCapacity(4000, 8<<30), withMaxActors(4),
					assignedFor("demo", "other", resources.CPUMemory(3000, 4<<30))),
			},
			constraints: Constraints{},
			limits:      resources.CPUMemory(2000, 0),
		},
		{
			name: "what the residents left over is still placeable",
			fleet: fleet{
				worker("w-half", "gvisor", "node-a", tierTwo, withCapacity(4000, 8<<30), withMaxActors(4),
					assignedFor("demo", "other", resources.CPUMemory(3000, 4<<30))),
			},
			constraints: Constraints{},
			limits:      resources.CPUMemory(1000, 4<<30),
			wantPod:     "w-half",
		},
		{
			name: "prefers less-loaded worker when first sample is busier",
			fleet: fleet{
				worker("w-busy", "gvisor", "node-a", tierTwo, withMaxActors(4), assigned("demo", "a"), assigned("demo", "b")),
				worker("w-idle", "gvisor", "node-b", tierTwo, withMaxActors(4)),
			},
			constraints: Constraints{},
			wantPod:     "w-idle",
		},
		{
			name: "keeps first sample when it is already less loaded than second",
			fleet: fleet{
				worker("w-idle", "gvisor", "node-a", tierTwo, withMaxActors(4)),
				worker("w-busy", "gvisor", "node-b", tierTwo, withMaxActors(4), assigned("demo", "a")),
			},
			constraints: Constraints{},
			wantPod:     "w-idle",
		},
		{
			name: "compares actor utilization across heterogeneous worker capacities",
			fleet: fleet{
				// 1/2 (50%) vs 2/16 (12.5%): w-large has more actors but lower utilization.
				worker("w-small", "gvisor", "node-a", tierTwo, withMaxActors(2), assigned("demo", "a")),
				worker("w-large", "gvisor", "node-b", tierTwo, withMaxActors(16), assigned("demo", "b"), assigned("demo", "c")),
			},
			constraints: Constraints{},
			wantPod:     "w-large",
		},
		{
			name: "prefers lower compute utilization at equal actor utilization",
			fleet: fleet{
				worker("w-heavy", "gvisor", "node-a", tierTwo, withCapacity(4000, 8<<30), withMaxActors(4),
					assignedFor("demo", "a", resources.CPUMemory(1000, 6<<30))),
				worker("w-light", "gvisor", "node-b", tierTwo, withCapacity(4000, 8<<30), withMaxActors(4),
					assignedFor("demo", "b", resources.CPUMemory(1000, 1<<30))),
			},
			constraints: Constraints{},
			limits:      resources.CPUMemory(500, 1<<30),
			wantPod:     "w-light",
		},
		{
			// With a uniform actor ceiling, slot utilization alone would pick
			// w-hot (5/1000 < 6/1000) even though its memory is 95% used.
			name: "compute hotspot outweighs one fewer actor",
			fleet: fleet{
				worker("w-hot", "gvisor", "node-a", tierTwo, withCapacity(4000, 20<<30), withMaxActors(1000),
					assignedFor("demo", "a1", resources.CPUMemory(100, 19<<30)),
					assigned("demo", "a2"), assigned("demo", "a3"), assigned("demo", "a4"), assigned("demo", "a5")),
				worker("w-cool", "gvisor", "node-b", tierTwo, withCapacity(4000, 20<<30), withMaxActors(1000),
					assignedFor("demo", "b1", resources.CPUMemory(100, 2<<30)),
					assigned("demo", "b2"), assigned("demo", "b3"), assigned("demo", "b4"), assigned("demo", "b5"),
					assigned("demo", "b6")),
			},
			constraints: Constraints{},
			limits:      resources.CPUMemory(100, 512<<20),
			wantPod:     "w-cool",
		},
		{
			name: "prefers worker with known resource utilization over unreported capacity",
			fleet: fleet{
				worker("w-unreported", "gvisor", "node-a", tierTwo, withMaxActors(4), assigned("demo", "a")),
				worker("w-known", "gvisor", "node-b", tierTwo, withCapacity(4000, 8<<30), withMaxActors(4),
					assignedFor("demo", "b", resources.CPUMemory(1000, 2<<30))),
			},
			constraints: Constraints{},
			wantPod:     "w-known",
		},
		{
			name: "breaks utilization ties using remaining actor slots",
			fleet: fleet{
				// Both workers are at 50% actor-slot utilization (1/2 vs 4/8) and
				// equal compute utilization, so w-large wins with 4 free slots vs 1.
				worker("w-small", "gvisor", "node-a", tierTwo, withMaxActors(2), assigned("demo", "a")),
				worker("w-large", "gvisor", "node-b", tierTwo, withMaxActors(8),
					assigned("demo", "b"), assigned("demo", "c"), assigned("demo", "d"), assigned("demo", "e")),
			},
			constraints: Constraints{},
			wantPod:     "w-large",
		},
		{
			name: "skips idle worker that lacks resource room and picks busier worker with room",
			fleet: fleet{
				worker("w-idle-small", "gvisor", "node-a", tierTwo, withCapacity(1000, 1<<30), withMaxActors(4)),
				worker("w-busy-big", "gvisor", "node-b", tierTwo, withCapacity(4000, 8<<30), withMaxActors(4),
					assignedFor("demo", "a", resources.CPUMemory(1000, 2<<30))),
			},
			constraints: Constraints{},
			limits:      resources.CPUMemory(1000, 2<<30),
			wantPod:     "w-busy-big",
		},
		{
			name: "required runtime matches worker default runtime",
			fleet: fleet{
				worker("w-arm", "gvisor", "node-a", tierTwo, withRuntimes(sandboxRuntime("gvisor", "arm64"))),
				worker("w-amd", "gvisor", "node-b", tierTwo, withRuntimes(sandboxRuntime("gvisor", "amd64"))),
			},
			constraints: Constraints{}.WithRuntime(hardware.NewRequirement(sandboxRuntime("gvisor", "amd64"))),
			wantPod:     "w-amd",
		},
		{
			name: "required runtime matches worker restorable runtime",
			fleet: fleet{
				worker("w-upgraded", "gvisor", "node-a", tierTwo,
					withRuntimes(sandboxRuntime("gvisor", "amd64", "new"), sandboxRuntime("gvisor", "amd64", "old"))),
			},
			constraints: Constraints{}.WithRuntime(hardware.NewRequirement(sandboxRuntime("gvisor", "amd64", "old"))),
			wantPod:     "w-upgraded",
		},
		{
			name: "required runtime with no matching worker yields ErrNoCapacity",
			fleet: fleet{
				worker("w-arm", "gvisor", "node-a", tierTwo, withRuntimes(sandboxRuntime("gvisor", "arm64"))),
			},
			constraints: Constraints{}.WithRuntime(hardware.NewRequirement(sandboxRuntime("gvisor", "amd64"))),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.fleet, WithIntn(firstIntn))
			got, err := s.Schedule(context.Background(), tc.constraints, tc.limits)

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

func TestMatches(t *testing.T) {
	tierSel := labels.SelectorFromSet(labels.Set{"tier": "1"})
	workloadSel := labels.SelectorFromSet(labels.Set{"workload": "a"})

	tests := []struct {
		name        string
		worker      *ateapipb.Worker
		constraints Constraints
		want        bool
	}{
		{
			name:        "satisfies all requirements",
			worker:      worker("w", "gvisor", "node-a", map[string]string{"tier": "1", "workload": "a"}),
			constraints: Constraints{}.WithWorkerSelectors(tierSel, workloadSel).WithNode("node-a"),
			want:        true,
		},
		{
			name:        "assignment is ignored",
			worker:      worker("w", "gvisor", "node-a", nil, assigned("demo", "someone-else")),
			constraints: Constraints{},
			want:        true,
		},
		{
			// A worker that already holds the actor must still match it.
			name:        "full worker matches",
			worker:      worker("w", "gvisor", "node-a", nil, withMaxActors(1), assigned("demo", "resident")),
			constraints: Constraints{},
			want:        true,
		},
		{
			name:        "worker with no sandbox class",
			worker:      worker("w", "", "node-a", nil),
			constraints: Constraints{},
			want:        false,
		},
		{
			name:        "snapshot class pins the worker class",
			worker:      worker("w", "microvm", "node-a", nil),
			constraints: Constraints{}.WithSandboxClass("gvisor"),
			want:        false,
		},
		{
			name:        "snapshot class matches without a runtime requirement",
			worker:      worker("w", "gvisor", "node-a", nil),
			constraints: Constraints{}.WithSandboxClass("gvisor"),
			want:        true,
		},
		{
			name:        "no snapshot class accepts any class",
			worker:      worker("w", "microvm", "node-a", nil),
			constraints: Constraints{},
			want:        true,
		},
		{
			name:        "template selector mismatch",
			worker:      worker("w", "gvisor", "node-a", map[string]string{"tier": "2"}),
			constraints: Constraints{}.WithWorkerSelectors(tierSel),
			want:        false,
		},
		{
			name:        "actor selector mismatch",
			worker:      worker("w", "gvisor", "node-a", map[string]string{"workload": "b"}),
			constraints: Constraints{}.WithWorkerSelectors(workloadSel),
			want:        false,
		},
		{
			name:        "node restriction excludes",
			worker:      worker("w", "gvisor", "node-b", nil),
			constraints: Constraints{}.WithNode("node-a"),
			want:        false,
		},
		{
			name:        "AND of template and actor selectors match",
			worker:      worker("w", "gvisor", "node-a", map[string]string{"tier": "1", "workload": "a"}),
			constraints: Constraints{}.WithWorkerSelectors(tierSel, workloadSel),
			want:        true,
		},
		{
			name:        "AND of two selectors, one fails",
			worker:      worker("w", "gvisor", "node-a", map[string]string{"tier": "1", "workload": "b"}),
			constraints: Constraints{}.WithWorkerSelectors(tierSel, workloadSel),
			want:        false,
		},
		{
			name:        "skips draining worker",
			worker:      worker("w", "gvisor", "node-a", nil, withState(ateapipb.WorkerState_WORKER_STATE_DRAINING)),
			constraints: Constraints{},
			want:        false,
		},
		{
			name:        "skips unavailable worker",
			worker:      worker("w", "gvisor", "node-a", nil, withState(ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE)),
			constraints: Constraints{},
			want:        false,
		},
		{
			name:        "skips unspecified worker",
			worker:      worker("w", "gvisor", "node-a", nil, withState(ateapipb.WorkerState_WORKER_STATE_UNSPECIFIED)),
			constraints: Constraints{},
			want:        false,
		},
		{
			name:        "skips worker with no sandbox classes",
			worker:      worker("w", "gvisor", "node-a", nil, withRuntimes(nil)),
			constraints: Constraints{},
			want:        false,
		},
		{
			name:        "matches required runtime via default runtime",
			worker:      worker("w", "gvisor", "node-a", nil, withRuntimes(sandboxRuntime("gvisor", "amd64", "old"))),
			constraints: Constraints{}.WithRuntime(hardware.NewRequirement(sandboxRuntime("gvisor", "amd64", "old"))),
			want:        true,
		},
		{
			name: "matches required runtime via restorable runtimes",
			worker: worker("w", "gvisor", "node-a", nil,
				withRuntimes(sandboxRuntime("gvisor", "amd64", "new"), sandboxRuntime("gvisor", "amd64", "old"))),
			constraints: Constraints{}.WithRuntime(hardware.NewRequirement(sandboxRuntime("gvisor", "amd64", "old"))),
			want:        true,
		},
		{
			name:        "skips worker when required runtime does not match",
			worker:      worker("w", "gvisor", "node-a", nil, withRuntimes(sandboxRuntime("gvisor", "arm64"))),
			constraints: Constraints{}.WithRuntime(hardware.NewRequirement(sandboxRuntime("gvisor", "amd64"))),
			want:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.constraints.Matches(tc.worker); got != tc.want {
				t.Fatalf("Matches() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasRoom(t *testing.T) {
	tests := []struct {
		name   string
		worker *ateapipb.Worker
		limits *ateapipb.Resources
		want   bool
	}{
		{
			name:   "free slot without limits",
			worker: worker("w", "gvisor", "node-a", nil),
			want:   true,
		},
		{
			name:   "full worker without limits",
			worker: worker("w", "gvisor", "node-a", nil, withMaxActors(1), assigned("demo", "resident")),
			want:   false,
		},
		{
			name:   "enough cpu",
			worker: worker("w", "gvisor", "node-a", nil, withCapacity(4000, 8<<30)),
			limits: resources.CPUMemory(2000, 0),
			want:   true,
		},
		{
			name:   "too little cpu",
			worker: worker("w", "gvisor", "node-a", nil, withCapacity(1000, 8<<30)),
			limits: resources.CPUMemory(2000, 0),
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := HasRoom(tc.worker, tc.limits); got != tc.want {
				t.Fatalf("HasRoom() = %v, want %v", got, tc.want)
			}
		})
	}
}

type fleet []*ateapipb.Worker

func (f fleet) Workers() ([]*ateapipb.Worker, error) { return f, nil }

func worker(pod, class, node string, lbls map[string]string, opts ...func(*ateapipb.Worker)) *ateapipb.Worker {
	w := &ateapipb.Worker{
		WorkerPod: pod,
		NodeName:  node,
		Labels:    lbls,
		// A stored Worker always carries a ceiling; CreateWorker reifies one.
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}
	if class != "" {
		withRuntimes(sandboxRuntime(class, "amd64"))(w)
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func sandboxRuntime(class, arch string, bundleHash ...string) *ateapipb.ActorSandboxRuntime {
	attrs := []*ateapipb.AttributeEntry{{Key: "architecture", Value: arch}}
	if len(bundleHash) > 0 && bundleHash[0] != "" {
		attrs = append(attrs, &ateapipb.AttributeEntry{Key: "sandbox_bundle_hash", Value: bundleHash[0]})
	}
	return &ateapipb.ActorSandboxRuntime{
		SandboxClass: class,
		Version: &ateapipb.VersionedSandboxCompat{
			SchemaVersion: "v1",
			Attributes:    attrs,
		},
	}
}

// sandboxClass is the WorkerSandboxClass entry of def's class, with def as
// its default runtime and restorable as the others.
func sandboxClass(def *ateapipb.ActorSandboxRuntime, restorable ...*ateapipb.ActorSandboxRuntime) *ateapipb.WorkerSandboxClass {
	c := &ateapipb.WorkerSandboxClass{SandboxClass: def.GetSandboxClass(), DefaultRuntime: &ateapipb.WorkerSandboxRuntime{Version: def.GetVersion()}}
	for _, r := range restorable {
		c.RestorableRuntimes = append(c.RestorableRuntimes, &ateapipb.WorkerSandboxRuntime{Version: r.GetVersion()})
	}
	return c
}

// withRuntimes gives the worker one sandbox class, def's. A nil def leaves
// the worker as before it registered: no class at all.
func withRuntimes(def *ateapipb.ActorSandboxRuntime, restorable ...*ateapipb.ActorSandboxRuntime) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) {
		w.Status.SandboxClasses = nil
		if def != nil {
			w.Status.SandboxClasses = []*ateapipb.WorkerSandboxClass{sandboxClass(def, restorable...)}
		}
	}
}

func withState(state ateapipb.WorkerState) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) {
		w.Status.State = state
	}
}

func assigned(atespace, name string) func(*ateapipb.Worker) {
	return assignedFor(atespace, name, nil)
}

// assignedFor books an actor that took resources from the worker, so a test can
// place against what is left rather than against the whole capacity. Only the
// allocation total, which is all placement reads: the assignments themselves are
// their own records and never on the worker.
func assignedFor(atespace, name string, took *ateapipb.Resources) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) {
		if w.Status == nil {
			w.Status = &ateapipb.WorkerStatus{}
		}
		w.Status.Allocated = resources.AddToAllocated(w.Status.Allocated,
			&ateapipb.ActorAssignment{ActorUid: atespace + "/" + name, Resources: took}, +1)
	}
}

func withMaxActors(n int32) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) {
		if w.Status == nil {
			w.Status = &ateapipb.WorkerStatus{}
		}
		if w.Status.Capacity == nil {
			w.Status.Capacity = &ateapipb.WorkerResources{}
		}
		w.Status.Capacity.Actors = n
	}
}

func withCapacity(cpuMilli, memBytes int64) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) {
		if w.Status.Capacity == nil {
			w.Status.Capacity = &ateapipb.WorkerResources{}
		}
		w.Status.Capacity.CpuMillicores = cpuMilli
		w.Status.Capacity.MemoryBytes = memBytes
	}
}

// firstIntn always returns 0, sampling candidates[0] and candidates[1] when >=2 exist.
func firstIntn(int) int { return 0 }

func TestSchedulePowerOfTwoChoicesTieBreaking(t *testing.T) {
	f := fleet{
		worker("w-0", "gvisor", "node-a", nil, withMaxActors(4), assigned("demo", "a")),
		worker("w-1", "gvisor", "node-b", nil, withMaxActors(4), assigned("demo", "b")),
		worker("w-2", "gvisor", "node-c", nil, withMaxActors(4), assigned("demo", "c")),
	}
	// On equal utilization, Schedule must preserve the first sampled candidate so
	// every tied worker remains reachable via the random source.
	for wantIdx, wantPod := range []string{"w-0", "w-1", "w-2"} {
		calls := 0
		s := New(f, WithIntn(func(n int) int {
			calls++
			if calls == 1 {
				return wantIdx
			}
			return 0
		}))
		got, err := s.Schedule(context.Background(), Constraints{}, nil)
		if err != nil {
			t.Fatalf("Schedule() for index %d error = %v", wantIdx, err)
		}
		if got.GetWorkerPod() != wantPod {
			t.Fatalf("Schedule() for index %d = %q, want %q", wantIdx, got.GetWorkerPod(), wantPod)
		}
	}
}

// seqIntn returns vals in order, one per call.
func seqIntn(vals ...int) func(int) int {
	i := 0
	return func(int) int {
		v := vals[i]
		i++
		return v
	}
}

func TestSchedulePowerOfTwoChoicesShiftedSecondSample(t *testing.T) {
	// intn yields i=0, then j=1; since j >= i the second sample shifts to
	// candidates[2], the only lighter worker. An off-by-one in the shift would
	// compare w-0 against w-1 and return a busy worker.
	f := fleet{
		worker("w-0", "gvisor", "node-a", nil, withMaxActors(4), assigned("demo", "a"), assigned("demo", "b")),
		worker("w-1", "gvisor", "node-b", nil, withMaxActors(4), assigned("demo", "c"), assigned("demo", "d")),
		worker("w-2", "gvisor", "node-c", nil, withMaxActors(4)),
	}
	s := New(f, WithIntn(seqIntn(0, 1)))
	got, err := s.Schedule(context.Background(), Constraints{}, nil)
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}
	if got.GetWorkerPod() != "w-2" {
		t.Fatalf("Schedule() = %q, want %q", got.GetWorkerPod(), "w-2")
	}
}
