// Copyright 2026 The Agent Substrate Authors
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
	"fmt"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/hardware"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"
)

// The benchmark fleets are shaped like a production gVisor pool: every worker
// is registered and ACTIVE, reports the three attributes atelet writes for
// gVisor, carries a runtime overhead, and already hosts some actors, so the
// room check and lessLoaded do real work. Fleets are built from the worker
// index alone, so every run and every branch sees the same input.
const (
	benchGiB            = int64(1) << 30
	benchMaxActors      = int32(8)
	benchWorkersPerNode = 4
	// benchCPUFeatures is a 64-digit mask, close to the width a current amd64
	// host reports, so parsing it costs what it costs in production.
	benchCPUFeatures = "f7ffbfffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
)

var benchFleetSizes = []int{100, 1000, 10000}

func benchCompat(bundleHash string) *ateapi.VersionedSandboxCompat {
	return &ateapi.VersionedSandboxCompat{
		SchemaVersion: hardware.SchemaVersionV1,
		Attributes: []*ateapi.AttributeEntry{
			{Key: hardware.AttrArchitecture, Value: "amd64"},
			{Key: hardware.AttrCPUFeatures, Value: benchCPUFeatures},
			{Key: hardware.AttrSandboxBundleHash, Value: bundleHash},
		},
	}
}

func benchWorkerRuntime(bundleHash string) *ateapi.WorkerSandboxRuntime {
	return &ateapi.WorkerSandboxRuntime{
		Version:       benchCompat(bundleHash),
		ActorOverhead: &ateapi.RuntimeOverhead{MemoryBytes: 64 << 20, MemoryBytesPerGib: 8 << 20},
	}
}

func benchStamp(bundleHash string) *ateapi.ActorSandboxRuntime {
	return &ateapi.ActorSandboxRuntime{SandboxClass: "gvisor", Version: benchCompat(bundleHash)}
}

func benchNode(i int) string { return fmt.Sprintf("node-%d", i/benchWorkersPerNode) }

// benchPinnedNode is the middle node a pinned scenario resumes on.
func benchPinnedNode(n int) string { return benchNode(n / 2) }

type benchFleetOpts struct {
	// oldEvery > 0 gives every oldEvery-th worker the "old" runtime as a
	// RestorableRuntime; oldFull fills those workers to capacity.
	oldEvery int
	oldFull  bool
	// oldOnNode gives the workers of that node the "old" runtime.
	oldOnNode string
	// restorable lists extra restorable runtime bundle hashes carried by
	// every worker ahead of "old", modeling a multi-version rollout.
	restorable []string
}

func benchFleet(n int, o benchFleetOpts) fleet {
	newRT := benchWorkerRuntime("new")
	oldRT := benchWorkerRuntime("old")
	var extraRTs []*ateapi.WorkerSandboxRuntime
	for _, h := range o.restorable {
		extraRTs = append(extraRTs, benchWorkerRuntime(h))
	}
	withOld := append(append([]*ateapi.WorkerSandboxRuntime(nil), extraRTs...), oldRT)
	f := make(fleet, 0, n)
	for i := range n {
		node := benchNode(i)
		class := &ateapi.WorkerSandboxClass{
			SandboxClass:       "gvisor",
			DefaultRuntime:     newRT,
			RestorableRuntimes: extraRTs,
		}
		used := int32(1 + i%6)
		if (o.oldEvery > 0 && i%o.oldEvery == 0) || (o.oldOnNode != "" && node == o.oldOnNode) {
			class.RestorableRuntimes = withOld
			if o.oldFull {
				used = benchMaxActors
			}
		}
		f = append(f, &ateapi.Worker{
			Metadata:  &ateapi.ResourceMetadata{Name: fmt.Sprintf("uid-%d", i)},
			WorkerPod: fmt.Sprintf("w-%d", i),
			NodeName:  node,
			Labels:    map[string]string{"workload": "sandbox", "pool": "p0"},
			Status: &ateapi.WorkerStatus{
				State:           ateapi.WorkerState_WORKER_STATE_ACTIVE,
				RegisteredEpoch: 1,
				SandboxClasses:  []*ateapi.WorkerSandboxClass{class},
				Capacity:        &ateapi.WorkerResources{Actors: benchMaxActors, CpuMillicores: 4000, MemoryBytes: 16 * benchGiB},
				Allocated:       &ateapi.WorkerResources{Actors: used, CpuMillicores: int64(used) * 500, MemoryBytes: int64(used) * benchGiB},
			},
		})
	}
	return f
}

type benchScenario struct {
	name  string
	fleet func(n int) fleet
	// snapshot is the bundle hash of the snapshot's runtime, "" for a cold
	// boot. relaxable is what canRelaxRuntime reports for the actor.
	snapshot  string
	relaxable bool
	pinned    bool
	// wantOld reports whether the chosen worker must (true) or must not
	// (false) carry the "old" runtime; nil accepts either.
	wantOld *bool
}

var benchScenarios = []benchScenario{
	{
		name:  "cold-boot",
		fleet: func(n int) fleet { return benchFleet(n, benchFleetOpts{}) },
	},
	{
		// Every worker's DefaultRuntime restores the snapshot.
		name:      "memory-hit",
		fleet:     func(n int) fleet { return benchFleet(n, benchFleetOpts{}) },
		snapshot:  "new",
		relaxable: true,
	},
	{
		// The workers that could restore the snapshot's memory are full, so
		// placement falls back to a cold boot on a worker of the class.
		name:      "memory-fallback",
		fleet:     func(n int) fleet { return benchFleet(n, benchFleetOpts{oldEvery: 20, oldFull: true}) },
		snapshot:  "old",
		relaxable: true,
		wantOld:   ptr.To(false),
	},
	{
		// A tenth of the fleet still restores the snapshot's memory and has
		// room, so placement stays at MEMORY.
		name:      "memory-mixed",
		fleet:     func(n int) fleet { return benchFleet(n, benchFleetOpts{oldEvery: 10}) },
		snapshot:  "old",
		relaxable: true,
		wantOld:   ptr.To(true),
	},
	{
		// Multi-version rollout: every worker carries two non-matching
		// restorable runtimes, and a tenth also carry "old" last in the list.
		name: "memory-rollout",
		fleet: func(n int) fleet {
			return benchFleet(n, benchFleetOpts{oldEvery: 10, restorable: []string{"prev-1", "prev-2"}})
		},
		snapshot:  "old",
		relaxable: true,
		wantOld:   ptr.To(true),
	},
	{
		// A pinned actor resumes on the node that holds its local snapshot.
		// Pinned actors detach before relaxing, so the runtime is required.
		name:      "pinned",
		fleet:     func(n int) fleet { return benchFleet(n, benchFleetOpts{oldOnNode: benchPinnedNode(n)}) },
		snapshot:  "old",
		relaxable: false,
		pinned:    true,
		wantOld:   ptr.To(true),
	},
}

// resumeConstraints and scheduleResume are how assignWorkerAttempt asks for a
// placement: the snapshot's runtime is required and, when the actor may fall
// back, a second Schedule with the requirement dropped follows ErrNoCapacity.

func resumeConstraints(c Constraints, snap hardware.Requirement) Constraints {
	c.RequiredRuntime = snap
	c.SnapshotRuntime = snap
	return c
}

func scheduleResume(ctx context.Context, s Scheduler, c Constraints, relaxable bool) (*ateapi.Worker, error) {
	w, err := s.Schedule(ctx, c)
	if errors.Is(err, ErrNoCapacity) && relaxable {
		c.RequiredRuntime = hardware.Requirement{}
		return s.Schedule(ctx, c)
	}
	return w, err
}

// benchConstraints is what schedulingConstraints would build for the
// scenario: the template's selector, the actor's empty selector, its limits,
// and for a resume the snapshot's class and runtime.
func benchConstraints(sc benchScenario, n int) Constraints {
	c := Constraints{
		TemplateSelector: labels.SelectorFromSet(labels.Set{"workload": "sandbox"}),
		ActorSelector:    labels.SelectorFromSet(nil),
		Limits:           &ateapi.Resources{CpuMillicores: 500, MemoryBytes: benchGiB},
	}
	if sc.snapshot != "" {
		c.SandboxClass = "gvisor"
		c = resumeConstraints(c, hardware.NewRequirement(benchStamp(sc.snapshot)))
	}
	if sc.pinned {
		c.RequiredNodes = []string{benchPinnedNode(n)}
	}
	return c
}

func carriesOld(w *ateapi.Worker) bool {
	for _, c := range w.GetStatus().GetSandboxClasses() {
		for _, rt := range c.GetRestorableRuntimes() {
			for _, a := range rt.GetVersion().GetAttributes() {
				if a.GetKey() == hardware.AttrSandboxBundleHash && a.GetValue() == "old" {
					return true
				}
			}
		}
	}
	return false
}

// TestBenchScenarios pins what each benchmark scenario measures: placement
// succeeds, and lands on the kind of worker the scenario is about.
func TestBenchScenarios(t *testing.T) {
	const n = 100
	for _, sc := range benchScenarios {
		t.Run(sc.name, func(t *testing.T) {
			s := New(sc.fleet(n))
			c := benchConstraints(sc, n)
			for range 20 {
				got, err := scheduleResume(context.Background(), s, c, sc.relaxable)
				if err != nil {
					t.Fatalf("scheduleResume() error = %v", err)
				}
				if sc.wantOld != nil && carriesOld(got) != *sc.wantOld {
					t.Fatalf("scheduleResume() = %s carrying old runtime %v, want %v", got.GetWorkerPod(), carriesOld(got), *sc.wantOld)
				}
				if sc.pinned && got.GetNodeName() != benchPinnedNode(n) {
					t.Fatalf("scheduleResume() = %s on %s, want node %s", got.GetWorkerPod(), got.GetNodeName(), benchPinnedNode(n))
				}
			}
		})
	}
}

// BenchmarkSchedule times one placement per scenario and fleet size, the way
// assignWorkerAttempt asks for it (see scheduleResume). The fleet is a plain
// slice, so the workercache copy is not included; BenchmarkWorkers in
// workercache measures that on its own.
func BenchmarkSchedule(b *testing.B) {
	for _, n := range benchFleetSizes {
		for _, sc := range benchScenarios {
			s := New(sc.fleet(n))
			c := benchConstraints(sc, n)
			b.Run(fmt.Sprintf("%s/n=%d", sc.name, n), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := scheduleResume(context.Background(), s, c, sc.relaxable); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
