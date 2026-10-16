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

package resources

import (
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/google/go-cmp/cmp"
)

func assignment(uid string, cpu, mem int64) *ateapi.ActorAssignment {
	return &ateapi.ActorAssignment{
		Actor:     &ateapi.ObjectRef{Atespace: "demo", Name: uid},
		ActorUid:  uid,
		Resources: CPUMemory(cpu, mem),
	}
}

// The total moves by one assignment's worth in each direction, and a Worker
// back to holding nothing carries no allocation at all rather than a zeroed
// message: emptied and never-filled have to be the same record.
func TestAddToAllocated(t *testing.T) {
	var total *ateapi.WorkerResources
	total = AddToAllocated(total, assignment("a", 1000, 1<<30), +1)
	total = AddToAllocated(total, assignment("b", 500, 2<<30), +1)

	want := &ateapi.WorkerResources{Actors: 2, CpuMillicores: 1500, MemoryBytes: 3 << 30}
	if diff := cmp.Diff(want, total, protocmp.Transform()); diff != "" {
		t.Errorf("allocated mismatch (-want +got):\n%s", diff)
	}

	total = AddToAllocated(total, assignment("b", 500, 2<<30), -1)
	want = &ateapi.WorkerResources{Actors: 1, CpuMillicores: 1000, MemoryBytes: 1 << 30}
	if diff := cmp.Diff(want, total, protocmp.Transform()); diff != "" {
		t.Errorf("allocated after release mismatch (-want +got):\n%s", diff)
	}

	if total = AddToAllocated(total, assignment("a", 1000, 1<<30), -1); total != nil {
		t.Errorf("allocated = %v for a Worker holding nothing, want nil", total)
	}
}

// An Actor that declared no limits reserves nothing but still costs a slot.
func TestAddToAllocatedCountsAnActorWithoutResources(t *testing.T) {
	total := AddToAllocated(nil, &ateapi.ActorAssignment{ActorUid: "a"}, +1)
	want := &ateapi.WorkerResources{Actors: 1}
	if diff := cmp.Diff(want, total, protocmp.Transform()); diff != "" {
		t.Errorf("allocated mismatch (-want +got):\n%s", diff)
	}
}

// SumAllocated rebuilds what AddToAllocated adjusts; the contract tests hold the
// two to each other, so they have to agree on the empty case as well.
func TestSumAllocated(t *testing.T) {
	if got := SumAllocated(nil); got != nil {
		t.Errorf("SumAllocated(nil) = %v, want nil", got)
	}

	got := SumAllocated([]*ateapi.ActorAssignment{assignment("a", 1000, 1<<30), assignment("b", 500, 0)})
	want := &ateapi.WorkerResources{Actors: 2, CpuMillicores: 1500, MemoryBytes: 1 << 30}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("SumAllocated() mismatch (-want +got):\n%s", diff)
	}
}

// Under overcommit a release frees the slot but leaves resources to the
// Worker, and a Worker holding nothing still carries no allocation at all.
func TestAddActorCountOnly(t *testing.T) {
	for _, tc := range []struct {
		name  string
		total *ateapi.WorkerResources
		delta int32
		want  *ateapi.WorkerResources
	}{
		{
			name:  "release keeps resources",
			total: &ateapi.WorkerResources{CpuMillicores: 1500, MemoryBytes: 3 << 30, Actors: 2},
			delta: -1,
			want:  &ateapi.WorkerResources{CpuMillicores: 1500, MemoryBytes: 3 << 30, Actors: 1},
		},
		{
			name:  "increment",
			total: &ateapi.WorkerResources{CpuMillicores: 1500, MemoryBytes: 3 << 30, Actors: 1},
			delta: +1,
			want:  &ateapi.WorkerResources{CpuMillicores: 1500, MemoryBytes: 3 << 30, Actors: 2},
		},
		{
			name:  "count clamps at zero",
			total: &ateapi.WorkerResources{CpuMillicores: 100},
			delta: -1,
			want:  &ateapi.WorkerResources{CpuMillicores: 100},
		},
		{
			name:  "emptied is nil",
			total: &ateapi.WorkerResources{Actors: 1},
			delta: -1,
			want:  nil,
		},
		{
			name:  "nil stays nil",
			total: nil,
			delta: -1,
			want:  nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := AddActorCountOnly(tc.total, tc.delta)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("AddActorCountOnly() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSubtractResourcesFromAllocated(t *testing.T) {
	for _, tc := range []struct {
		name  string
		total *ateapi.WorkerResources
		delta *ateapi.Resources
		want  *ateapi.WorkerResources
	}{
		{
			name:  "reclaim",
			total: &ateapi.WorkerResources{CpuMillicores: 2000, MemoryBytes: 4 << 30, Actors: 2},
			delta: CPUMemory(1500, 1<<30),
			want:  &ateapi.WorkerResources{CpuMillicores: 500, MemoryBytes: 3 << 30, Actors: 2},
		},
		{
			name:  "one dimension clamps at zero, the other stays",
			total: &ateapi.WorkerResources{CpuMillicores: 500, MemoryBytes: 1 << 30, Actors: 1},
			delta: CPUMemory(900, 512<<20),
			want:  &ateapi.WorkerResources{MemoryBytes: 512 << 20, Actors: 1},
		},
		{
			name:  "exactly to zero",
			total: &ateapi.WorkerResources{CpuMillicores: 500, MemoryBytes: 1 << 30, Actors: 1},
			delta: CPUMemory(500, 0),
			want:  &ateapi.WorkerResources{MemoryBytes: 1 << 30, Actors: 1},
		},
		{
			name:  "subtracting from an empty dimension stays at zero",
			total: &ateapi.WorkerResources{CpuMillicores: 500, Actors: 1},
			delta: CPUMemory(0, 1<<30),
			want:  &ateapi.WorkerResources{CpuMillicores: 500, Actors: 1},
		},
		{
			name:  "from nothing stays nothing",
			total: nil,
			delta: CPUMemory(500, 1<<30),
			want:  nil,
		},
		{
			name:  "emptied with no actors is nil",
			total: &ateapi.WorkerResources{CpuMillicores: 500, MemoryBytes: 1 << 30},
			delta: CPUMemory(500, 2<<30),
			want:  nil,
		},
		{
			name:  "emptied with actors keeps the count",
			total: &ateapi.WorkerResources{CpuMillicores: 500, MemoryBytes: 1 << 30, Actors: 2},
			delta: CPUMemory(9000, 9<<30),
			want:  &ateapi.WorkerResources{Actors: 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := SubtractResourcesFromAllocated(tc.total, tc.delta)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("SubtractResourcesFromAllocated() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
