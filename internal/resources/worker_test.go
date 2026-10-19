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

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/google/go-cmp/cmp"
)

func assignment(uid string, cpu, mem int64) *ateapipb.ActorAssignment {
	return &ateapipb.ActorAssignment{
		Actor:     &ateapipb.ObjectRef{Atespace: "demo", Name: uid},
		ActorUid:  uid,
		Resources: CPUMemory(cpu, mem),
	}
}

// The total moves by one assignment's worth in each direction, and a Worker
// back to holding nothing carries no allocation at all rather than a zeroed
// message: emptied and never-filled have to be the same record.
func TestAddToAllocated(t *testing.T) {
	var total *ateapipb.WorkerResources
	total = AddToAllocated(total, assignment("a", 1000, 1<<30), +1)
	total = AddToAllocated(total, assignment("b", 500, 2<<30), +1)

	want := &ateapipb.WorkerResources{Actors: 2, CpuMillicores: 1500, MemoryBytes: 3 << 30}
	if diff := cmp.Diff(want, total, protocmp.Transform()); diff != "" {
		t.Errorf("allocated mismatch (-want +got):\n%s", diff)
	}

	total = AddToAllocated(total, assignment("b", 500, 2<<30), -1)
	want = &ateapipb.WorkerResources{Actors: 1, CpuMillicores: 1000, MemoryBytes: 1 << 30}
	if diff := cmp.Diff(want, total, protocmp.Transform()); diff != "" {
		t.Errorf("allocated after release mismatch (-want +got):\n%s", diff)
	}

	if total = AddToAllocated(total, assignment("a", 1000, 1<<30), -1); total != nil {
		t.Errorf("allocated = %v for a Worker holding nothing, want nil", total)
	}
}

// An Actor that declared no limits reserves nothing but still costs a slot.
func TestAddToAllocatedCountsAnActorWithoutResources(t *testing.T) {
	total := AddToAllocated(nil, &ateapipb.ActorAssignment{ActorUid: "a"}, +1)
	want := &ateapipb.WorkerResources{Actors: 1}
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

	got := SumAllocated([]*ateapipb.ActorAssignment{assignment("a", 1000, 1<<30), assignment("b", 500, 0)})
	want := &ateapipb.WorkerResources{Actors: 2, CpuMillicores: 1500, MemoryBytes: 1 << 30}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("SumAllocated() mismatch (-want +got):\n%s", diff)
	}
}
