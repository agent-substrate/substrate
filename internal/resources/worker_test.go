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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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

// mustAdd is AddToAllocated where the fixtures are known to parse.
func mustAdd(t *testing.T, total *ateapipb.WorkerResources, a *ateapipb.ActorAssignment, sign int64) *ateapipb.WorkerResources {
	t.Helper()
	got, err := AddToAllocated(total, a, sign)
	if err != nil {
		t.Fatalf("AddToAllocated(%v, %d): %v", a, sign, err)
	}
	return got
}

// The total moves by one assignment's worth in each direction, and a Worker
// back to holding nothing carries no allocation at all rather than a zeroed
// message: emptied and never-filled have to be the same record.
func TestAddToAllocated(t *testing.T) {
	var total *ateapipb.WorkerResources
	total = mustAdd(t, total, assignment("a", 1000, 1<<30), +1)
	total = mustAdd(t, total, assignment("b", 500, 2<<30), +1)

	want := &ateapipb.WorkerResources{Actors: 2, Resources: CPUMemory(1500, 3<<30)}
	if diff := cmp.Diff(want, total, protocmp.Transform()); diff != "" {
		t.Errorf("allocated mismatch (-want +got):\n%s", diff)
	}

	total = mustAdd(t, total, assignment("b", 500, 2<<30), -1)
	want = &ateapipb.WorkerResources{Actors: 1, Resources: CPUMemory(1000, 1<<30)}
	if diff := cmp.Diff(want, total, protocmp.Transform()); diff != "" {
		t.Errorf("allocated after release mismatch (-want +got):\n%s", diff)
	}

	if total = mustAdd(t, total, assignment("a", 1000, 1<<30), -1); total != nil {
		t.Errorf("allocated = %v for a Worker holding nothing, want nil", total)
	}
}

// An Actor that declared no limits reserves nothing but still costs a slot.
func TestAddToAllocatedCountsAnActorWithoutResources(t *testing.T) {
	total := mustAdd(t, nil, &ateapipb.ActorAssignment{ActorUid: "a"}, +1)
	want := &ateapipb.WorkerResources{Actors: 1}
	if diff := cmp.Diff(want, total, protocmp.Transform()); diff != "" {
		t.Errorf("allocated mismatch (-want +got):\n%s", diff)
	}
}

// A quantity that will not parse is refused rather than counted as nothing:
// silently booking zero would overcommit the Worker.
func TestAddToAllocatedRejectsAnUnparseableQuantity(t *testing.T) {
	bad := &ateapipb.ActorAssignment{
		ActorUid:  "a",
		Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "two"}}},
	}
	if _, err := AddToAllocated(nil, bad, +1); err == nil {
		t.Fatal("AddToAllocated() = nil error for an unparseable quantity, want one")
	}
}

// SumAllocated rebuilds what AddToAllocated adjusts; the contract tests hold the
// two to each other, so they have to agree on the empty case as well.
func TestSumAllocated(t *testing.T) {
	got, err := SumAllocated(nil)
	if err != nil {
		t.Fatalf("SumAllocated(nil): %v", err)
	}
	if got != nil {
		t.Errorf("SumAllocated(nil) = %v, want nil", got)
	}

	got, err = SumAllocated([]*ateapipb.ActorAssignment{assignment("a", 1000, 1<<30), assignment("b", 500, 0)})
	if err != nil {
		t.Fatalf("SumAllocated(): %v", err)
	}
	want := &ateapipb.WorkerResources{Actors: 2, Resources: CPUMemory(1500, 1<<30)}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("SumAllocated() mismatch (-want +got):\n%s", diff)
	}
}
