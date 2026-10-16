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

package atepg

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
)

const overcommitWorker = "overcommit-worker"

func createOvercommitWorker(t *testing.T, p *Persistence) *ateapi.Worker {
	t.Helper()
	created, err := p.CreateWorker(context.Background(), &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: overcommitWorker},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
	})
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	return created
}

func overcommitAssignment(uid string, cpuMilli, memBytes int64) *ateapi.ActorAssignment {
	return &ateapi.ActorAssignment{
		Actor:     &ateapi.ObjectRef{Atespace: "demo", Name: uid},
		ActorUid:  uid,
		Resources: resources.CPUMemory(cpuMilli, memBytes),
	}
}

func mustBind(t *testing.T, p *Persistence, a *ateapi.ActorAssignment) {
	t.Helper()
	if err := p.BindActorToWorker(context.Background(), overcommitWorker, a, nil); err != nil {
		t.Fatalf("BindActorToWorker(%s) failed: %v", a.GetActorUid(), err)
	}
}

func allocatedOf(t *testing.T, p *Persistence) *ateapi.WorkerResources {
	t.Helper()
	w, err := p.GetWorker(context.Background(), overcommitWorker)
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}
	return w.GetStatus().GetAllocated()
}

func wantAllocated(t *testing.T, p *Persistence, want *ateapi.WorkerResources) {
	t.Helper()
	if diff := cmp.Diff(want, allocatedOf(t, p), protocmp.Transform()); diff != "" {
		t.Errorf("allocated mismatch (-want +got):\n%s", diff)
	}
}

// handBack takes delta off the Worker's allocated resources, standing in for
// the Worker handing back what its Actors are not predicted to use.
func handBack(t *testing.T, p *Persistence, delta *ateapi.Resources) {
	t.Helper()
	ctx := context.Background()
	tx, err := p.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed
	worker, err := getWorkerForUpdate(ctx, tx, overcommitWorker)
	if err != nil {
		t.Fatalf("getWorkerForUpdate failed: %v", err)
	}
	worker.Status.Allocated = resources.SubtractResourcesFromAllocated(worker.GetStatus().GetAllocated(), delta)
	if err := saveWorker(ctx, tx, worker); err != nil {
		t.Fatalf("saveWorker failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

// With overcommit off, a release gives back the Actor's full limits, as it
// always has.
func TestReleaseActorFromWorker_OvercommitDisabledSubtractsLimits(t *testing.T) {
	p := setupPostgresPersistence(t)
	createOvercommitWorker(t, p)
	mustBind(t, p, overcommitAssignment("uid-a", 1000, 1<<30))
	mustBind(t, p, overcommitAssignment("uid-b", 500, 1<<30))

	if _, err := p.ReleaseActorFromWorker(context.Background(), overcommitWorker, "uid-a"); err != nil {
		t.Fatalf("ReleaseActorFromWorker failed: %v", err)
	}
	wantAllocated(t, p, &ateapi.WorkerResources{CpuMillicores: 500, MemoryBytes: 1 << 30, Actors: 1})
}

// With overcommit on, a release frees only the slot: the Worker hands the
// resources back itself, and subtracting them here would count them twice.
func TestReleaseActorFromWorker_OvercommitFreesOnlyTheSlot(t *testing.T) {
	p := setupPostgresPersistence(t)
	p.SetOvercommitEnabled(true)
	createOvercommitWorker(t, p)
	mustBind(t, p, overcommitAssignment("uid-a", 1000, 1<<30))
	mustBind(t, p, overcommitAssignment("uid-b", 500, 1<<30))

	// The Worker reclaims most of what it predicts it will not use.
	handBack(t, p, resources.CPUMemory(1200, 1<<30))
	if _, err := p.ReleaseActorFromWorker(context.Background(), overcommitWorker, "uid-a"); err != nil {
		t.Fatalf("ReleaseActorFromWorker failed: %v", err)
	}
	wantAllocated(t, p, &ateapi.WorkerResources{CpuMillicores: 300, MemoryBytes: 1 << 30, Actors: 1})
}

// A rebind takes the previous booking back out before counting the new one.
// Under overcommit the Worker may already have handed back more than that
// booking, so the subtraction clamps rather than writing a negative quantity.
func TestBindActorToWorker_OvercommitRebindClampsAtZero(t *testing.T) {
	p := setupPostgresPersistence(t)
	p.SetOvercommitEnabled(true)
	createOvercommitWorker(t, p)
	mustBind(t, p, overcommitAssignment("uid-a", 1000, 1<<30))
	handBack(t, p, resources.CPUMemory(800, 1<<30))
	wantAllocated(t, p, &ateapi.WorkerResources{CpuMillicores: 200, Actors: 1})

	mustBind(t, p, overcommitAssignment("uid-a", 1000, 1<<30))
	wantAllocated(t, p, &ateapi.WorkerResources{CpuMillicores: 1000, MemoryBytes: 1 << 30, Actors: 1})
}

func TestAddWorkerAllocated(t *testing.T) {
	ctx := context.Background()
	p := setupPostgresPersistence(t)
	p.SetOvercommitEnabled(true)
	createOvercommitWorker(t, p)
	mustBind(t, p, overcommitAssignment("uid-a", 500, 1<<30))

	got, err := p.AddWorkerAllocated(ctx, overcommitWorker, resources.CPUMemory(250, 1<<30))
	if err != nil {
		t.Fatalf("AddWorkerAllocated failed: %v", err)
	}
	want := &ateapi.WorkerResources{CpuMillicores: 750, MemoryBytes: 2 << 30, Actors: 1}
	if diff := cmp.Diff(want, got.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
		t.Errorf("returned allocated mismatch (-want +got):\n%s", diff)
	}
	wantAllocated(t, p, want)
}

func TestSubtractWorkerAllocated(t *testing.T) {
	ctx := context.Background()
	p := setupPostgresPersistence(t)
	p.SetOvercommitEnabled(true)
	createOvercommitWorker(t, p)
	mustBind(t, p, overcommitAssignment("uid-a", 2000, 4<<30))

	got, err := p.SubtractWorkerAllocated(ctx, overcommitWorker, resources.CPUMemory(1500, 1<<30))
	if err != nil {
		t.Fatalf("SubtractWorkerAllocated failed: %v", err)
	}
	want := &ateapi.WorkerResources{CpuMillicores: 500, MemoryBytes: 3 << 30, Actors: 1}
	if diff := cmp.Diff(want, got.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
		t.Errorf("returned allocated mismatch (-want +got):\n%s", diff)
	}
	wantAllocated(t, p, want)

	// Handing back more than is booked empties the dimension rather than
	// taking it negative.
	if _, err := p.SubtractWorkerAllocated(ctx, overcommitWorker, resources.CPUMemory(5000, 0)); err != nil {
		t.Fatalf("SubtractWorkerAllocated(clamp) failed: %v", err)
	}
	wantAllocated(t, p, &ateapi.WorkerResources{MemoryBytes: 3 << 30, Actors: 1})
}

// A delta clamped to nothing is not a write, so it must not churn the version.
func TestSubtractWorkerAllocated_NoOpDoesNotWrite(t *testing.T) {
	ctx := context.Background()
	p := setupPostgresPersistence(t)
	created := createOvercommitWorker(t, p)

	got, err := p.SubtractWorkerAllocated(ctx, overcommitWorker, resources.CPUMemory(100, 0))
	if err != nil {
		t.Fatalf("SubtractWorkerAllocated failed: %v", err)
	}
	if got.GetMetadata().GetVersion() != created.GetMetadata().GetVersion() {
		t.Errorf("version = %d, want %d unchanged", got.GetMetadata().GetVersion(), created.GetMetadata().GetVersion())
	}
	if stored := allocatedOf(t, p); stored != nil {
		t.Errorf("allocated = %v, want nil", stored)
	}
}

func TestAdjustWorkerAllocated_Errors(t *testing.T) {
	ctx := context.Background()
	p := setupPostgresPersistence(t)
	createOvercommitWorker(t, p)

	for name, adjust := range map[string]func(context.Context, string, *ateapi.Resources) (*ateapi.Worker, error){
		"add":      p.AddWorkerAllocated,
		"subtract": p.SubtractWorkerAllocated,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := adjust(ctx, "no-such-worker", resources.CPUMemory(1, 0)); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("absent worker: err = %v, want ErrNotFound", err)
			}
		})
	}
}
