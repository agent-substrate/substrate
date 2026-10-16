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
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
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
