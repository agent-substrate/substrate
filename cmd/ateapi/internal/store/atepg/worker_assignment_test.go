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

package atepg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestSaveWorker_RejectsAStaleWrite proves the precondition queueSaveWorker states
// on top of the row lock its callers hold: a Worker read before someone else
// wrote it cannot overwrite that write.
func TestSaveWorker_RejectsAStaleWrite(t *testing.T) {
	requirePool(t)
	ctx := context.Background()

	p, err := Connect(ctx, testConnectConfig("public"))
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.pool.Close()
	defer p.Close()
	clearAll(t, p)

	created, err := p.CreateWorker(ctx, &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: "stale-write-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
	})
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Move the stored Worker on, so the copy above is a version behind.
	if _, err := p.UpdateWorker(ctx, created.GetMetadata().GetName(), store.PreconditionFrom(created), func(toUpdate *ateapi.Worker) error {
		toUpdate.Labels = map[string]string{"tier": "batch"}
		return nil
	}); err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := saveWorker(ctx, tx, created); !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("queueSaveWorker() with a stale Worker = %v, want ErrVersionConflict", err)
	}
}

// TestSaveWorker_RejectsAVanishedWorker keeps a deleted row from being an
// update of nothing.
func TestSaveWorker_RejectsAVanishedWorker(t *testing.T) {
	requirePool(t)
	ctx := context.Background()

	p, err := Connect(ctx, testConnectConfig("public"))
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.pool.Close()
	defer p.Close()
	clearAll(t, p)

	created, err := p.CreateWorker(ctx, &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: "vanished-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
	})
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if _, err := p.DeleteWorker(ctx, created.GetMetadata().GetName(), store.Precondition{}); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := saveWorker(ctx, tx, created); !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("queueSaveWorker() on a deleted Worker = %v, want ErrVersionConflict", err)
	}
}

// saveWorker queues the Worker's write and sends it on tx, as the batched
// paths do at the end of their transaction.
func saveWorker(ctx context.Context, tx pgx.Tx, worker *ateapi.Worker) error {
	var w writes
	if err := queueSaveWorker(&w, worker); err != nil {
		return err
	}
	return w.send(ctx, tx)
}

type roundTripCounter struct {
	queries atomic.Int64
	batches atomic.Int64
}

func (r *roundTripCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	verb := strings.ToUpper(strings.TrimSpace(data.SQL))
	if !strings.HasPrefix(verb, "BEGIN") && !strings.HasPrefix(verb, "COMMIT") && !strings.HasPrefix(verb, "ROLLBACK") {
		r.queries.Add(1)
	}
	return ctx
}

func (r *roundTripCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (r *roundTripCounter) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	r.batches.Add(1)
	return ctx
}

func (r *roundTripCounter) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (r *roundTripCounter) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

// TestBindAndRelease_StatementBatching verifies that BindActorToWorker and
// UpdateActorAndRelease each send their reads in one batch and their writes in
// one batch, issuing zero standalone SQL statements inside the transaction.
func TestBindAndRelease_StatementBatching(t *testing.T) {
	pool := migratedPool(t, "batch-roundtrips")
	tracer := &roundTripCounter{}
	tracedPool := openPool(t, "batch-roundtrips", tracer)

	p, err := NewPersistence(t.Context(), tracedPool)
	if err != nil {
		t.Fatalf("NewPersistence failed: %v", err)
	}
	t.Cleanup(p.Close)
	_ = pool
	setTestPolicyManager(t, p)

	ctx := t.Context()
	if _, err := p.CreateAtespace(ctx, &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "ns"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	worker, err := p.CreateWorker(ctx, &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: "w1"},
		WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod",
	})
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	actor, err := p.CreateActor(ctx, &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: "ns", Name: "a1"},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Stop background maintenance on p before counting traced statements on tracedPool.
	p.stopMaintenance()
	<-p.maintenanceDone

	tracer.queries.Store(0)
	tracer.batches.Store(0)
	ref := resources.ActorRef{Atespace: "ns", Name: "a1"}
	bound, err := p.BindActorToWorker(ctx, worker.GetMetadata().GetName(), &ateapi.ActorAssignment{
		Actor:     &ateapi.ObjectRef{Atespace: ref.Atespace, Name: ref.Name},
		ActorUid:  actor.GetMetadata().GetUid(),
		Resources: resources.CPUMemory(1000, 1<<20),
	}, nil, ref, store.PreconditionFrom(actor), func(a *ateapi.Actor) error {
		a.Status = &ateapi.ActorStatus{
			State:            ateapi.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapi.WorkerAssignment{Worker: &ateapi.ObjectRef{Name: "w1"}},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("BindActorToWorker failed: %v", err)
	}
	if q, b := tracer.queries.Load(), tracer.batches.Load(); q != 0 || b != 2 {
		t.Errorf("BindActorToWorker ran %d standalone queries and %d batches, want 0 standalone queries and 2 batches", q, b)
	}

	tracer.queries.Store(0)
	tracer.batches.Store(0)
	if _, err := p.UpdateActorAndRelease(ctx, ref, store.PreconditionFrom(bound), func(a *ateapi.Actor) error {
		a.Status = &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}
		return nil
	}); err != nil {
		t.Fatalf("UpdateActorAndRelease failed: %v", err)
	}
	if q, b := tracer.queries.Load(), tracer.batches.Load(); q != 0 || b != 2 {
		t.Errorf("UpdateActorAndRelease ran %d standalone queries and %d batches, want 0 standalone queries and 2 batches", q, b)
	}
}

// TestBindAndRelease_ConcurrentSameWorker stresses concurrent binds and
// releases behind a start barrier against a single Worker row and verifies
// final Worker allocation equals remaining assignments.
func TestBindAndRelease_ConcurrentSameWorker(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := t.Context()

	if _, err := s.CreateAtespace(ctx, &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "ns"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if _, err := s.CreateWorker(ctx, &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: "hot-worker"},
		WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod",
	}); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	const n = 16
	actors := make([]*ateapi.Actor, n)
	for i := range n {
		a, err := s.CreateActor(ctx, &ateapi.Actor{
			Metadata: &ateapi.ResourceMetadata{Atespace: "ns", Name: fmt.Sprintf("a-%d", i)},
			Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
		})
		if err != nil {
			t.Fatalf("CreateActor(%d) failed: %v", i, err)
		}
		actors[i] = a
	}

	// Release all 16 goroutines simultaneously onto the single hot-worker row.
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		ready.Add(1)
		done.Add(1)
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			ref := resources.ActorRef{Atespace: "ns", Name: fmt.Sprintf("a-%d", i)}
			bound, err := s.BindActorToWorker(ctx, "hot-worker", &ateapi.ActorAssignment{
				Actor:     &ateapi.ObjectRef{Atespace: ref.Atespace, Name: ref.Name},
				ActorUid:  actors[i].GetMetadata().GetUid(),
				Resources: resources.CPUMemory(100, 1<<20),
			}, nil, ref, store.PreconditionFrom(actors[i]), func(a *ateapi.Actor) error {
				a.Status = &ateapi.ActorStatus{
					State:            ateapi.ActorState_ACTOR_STATE_RUNNING,
					WorkerAssignment: &ateapi.WorkerAssignment{Worker: &ateapi.ObjectRef{Name: "hot-worker"}},
				}
				return nil
			})
			if err != nil {
				errs <- fmt.Errorf("bind %d: %w", i, err)
				return
			}
			if i%2 == 0 {
				if _, err := s.UpdateActorAndRelease(ctx, ref, store.PreconditionFrom(bound), func(a *ateapi.Actor) error {
					a.Status = &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}
					return nil
				}); err != nil {
					errs <- fmt.Errorf("release %d: %w", i, err)
				}
			}
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	gotWorker, err := s.GetWorker(ctx, "hot-worker")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}
	const wantRemaining = n / 2
	wantAlloc := &ateapi.WorkerResources{
		Actors:        wantRemaining,
		CpuMillicores: wantRemaining * 100,
		MemoryBytes:   wantRemaining * (1 << 20),
	}
	if diff := cmp.Diff(wantAlloc, gotWorker.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
		t.Errorf("worker allocated mismatch (-want +got):\n%s", diff)
	}
	listed, err := s.ListWorkerAssignments(ctx, "hot-worker", store.ListOptions{PageSize: 100})
	if err != nil {
		t.Fatalf("ListWorkerAssignments failed: %v", err)
	}
	if len(listed.Items) != wantRemaining {
		t.Errorf("remaining assignments = %d, want %d", len(listed.Items), wantRemaining)
	}
}
