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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
)

// TestSaveWorker_RejectsAStaleWrite proves the precondition saveWorker states
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
		t.Errorf("saveWorker() with a stale Worker = %v, want ErrVersionConflict", err)
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
	if _, err := p.DeleteWorker(ctx, created.GetMetadata().GetName(), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := saveWorker(ctx, tx, created); !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("saveWorker() on a deleted Worker = %v, want ErrVersionConflict", err)
	}
}

func newTestWorker(name string) *ateapi.Worker {
	return &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: name},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-" + name,
	}
}

func newTestAssignableActor(atespace, name string) *ateapi.Actor {
	return &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: name},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
	}
}

func newTestAssignment(actor *ateapi.Actor) *ateapi.ActorAssignment {
	return &ateapi.ActorAssignment{
		Actor:     &ateapi.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()},
		ActorUid:  actor.GetMetadata().GetUid(),
		Resources: resources.CPUMemory(100, 64<<20),
	}
}

// TestBindActorToWorker_SmallPoolContention runs 16 goroutines of binds and
// releases against a 4-connection pool. A transaction that takes a second pool
// connection deadlocks here, and ctx's deadline fails the test.
func TestBindActorToWorker_SmallPoolContention(t *testing.T) {
	requirePool(t)
	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}
	cfg.MaxConns = 4
	smallPool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("NewWithConfig failed: %v", err)
	}
	t.Cleanup(smallPool.Close)
	s := newTestPersistence(t, smallPool)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	createTestAtespace(t, s, "team-a")
	workerNames := []string{"w-small-0", "w-small-1"}
	for _, name := range workerNames {
		if _, err := s.CreateWorker(ctx, newTestWorker(name)); err != nil {
			t.Fatalf("CreateWorker(%s) failed: %v", name, err)
		}
	}
	const numActors = 8
	actorRefs := make([]resources.ActorRef, numActors)
	for i := range numActors {
		name := fmt.Sprintf("a-small-%d", i)
		created, err := s.CreateActor(ctx, newTestAssignableActor("team-a", name))
		if err != nil {
			t.Fatalf("CreateActor(%s) failed: %v", name, err)
		}
		actorRefs[i] = resources.ActorRefFromActor(created)
	}

	const (
		goroutines = 16
		steps      = 10
	)
	var ready, start, done sync.WaitGroup
	ready.Add(goroutines)
	start.Add(1)
	done.Add(goroutines)

	errCapExceeded := errors.New("worker capacity exceeded")
	for g := range goroutines {
		go func(g int) {
			defer done.Done()
			ready.Done()
			start.Wait()
			for step := range steps {
				aRef := actorRefs[(g+step)%numActors]
				wName := workerNames[(g+step)%len(workerNames)]
				cur, err := s.GetActor(ctx, aRef)
				if err != nil {
					t.Errorf("goroutine %d step %d: GetActor = %v", g, step, err)
					return
				}
				if step%2 == 0 {
					if cur.GetStatus().GetWorkerAssignment() != nil {
						continue
					}
					_, err := s.BindActorToWorker(
						ctx,
						wName,
						newTestAssignment(cur),
						func(w *ateapi.Worker) error {
							if w.GetStatus().GetAllocated().GetActors() >= 2 {
								return errCapExceeded
							}
							return nil
						},
						aRef,
						store.PreconditionFrom(cur),
						func(a *ateapi.Actor) error {
							a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
							a.Status.WorkerAssignment = &ateapi.WorkerAssignment{
								Worker: &ateapi.ObjectRef{Name: wName},
							}
							return nil
						},
					)
					if err != nil &&
						!errors.Is(err, errCapExceeded) &&
						!errors.Is(err, store.ErrVersionConflict) {
						t.Errorf("goroutine %d step %d: BindActorToWorker = %v", g, step, err)
						return
					}
				} else {
					_, err := s.UpdateActorAndRelease(
						ctx,
						aRef,
						store.PreconditionFrom(cur),
						func(a *ateapi.Actor) error {
							a.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
							a.Status.WorkerAssignment = nil
							return nil
						},
					)
					if err != nil && !errors.Is(err, store.ErrVersionConflict) {
						t.Errorf("goroutine %d step %d: UpdateActorAndRelease = %v", g, step, err)
						return
					}
				}
			}
		}(g)
	}

	ready.Wait()
	start.Done()
	done.Wait()
}
