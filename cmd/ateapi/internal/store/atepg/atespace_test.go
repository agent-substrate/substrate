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
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
)

// TestHotForeignKeysDropped_NoAtespaceRowLocksOnCreate verifies that
// 000002_drop_hot_foreign_keys.sql removes the three hot-path foreign keys and
// that CreateActor and CreateTag no longer acquire FOR KEY SHARE row locks on
// the parent atespaces row.
func TestHotForeignKeysDropped_NoAtespaceRowLocksOnCreate(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := t.Context()

	var remaining int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_constraint
		WHERE connamespace = current_schema()::regnamespace
		  AND conname IN ('actors_atespace_fkey', 'tags_atespace_fk', 'worker_assignments_worker_name_fkey')`).Scan(&remaining); err != nil {
		t.Fatalf("querying pg_constraint failed: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected 0 hot foreign keys remaining, found %d", remaining)
	}

	if _, err := s.CreateAtespace(ctx, &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "hot-ns"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}

	// Hold FOR UPDATE on the atespaces row in a separate transaction. With a
	// PostgreSQL foreign key (FOR KEY SHARE), any concurrent INSERT INTO actors
	// or INSERT INTO tags would block on this row lock until timeout.
	holder, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin holder failed: %v", err)
	}
	defer holder.Rollback(ctx) //nolint:errcheck // cleaned up on exit
	var locked string
	if err := holder.QueryRow(ctx, `SELECT name FROM atespaces WHERE name = 'hot-ns' FOR UPDATE`).Scan(&locked); err != nil {
		t.Fatalf("locking atespaces row FOR UPDATE failed: %v", err)
	}

	createCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := s.CreateActor(createCtx, &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: "hot-ns", Name: "a1"},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
	}); err != nil {
		t.Fatalf("CreateActor blocked or failed while atespaces row was locked FOR UPDATE: %v", err)
	}
	if _, err := s.CreateTag(createCtx, &ateapi.Tag{
		Metadata: &ateapi.ResourceMetadata{Atespace: "hot-ns", Name: "t1"},
	}); err != nil {
		t.Fatalf("CreateTag blocked or failed while atespaces row was locked FOR UPDATE: %v", err)
	}
}

// TestDeleteAtespace_ConcurrentCreateActorAndTagNoOrphans races concurrent
// CreateActor and CreateTag calls against DeleteAtespace behind a start barrier
// and verifies that no orphan actors or tags can survive a committed atespace
// deletion.
func TestDeleteAtespace_ConcurrentCreateActorAndTagNoOrphans(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := t.Context()

	for iter := range 100 {
		ns := fmt.Sprintf("race-ns-%d", iter)
		if _, err := s.CreateAtespace(ctx, &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: ns}}); err != nil {
			t.Fatalf("iter %d CreateAtespace failed: %v", iter, err)
		}

		const creators = 8
		start := make(chan struct{})
		var ready, done sync.WaitGroup
		var delErr error

		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			_, delErr = s.DeleteAtespace(ctx, ns, store.Precondition{})
		}()

		createErrs := make([]error, creators)
		for g := range creators {
			ready.Add(1)
			done.Add(1)
			go func(g int) {
				defer done.Done()
				ready.Done()
				<-start
				if g%2 == 0 {
					_, createErrs[g] = s.CreateActor(ctx, &ateapi.Actor{
						Metadata: &ateapi.ResourceMetadata{Atespace: ns, Name: fmt.Sprintf("a-%d", g)},
						Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
					})
				} else {
					_, createErrs[g] = s.CreateTag(ctx, &ateapi.Tag{
						Metadata: &ateapi.ResourceMetadata{Atespace: ns, Name: fmt.Sprintf("t-%d", g)},
					})
				}
			}(g)
		}

		ready.Wait()
		close(start)
		done.Wait()

		for g, err := range createErrs {
			if err != nil && !errors.Is(err, store.ErrFailedPrecondition) {
				t.Fatalf("iter %d creator %d returned unexpected error: %v", iter, g, err)
			}
		}

		switch {
		case delErr == nil:
			var orphanActors, orphanTags int
			if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM actors WHERE atespace = $1), (SELECT count(*) FROM tags WHERE atespace = $1)`, ns).Scan(&orphanActors, &orphanTags); err != nil {
				t.Fatalf("iter %d counting orphans failed: %v", iter, err)
			}
			if orphanActors != 0 || orphanTags != 0 {
				t.Fatalf("iter %d DeleteAtespace succeeded but left %d orphan actors and %d orphan tags", iter, orphanActors, orphanTags)
			}
		case errors.Is(delErr, store.ErrFailedPrecondition):
			if _, err := s.GetAtespace(ctx, ns); err != nil {
				t.Fatalf("iter %d DeleteAtespace returned ErrFailedPrecondition, but GetAtespace failed: %v", iter, err)
			}
		default:
			t.Fatalf("iter %d DeleteAtespace returned unexpected error: %v", iter, delErr)
		}
	}
}
