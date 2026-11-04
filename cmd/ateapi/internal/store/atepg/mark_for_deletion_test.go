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
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
)

// An unconditional mark for deletion must succeed when another write to the
// row lands after its read. The concurrent write starts inside mutate and
// either commits first or waits on the row lock.
func TestMarkForDeletion_ConcurrentUpdate(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		table string
		// setup creates the resource and returns functions that update it
		// and mark it for deletion. mark calls during from inside mutate.
		setup func(t *testing.T, s *Persistence) (update func() error, mark func(during func()) error)
	}{
		{
			name:  "actor",
			table: "actors",
			setup: func(t *testing.T, s *Persistence) (func() error, func(func()) error) {
				created, err := s.CreateActor(ctx, &ateapi.Actor{
					Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-a"},
					ActorTemplate: &ateapi.ObjectRef{Atespace: "default", Name: "template-a"},
					Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
				})
				if err != nil {
					t.Fatalf("CreateActor failed: %v", err)
				}
				ref := resources.ActorRefFromActor(created)
				update := func() error {
					_, err := s.UpdateActor(ctx, ref, store.PreconditionFrom(created), func(a *ateapi.Actor) error {
						a.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"tier": "paid"}}
						return nil
					})
					return err
				}
				mark := func(during func()) error {
					_, err := s.MarkActorForDeletion(ctx, ref, store.DeletePreconditions{}, func(*ateapi.Actor) error {
						during()
						return nil
					})
					return err
				}
				return update, mark
			},
		},
		{
			name:  "actor template",
			table: "actor_templates",
			setup: func(t *testing.T, s *Persistence) (func() error, func(func()) error) {
				createTestActorTemplate(t, s, "team-a", "template-a")
				ref := resources.ActorTemplateRef{Atespace: "team-a", Name: "template-a"}
				created, err := s.GetActorTemplate(ctx, ref)
				if err != nil {
					t.Fatalf("GetActorTemplate failed: %v", err)
				}
				update := func() error {
					_, err := s.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(created), nil)
					return err
				}
				mark := func(during func()) error {
					_, err := s.MarkActorTemplateForDeletion(ctx, ref, store.DeletePreconditions{}, func(*ateapi.ActorTemplate) error {
						during()
						return nil
					})
					return err
				}
				return update, mark
			},
		},
		{
			name:  "tag",
			table: "tags",
			setup: func(t *testing.T, s *Persistence) (func() error, func(func()) error) {
				created := createTestTag(t, s, "team-a", "tag-a")
				ref := resources.TagRef{Atespace: "team-a", Name: "tag-a"}
				update := func() error {
					_, err := s.UpdateTag(ctx, ref, store.PreconditionFrom(created), nil)
					return err
				}
				mark := func(during func()) error {
					_, err := s.MarkTagForDeletion(ctx, ref, store.DeletePreconditions{}, func(*ateapi.Tag) error {
						during()
						return nil
					})
					return err
				}
				return update, mark
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := setupPostgresPersistence(t)
			createTestAtespace(t, s, "team-a")
			update, mark := tc.setup(t, s)

			updateErr := make(chan error, 1)
			if err := mark(func() {
				go func() { updateErr <- update() }()
				waitForUpdateDoneOrBlocked(t, s.pool, tc.table, updateErr)
			}); err != nil {
				t.Fatalf("mark for deletion with a concurrent update = %v, want nil", err)
			}
			// The update waited on the row lock, then found the version moved.
			if err := <-updateErr; !errors.Is(err, store.ErrVersionConflict) {
				t.Errorf("concurrent update error = %v, want ErrVersionConflict", err)
			}
		})
	}
}

// waitForUpdateDoneOrBlocked returns once an UPDATE of table is waiting on a
// lock, or once the update sends its result on done. In that case it puts the
// result back for the caller.
func waitForUpdateDoneOrBlocked(t *testing.T, pool *pgxpool.Pool, table string, done chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			done <- err
			return
		default:
		}
		var waiting bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%UPDATE ' || $1 || '%')`, table).Scan(&waiting); err != nil {
			t.Fatalf("querying pg_stat_activity for lock waiters: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for the concurrent update to finish or block")
		case <-tick.C:
		}
	}
}
