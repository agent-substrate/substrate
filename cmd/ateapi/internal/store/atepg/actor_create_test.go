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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestCreateActorWithTemplate_HoldsSharedLockThroughInsert(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	createTestAtespace(t, s, "team-a")
	createTestActorTemplate(t, s, "team-a", "tmpl")
	ref := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl"}
	tmpl, err := s.GetActorTemplate(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx) //nolint:errcheck // Released explicitly below.
	// Pause both creates at INSERT, after they have acquired the template lock.
	if _, err := blocker.Exec(ctx, `LOCK TABLE actors IN SHARE MODE`); err != nil {
		t.Fatal(err)
	}
	created := make(chan error, 2)
	for i := range 2 {
		go func() {
			_, err := s.CreateActorWithTemplate(ctx, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: fmt.Sprintf("actor-%d", i)},
				ActorTemplate: ref.ToObjectRef(),
			}, tmpl.GetMetadata().GetUid())
			created <- err
		}()
	}
	waitForActorQueryLock(t, ctx, s, "%INSERT INTO actors%", 2)

	// Ordinary status updates remain compatible with the shared locks.
	if _, err := s.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(tmpl), func(db *ateapipb.ActorTemplate) error {
		db.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{ErrorMessage: "updated"}}
		return nil
	}); err != nil {
		t.Fatalf("template status update while creates hold locks: %v", err)
	}
	deleted := make(chan error, 1)
	go func() {
		_, err := s.DeleteActorTemplate(ctx, ref, store.DeletePreconditions{})
		deleted <- err
	}()
	waitForActorQueryLock(t, ctx, s, "%DELETE FROM actor_templates%", 1)
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-created; err != nil {
			t.Fatalf("CreateActorWithTemplate: %v", err)
		}
	}
	if err := <-deleted; err != nil {
		t.Fatalf("DeleteActorTemplate after creates commit: %v", err)
	}
	for i := range 2 {
		if _, err := s.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: fmt.Sprintf("actor-%d", i)}); err != nil {
			t.Fatalf("committed actor missing: %v", err)
		}
	}
}

func TestCreateActorWithTemplate_WaitsForDelete(t *testing.T) {
	for _, commit := range []bool{true, false} {
		t.Run(fmt.Sprintf("commit=%t", commit), func(t *testing.T) {
			s := setupPostgresPersistence(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			createTestAtespace(t, s, "team-a")
			createTestActorTemplate(t, s, "team-a", "tmpl")
			ref := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl"}
			tmpl, err := s.GetActorTemplate(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			deleting, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer deleting.Rollback(ctx) //nolint:errcheck // Released explicitly below.
			if _, err := deleting.Exec(ctx, `DELETE FROM actor_templates WHERE atespace = $1 AND name = $2`, ref.Atespace, ref.Name); err != nil {
				t.Fatal(err)
			}
			actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor"}, ActorTemplate: ref.ToObjectRef()}
			created := make(chan error, 1)
			go func() {
				_, err := s.CreateActorWithTemplate(ctx, actor, tmpl.GetMetadata().GetUid())
				created <- err
			}()
			waitForActorQueryLock(t, ctx, s, "%SELECT uid FROM actor_templates%", 1)
			finish := deleting.Rollback
			var want error
			if commit {
				finish, want = deleting.Commit, store.ErrNotFound
			}
			if err := finish(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-created; !errors.Is(err, want) {
				t.Fatalf("CreateActorWithTemplate = %v, want %v", err, want)
			}
			_, err = s.GetActor(ctx, resources.ActorRefFromActor(actor))
			if !errors.Is(err, want) {
				t.Fatalf("GetActor = %v, want %v", err, want)
			}
		})
	}
}

func TestCreateActorWithTemplate_CancellationReleasesLock(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	createTestAtespace(t, s, "team-a")
	createTestActorTemplate(t, s, "team-a", "tmpl")
	ref := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl"}
	tmpl, err := s.GetActorTemplate(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx) //nolint:errcheck // Cleanup releases the table lock.
	if _, err := blocker.Exec(ctx, `LOCK TABLE actors IN SHARE MODE`); err != nil {
		t.Fatal(err)
	}
	createCtx, cancelCreate := context.WithCancel(ctx)
	defer cancelCreate()
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor"}, ActorTemplate: ref.ToObjectRef()}
	created := make(chan error, 1)
	go func() {
		_, err := s.CreateActorWithTemplate(createCtx, actor, tmpl.GetMetadata().GetUid())
		created <- err
	}()
	waitForActorQueryLock(t, ctx, s, "%INSERT INTO actors%", 1)
	cancelCreate()
	if err := <-created; !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateActorWithTemplate = %v, want context.Canceled", err)
	}
	if _, err := s.DeleteActorTemplate(ctx, ref, store.DeletePreconditions{}); err != nil {
		t.Fatalf("template lock survived cancellation: %v", err)
	}
	if _, err := s.GetActor(ctx, resources.ActorRefFromActor(actor)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancelled create persisted an actor: %v", err)
	}
}

func waitForActorQueryLock(t *testing.T, ctx context.Context, s *Persistence, pattern string, count int) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE $1`, pattern).Scan(&blocked); err != nil {
			t.Fatalf("checking blocked queries: %v", err)
		}
		if blocked == count {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %d blocked queries matching %q: %v", count, pattern, ctx.Err())
		case <-ticker.C:
		}
	}
}
