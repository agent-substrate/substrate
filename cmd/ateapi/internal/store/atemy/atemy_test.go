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

package atemy

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// A listing fails as a whole on one undecodable row, so the error has to name
// the row.
func TestList_NamesAnUndecodableRow(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "team-a")
	createTestActorTemplate(t, s, "team-a", "t1")
	createTestTag(t, s, "team-a", "v1")
	if _, err := s.CreateActor(ctx, newTestActor("team-a", "a1")); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := s.CreateWorker(ctx, newTestWorker("w1")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Not a valid encoding of any message.
	bad := []byte{0xff}
	for _, table := range []string{"atespaces", "actors", "actor_templates", "tags", "workers"} {
		if _, err := s.db.ExecContext(ctx, "UPDATE "+table+" SET proto = ?", bad); err != nil {
			t.Fatalf("corrupting %s: %v", table, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO worker_assignments (actor_uid, worker_name, proto) VALUES ('uid-1', 'w1', ?)`, bad); err != nil {
		t.Fatalf("inserting assignment: %v", err)
	}

	opts := store.ListOptions{PageSize: 10}
	tests := []struct {
		name string
		list func() error
		want string
	}{
		{"atespaces", func() error { _, err := s.ListAtespaces(ctx, opts); return err }, "atespace team-a"},
		{"actors", func() error { _, err := s.ListActors(ctx, "team-a", opts); return err }, "actor team-a/a1"},
		{"actors, all atespaces", func() error { _, err := s.ListActors(ctx, "", opts); return err }, "actor team-a/a1"},
		{"actor templates", func() error { _, err := s.ListActorTemplates(ctx, "team-a", opts); return err }, "actor template team-a/t1"},
		{"actor templates, all atespaces", func() error { _, err := s.ListActorTemplates(ctx, "", opts); return err }, "actor template team-a/t1"},
		{"tags", func() error { _, err := s.ListTags(ctx, "team-a", opts); return err }, "tag team-a/v1"},
		{"tags, all atespaces", func() error { _, err := s.ListTags(ctx, "", opts); return err }, "tag team-a/v1"},
		{"workers", func() error { _, err := s.ListWorkers(ctx, opts); return err }, "worker w1"},
		{"worker assignments", func() error { _, err := s.ListWorkerAssignments(ctx, "w1", opts); return err }, "actor uid-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.list()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("list error = %v, want one naming %q", err, tc.want)
			}
		})
	}
}

// Key columns use utf8mb4_0900_bin, so names that a case-insensitive or
// PAD SPACE collation would fold together stay distinct, as they do in
// PostgreSQL, and listings and their page tokens follow byte order.
func TestKeys_CompareByteForByte(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	names := []string{"team", "Team", "TEAM", "team ", "a", "B"}
	for _, name := range names {
		createTestAtespace(t, s, name)
	}
	for _, name := range names {
		got, err := s.GetAtespace(ctx, name)
		if err != nil {
			t.Fatalf("GetAtespace(%q) failed: %v", name, err)
		}
		if got.GetMetadata().GetName() != name {
			t.Errorf("GetAtespace(%q) returned %q", name, got.GetMetadata().GetName())
		}
	}
	want := slices.Sorted(slices.Values(names))
	listed := listByOne(t, func(opts store.ListOptions) (store.ListResponse[*ateapipb.Atespace], error) {
		return s.ListAtespaces(ctx, opts)
	}, func(a *ateapipb.Atespace) string { return a.GetMetadata().GetName() })
	if !slices.Equal(listed, want) {
		t.Errorf("atespaces listed in order %q, want byte order %q", listed, want)
	}

	// The unscoped listing pages over the composite (atespace, name) key.
	actorNames := []string{"x", "X", "x "}
	var wantActors []string
	for _, atespace := range []string{"team", "Team"} {
		for _, name := range actorNames {
			if _, err := s.CreateActor(ctx, newTestActor(atespace, name)); err != nil {
				t.Fatalf("CreateActor(%q/%q) failed: %v", atespace, name, err)
			}
			wantActors = append(wantActors, atespace+"/"+name)
		}
	}
	slices.Sort(wantActors)
	listedActors := listByOne(t, func(opts store.ListOptions) (store.ListResponse[*ateapipb.Actor], error) {
		return s.ListActors(ctx, "", opts)
	}, func(a *ateapipb.Actor) string { return a.GetMetadata().GetAtespace() + "/" + a.GetMetadata().GetName() })
	if !slices.Equal(listedActors, wantActors) {
		t.Errorf("actors listed in order %q, want byte order %q", listedActors, wantActors)
	}
	if _, err := s.GetActor(ctx, resources.ActorRef{Atespace: "team", Name: "x  "}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActor with an extra trailing space = %v, want ErrNotFound", err)
	}
}

// listByOne lists one item per page through the last page and returns the
// key of each item in order.
func listByOne[T any](t *testing.T, list func(store.ListOptions) (store.ListResponse[T], error), key func(T) string) []string {
	t.Helper()
	var keys []string
	opts := store.ListOptions{PageSize: 1}
	for {
		page, err := list(opts)
		if err != nil {
			t.Fatalf("listing failed: %v", err)
		}
		for _, item := range page.Items {
			keys = append(keys, key(item))
		}
		if opts.PageToken = page.NextPageToken; opts.PageToken == "" {
			return keys
		}
	}
}

// selectForUpdate locks one atespace row for the rest of tx.
func selectForUpdate(ctx context.Context, tx *sql.Tx, name string) error {
	var one int
	return tx.QueryRowContext(ctx, `SELECT 1 FROM atespaces WHERE name = ? FOR UPDATE`, name).Scan(&one)
}

// TestInTx_RetriesADeadlockVictim runs two transactions that lock two rows in
// opposite orders. InnoDB rolls one back, and inTx must run it again so both
// commit.
func TestInTx_RetriesADeadlockVictim(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "a")
	createTestAtespace(t, s, "b")

	aLocked := make(chan struct{})
	bLocked := make(chan struct{})
	var signalA, signalB sync.Once
	var attempts atomic.Int32
	errs := make([]error, 2)
	var wg sync.WaitGroup
	wg.Go(func() {
		errs[0] = inTx(ctx, s.db, func(tx *sql.Tx) error {
			attempts.Add(1)
			if err := selectForUpdate(ctx, tx, "a"); err != nil {
				return err
			}
			signalA.Do(func() { close(aLocked) })
			<-bLocked
			return selectForUpdate(ctx, tx, "b")
		})
	})
	wg.Go(func() {
		errs[1] = inTx(ctx, s.db, func(tx *sql.Tx) error {
			attempts.Add(1)
			<-aLocked
			if err := selectForUpdate(ctx, tx, "b"); err != nil {
				return err
			}
			signalB.Do(func() { close(bLocked) })
			return selectForUpdate(ctx, tx, "a")
		})
	})
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("transaction %d = %v, want the deadlock victim retried to a commit", i, err)
		}
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("transactions ran %d attempts, want 3 (one victim retried once)", got)
	}
}

// Deadlock retries stop after txRetries or when ctx ends.
func TestInTx_StopsRetryingDeadlocks(t *testing.T) {
	s := setupMySQLPersistence(t)
	deadlock := &mysql.MySQLError{Number: errDeadlock, Message: "Deadlock found when trying to get lock"}

	attempts := 0
	err := inTx(t.Context(), s.db, func(*sql.Tx) error {
		attempts++
		return deadlock
	})
	if !errors.Is(err, store.ErrVersionConflict) || !errors.Is(err, deadlock) {
		t.Errorf("inTx after repeated deadlocks = %v, want ErrVersionConflict wrapping the deadlock", err)
	}
	if attempts != txRetries+1 {
		t.Errorf("inTx ran %d attempts, want %d", attempts, txRetries+1)
	}

	ctx, cancel := context.WithCancel(t.Context())
	attempts = 0
	err = inTx(ctx, s.db, func(*sql.Tx) error {
		attempts++
		cancel()
		return deadlock
	})
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Errorf("inTx with ctx canceled after a deadlock = %v after %d attempts, want context.Canceled after 1", err, attempts)
	}
}

// A lock wait timeout is not a deadlock, so inTx returns it as is.
func TestInTx_ReturnsLockWaitTimeout(t *testing.T) {
	lockWait := &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"}
	attempts := 0
	err := inTx(t.Context(), requireDB(t), func(*sql.Tx) error {
		attempts++
		return lockWait
	})
	if attempts != 1 || !errors.Is(err, lockWait) || errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("inTx returning a lock wait timeout = %v after %d attempts, want it as is after 1", err, attempts)
	}
}
