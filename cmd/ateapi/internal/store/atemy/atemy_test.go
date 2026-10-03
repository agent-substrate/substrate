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
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
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
	var listed []string
	var token string
	for {
		page, err := s.ListAtespaces(ctx, store.ListOptions{PageSize: 1, PageToken: token})
		if err != nil {
			t.Fatalf("ListAtespaces failed: %v", err)
		}
		for _, a := range page.Items {
			listed = append(listed, a.GetMetadata().GetName())
		}
		if token = page.NextPageToken; token == "" {
			break
		}
	}
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
	var listedActors []string
	token = ""
	for {
		page, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 1, PageToken: token})
		if err != nil {
			t.Fatalf("ListActors failed: %v", err)
		}
		for _, a := range page.Items {
			listedActors = append(listedActors, a.GetMetadata().GetAtespace()+"/"+a.GetMetadata().GetName())
		}
		if token = page.NextPageToken; token == "" {
			break
		}
	}
	if !slices.Equal(listedActors, wantActors) {
		t.Errorf("actors listed in order %q, want byte order %q", listedActors, wantActors)
	}
	if _, err := s.GetActor(ctx, resources.ActorRef{Atespace: "team", Name: "x  "}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActor with an extra trailing space = %v, want ErrNotFound", err)
	}
}

func TestIsUniqueViolation(t *testing.T) {
	deadlock := &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}
	duplicate := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	if !isUniqueViolation(fmt.Errorf("inserting: %w", duplicate)) || isUniqueViolation(deadlock) || isUniqueViolation(errors.New("connection refused")) {
		t.Error("isUniqueViolation must match exactly MySQL error 1062")
	}
}

// selectForUpdate locks one atespace row for the rest of tx.
func selectForUpdate(t *testing.T, tx *sql.Tx, name string) error {
	var one int
	return tx.QueryRowContext(t.Context(), `SELECT 1 FROM atespaces WHERE name = ? FOR UPDATE`, name).Scan(&one)
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
			if err := selectForUpdate(t, tx, "a"); err != nil {
				return err
			}
			signalA.Do(func() { close(aLocked) })
			<-bLocked
			return selectForUpdate(t, tx, "b")
		})
	})
	wg.Go(func() {
		errs[1] = inTx(ctx, s.db, func(tx *sql.Tx) error {
			attempts.Add(1)
			<-aLocked
			if err := selectForUpdate(t, tx, "b"); err != nil {
				return err
			}
			signalB.Do(func() { close(bLocked) })
			return selectForUpdate(t, tx, "a")
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

// TestInTx_ReportsARepeatedDeadlockAsVersionConflict makes the transaction
// lose a deadlock on every attempt: a heavier partner transaction, which
// InnoDB keeps over the lighter one, closes the lock cycle each time. Once
// its attempts run out, inTx reports a lost race the caller may retry.
func TestInTx_ReportsARepeatedDeadlockAsVersionConflict(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "a")
	createTestAtespace(t, s, "b")

	aLocked := make(chan struct{})
	bLocked := make(chan struct{})
	partnerErr := make(chan error, 1)
	go func() {
		partnerErr <- func() error {
			for attempt := range txRetries + 1 {
				<-aLocked
				tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
				if err != nil {
					return err
				}
				// Rows written make this transaction the heavier one, so InnoDB
				// rolls back the other.
				for i := range 20 {
					if _, err := tx.ExecContext(ctx, `INSERT INTO leases (lease_key, token, expires_at) VALUES (?, 'weight', UTC_TIMESTAMP(6))`,
						fmt.Sprintf("weight-%d-%d", attempt, i)); err != nil {
						tx.Rollback() //nolint:errcheck // reporting the insert error
						return err
					}
				}
				if err := selectForUpdate(t, tx, "b"); err != nil {
					tx.Rollback() //nolint:errcheck // reporting the lock error
					return err
				}
				bLocked <- struct{}{}
				if err := selectForUpdate(t, tx, "a"); err != nil {
					tx.Rollback() //nolint:errcheck // reporting the lock error
					return fmt.Errorf("partner lost the deadlock on attempt %d: %w", attempt, err)
				}
				if err := tx.Rollback(); err != nil {
					return err
				}
			}
			return nil
		}()
	}()

	attempts := 0
	err := inTx(ctx, s.db, func(tx *sql.Tx) error {
		attempts++
		if err := selectForUpdate(t, tx, "a"); err != nil {
			return err
		}
		aLocked <- struct{}{}
		<-bLocked
		return selectForUpdate(t, tx, "b")
	})
	if perr := <-partnerErr; perr != nil {
		t.Fatalf("partner transaction failed: %v", perr)
	}
	var myErr *mysql.MySQLError
	if !errors.Is(err, store.ErrVersionConflict) || !errors.As(err, &myErr) || myErr.Number != 1213 {
		t.Errorf("inTx after repeated deadlocks = %v, want ErrVersionConflict wrapping a deadlock", err)
	}
	if attempts != txRetries+1 {
		t.Errorf("inTx ran %d attempts, want %d", attempts, txRetries+1)
	}
}

// Deadlock retries back off exponentially, for at least the sum of the base
// delays, and stop when ctx ends.
func TestInTx_BacksOffBetweenDeadlockRetries(t *testing.T) {
	s := setupMySQLPersistence(t)
	deadlock := &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}

	attempts := 0
	start := time.Now()
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
	var minDelay time.Duration
	for b := txBackoff(); b.Steps > 0; {
		d := b.Duration
		b.Step()
		minDelay += d
	}
	if elapsed := time.Since(start); elapsed < minDelay {
		t.Errorf("inTx retried for %v, want at least %v of backoff", elapsed, minDelay)
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

func TestInTx_ReturnsLockWaitTimeout(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "a")

	cfg, err := mysql.ParseDSN(containerDSN)
	if err != nil {
		t.Fatalf("parsing container DSN: %v", err)
	}
	// The driver sets unknown parameters as session variables.
	cfg.Params = map[string]string{"innodb_lock_wait_timeout": "1"}
	impatient, err := Open(cfg.FormatDSN())
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	defer impatient.Close()

	holder, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("beginning holder transaction: %v", err)
	}
	defer holder.Rollback() //nolint:errcheck // no-op once rolled back
	if err := selectForUpdate(t, holder, "a"); err != nil {
		t.Fatalf("locking row: %v", err)
	}

	err = inTx(ctx, impatient, func(tx *sql.Tx) error { return selectForUpdate(t, tx, "a") })
	var myErr *mysql.MySQLError
	if errors.Is(err, store.ErrVersionConflict) || !errors.As(err, &myErr) || myErr.Number != 1205 {
		t.Errorf("inTx behind a held row lock = %v, want the lock wait timeout as is, as PostgreSQL reports a lock wait that runs out", err)
	}
}

// Without ClientFoundRows, MySQL reports an UPDATE that leaves a row
// unchanged as affecting no rows, and updateGuarded would report a matched
// row as a lost race.
func TestUpdateGuarded_CountsAMatchedUnchangedRow(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "team-a")

	if err := updateGuarded(ctx, s.db, "atespace team-a", `UPDATE atespaces SET proto = proto WHERE name = ?`, "team-a"); err != nil {
		t.Errorf("updateGuarded on a matched, unchanged row = %v, want nil", err)
	}
	if err := updateGuarded(ctx, s.db, "atespace gone", `UPDATE atespaces SET proto = proto WHERE name = ?`, "gone"); !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("updateGuarded on no row = %v, want ErrVersionConflict", err)
	}
}
