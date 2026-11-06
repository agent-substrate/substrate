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
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestAtespaceTablesShardable exists to keep it possible to move each
// atespace's tables to a database of their own later. It fails on a foreign
// key between those tables and the global ones, and on a new table that is
// not classified as one or the other.
func TestAtespaceTablesShardable(t *testing.T) {
	// exemptions are foreign keys allowed to cross between an atespace's
	// tables and the global ones. Do not add one without discussion and
	// agreement in the community.
	var exemptions []string

	for _, v := range shardingViolations(t, migratedPool(t, "shardable"), exemptions) {
		t.Error(v)
	}

	t.Run("rejects a foreign key to a global table", func(t *testing.T) {
		pool := migratedPool(t, "shardable-fk")
		if _, err := pool.Exec(t.Context(), `ALTER TABLE actors ADD COLUMN worker text REFERENCES workers (name)`); err != nil {
			t.Fatal(err)
		}
		violations := shardingViolations(t, pool, nil)
		if len(violations) != 1 || !strings.Contains(violations[0], "actors_worker_fkey") {
			t.Fatalf("violations = %q, want exactly the actors to workers foreign key", violations)
		}
		t.Log(violations[0])
	})
	t.Run("classifies a table by its atespace column", func(t *testing.T) {
		pool := migratedPool(t, "shardable-column")
		if _, err := pool.Exec(t.Context(), `CREATE TABLE notes (atespace text, worker text REFERENCES workers (name))`); err != nil {
			t.Fatal(err)
		}
		violations := shardingViolations(t, pool, nil)
		if len(violations) != 1 || !strings.Contains(violations[0], "notes_worker_fkey") {
			t.Fatalf("violations = %q, want exactly the notes to workers foreign key", violations)
		}
		t.Log(violations[0])
	})
	t.Run("rejects an unclassified table", func(t *testing.T) {
		pool := migratedPool(t, "shardable-table")
		if _, err := pool.Exec(t.Context(), `CREATE TABLE notes (id int PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		violations := shardingViolations(t, pool, nil)
		if len(violations) != 1 || !strings.Contains(violations[0], "table notes is neither") {
			t.Fatalf("violations = %q, want exactly the unclassified notes table", violations)
		}
		t.Log(violations[0])
	})
}

// globalTables hold state shared by every atespace. Every other table is an
// atespace's: atespaces itself, and the tables with an atespace column, which
// TestActorsTablePartitionable partitions by it.
var globalTables = []string{"workers", "worker_assignments", "worker_outbox", "worker_outbox_trim", "leases", migrationTableName}

// shardingViolations reports every table that is neither an atespace table
// nor a global one, and every foreign key between the two sets other than
// the exempt ones.
func shardingViolations(t *testing.T, pool *pgxpool.Pool, exempt []string) []string {
	t.Helper()
	atespaceTables := append([]string{"atespaces"}, tablesWithColumn(t, pool, "atespace")...)
	var violations []string
	for _, table := range schemaTables(t, pool) {
		if !slices.Contains(atespaceTables, table) && !slices.Contains(globalTables, table) {
			violations = append(violations, fmt.Sprintf("table %s is neither an atespace table nor a global table; give it an atespace column or add it to globalTables in TestAtespaceTablesShardable", table))
		}
	}
	fks, err := foreignKeys(t.Context(), pool)
	if err != nil {
		t.Fatal(err)
	}
	for _, fk := range fks {
		if slices.Contains(exempt, fk.Name) {
			continue
		}
		if slices.Contains(atespaceTables, fk.From) != slices.Contains(atespaceTables, fk.To) {
			violations = append(violations, fmt.Sprintf("foreign key %s from %s to %s crosses between an atespace's tables and the global ones, so the atespace could not move to its own database; remove it or add it to the exemptions in TestAtespaceTablesShardable", fk.Name, fk.From, fk.To))
		}
	}
	return violations
}
