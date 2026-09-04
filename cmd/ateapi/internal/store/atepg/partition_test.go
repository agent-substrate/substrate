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
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storecontract"
)

// TestActorsTablePartitionable exists to keep it possible to partition the
// actors table by atespace or by name later, with the other atespace-scoped
// tables partitioned alongside it by atespace. It runs the store contract
// suite against a copy of the schema partitioned that way, and fails on any
// schema change or query that would not work in that layout.
func TestActorsTablePartitionable(t *testing.T) {
	// exemptions are statements allowed to read every partition even though
	// their result lives in one. Do not add one without discussion and
	// agreement in the community.
	var exemptions []string

	// spansPartitions are the statements whose result covers every partition
	// by definition, under each partition key.
	spansPartitions := map[string][]string{
		// A global list walks every atespace.
		"atespace": {globalList("actors"), globalList("actor_templates"), globalList("tags")},
		// A global list walks every name, and a list within one atespace
		// spans every name hash.
		"name": {globalList("actors"), scopedActorList},
	}

	t.Run("by atespace", func(t *testing.T) {
		// A nil table list partitions every table that has an atespace column.
		runContractSuitePartitioned(t, "atespace", nil, slices.Concat(spansPartitions["atespace"], exemptions))
	})
	t.Run("by name", func(t *testing.T) {
		runContractSuitePartitioned(t, "name", []string{"actors"}, slices.Concat(spansPartitions["name"], exemptions))
	})

	t.Run("rejects a unique index that omits the key", func(t *testing.T) {
		pool := migratedPool(t, "partitioned-unique")
		// This also rules out foreign keys onto uid, which need this index.
		if _, err := pool.Exec(t.Context(), `CREATE UNIQUE INDEX actors_uid_key ON actors (uid)`); err != nil {
			t.Fatal(err)
		}
		err := partitionTable(t.Context(), pool, "actors", "atespace")
		if err == nil || !strings.Contains(err.Error(), "must include all partitioning columns") {
			t.Fatalf("partitionTable error = %v, want unique index rejection", err)
		}
		t.Log(err)
	})
	t.Run("rejects a query that omits the key", func(t *testing.T) {
		var got string
		pool, tables := partitionedPool(t, "partitioned-fanout", "atespace", []string{"actors"})
		check := newFanOutCheck("atespace", tables, nil, pool, func(format string, args ...any) { got = fmt.Sprintf(format, args...) })
		var n int
		if err := openPool(t, "partitioned-fanout", check).QueryRow(t.Context(), `SELECT count(*) FROM actors WHERE uid = $1`, "u1").Scan(&n); err != nil {
			t.Fatal(err)
		}
		for i := range partitions {
			if p := fmt.Sprintf("actors_p%d", i); !strings.Contains(got, p) {
				t.Fatalf("fan-out check reported %q, want the uid lookup reading %s", got, p)
			}
		}
		t.Log(got)
	})
	t.Run("rejects a multi-valued predicate", func(t *testing.T) {
		var got string
		pool, tables := partitionedPool(t, "partitioned-multi", "atespace", []string{"actors"})
		check := newFanOutCheck("atespace", tables, nil, pool, func(format string, args ...any) { got = fmt.Sprintf(format, args...) })
		var n int
		// The suite's most used pair of atespaces, which partitions keeps apart.
		if err := openPool(t, "partitioned-multi", check).QueryRow(t.Context(), `SELECT count(*) FROM actors WHERE atespace = ANY($1)`, []string{"team-a", "team-b"}).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(got, "reads partitions") {
			t.Fatalf("fan-out check reported %q, want the two-atespace lookup reading two partitions", got)
		}
		t.Log(got)
	})
}

// globalList is the statement that lists table across every atespace.
func globalList(table string) string {
	return normalizeSQL(`
		SELECT atespace, name, proto FROM ` + table + `
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`)
}

// scopedActorList is the statement that lists one atespace's actors.
var scopedActorList = normalizeSQL(`
	SELECT name, proto FROM actors
	WHERE atespace = $1 AND ($2::text IS NULL OR name > $2)
	ORDER BY name
	LIMIT $3`)

// runContractSuitePartitioned runs the store contract suite with tables
// partitioned on key and fails on any statement, other than the allowed
// ones, whose plan reads more than one partition of a table.
func runContractSuitePartitioned(t *testing.T, key string, tables []string, allowed []string) {
	schema := "partitioned-by-" + key
	pool, tables := partitionedPool(t, schema, key, tables)
	check := newFanOutCheck(key, tables, allowed, pool, t.Errorf)
	traced := openPool(t, schema, check)
	storecontract.RunContractTests(t, func(t *testing.T) store.Interface {
		p, err := NewPersistence(t.Context(), traced)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		clearAll(t, p)
		return p
	})
	// A table the suite never reads or writes goes unchecked, so a missing
	// contract test fails here rather than passing silently.
	for _, table := range check.unchecked() {
		t.Errorf("no statement on %s was checked; the store contract suite must exercise it", table)
	}
}

// partitions is the number of hash partitions per table. A multi-valued
// predicate, such as atespace = ANY($1), is only reported when its values
// land in different partitions. Nine is the smallest modulus that separates
// the atespace names the store contract suite creates. A power of two takes
// the low bits of the hash, and any up to 32 puts team-a and team-b in one
// partition. Every contract subtest truncates every partition, so more of
// them cost run time.
const partitions = 9

// partitionTable rebuilds the empty, freshly migrated table as a
// hash-partitioned table on key, keeping its indexes, constraints and
// foreign keys. PostgreSQL rejects any of them that omits key.
func partitionTable(ctx context.Context, pool *pgxpool.Pool, table, key string) error {
	rows, err := pool.Query(ctx, `
		SELECT format('ALTER TABLE %s ADD CONSTRAINT %I %s', conrelid::regclass, conname, pg_get_constraintdef(oid))
		FROM pg_constraint
		WHERE contype = 'f' AND conparentid = 0 AND $1::regclass IN (conrelid, confrelid)`, table)
	if err != nil {
		return err
	}
	foreignKeys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	ddl := []string{fmt.Sprintf(`CREATE TABLE %[1]s_partitioned (LIKE %[1]s INCLUDING ALL) PARTITION BY HASH (%[2]s)`, table, key)}
	for i := range partitions {
		ddl = append(ddl, fmt.Sprintf(`CREATE TABLE %[1]s_p%[2]d PARTITION OF %[1]s_partitioned FOR VALUES WITH (MODULUS %[3]d, REMAINDER %[2]d)`, table, i, partitions))
	}
	ddl = append(ddl, fmt.Sprintf(`DROP TABLE %[1]s CASCADE`, table), fmt.Sprintf(`ALTER TABLE %[1]s_partitioned RENAME TO %[1]s`, table))
	if _, err := pool.Exec(ctx, strings.Join(ddl, ";\n")); err != nil {
		return fmt.Errorf("%s cannot be partitioned by %s: %w", table, key, err)
	}
	for _, fk := range foreignKeys {
		if _, err := pool.Exec(ctx, fk); err != nil {
			return fmt.Errorf("foreign key cannot reference %s partitioned by %s: %s: %w", table, key, fk, err)
		}
	}
	return nil
}

// fanOutCheck is a pgx tracer that explains every statement on the
// partitioned tables and fails when its plan reads more than one partition
// of any of them.
type fanOutCheck struct {
	key       string
	tables    []string
	partition *regexp.Regexp // matches a partition name, capturing its table
	allowed   []string
	explain   *pgxpool.Pool
	fail      func(format string, args ...any)
	mu        sync.Mutex
	checked   map[string]bool // tables that a checked plan has read
	reported  map[string]bool // failures already reported, by table and statement
}

func newFanOutCheck(key string, tables []string, allowed []string, explain *pgxpool.Pool, fail func(string, ...any)) *fanOutCheck {
	quoted := make([]string, len(tables))
	for i, table := range tables {
		quoted[i] = regexp.QuoteMeta(table)
	}
	return &fanOutCheck{
		key:       key,
		tables:    tables,
		partition: regexp.MustCompile(`\b(` + strings.Join(quoted, "|") + `)_p\d+\b`),
		allowed:   allowed,
		explain:   explain,
		fail:      fail,
		checked:   map[string]bool{},
		reported:  map[string]bool{},
	}
}

func (c *fanOutCheck) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	c.check(ctx, data.SQL, data.Args)
	return ctx
}

func (c *fanOutCheck) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// TraceBatchStart checks the statements of a batch, which pgx does not pass
// to TraceQueryStart.
func (c *fanOutCheck) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	for _, q := range data.Batch.QueuedQueries {
		c.check(ctx, q.SQL, q.Arguments)
	}
	return ctx
}

func (c *fanOutCheck) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (c *fanOutCheck) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

// check explains one execution of a statement. The plan depends on the
// arguments, for example a NULL page token turns a one-partition scan into a
// scan of every partition, so every execution is explained rather than the
// first one per statement.
func (c *fanOutCheck) check(ctx context.Context, rawSQL string, args []any) {
	sql := normalizeSQL(rawSQL)
	if !slices.ContainsFunc(c.tables, func(t string) bool { return strings.Contains(sql, t) }) || slices.Contains(c.allowed, sql) {
		return
	}
	// EXPLAIN accepts every statement that reads or writes rows, WITH and
	// MERGE included. TRUNCATE is the one utility statement run on these
	// tables. Any other statement EXPLAIN rejects fails the test below rather
	// than passing unchecked.
	if verb, _, _ := strings.Cut(strings.ToUpper(sql), " "); verb == "TRUNCATE" {
		return
	}
	// EXPLAIN without ANALYZE plans but never executes, so writes are safe.
	var plan []string
	rows, err := c.explain.Query(ctx, "EXPLAIN "+rawSQL, args...)
	if err == nil {
		plan, err = pgx.CollectRows(rows, pgx.RowTo[string])
	}
	if err != nil {
		c.report(sql, "explaining %s: %v", sql, err)
		return
	}
	byTable := map[string][]string{}
	for _, m := range c.partition.FindAllStringSubmatch(strings.Join(plan, "\n"), -1) {
		byTable[m[1]] = append(byTable[m[1]], m[0])
	}
	for table, partitions := range byTable {
		c.mu.Lock()
		c.checked[table] = true
		c.mu.Unlock()
		if partitions = slices.Compact(slices.Sorted(slices.Values(partitions))); len(partitions) > 1 {
			c.report(table+" "+sql, "statement on %s reads partitions %v instead of one; filter on %s or list it in TestActorsTablePartitionable:\n\t%s\n\targs=%v", table, partitions, c.key, sql, args)
		}
	}
}

// report fails the test once per key, so a statement the suite runs many
// times is reported once.
func (c *fanOutCheck) report(key, format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reported[key] {
		return
	}
	c.reported[key] = true
	c.fail(format, args...)
}

// unchecked lists the partitioned tables no checked plan has read.
func (c *fanOutCheck) unchecked() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(c.tables), func(table string) bool { return c.checked[table] })
}

func normalizeSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

// migratedPool opens a pool on a fresh schema with the migrations applied.
func migratedPool(t *testing.T, schema string) *pgxpool.Pool {
	t.Helper()
	admin := requirePool(t)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(t.Context(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE; CREATE SCHEMA `+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`) })
	pool := openPool(t, schema, nil)
	p, err := NewPersistence(t.Context(), pool)
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	return pool
}

// partitionedPool opens a pool on a fresh schema with tables partitioned on
// key, and returns the tables it partitioned. A nil tables partitions every
// table that has a column named key.
func partitionedPool(t *testing.T, schema, key string, tables []string) (*pgxpool.Pool, []string) {
	t.Helper()
	pool := migratedPool(t, schema)
	if tables == nil {
		tables = tablesWithColumn(t, pool, key)
	}
	for _, table := range tables {
		if err := partitionTable(t.Context(), pool, table, key); err != nil {
			t.Fatal(err)
		}
	}
	return pool, tables
}

// tablesWithColumn lists the tables in the pool's schema that have column.
func tablesWithColumn(t *testing.T, pool *pgxpool.Pool, column string) []string {
	t.Helper()
	rows, err := pool.Query(t.Context(), `
		SELECT table_name FROM information_schema.columns
		WHERE table_schema = current_schema() AND column_name = $1
		ORDER BY table_name`, column)
	if err != nil {
		t.Fatal(err)
	}
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) == 0 {
		t.Fatalf("no table has a %s column", column)
	}
	return tables
}

// openPool opens a pool on schema, tracing every statement with tracer.
func openPool(t *testing.T, schema string, tracer pgx.QueryTracer) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
