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

	"github.com/jackc/pgx/v5/pgxpool"
)

// schema is atepg's idempotent embedded schema.
//
// Resource fields are projected into columns only when PostgreSQL needs them
// for identity, relationships, queries, ordering, or atomic concurrency
// checks. All other resource state remains authoritative in the opaque proto.
const schema = `
CREATE TABLE IF NOT EXISTS atespaces (
    name   text PRIMARY KEY,
    proto  bytea NOT NULL
);

CREATE TABLE IF NOT EXISTS actors (
    atespace  text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name      text NOT NULL,
    uid       text NOT NULL UNIQUE,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_templates (
    atespace  text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name      text NOT NULL,
    uid       text NOT NULL UNIQUE,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_snapshots (
    atespace  text NOT NULL,
    name      text NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_snapshot_tags (
    atespace           text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name               text NOT NULL,
    snapshot_atespace  text NOT NULL,
    snapshot_name      text NOT NULL,
    version            bigint NOT NULL,
    proto              bytea NOT NULL,
    PRIMARY KEY (atespace, name),
    FOREIGN KEY (snapshot_atespace, snapshot_name)
        REFERENCES actor_snapshots(atespace, name) ON DELETE RESTRICT
);

-- Workers are global-scoped and named by their Kubernetes pod UID, so name
-- alone is the primary key.
CREATE TABLE IF NOT EXISTS workers (
    name     text PRIMARY KEY,
    uid      text NOT NULL UNIQUE,
    version  bigint NOT NULL,
    proto    bytea NOT NULL
);

-- Transactional change feed backing WatchWorkers. Events are appended in
-- the same transaction as the worker write and delivered by polling past
-- an xid cursor;
-- payload is one event-type byte followed by the binary Worker proto.
--
-- xid is the whole ordering: writeAndAppendChange appends exactly ONE row
-- per transaction (the only insert site), so every feed row has a distinct
-- xid and a poll batch can never split a same-xid group. That invariant is
-- load-bearing for the watch cursor and pinned by a test.
--
-- Partitioned by created_at range (width: changeFeedPartitionInterval,
-- kept <= retention so rows outlive it by at most one interval) so
-- retention is a partition DROP — a metadata operation with no row
-- deletes, dead tuples, or vacuum debt — instead of bulk DELETEs whose I/O
-- competes with foreground traffic. The maintenance loop
-- (changeFeedMaintenance) creates upcoming partitions and drops expired
-- ones; the DEFAULT partition only receives writes if partition creation
-- ever stalls, and is truncated wholesale once maintenance notices
-- (watchers that lose events to that resync via the trim mark).
--
-- Partitions are UNLOGGED: the feed is ephemeral by design (cursors are
-- not durable, subscriptions start "from now", and every consumer rebuilds
-- from the workers table on resync), so paying WAL on every event — inside
-- every worker-write transaction — buys nothing. Crash/failover truncates
-- unlogged tables; see WatchWorkers for how watchers recover.
-- worker_changes_trim stays logged — the trim mark must survive a crash.
CREATE TABLE IF NOT EXISTS worker_changes (
    xid         xid8 NOT NULL DEFAULT pg_current_xact_id(),
    -- clock_timestamp(), not now(): now() is transaction-START time, so a
    -- slow transaction would route its event by a stale timestamp — worst
    -- case into an already-dropped partition (the DEFAULT partition would
    -- catch it). The feed insert is the last statement before commit, so
    -- statement time routes into the partition closest to commit time.
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload     bytea NOT NULL
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS worker_changes_xid ON worker_changes (xid);

CREATE UNLOGGED TABLE IF NOT EXISTS worker_changes_default PARTITION OF worker_changes DEFAULT WITH (autovacuum_enabled = off);

-- Single-row high-water mark of retention: the greatest xid ever discarded
-- from worker_changes (dropped with an expired partition, or truncated
-- with the DEFAULT partition). Watchers compare it against their cursor to
-- detect exactly that unconsumed rows were discarded out from under them.
CREATE TABLE IF NOT EXISTS worker_changes_trim (
    id   boolean PRIMARY KEY DEFAULT true CHECK (id),
    xid  xid8 NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    key         text PRIMARY KEY,
    token       text NOT NULL,
    expires_at  timestamptz NOT NULL
);
`

// applySchema idempotently creates atepg's tables.
func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning atepg schema transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	// The schema needs PostgreSQL 13+ (xid8, pg_current_xact_id,
	// pg_current_snapshot); fail with a clear message rather than an
	// opaque DDL or function error.
	var version int
	if err := tx.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		return fmt.Errorf("reading PostgreSQL version: %w", err)
	}
	if version < 130000 {
		return fmt.Errorf("atepg requires PostgreSQL 13 or newer (xid8 and pg_current_snapshot); server_version_num is %d", version)
	}

	// Multiple ateapi replicas can start against an empty database together.
	// PostgreSQL's IF NOT EXISTS does not eliminate every concurrent-DDL race,
	// so serialize schema application with a transaction-scoped advisory lock.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('agent-substrate-atepg-schema'))`); err != nil {
		return fmt.Errorf("locking atepg schema: %w", err)
	}
	if _, err := tx.Exec(ctx, schema); err != nil {
		return fmt.Errorf("applying atepg schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing atepg schema: %w", err)
	}
	return nil
}
