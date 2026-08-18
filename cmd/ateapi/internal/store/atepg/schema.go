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
    uid    text NOT NULL,
    version bigint NOT NULL,
    proto  bytea NOT NULL
);

CREATE TABLE IF NOT EXISTS actors (
    atespace  text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_templates (
    atespace  text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_snapshots (
    atespace  text NOT NULL,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_snapshot_tags (
    atespace           text NOT NULL,
    name               text NOT NULL,
    snapshot_atespace  text NOT NULL,
    snapshot_name      text NOT NULL,
    uid                text NOT NULL,
    version            bigint NOT NULL,
    proto              bytea NOT NULL,
    PRIMARY KEY (atespace, name),
    CONSTRAINT actor_snapshot_tags_atespace_fk
        FOREIGN KEY (atespace) REFERENCES atespaces(name) ON DELETE RESTRICT,
    CONSTRAINT actor_snapshot_tags_snapshot_fk
        FOREIGN KEY (snapshot_atespace, snapshot_name)
        REFERENCES actor_snapshots(atespace, name) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS actor_snapshot_tags_snapshot_idx
    ON actor_snapshot_tags (snapshot_atespace, snapshot_name);

-- Workers are global-scoped and named by their Kubernetes pod UID, so name
-- alone is the primary key.
CREATE TABLE IF NOT EXISTS workers (
    name     text PRIMARY KEY,
    uid      text NOT NULL UNIQUE,
    version  bigint NOT NULL,
    proto    bytea NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    key         text PRIMARY KEY,
    token       text NOT NULL,
    expires_at  timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS leases_expires_at_idx ON leases (expires_at);
`

// applySchema idempotently creates atepg's tables.
func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning atepg schema transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

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
