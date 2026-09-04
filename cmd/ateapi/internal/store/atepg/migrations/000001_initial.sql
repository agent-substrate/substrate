-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- +goose Up

CREATE TABLE atespaces (
    name     text PRIMARY KEY,
    uid      text NOT NULL,
    version  bigint NOT NULL,
    proto    bytea NOT NULL
);

CREATE TABLE actors (
    atespace  text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE actor_egress_policies (
    atespace    text NOT NULL,
    actor_name  text NOT NULL,
    uid         text NOT NULL,
    version     bigint NOT NULL,
    proto       bytea NOT NULL,
    PRIMARY KEY (atespace, actor_name),
    FOREIGN KEY (atespace, actor_name)
        REFERENCES actors(atespace, name) ON DELETE CASCADE
);

CREATE TABLE actor_templates (
    atespace  text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE actor_snapshots (
    atespace  text NOT NULL,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE actor_snapshot_tags (
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

CREATE INDEX actor_snapshot_tags_snapshot_idx
    ON actor_snapshot_tags (snapshot_atespace, snapshot_name);

-- Workers are global-scoped and named by their Kubernetes pod UID, so name
-- alone is the primary key.
CREATE TABLE workers (
    name     text PRIMARY KEY,
    uid      text NOT NULL UNIQUE,
    version  bigint NOT NULL,
    proto    bytea NOT NULL
);

-- One row per Actor, keyed by Actor UID because an Actor has at most one
-- Worker. Kept separate from workers so Worker reads, writes, and watch events
-- do not grow with occupancy. The primary key finds an Actor's Worker;
-- worker_name lists a Worker's Actors.
CREATE TABLE worker_assignments (
    actor_uid    text PRIMARY KEY,
    worker_name  text NOT NULL
        REFERENCES workers(name) ON DELETE CASCADE,
    proto        bytea NOT NULL
);

CREATE INDEX worker_assignments_worker_idx
    ON worker_assignments (worker_name);

-- Transactional outbox backing WatchWorkers.
--
-- 1. Ordering (xid): writeAndAppendEvent guarantees exactly one row per tx,
--    ensuring distinct xids so polling batches never split a transaction.
-- 2. Retention (created_at partitions): outboxMaintenance drops expired
--    partitions to avoid VACUUM I/O debt. A DEFAULT partition catches overflow.
-- 3. Durability (UNLOGGED): Skips WAL overhead. Crash recoveries trigger
--    watchers to rebuild from the primary workers table. worker_outbox_trim
--    remains LOGGED to preserve the high-water mark across restarts.
CREATE TABLE worker_outbox (
    xid         xid8 NOT NULL DEFAULT pg_current_xact_id(),
    -- MUST use clock_timestamp() instead of now(). now() freezes at tx start,
    -- causing slow transactions to route into expired partitions.
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload     bytea NOT NULL
) PARTITION BY RANGE (created_at);

CREATE INDEX worker_outbox_xid ON worker_outbox (xid);

CREATE UNLOGGED TABLE worker_outbox_default PARTITION OF worker_outbox DEFAULT
    WITH (autovacuum_enabled = off);

-- Single-row high-water mark of retention: the greatest xid ever discarded
-- from worker_outbox (dropped with an expired partition, or truncated
-- with the DEFAULT partition). Watchers compare it against their cursor to
-- detect exactly that unconsumed rows were discarded out from under them.
CREATE TABLE worker_outbox_trim (
    id   boolean PRIMARY KEY DEFAULT true CHECK (id),
    xid  xid8 NOT NULL
);

CREATE TABLE leases (
    key         text PRIMARY KEY,
    token       text NOT NULL,
    expires_at  timestamptz NOT NULL
);

CREATE INDEX leases_expires_at_idx ON leases (expires_at);
