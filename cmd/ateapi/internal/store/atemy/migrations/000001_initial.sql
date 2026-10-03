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

-- No table declares a foreign key. PlanetScale disables them by default and
-- InnoDB rejects them on partitioned tables, so atemy enforces every parent
-- and child relationship in the transaction that writes the child or deletes
-- the parent. Key columns use a binary collation so names compare byte for
-- byte, as they do in PostgreSQL.

CREATE TABLE atespaces (
    name     VARCHAR(255) NOT NULL,
    uid      VARCHAR(255) NOT NULL,
    version  BIGINT NOT NULL,
    proto    LONGBLOB NOT NULL,
    PRIMARY KEY (name)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE actors (
    atespace  VARCHAR(255) NOT NULL,
    name      VARCHAR(255) NOT NULL,
    uid       VARCHAR(255) NOT NULL,
    version   BIGINT NOT NULL,
    proto     LONGBLOB NOT NULL,
    PRIMARY KEY (atespace, name)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE actor_egress_policies (
    atespace    VARCHAR(255) NOT NULL,
    actor_name  VARCHAR(255) NOT NULL,
    uid         VARCHAR(255) NOT NULL,
    version     BIGINT NOT NULL,
    proto       LONGBLOB NOT NULL,
    PRIMARY KEY (atespace, actor_name)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE actor_templates (
    atespace  VARCHAR(255) NOT NULL,
    name      VARCHAR(255) NOT NULL,
    uid       VARCHAR(255) NOT NULL,
    version   BIGINT NOT NULL,
    proto     LONGBLOB NOT NULL,
    PRIMARY KEY (atespace, name)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE tags (
    atespace  VARCHAR(255) NOT NULL,
    name      VARCHAR(255) NOT NULL,
    uid       VARCHAR(255) NOT NULL,
    version   BIGINT NOT NULL,
    proto     LONGBLOB NOT NULL,
    PRIMARY KEY (atespace, name)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

-- Workers are global-scoped and named by their Kubernetes pod UID, so name
-- alone is the primary key.
CREATE TABLE workers (
    name     VARCHAR(255) NOT NULL,
    uid      VARCHAR(255) NOT NULL,
    version  BIGINT NOT NULL,
    proto    LONGBLOB NOT NULL,
    PRIMARY KEY (name),
    UNIQUE KEY workers_uid (uid)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

-- One row per Actor, keyed by Actor UID because an Actor has at most one
-- Worker. Kept separate from workers so Worker reads, writes, and watch events
-- do not grow with occupancy. The primary key finds an Actor's Worker;
-- worker_name lists a Worker's Actors.
CREATE TABLE worker_assignments (
    actor_uid    VARCHAR(255) NOT NULL,
    worker_name  VARCHAR(255) NOT NULL,
    proto        LONGBLOB NOT NULL,
    PRIMARY KEY (actor_uid),
    KEY worker_assignments_worker_idx (worker_name, actor_uid)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

-- Transactional outbox backing WatchWorkers. Each worker write takes the next
-- seq from worker_outbox_sequence and holds that row lock until it commits, so
-- rows commit in seq order and a poller reading past its cursor never skips
-- a row that commits later. Retention deletes the oldest rows and records the
-- greatest deleted seq in worker_outbox_trim.
CREATE TABLE worker_outbox (
    seq         BIGINT UNSIGNED NOT NULL,
    created_at  DATETIME(6) NOT NULL,
    payload     LONGBLOB NOT NULL,
    PRIMARY KEY (seq)
);

-- Single row (id = 1): the last seq handed to a worker write.
CREATE TABLE worker_outbox_sequence (
    id   TINYINT UNSIGNED NOT NULL,
    seq  BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO worker_outbox_sequence (id, seq) VALUES (1, 0);

-- Single row (id = 1): the greatest seq retention has deleted. Watchers
-- compare it against their cursor to detect that unconsumed rows were
-- deleted out from under them.
CREATE TABLE worker_outbox_trim (
    id   TINYINT UNSIGNED NOT NULL,
    seq  BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO worker_outbox_trim (id, seq) VALUES (1, 0);

CREATE TABLE leases (
    lease_key   VARCHAR(512) NOT NULL,
    token       VARCHAR(255) NOT NULL,
    expires_at  DATETIME(6) NOT NULL,
    PRIMARY KEY (lease_key),
    KEY leases_expires_at_idx (expires_at)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;
