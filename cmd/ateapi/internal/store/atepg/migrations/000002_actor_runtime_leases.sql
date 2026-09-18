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

-- Runtime ownership is separate from workflow leases: generic lease cleanup
-- must never be able to erase a lease that protects a live actor workload.
CREATE SEQUENCE actor_runtime_lease_generation_seq AS bigint;

CREATE TABLE actor_runtime_leases (
    actor_uid       text PRIMARY KEY,
    actor_atespace  text NOT NULL,
    actor_name      text NOT NULL,
    token           text NOT NULL,
    generation      bigint NOT NULL,
    expires_at      timestamptz NOT NULL,
    reclaiming_until timestamptz NOT NULL DEFAULT 'epoch'
);

CREATE INDEX actor_runtime_leases_expires_at_idx
    ON actor_runtime_leases (expires_at);
