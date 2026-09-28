-- Copyright 2026 Google LLC and The OpenFGA Authors
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

-- OpenFGA storage schema (pinned to github.com/openfga/openfga PostgreSQL
-- migration version 6). Kept in the same schema and Goose migration directory
-- as Substrate tables so resource mutations and authorization tuple updates
-- execute within the same PostgreSQL schema and transaction.

CREATE TABLE tuple (
    store             TEXT NOT NULL,
    object_type       TEXT NOT NULL,
    object_id         TEXT NOT NULL,
    relation          TEXT NOT NULL,
    _user             TEXT NOT NULL,
    user_type         TEXT NOT NULL,
    ulid              TEXT NOT NULL,
    inserted_at       TIMESTAMPTZ NOT NULL,
    condition_name    TEXT,
    condition_context BYTEA,
    PRIMARY KEY (store, object_type, object_id, relation, _user)
);

CREATE INDEX idx_tuple_partial_user
    ON tuple (store, object_type, object_id, relation, _user)
    WHERE user_type = 'user';

CREATE INDEX idx_tuple_partial_userset
    ON tuple (store, object_type, object_id, relation, _user)
    WHERE user_type = 'userset';

CREATE UNIQUE INDEX idx_tuple_ulid ON tuple (ulid);

CREATE INDEX idx_user_lookup ON tuple (
    store,
    _user,
    relation,
    object_type,
    object_id COLLATE "C"
);

CREATE TABLE authorization_model (
    store                 TEXT NOT NULL,
    authorization_model_id TEXT NOT NULL,
    type                  TEXT NOT NULL,
    type_definition       BYTEA,
    schema_version        TEXT NOT NULL DEFAULT '1.0',
    serialized_protobuf   BYTEA,
    PRIMARY KEY (store, authorization_model_id, type)
);

CREATE TABLE store (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE assertion (
    store                  TEXT NOT NULL,
    authorization_model_id TEXT NOT NULL,
    assertions             BYTEA,
    PRIMARY KEY (store, authorization_model_id)
);

CREATE TABLE changelog (
    store             TEXT NOT NULL,
    object_type       TEXT NOT NULL,
    object_id         TEXT NOT NULL,
    relation          TEXT NOT NULL,
    _user             TEXT NOT NULL,
    operation         INTEGER NOT NULL,
    ulid              TEXT NOT NULL,
    inserted_at       TIMESTAMPTZ NOT NULL,
    condition_name    TEXT,
    condition_context BYTEA,
    PRIMARY KEY (store, ulid, object_type)
);

