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

-- OpenFGA storage schema: the final state of github.com/openfga/openfga MySQL
-- migrations 001 through 008, the counterpart of the PostgreSQL schema pinned
-- in atepg. Kept in the same database and Goose migration directory as
-- Substrate tables so resource mutations and authorization tuple updates
-- execute within the same MySQL transaction.

CREATE TABLE tuple (
    store             CHAR(26) NOT NULL,
    object_type       VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    object_id         VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    relation          VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    _user             VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    user_type         VARCHAR(7) NOT NULL,
    ulid              CHAR(26) NOT NULL,
    inserted_at       TIMESTAMP NOT NULL,
    condition_name    VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    condition_context LONGBLOB,
    PRIMARY KEY (store, object_type, object_id, relation, _user),
    UNIQUE KEY idx_tuple_ulid (ulid),
    KEY idx_user_lookup (store, _user, relation, object_type, object_id)
) DEFAULT CHARSET = utf8mb4;

CREATE TABLE authorization_model (
    store                  CHAR(26) NOT NULL,
    authorization_model_id CHAR(26) NOT NULL,
    type                   VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    type_definition        BLOB,
    schema_version         VARCHAR(5) NOT NULL DEFAULT '1.0',
    serialized_protobuf    LONGBLOB,
    PRIMARY KEY (store, authorization_model_id, type)
) DEFAULT CHARSET = utf8mb4;

CREATE TABLE store (
    id         CHAR(26) NOT NULL,
    name       VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NULL,
    deleted_at TIMESTAMP NULL,
    PRIMARY KEY (id)
) DEFAULT CHARSET = utf8mb4;

CREATE TABLE assertion (
    store                  CHAR(26) NOT NULL,
    authorization_model_id CHAR(26) NOT NULL,
    assertions             BLOB,
    PRIMARY KEY (store, authorization_model_id)
) DEFAULT CHARSET = utf8mb4;

CREATE TABLE changelog (
    store             CHAR(26) NOT NULL,
    object_type       VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    object_id         VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    relation          VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    _user             VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    operation         INTEGER NOT NULL,
    ulid              CHAR(26) NOT NULL,
    inserted_at       TIMESTAMP NOT NULL,
    condition_name    VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    condition_context LONGBLOB,
    PRIMARY KEY (store, ulid, object_type)
) DEFAULT CHARSET = utf8mb4;
