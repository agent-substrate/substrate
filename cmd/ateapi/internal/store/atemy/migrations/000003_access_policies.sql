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

-- Singleton: atemy only ever writes id = 1, so the table holds at most one row.
CREATE TABLE IF NOT EXISTS global_access_policy (
    id      TINYINT UNSIGNED NOT NULL,
    uid     VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    proto   LONGBLOB NOT NULL,
    PRIMARY KEY (id)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS atespace_access_policies (
    atespace_name VARCHAR(255) NOT NULL,
    uid           VARCHAR(255) NOT NULL,
    version       BIGINT NOT NULL,
    proto         LONGBLOB NOT NULL,
    PRIMARY KEY (atespace_name)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;
