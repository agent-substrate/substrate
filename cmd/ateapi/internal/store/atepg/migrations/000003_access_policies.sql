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

-- Singleton: the id column only admits true, so the table holds at most one row.
CREATE TABLE global_access_policy (
    id      boolean PRIMARY KEY DEFAULT true CHECK (id),
    uid     text NOT NULL,
    version bigint NOT NULL,
    proto   bytea NOT NULL
);

CREATE TABLE atespace_access_policies (
    atespace_name text NOT NULL,
    uid           text NOT NULL,
    version       bigint NOT NULL,
    proto         bytea NOT NULL,
    PRIMARY KEY (atespace_name),
    FOREIGN KEY (atespace_name)
        REFERENCES atespaces(name) ON DELETE CASCADE
);
