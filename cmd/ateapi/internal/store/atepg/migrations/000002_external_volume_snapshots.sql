-- Copyright 2026 The Agent Substrate Authors
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

-- tag_name and tag_uid are set for snapshots a tag owns, and NULL for
-- snapshots with another origin. A tag cannot be deleted while it owns
-- snapshots.
CREATE TABLE external_volume_snapshots (
    atespace  text NOT NULL,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    tag_name  text,
    tag_uid   text,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name),
    CONSTRAINT external_volume_snapshots_atespace_fk
        FOREIGN KEY (atespace) REFERENCES atespaces(name) ON DELETE RESTRICT,
    CONSTRAINT external_volume_snapshots_tag_fk
        FOREIGN KEY (atespace, tag_name) REFERENCES tags(atespace, name) ON DELETE RESTRICT,
    CONSTRAINT external_volume_snapshots_tag_check
        CHECK ((tag_name IS NULL) = (tag_uid IS NULL))
);

-- Finds a tag's snapshots, and backs the check that a deleted tag owns none.
CREATE INDEX external_volume_snapshots_tag_idx
    ON external_volume_snapshots (atespace, tag_name, tag_uid);
