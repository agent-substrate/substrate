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

-- RESTRICT so that deleting an atespace cannot strand registered storage.
CREATE TABLE external_volumes (
    atespace  text NOT NULL,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name),
    CONSTRAINT external_volumes_atespace_fk
        FOREIGN KEY (atespace) REFERENCES atespaces(name) ON DELETE RESTRICT
);

-- One record per storage volume across all atespaces. A table of its own,
-- because a unique key without the atespace cannot live on external_volumes
-- once that is partitioned by atespace (TestActorsTablePartitionable).
CREATE TABLE external_volume_keys (
    driver_name      text NOT NULL,
    volume_id        text NOT NULL,
    record_atespace  text NOT NULL,
    record_name      text NOT NULL,
    PRIMARY KEY (driver_name, volume_id),
    UNIQUE (record_atespace, record_name),
    CONSTRAINT external_volume_keys_record_fk
        FOREIGN KEY (record_atespace, record_name)
        REFERENCES external_volumes(atespace, name) ON DELETE CASCADE
);
