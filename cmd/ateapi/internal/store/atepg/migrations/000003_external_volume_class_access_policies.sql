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

CREATE TABLE external_volume_class_access_policies (
    external_volume_class_name text NOT NULL,
    uid                        text NOT NULL,
    version                    bigint NOT NULL,
    proto                      bytea NOT NULL,
    PRIMARY KEY (external_volume_class_name),
    FOREIGN KEY (external_volume_class_name)
        REFERENCES external_volume_classes(name) ON DELETE CASCADE
);
