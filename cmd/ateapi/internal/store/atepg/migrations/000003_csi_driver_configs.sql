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

ALTER TABLE external_volume_classes
    ADD COLUMN driver_name text NOT NULL DEFAULT '';

CREATE INDEX external_volume_classes_driver_name_idx
    ON external_volume_classes (driver_name);

-- CsiDriverConfigs are global-scoped, so name alone is the primary key.
-- driver_name is unique so at most one config addresses each CSI driver.
CREATE TABLE csi_driver_configs (
    name        text PRIMARY KEY,
    uid         text NOT NULL,
    version     bigint NOT NULL,
    driver_name text NOT NULL CONSTRAINT csi_driver_configs_driver_name_key UNIQUE,
    proto       bytea NOT NULL
);
