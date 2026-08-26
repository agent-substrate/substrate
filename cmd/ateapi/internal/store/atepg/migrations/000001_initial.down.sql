-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

BEGIN;

DROP TABLE IF EXISTS leases;
DROP TABLE IF EXISTS worker_outbox_trim;
DROP TABLE IF EXISTS worker_outbox_default;
DROP TABLE IF EXISTS worker_outbox;
DROP TABLE IF EXISTS workers;
DROP TABLE IF EXISTS actor_snapshot_tags;
DROP TABLE IF EXISTS actor_snapshots;
DROP TABLE IF EXISTS actor_templates;
DROP TABLE IF EXISTS actors;
DROP TABLE IF EXISTS atespaces;

COMMIT;
