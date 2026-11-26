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

-- Drop hot-path foreign keys whose FOR KEY SHARE parent-row triggers serialize
-- high-concurrency sandbox creation and worker binding on MultiXact and tuple
-- locks:
-- 1. worker_assignments_worker_name_fkey is redundant because both
--    BindActorToWorker and DeleteWorker already lock the workers row FOR UPDATE.
-- 2. actors_atespace_fkey and tags_atespace_fk are replaced by 64-way striped
--    shared advisory locks on CreateActor and CreateTag paired with an
--    all-stripes exclusive advisory lock and emptiness checks in DeleteAtespace.
ALTER TABLE actors DROP CONSTRAINT actors_atespace_fkey;
ALTER TABLE tags DROP CONSTRAINT tags_atespace_fk;
ALTER TABLE worker_assignments DROP CONSTRAINT worker_assignments_worker_name_fkey;
