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

-- PostgreSQL identity setup for Substrate. This is separate from table migrations.
-- Run as an administrator inside a transaction after setting these transaction-local
-- settings: substrate.bootstrap_owner_username, substrate.bootstrap_owner_password,
-- substrate.bootstrap_readwrite_username, substrate.bootstrap_readwrite_password,
-- and substrate.bootstrap_schema. Optionally set substrate.bootstrap_owner_role
-- and substrate.bootstrap_readwrite_role (defaults substrate_owner and
-- substrate_readwrite). Bundled bootstrap supplies its fixed development
-- credentials; operators may supply their own values when running this file directly.

DO $bootstrap$
DECLARE
    owner_user text := current_setting('substrate.bootstrap_owner_username');
    owner_password text := current_setting('substrate.bootstrap_owner_password');
    readwrite_user text := current_setting('substrate.bootstrap_readwrite_username');
    readwrite_password text := current_setting('substrate.bootstrap_readwrite_password');
    schema_name text := current_setting('substrate.bootstrap_schema');
    owner_role text := COALESCE(NULLIF(current_setting('substrate.bootstrap_owner_role', true), ''), 'substrate_owner');
    readwrite_role text := COALESCE(NULLIF(current_setting('substrate.bootstrap_readwrite_role', true), ''), 'substrate_readwrite');
    managed record;
    role_attrs record;
    schema_owner text;
BEGIN
    IF owner_user = '' OR owner_password = '' OR readwrite_user = ''
       OR readwrite_password = '' OR schema_name = '' THEN
        RAISE EXCEPTION 'Substrate bootstrap usernames, passwords, and schema must not be empty';
    END IF;
    IF owner_user = readwrite_user THEN
        RAISE EXCEPTION 'Substrate owner and read/write usernames must differ';
    END IF;
    IF owner_role = readwrite_role OR owner_role IN (owner_user, readwrite_user)
       OR readwrite_role IN (owner_user, readwrite_user) THEN
        RAISE EXCEPTION 'Substrate owner/read-write roles and login usernames must differ';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('agent-substrate:bootstrap:' || schema_name, 0));

    FOR managed IN SELECT * FROM (VALUES
        (owner_role, false, NULL::text),
        (readwrite_role, false, NULL::text),
        (owner_user, true, owner_password),
        (readwrite_user, true, readwrite_password)
    ) AS roles(name, can_login, password) LOOP
        SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolinherit
          INTO role_attrs FROM pg_roles WHERE rolname = managed.name;
        IF NOT FOUND THEN
            IF managed.can_login THEN
                EXECUTE format('CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD %L', managed.name, managed.password);
            ELSE
                EXECUTE format('CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION', managed.name);
            END IF;
        ELSIF role_attrs.rolcanlogin <> managed.can_login OR role_attrs.rolsuper
           OR role_attrs.rolcreatedb OR role_attrs.rolcreaterole OR role_attrs.rolreplication
           OR (managed.can_login AND role_attrs.rolinherit) THEN
            RAISE EXCEPTION 'managed PostgreSQL role "%" conflicts with the required attributes', managed.name;
        END IF;
    END LOOP;

    EXECUTE format('GRANT %I TO %I', owner_role, owner_user);
    EXECUTE format('GRANT %I TO %I', readwrite_role, readwrite_user);
    SELECT pg_get_userbyid(nspowner) INTO schema_owner FROM pg_namespace WHERE nspname = schema_name;
    IF schema_name = 'public' THEN
        IF NOT FOUND THEN
            RAISE EXCEPTION 'PostgreSQL public schema is missing';
        END IF;
        EXECUTE format('GRANT USAGE, CREATE ON SCHEMA public TO %I', owner_role);
    ELSIF NOT FOUND THEN
        EXECUTE format('CREATE SCHEMA %I AUTHORIZATION %I', schema_name, owner_role);
    ELSIF schema_owner <> owner_role THEN
        RAISE EXCEPTION 'PostgreSQL schema "%" is owned by "%", not "%"', schema_name, schema_owner, owner_role;
    END IF;

    REVOKE CREATE ON SCHEMA public FROM PUBLIC;
    EXECUTE format('REVOKE ALL ON SCHEMA %I FROM PUBLIC', schema_name);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', schema_name, readwrite_role);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', owner_role, schema_name, readwrite_role);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO %I', owner_role, schema_name, readwrite_role);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT EXECUTE ON ROUTINES TO %I', owner_role, schema_name, readwrite_role);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT USAGE ON TYPES TO %I', owner_role, schema_name, readwrite_role);
END
$bootstrap$;
