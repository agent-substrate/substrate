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

SELECT
    set_config('agent_substrate.setup_schema', :'substrate_schema', true) AS schema,
    set_config('agent_substrate.setup_owner_role', :'substrate_owner_role', true) AS owner_role,
    set_config('agent_substrate.setup_owner_user', :'substrate_owner_user', true) AS owner_user,
    set_config('agent_substrate.setup_owner_password', :'substrate_owner_password', true) AS owner_password,
    set_config('agent_substrate.setup_readwrite_role', :'substrate_readwrite_role', true) AS readwrite_role,
    set_config('agent_substrate.setup_readwrite_user', :'substrate_readwrite_user', true) AS readwrite_user,
    set_config('agent_substrate.setup_readwrite_password', :'substrate_readwrite_password', true) AS readwrite_password
\gset

DO $setup$
DECLARE
    schema_name text := current_setting('agent_substrate.setup_schema');
    owner_role text := current_setting('agent_substrate.setup_owner_role');
    owner_user text := current_setting('agent_substrate.setup_owner_user');
    owner_password text := current_setting('agent_substrate.setup_owner_password');
    readwrite_role text := current_setting('agent_substrate.setup_readwrite_role');
    readwrite_user text := current_setting('agent_substrate.setup_readwrite_user');
    readwrite_password text := current_setting('agent_substrate.setup_readwrite_password');
    managed record;
    role_attrs record;
    schema_owner text;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('agent-substrate:setup:' || schema_name, 0));

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
                EXECUTE format('CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION', managed.name);
            ELSE
                EXECUTE format('CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION', managed.name);
            END IF;
        ELSIF role_attrs.rolcanlogin <> managed.can_login OR role_attrs.rolsuper
           OR role_attrs.rolcreatedb OR role_attrs.rolcreaterole OR role_attrs.rolreplication
           OR (managed.can_login AND role_attrs.rolinherit) THEN
            RAISE EXCEPTION 'managed PostgreSQL role "%" conflicts with the required attributes', managed.name;
        END IF;
        IF managed.can_login THEN
            EXECUTE format('ALTER ROLE %I PASSWORD %L', managed.name, managed.password);
        END IF;
    END LOOP;

    EXECUTE format('GRANT %I TO %I', owner_role, owner_user);
    EXECUTE format('GRANT %I TO %I', readwrite_role, readwrite_user);

    SELECT pg_get_userbyid(nspowner) INTO schema_owner FROM pg_namespace WHERE nspname = schema_name;
    IF NOT FOUND THEN
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
$setup$;
