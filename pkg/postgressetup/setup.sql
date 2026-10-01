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

DO $setup$
DECLARE
    managed record;
    role_attrs record;
    schema_owner text;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('agent-substrate:setup:substrate', 0));

    FOR managed IN SELECT * FROM (VALUES
        ('substrate_owner', false, NULL::text),
        ('substrate_readwrite', false, NULL::text),
        ('substrate_owner_user', true, 'substrate-owner'),
        ('substrate_readwrite_user', true, 'substrate-readwrite')
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
        IF managed.can_login THEN
            EXECUTE format('ALTER ROLE %I PASSWORD %L', managed.name, managed.password);
        END IF;
    END LOOP;

    GRANT substrate_owner TO substrate_owner_user;
    GRANT substrate_readwrite TO substrate_readwrite_user;

    SELECT pg_get_userbyid(nspowner) INTO schema_owner FROM pg_namespace WHERE nspname = 'substrate';
    IF NOT FOUND THEN
        CREATE SCHEMA substrate AUTHORIZATION substrate_owner;
    ELSIF schema_owner <> 'substrate_owner' THEN
        RAISE EXCEPTION 'PostgreSQL schema "substrate" is owned by "%", not "substrate_owner"', schema_owner;
    END IF;

    REVOKE CREATE ON SCHEMA public FROM PUBLIC;
    REVOKE ALL ON SCHEMA substrate FROM PUBLIC;
    GRANT USAGE ON SCHEMA substrate TO substrate_readwrite;
    ALTER DEFAULT PRIVILEGES FOR ROLE substrate_owner IN SCHEMA substrate
      GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO substrate_readwrite;
    ALTER DEFAULT PRIVILEGES FOR ROLE substrate_owner IN SCHEMA substrate
      GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO substrate_readwrite;
    ALTER DEFAULT PRIVILEGES FOR ROLE substrate_owner IN SCHEMA substrate
      GRANT EXECUTE ON ROUTINES TO substrate_readwrite;
    ALTER DEFAULT PRIVILEGES FOR ROLE substrate_owner IN SCHEMA substrate
      GRANT USAGE ON TYPES TO substrate_readwrite;
END
$setup$;
