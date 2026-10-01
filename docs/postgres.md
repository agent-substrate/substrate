# Database Configuration

## Overview
Substrate supports separate connections for schema ownership (DDL) and normal reads and writes (DML). Migrations and partition maintenance use the owner connection; application queries use the read/write connection. Each connection assumes its configured PostgreSQL role.

The bundled development database uses fixed owner and read/write roles and separate login users. `ate-setup` creates them from [`pkg/postgressetup/setup.sql`](../pkg/postgressetup/setup.sql) before starting `ateapi`. Those fixed usernames and passwords are for development installations only.

External databases are never provisioned by `ateapi` or `ate-setup`. Their operators must create the database identities and schema described below before deploying Substrate.

## BYO DB Configuration
A custom database can be provided to Substrate.

`ATE_API_POSTGRES_SCHEMA` (or `--postgres-schema` when running `ateapi` directly) selects the schema, defaulting to `substrate`. The operator must provision the schema and its grants before starting Substrate. `ateapi` then uses the schema for both connection pools and migrations, overriding any `search_path` in the connection strings.

### Operator Provisioned BYO DB
For production systems, the recommendation is to fully provision an external Postgres Database and provide credentials to Substrate to interact with that database. The `ATE_API_POSTGRES_OWNER_CONNECTION_STRING` and `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING` are used to configure Substrate's owner and runtime connection pools.

The external database must contain:

- A database selected by both connection strings. Substrate does not require a particular database name.
- A schema owned by the owner role. The default schema name is `substrate`.
- A stable `NOLOGIN` owner role. The default is `substrate_owner`.
- A stable `NOLOGIN` read/write role. The default is `substrate_readwrite`.
- One or two login users. The owner login must be a member of the owner role, and the runtime login must be a member of the read/write role. One login may be a member of both roles when separate credentials are unavailable.

The login users come from the database provider: they may be ordinary PostgreSQL users, IAM identities, or another provider-managed identity. Substrate does not create them or require specific login names.

Create an owner role with schema ownership and a read/write role with schema usage. Grant the runtime role SELECT, INSERT, UPDATE, and DELETE on tables; USAGE, SELECT, and UPDATE on sequences; EXECUTE on routines; and USAGE on types. Configure the owner's default privileges so newly migrated objects receive these grants. Grant the respective roles to the login users. The [Cloud SQL setup example](../tools/setup-gcp/cloud-sql.md#2-one-time-roles-and-schema-privileges) shows the SQL for one login with both roles; use separate logins for stronger isolation.

Apply these grants before the first `ateapi` startup. The owner connection creates and updates tables through migrations; the read/write connection cannot create schema objects.

Set `ATE_API_POSTGRES_OWNER_ROLE` and `ATE_API_POSTGRES_READ_WRITE_ROLE` to the roles you created, and leave role settings out of the connection strings: `ateapi` runs `SET ROLE` after connecting.

User credentials are passed as connection strings through `ATE_API_POSTGRES_OWNER_CONNECTION_STRING` and `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING`. When running `ateapi` directly, pass the DSNs as flags or use `@env` flags to read these environment variables; the installer manifest already uses `@env`.

`ATE_API_POSTGRES_POOL_MAX_CONNS` (or `--postgres-pool-max-conns` when running `ateapi` directly) sets the read/write pool limit after the connection string is loaded. When unset, a `pool_max_conns` value in the DSN or the pgxpool default applies. This setting does not affect the owner or watch pools; they are capped at 2 and 3 connections, respectively.

`ateapi` uses the same `@env` flag pattern for role names and schema.

### CloudSQL Configuration
CloudSQL setup information can be found in the dedicated guide [here](../tools/setup-gcp/cloud-sql.md).
