# Database Configuration

## Overview
Substrate includes a bundled Postgres database by default. A `bootstrap` step in `ateapi` runs before database migrations which utilizes an administrator account to create a default set of `owner` and `reader_writer` roles as well as users and their role membership. The default role names, usernames, and passwords are hardcoded and are purposfully not exposed for configuration in order to avoid Substrate handling the intricacies of database credential rotation. These defaults are created for development and evaluation purposes. Production deployments should provision an external database along with the associated schema, roles and user accounts.

Substrate supports separate users for DDL (ownership) and DML (runtime) use. A single user can be used for both if desired, but the separation allows for a stronger security posture. For a single user, provision both roles manually, grant that user membership in both, and disable bootstrap; `identity.sql` requires distinct owner and read/write usernames.

## BYO DB Configuration
A custom database can be provided to Substrate.

`ATE_API_POSTGRES_SCHEMA` (or `--postgres-schema` when running `ateapi` directly) selects the schema, defaulting to `public`. Bootstrap prepares that schema and its grants when enabled; otherwise, the operator must ensure they exist. `ateapi` then uses the schema for both connection pools and migrations, overriding any `search_path` in the connection strings.

### Operator Provisioned BYO DB
For production systems, the recommendation is to fully provision an external Postgres Database and provide credentials to Substrate to interact with that database. The `ATE_API_POSTGRES_OWNER_CONNECTION_STRING` and `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING` are used to configure Substrate's owner and runtime connection pools.

To provision the roles and users, run [identity.sql](../cmd/ateapi/internal/store/atepg/identity.sql) as a database administrator inside a transaction, after setting the five required transaction-local `substrate.bootstrap_*` values listed in the file. Disable `ATE_API_POSTGRES_BOOTSTRAP`, set `ATE_API_POSTGRES_OWNER_ROLE` and `ATE_API_POSTGRES_READ_WRITE_ROLE` to the roles you created, and leave role settings out of the connection strings as `ateapi` runs `SET ROLE` after connecting.

User credentials are passed into Substrate through `ATE_API_POSTGRES_OWNER_CONNECTION_STRING` and `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING`. These can be defined as DSNs directly or can be read by Substrate on connect from files using the following syntax: `@file:<path-to-file>`. The file path must be absolute. This approach can be used with the `--postgres-max-conn-lifetime` configuration in order to update connection credentials. The file will be reread when a new connection is created. When running `ateapi` directly, pass the DSNs as flags or use `@env` flags to read these environment variables; the installer manifest already uses `@env`.

`ateapi` uses the same `@env` flag pattern for role names and schema. `ATE_API_POSTGRES_BOOTSTRAP` is read directly unless `--postgres-bootstrap` is set; the administrator credential file paths and `--postgres-max-conn-lifetime` are flag-only.

### Bootstrapped BYO DB
Substrate can optionally create the same default set of roles and users with `ATE_API_POSTGRES_BOOTSTRAP` set to `true`. This is useful for evaluation purposes, but should not be used for production environments. The database admin credentials are provided to `ateapi` through `--postgres-admin-username-file` and `--postgres-admin-password-file`.

Bootstrap takes the host, port, database, and TLS settings from the owner connection string, then replaces its username and password with the administrator credentials from those files before connecting. The read/write connection string must target the same host, port, and database.

### CloudSQL Configuration
CloudSQL setup information can be found in the dedicated guide [here](../tools/setup-gcp/cloud-sql.md).
