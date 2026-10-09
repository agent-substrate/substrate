# Database Configuration

## Overview
Substrate supports separate connections for schema ownership (DDL) and normal reads and writes (DML). Migrations and partition maintenance use the owner connection; application queries use the read/write connection. Each connection assumes its configured PostgreSQL role.

The bundled development database uses fixed owner and read/write roles and separate login users. `ate-setup` creates them from [`pkg/postgressetup/setup.sql`](../pkg/postgressetup/setup.sql) before starting `ateapi`. The script has no built-in identity defaults; `ate-setup` supplies the development values from `postgressetup.DefaultConfig`. The bundled login users have no database passwords; they authenticate with certificates.

External databases are never provisioned by `ateapi` or `ate-setup`. Their operators must create the database identities and schema described below before deploying Substrate.

## Bundled database authentication

The API server connects without a password using two Kubernetes projected Pod
Certificates from `postgres.podcert.ate.dev/identity`. This signer only issues
client certificates to pods using the `ate-api-server` service account in the
namespace configured by the controller's `--postgres-client-namespace` flag
(default: `ate-system`). Set this flag to the API server's namespace when
relocating an installation; the certificate controller runs in a separate
namespace. Each projection sets `userAnnotations.postgres.podcert.ate.dev/username`
to either `substrate_owner_user` or `substrate_readwrite_user`; the signer rejects
missing annotations, unknown keys, and every other username. It sets the
certificate Common Name (CN) to that authorized login. Each connection pool
presents its own certificate.

The namespace and service account are the authorization boundary. Kubernetes
validates those identity fields in each PodCertificateRequest; the username
annotation only selects between the two logins authorized for that identity.
Anyone allowed to create pods using `ate-api-server` in that namespace can
request either login, so operators must restrict that capability to trusted control-plane
administrators. Both credentials are available to the API-server process;
separate logins do not isolate the owner role from a compromised API server.

The bundled PostgreSQL server trusts only this signer's dedicated CA for client
certificates. Its `hostssl ... cert` rule requires the certificate CN to match
the requested database username. The API server uses `sslmode=verify-full` to
verify the server's service DNS certificate. New database connections reread the
projected client credentials and server trust roots. The bundled PostgreSQL TLS-reloader
checks its server credential bundle and client trust bundle every 60 seconds and
requests a configuration reload when they change. Existing database connections
remain open. Pod Certificates require a minimum lifetime of one hour; the
signer's default is 24 hours. Local Unix-socket access inside the database pod
remains trusted for bootstrap, health checks, and TLS reloads.

The installer stores the signer's CA pool in the `postgres-ca-pool` Secret in
`podcertificate-controller-system`; the API pod receives its client private key
and certificate through a projected volume rather than a username/password
Secret. The owner and read/write logins retain their respective role grants from
the bootstrap script. The `postgres` administrator is restricted to local
connections inside the database pod.

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

Operators using ordinary PostgreSQL login users may run [`pkg/postgressetup/setup.sql`](../pkg/postgressetup/setup.sql) with an administrator connection. It is a `psql` script and requires all of these variables:

| Variable | Bundled development value |
|---|---|
| `substrate_schema` | `substrate` |
| `substrate_owner_role` | `substrate_owner` |
| `substrate_owner_user` | `substrate_owner_user` |
| `substrate_owner_password` | Empty (certificate authentication) |
| `substrate_readwrite_role` | `substrate_readwrite` |
| `substrate_readwrite_user` | `substrate_readwrite_user` |
| `substrate_readwrite_password` | Empty (certificate authentication) |

The login users come from the database provider: they may be ordinary PostgreSQL users, IAM identities, or another provider-managed identity. Substrate does not create them or require specific login names.

Create an owner role with schema ownership and a read/write role with schema usage. Grant the runtime role SELECT, INSERT, UPDATE, and DELETE on tables; USAGE, SELECT, and UPDATE on sequences; EXECUTE on routines; and USAGE on types. Configure the owner's default privileges so newly migrated objects receive these grants. Grant the respective roles to the login users. The [Cloud SQL setup example](../tools/setup-gcp/cloud-sql.md#2-one-time-roles-and-schema-privileges) shows the SQL for one login with both roles; use separate logins for stronger isolation.

Apply these grants before the first `ateapi` startup. The owner connection creates and updates tables through migrations; the read/write connection cannot create schema objects.

Set `ATE_API_POSTGRES_OWNER_ROLE` and `ATE_API_POSTGRES_READ_WRITE_ROLE` to the roles you created, and leave role settings out of the connection strings: `ateapi` runs `SET ROLE` after connecting.

The connection strings are set through `ATE_API_POSTGRES_OWNER_CONNECTION_STRING` and `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING` (or `--ateapi-postgres-owner-connection-string` and `--ateapi-postgres-read-write-connection-string`). `ate-setup` writes them, along with the roles, schema, and pool size, onto the `ate-api-server` Deployment as `ateapi` flags (`--postgres-owner-connection-string`, `--postgres-read-write-connection-string`, and so on), so changing any of them and redeploying rolls the API server.

Because the connection strings end up in the pod spec, they must not contain secrets. Both `ate-setup` and `ateapi` reject a connection string that sets a password (`password=...`, or `user:password@` in a URI) or a client key passphrase (`sslpassword`); see the [libpq parameter keywords](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS). Supply a password through a [password file](https://www.postgresql.org/docs/current/libpq-pgpass.html) named by the `passfile` parameter instead:

```sh
kubectl -n ate-system create secret generic ate-api-server-pgpass \
  --from-literal=pgpass='db.example.internal:5432:atepg:substrate_app:<password>'
export ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING='postgresql://substrate_app@db.example.internal:5432/atepg?sslmode=verify-full&passfile=/run/pgpass/pgpass'
```

Mount the Secret into the `ate-api-server` container at `/run/pgpass`, for example with `kubectl patch`. `ate-setup` applies the Deployment server-side, so a later redeploy leaves alone the volume and mount it does not manage. `ateapi` re-reads the passfile, and the certificate files the DSN names, every time it opens a connection. After you update the Secret and the kubelet refreshes the mounted file, new connections use the new password and existing connections stay open. A `subPath` mount never refreshes, so mount the whole volume.

`ATE_API_POSTGRES_POOL_MAX_CONNS` (or `--postgres-pool-max-conns` when running `ateapi` directly) sets the read/write pool limit after the connection string is loaded. When unset, a `pool_max_conns` value in the DSN or the pgxpool default applies. This setting does not affect the owner or watch pools; they are capped at 2 and 3 connections, respectively.

### CloudSQL Configuration
CloudSQL setup information can be found in the dedicated guide [here](../tools/setup-gcp/cloud-sql.md).
