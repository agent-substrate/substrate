# Cloud SQL for the PostgreSQL store backend

The ateapi PostgreSQL store can run against
[Cloud SQL for PostgreSQL](https://cloud.google.com/sql/docs/postgres). The
supported, most secure configuration uses the
[Cloud SQL Auth Proxy](https://docs.cloud.google.com/sql/docs/postgres/sql-proxy)
([source](https://github.com/GoogleCloudPlatform/cloud-sql-proxy)) as a
sidecar on `ate-api-server` with [**automatic IAM database
authentication**](https://docs.cloud.google.com/sql/docs/postgres/iam-authentication):

- **Transport security** — the proxy establishes a TLS 1.3 tunnel to the
  instance using ephemeral certificates and verifies the instance's identity.
  No server CA files to download, mount, or rotate. ateapi connects to the
  proxy over pod-local loopback; that traffic never leaves the pod's network
  namespace.
- **Authentication** — establishing the tunnel requires the IAM permission
  `cloudsql.instances.connect`, and the database session itself is
  authenticated with a short-lived OAuth token for the pod's Workload
  Identity (`--auto-iam-authn`). **No database password exists anywhere** —
  nothing to store, leak, or rotate; access is revoked in IAM.
- **Cloud-agnostic code** — ateapi itself knows nothing about Cloud SQL. The
  sidecar is a deployment-time patch
  (`manifests/ate-install/cloudsql/proxy-sidecar-patch.yaml`) applied by
  `hack/install-ate.sh` only when a Cloud SQL instance is configured.

## 1. Provision

The cluster must have Workload Identity enabled (clusters created by
`setup-gcp create cluster` do), and the VPC needs [private services
access](https://cloud.google.com/sql/docs/postgres/configure-private-services-access)
(one-time per VPC; the tool prints the two `gcloud` commands if it is
missing). Then:

```sh
export PROJECT_ID=<project>
go run ./tools/setup-gcp create cloudsql --region=<cluster region>
# other flags: --instance, --tier, --edition, --storage-size, --gsa-name, --network
```

This idempotently creates:

| Resource | Value |
|---|---|
| Cloud SQL instance | PostgreSQL 18, Enterprise edition, private IP only, `cloudsql.iam_authentication=on`, automated backups + point-in-time recovery, deletion protection |
| Database | `atepg` |
| Google service account | `ate-api-server@<project>.iam.gserviceaccount.com` |
| Project IAM roles | `roles/cloudsql.client`, `roles/cloudsql.instanceUser` on the GSA |
| Workload Identity binding | `roles/iam.workloadIdentityUser` for `<project>.svc.id.goog[ate-system/ate-api-server]` on the GSA |
| IAM database user | the GSA, type `CLOUD_IAM_SERVICE_ACCOUNT` |

> **Note:** The repo otherwise uses WIF-direct
> `principal://` bindings, but Cloud SQL IAM database users must be service
> accounts — federated Kubernetes principals cannot log into the database. The
> KSA is therefore annotated with `iam.gke.io/gcp-service-account` so the
> proxy's ambient credentials resolve to the GSA.

Deletion protection means an instance cannot be deleted until it is cleared:

```sh
gcloud sql instances patch <instance> --no-deletion-protection
gcloud sql instances delete <instance>
```

Backups, point-in-time recovery, and the shape of an existing instance are
never reconciled — the settings above apply at creation. Change them later
with `gcloud sql instances patch`.

## 2. One-time roles and schema privileges

[IAM database users](https://docs.cloud.google.com/sql/docs/postgres/add-manage-iam-users)
are created with no privileges. By default, ateapi connects as the
IAM user and then assumes `substrate_owner` for migrations or
`substrate_readwrite` for application queries. Create these roles, grant the
IAM user membership, create the `substrate` schema, and configure the
owner's default privileges before deploying. The IAM database username is the GSA email **without**
`.gserviceaccount.com`.

Getting that `postgres` session on a private-IP-only instance takes two
steps: give `postgres` a temporary password (fresh instances have none), and
run `psql` from inside the cluster, which is the only place with a network
path to the instance. Replace `<project>`, `<instance>`, and `<temp-pw>` below:

```sh
gcloud sql users set-password postgres --instance=<instance> --password='<temp-pw>'
IP=$(gcloud sql instances describe <instance> --format="value(ipAddresses[0].ipAddress)")
kubectl run psql-grant --rm -i --restart=Never --image=postgres:18-alpine -- \
  psql "postgresql://postgres:<temp-pw>@${IP}:5432/atepg?sslmode=require" \
  -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;
CREATE ROLE substrate_owner NOLOGIN;
CREATE ROLE substrate_readwrite NOLOGIN;
GRANT substrate_owner TO postgres WITH SET TRUE;
GRANT substrate_owner, substrate_readwrite TO "ate-api-server@<project>.iam";
CREATE SCHEMA substrate AUTHORIZATION substrate_owner;
GRANT USAGE ON SCHEMA substrate TO substrate_readwrite;
ALTER DEFAULT PRIVILEGES FOR ROLE substrate_owner IN SCHEMA substrate
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO substrate_readwrite;
ALTER DEFAULT PRIVILEGES FOR ROLE substrate_owner IN SCHEMA substrate
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO substrate_readwrite;
ALTER DEFAULT PRIVILEGES FOR ROLE substrate_owner IN SCHEMA substrate
  GRANT EXECUTE ON ROUTINES TO substrate_readwrite;
ALTER DEFAULT PRIVILEGES FOR ROLE substrate_owner IN SCHEMA substrate
  GRANT USAGE ON TYPES TO substrate_readwrite;
COMMIT;
SQL
```

The grant back to `postgres` lets this non-superuser administrator configure
the owner role's default privileges.

Nothing deployed ever uses this password — afterwards you can scramble it
(`gcloud sql users set-password postgres --instance=<instance>
--password="$(openssl rand -hex 16)"`) or keep it for admin access such as
Cloud SQL Studio. Note that `postgres` is not a superuser on Cloud SQL.

This identity layout requires a fresh `atepg` database. Existing tables and
users need a separate migration of ownership and grants.

## 3. Deploy

```sh
export ATE_API_POSTGRES_CLOUDSQL_INSTANCE=<project>:<region>:<instance>
export ATE_API_POSTGRES_CLOUDSQL_GSA=ate-api-server@<project>.iam.gserviceaccount.com
./hack/install-ate.sh --deploy-ate-system
# Existing installation: --deploy-ate-apiserver instead of --deploy-ate-system
```

What this does differently from a plain install:

- Skips the bundled PostgreSQL StatefulSet (a configured Cloud SQL instance
  counts as an external database, exactly like an explicit
  `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING`); the install logs the skip and the
  database it deferred to.
- Writes the proxy's configuration (`CSQL_PROXY_*`) into the
  `ate-api-server-envvars` ConfigMap and synthesizes a passwordless DSN
  (`user=<gsa-user> host=127.0.0.1 ... sslmode=disable`) into the
  `ate-api-server-secret-envvars` Secret.
- Annotates the `ate-api-server` KSA with the GSA and patches the
  `cloud-sql-proxy` native sidecar (initContainer with
  `restartPolicy: Always`; requires Kubernetes 1.29+) into the deployment.
  The sidecar's `/startup` probe gates ateapi, so ateapi never races the
  tunnel. Leaving `ATE_API_POSTGRES_CLOUDSQL_INSTANCE` **unset** on later
  redeploys keeps the current Cloud SQL configuration (it is adopted from
  the cluster's record); to remove the sidecar and annotation, set it to
  the empty string explicitly: `ATE_API_POSTGRES_CLOUDSQL_INSTANCE=""`.

Optional environment variables:

- `ATE_API_POSTGRES_CLOUDSQL_IP_TYPE` — `private` (default), `public`, `psc`.
  Selects which of the instance's addresses the proxy dials, for connecting
  to instances provisioned outside this tool: `setup-gcp create cloudsql`
  itself only creates private-services-access (private IP) instances —
  `public` and `psc` require an instance configured accordingly out-of-band.
- `ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH` — set `false` to fall back to password
  authentication through the proxy (still encrypted and identity-verified);
  you must then provide an explicit application connection string with the
  password yourself (the install script rejects `false` without one because
  a synthesized passwordless DSN cannot log in once the
  proxy stops injecting IAM tokens).
- `ATE_API_POSTGRES_OWNER_CONNECTION_STRING` — a separate schema-owner DSN for
  migrations and outbox partition maintenance. If no read/write DSN is set,
  the read/write pool uses this DSN too. Otherwise, the owner DSN defaults to
  `ATE_API_POSTGRES_CONNECTION_STRING`, then the read/write DSN.
  The legacy variable alone still supplies both pools; adding a separate
  read/write DSN leaves the legacy connection as the owner connection.
  With separate logins, grant each login membership in its corresponding
  role. Provision the schema and default object grants as in section 2;
  ateapi does not grant them when bootstrap is disabled. Use a schema
  dedicated to Substrate when configuring separate identities.
- `ATE_API_POSTGRES_READ_WRITE_ROLE` and `ATE_API_POSTGRES_OWNER_ROLE` — stable
  `NOLOGIN` roles (defaults: `substrate_readwrite` and `substrate_owner`).
  Neither ateapi nor the installer creates them for external databases.
  Provision custom role names and matching grants if you override these values;
  grant each incoming login membership before publishing its connection string.
  ateapi runs `SET ROLE` on every new connection, so these settings take
  precedence over a role set through connection-string `options`. A DSN role
  must still be valid at startup; leave it out of operator-provided DSNs to
  avoid conflicting configuration.
  Stable roles keep grants and object ownership across username rotations.
- `ATE_API_POSTGRES_SCHEMA` — the schema holding the store's tables
  (default `substrate`). If you override it, create the named schema with
  the owner role as owner and target it in the grants in section 2.
- `ATE_API_POSTGRES_POOL_MAX_CONNS` — connections per read/write pool
  (default: `max(4, NumCPU)`). Ateapi has separate store and OpenFGA pools
  with this limit. It does not affect the owner and watch pools, which are
  capped at 2 and 3 connections. The setting applies after loading the DSN,
  including from `@file:`, and overrides `pool_max_conns` in the DSN when both
  are set.

Changing the installed configuration rolls ate-api-server: the install script
stamps a hash of the rendered configuration into the pod template
(`ate.dev/env-hash`). For an `@file:` connection, changes to `pool_max_conns`
inside the referenced DSN require an ate-api-server restart; credential changes
are picked up on new connections.

## 4. Verify

```sh
kubectl rollout status deployment/ate-api-server -n ate-system
kubectl logs deployment/ate-api-server -n ate-system -c cloud-sql-proxy | head
# expect: "The proxy has started successfully and is ready for new connections"
kubectl logs deployment/ate-api-server -n ate-system | head -5
# expect the startup flag dump and no store connection errors
kubectl get secret ate-api-server-secret-envvars -n ate-system \
  -o jsonpath='{.data.ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING}' | base64 -d
# expect: no password in the DSN
```

Common failure modes:

| Symptom | Cause |
|---|---|
| proxy: `PERMISSION_DENIED` on startup | GSA missing `roles/cloudsql.client`, or the Workload Identity annotation/binding is absent |
| `FATAL: Cloud SQL IAM service account authentication failed` | GSA missing `roles/cloudsql.instanceUser`, or the IAM database user was not created |
| ateapi: `role "substrate_owner" does not exist` or `permission denied to set role` | Create the roles and grant the IAM database user membership (section 2) |
| ateapi: `permission denied for schema substrate` | The one-time schema grants (section 2) were not run |
| ateapi: `permission denied for table` | The owner role's default privileges (section 2) were not configured before migrations |
| proxy: instance connection errors mentioning IAM | `cloudsql.iam_authentication` flag is off on the instance |

## 5. Scaling the database

The provisioning defaults (`db-custom-2-8192`, 10 GB disk) suit development
and modest fleets. For sizing at large actor counts and high request rates —
instance shape and the data cache, storage/IOPS, connection-pool math,
proxy sidecar resources, and Managed Connection Pooling — see
[docs/dev/cloud-sql-scaling-guide.md](../../docs/dev/cloud-sql-scaling-guide.md).

## Alternative: any external PostgreSQL (non-GCP)

For a non-Cloud-SQL database, provide a DSN directly; password lives in a
Secret and the server certificate is verified against a mounted CA. (This is
also the shape of a [direct
connection](https://docs.cloud.google.com/sql/docs/postgres/connection-options)
to Cloud SQL without the proxy, if you ever need one — you then manage the
server CA and credentials yourself.)

```sh
export ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING='postgresql://<user>:<pw>@<host>:5432/atepg?sslmode=verify-ca&sslrootcert=/run/postgres-server-ca/server-ca.pem'
export ATE_API_POSTGRES_OWNER_CONNECTION_STRING='postgresql://<schema-owner>:<pw>@<host>:5432/atepg?sslmode=verify-ca&sslrootcert=/run/postgres-server-ca/server-ca.pem'
export ATE_API_POSTGRES_SERVER_CA_FILE=/path/to/server-ca.pem
./hack/install-ate.sh --deploy-ate-system
```

Provision the configured owner and read/write roles, memberships, schema,
and default privileges before deploying, as in section 2.
