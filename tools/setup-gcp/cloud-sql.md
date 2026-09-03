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

## 2. One-time schema privileges

[IAM database users](https://docs.cloud.google.com/sql/docs/postgres/add-manage-iam-users)
are created with no privileges, and PostgreSQL 15+ removed
`PUBLIC`'s `CREATE` on the `public` schema. Before serving, `ateapi` runs versioned
migrations to create its tables. The connecting user needs DDL rights to do this.
Grant them once as the built-in `postgres` user (note the database username is
the GSA email **without** `.gserviceaccount.com`):

```sql
GRANT CREATE ON DATABASE atepg TO "ate-api-server@<project>.iam";
GRANT USAGE, CREATE ON SCHEMA public TO "ate-api-server@<project>.iam";
```

Getting that `postgres` session on a private-IP-only instance takes two
steps: give `postgres` a temporary password (fresh instances have none), and
run `psql` from inside the cluster, which is the only place with a network
path to the instance:

```sh
gcloud sql users set-password postgres --instance=<instance> --password='<temp-pw>'
IP=$(gcloud sql instances describe <instance> --format="value(ipAddresses[0].ipAddress)")
kubectl run psql-grant --rm -i --restart=Never --image=postgres:18-alpine -- \
  psql "postgresql://postgres:<temp-pw>@${IP}:5432/atepg?sslmode=require" \
  -c 'GRANT CREATE ON DATABASE atepg TO "ate-api-server@<project>.iam"; GRANT USAGE, CREATE ON SCHEMA public TO "ate-api-server@<project>.iam";'
```

Nothing deployed ever uses this password — afterwards you can scramble it
(`gcloud sql users set-password postgres --instance=<instance>
--password="$(openssl rand -hex 16)"`) or keep it for admin access such as
Cloud SQL Studio. Note that `postgres` is not a superuser on Cloud SQL: it
can list the IAM user's tables but needs explicit `GRANT SELECT` from that
user to read them.

If the `atepg` tables already exist from a previous password-based user,
transfer ownership instead (future in-place DDL requires it):

```sql
GRANT "ate-api-server@<project>.iam" TO "<olduser>";
REASSIGN OWNED BY "<olduser>" TO "ate-api-server@<project>.iam";  -- run inside atepg
```

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
  `ATE_API_POSTGRES_CONNECTION_STRING`); the install logs the skip and the
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
  you must then provide `ATE_API_POSTGRES_CONNECTION_STRING` with the
  password yourself (the install script rejects `false` without an explicit
  connection string — a synthesized passwordless DSN cannot log in once the
  proxy stops injecting IAM tokens).
- `ATE_API_POSTGRES_SCHEMA` — the schema holding the store's tables
  (default `public`). If using a custom schema, the one-time schema grant
  in §2 (`GRANT USAGE, CREATE ON SCHEMA ...`) must target your custom schema
  instead of `public`.
- `ATE_API_POSTGRES_POOL_MAX_CONNS` — pgxpool connections per ateapi replica
  (default: `max(4, NumCPU)`); appended as `pool_max_conns` to whichever DSN
  is in effect (synthesized, in-cluster default, or explicitly provided —
  a `pool_max_conns` already present in an explicit DSN wins).

Changing any of these on a running installation takes effect immediately:
the install script stamps a hash of the rendered configuration into the
pod template (`ate.dev/env-hash`), so a config change rolls ate-api-server
and an unchanged config triggers nothing.

## 4. Verify

```sh
kubectl rollout status deployment/ate-api-server -n ate-system
kubectl logs deployment/ate-api-server -n ate-system -c cloud-sql-proxy | head
# expect: "The proxy has started successfully and is ready for new connections"
kubectl logs deployment/ate-api-server -n ate-system | head -5
# expect the startup flag dump and no store connection errors
kubectl get secret ate-api-server-secret-envvars -n ate-system \
  -o jsonpath='{.data.ATE_API_POSTGRES_CONNECTION_STRING}' | base64 -d
# expect: no password in the DSN
```

Common failure modes:

| Symptom | Cause |
|---|---|
| proxy: `PERMISSION_DENIED` on startup | GSA missing `roles/cloudsql.client`, or the Workload Identity annotation/binding is absent |
| `FATAL: Cloud SQL IAM service account authentication failed` | GSA missing `roles/cloudsql.instanceUser`, or the IAM database user was not created |
| ateapi: `permission denied for schema public` | the one-time schema `GRANT` (section 2) was not run |
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
export ATE_API_POSTGRES_CONNECTION_STRING='postgresql://<user>:<pw>@<host>:5432/atepg?sslmode=verify-ca&sslrootcert=/run/postgres-server-ca/server-ca.pem'
export ATE_API_POSTGRES_SERVER_CA_FILE=/path/to/server-ca.pem
./hack/install-ate.sh --deploy-ate-system
```
