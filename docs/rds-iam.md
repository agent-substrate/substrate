# RDS / Aurora IAM database authentication

`ate-api-server` can authenticate to Amazon RDS or Aurora PostgreSQL with IAM
instead of a static password. As with Cloud SQL, a sidecar owns the cloud
credential: `ateapi` itself carries no AWS code.

RDS has no auth proxy, so the `rds-iam-token-refresher` sidecar
(`manifests/ate-install/rds/token-refresher-patch.yaml`) mints an IAM auth
token with the pod's AWS identity every five minutes (tokens last 15) and
writes it atomically to a libpq passfile at `/run/rds-iam/pgpass` on a shared
in-memory volume. `ateapi` connects to the database directly over TLS; its
connection string names the passfile, and it re-reads the passfile for every
new connection.

## Prerequisites

- Kubernetes 1.29+ (native sidecars).
- IAM database authentication enabled on the instance or cluster, and a
  database user granted `rds_iam`.
- An IAM role with `rds-db:connect` for that user, assumed by the
  `ate-api-server` service account through IRSA or EKS Pod Identity.
- The RDS CA bundle, which `verify-full` checks the server against.

## Install

```sh
export ATE_API_POSTGRES_AWS_IAM_AUTH=true
export ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING='postgres://ate_api@mydb.cluster-abc.eu-west-1.rds.amazonaws.com:5432/atepg?sslmode=verify-full&sslrootcert=/run/postgres-server-ca/server-ca.pem'
export ATE_API_POSTGRES_SERVER_CA_FILE=./global-bundle.pem
export ATE_API_POSTGRES_AWS_IAM_ROLE_ARN=arn:aws:iam::123456789012:role/ate-api  # IRSA only
hack/install-ate.sh
```

The connection string must use TLS and carry no password. If you set
`ATE_API_POSTGRES_OWNER_CONNECTION_STRING` too, it must use the same host, port
and user, since one token serves both pools; the pools differ by role. `ate-setup` appends
`passfile=/run/rds-iam/pgpass` to it, derives the sidecar's host, port and user
from it, and adds the sidecar.

The sidecar takes the region from its own `AWS_REGION`, which the IRSA webhook
injects. With EKS Pod Identity, set `AWS_REGION` on the sidecar yourself.

A redeploy that does not export `ATE_API_POSTGRES_AWS_IAM_AUTH` keeps the
existing configuration; `false` removes the sidecar. IAM auth and Cloud SQL
are mutually exclusive, and the bundled PostgreSQL is skipped.

## Failure modes

- The sidecar's startup probe gates `ateapi` on the first token.
- If token minting keeps failing, the passfile goes stale after 10 minutes and
  the sidecar's liveness probe restarts it. Established connections keep
  working; only new connections need a fresh token.
- The sidecar calls AWS STS, which egress policy must allow.
