# substrate

Helm chart for installing Agent Substrate.

The chart uses mTLS and requires a prepared PostgreSQL database. It requires the
`ClusterTrustBundle`, `ClusterTrustBundleProjection`, and
`PodCertificateRequest` feature gates plus the `certificates.k8s.io/v1beta1`
API.

Install the [certificate controller](../substrate-podcert/README.md) as a separate
release before preparing PostgreSQL or installing this chart. One controller
serves the cluster; the application chart does not deploy it.

```bash
# CRDs
helm upgrade --install substrate-crds ./charts/substrate-crds

# Create CA pools, then install their certificate controller
go run ./cmd/ate-setup create podcertificate-controller-cas
helm upgrade --install substrate-podcert ./charts/substrate-podcert \
  --namespace podcertificate-controller-system --create-namespace --wait

# Install atelet as its own release, using the bundled RustFS
kubectl create namespace ate-system
kubectl create secret generic rustfs-credentials -n ate-system \
  --from-literal=AWS_ACCESS_KEY_ID=rustfsadmin \
  --from-literal=AWS_SECRET_ACCESS_KEY=rustfsadmin
helm upgrade --install substrate-atelet ./charts/substrate-atelet \
  --namespace ate-system \
  --set objectStore.endpoint=http://rustfs.ate-system.svc:9000 \
  --set objectStore.region=us-east-1 \
  --set objectStore.pathStyle=true \
  --set objectStore.credentialsSecret=rustfs-credentials

# Install Substrate after creating the database identities and Secrets
helm upgrade --install substrate ./charts/substrate \
  --namespace ate-system \
  --set postgres.readWriteConnectionStringSecretRef.name=substrate-postgres-readwrite \
  --set postgres.ownerConnectionStringSecretRef.name=substrate-postgres-owner
```

Install substrate-atelet before substrate, and upgrade them in that order. The
[version skew policy](../../docs/reference/version-skew-policy.md) upgrades
atelet before ate-api-server.

By default, component images are pulled from `ghcr.io/agent-substrate/substrate`
using the chart `appVersion` as the tag. Override `image.registry` and
`image.tag` to install from a different image repository or tag.

The chart installs the Kubernetes credential provider and enables HTTPS egress
interception. Create the `egress-mitm-ca-pool` Secret with
`go run ./cmd/ate-setup create egress-mitm-ca-pool`, and configure actor trust
as described in [egress credential injection](../../docs/guides/egress-credential-injection.md).
Namespace grants default to an empty list, denying credential access.

## Render manifests without applying

```bash
helm template substrate ./charts/substrate
```

On OpenShift the chart leaves pod UIDs and fsGroups for the SCC to assign. It
detects OpenShift from the cluster's API versions, which `helm template` cannot
see, so add `--api-versions security.openshift.io/v1` when rendering for it.

When you upgrade on OpenShift, remove an `anyuid` grant made for RustFS. `anyuid`
takes priority over restricted-v2 and assigns no fsGroup, so RustFS cannot write
a new volume. Kubelet moves existing RustFS data to the fsGroup restricted-v2
assigns. Storage that skips that ownership change, as many NFS setups do, leaves
the data writable only by UID 10001: keep the `anyuid` grant there, so RustFS
still runs as that user.

The separate `substrate-crds` chart mirrors `manifests/ate-install/generated/`.

## Values

See `values.yaml` for the full set; the important keys:

| Key | Default | Notes |
|-----|---------|-------|
| `postgres.readWriteConnectionStringSecretRef` | `substrate-postgres-readwrite` | Read the read/write connection from a pre-created Secret |
| `postgres.ownerConnectionStringSecretRef` | `substrate-postgres-owner` | Read the owner connection from a pre-created Secret |
| `postgres.readWriteRole` | `substrate_readwrite` | Role assumed by read/write connections |
| `postgres.ownerRole` | `substrate_owner` | Role assumed by owner connections |
| `preview` | `[]` | Preview gates for the API server, controller and workers; set the same gates in the substrate-atelet release. Set `[ExternalVolumes]` to enable external volume APIs and mounts |
| `postgres.schema` | `substrate` | Store the Substrate tables in this PostgreSQL schema |
| `postgres.clientCertificates.enabled` | `false` | Project separate owner and runtime login certificates into the API server |
| `<component>.resources` | see `values.yaml` | Requests and limits for each container, e.g. `ateApi.resources` and `atenetRouter.agentgateway.resources` |
| `atenetRouter.requestParking.budget` | `60s` | Total budget for actor resolution and resume retries |
| `atenetRouter.requestTimeout` | `60s` | Timeout for each Substrate ingress API call, including actor resolution |
| `atenetEgress.extraCredentialProviders` | `[]` | Credential providers you deploy, added after the bundled `k8s.io` provider, each `{name, address, cacheTtl}`. `name` is the host of the `ate-secret://` URIs it serves; `address` is the `host:port` the egress gateway dials over mTLS. Its serving certificate must chain to the servicedns CA. Optional `cacheTtl` sets how long the gateway reuses a fetched credential (default `5m`; `0s` disables caching). See [egress credential injection](../../docs/guides/egress-credential-injection.md#the-plugin-api) |
| `atenetEgress.agentgateway.logLevel` | `info` | Egress proxy `RUST_LOG` filter, e.g. `debug` or `info,agentgateway=debug` when troubleshooting |
| `rustfs.enabled` | `true` | Deploy an in-cluster S3-compatible RustFS bucket for snapshots |
| `ateApi.snapshotLocation` | `""` | Cluster-wide snapshot URI; defaults to `gs://<rustfs.bucket>/` with bundled RustFS |
| `atelet.storageBackend` | `s3` | Default snapshot backend, wired to RustFS when `rustfs.enabled=true`. Set `objectStore.backend` in the substrate-atelet release to the same value |
| `credentialProvider.enabled` | `true` | Deploy the Kubernetes Secret provider and route `k8s.io` credential URIs to it. With no provider, every credential injection fails closed |
| `credentialProvider.namespacePolicies` | `[]` | Default-deny atespace-to-namespace grants; the chart includes get-only Secret RBAC for the provider |
| `ateApi.actorJWTIssuer` | `https://<idp-service>.<namespace>.svc` | Issuer placed in actor JWTs; set a stable URL reachable by relying parties |
| `objectStorePlugin.extraEnv` | `[]` | Object storage credentials/settings for the API server's object-store plugin. Set the same values in the substrate-atelet release |
| `objectStorePlugin.control.resources` | see `values.yaml` | Resources for the API server's object-store sidecar |
| `ateApi.logLevel` | `info` | API server log level in its mounted config; one of `debug`, `info`, `warn`, or `error` |
| `ateApi.authorization.bootstrapOwners` | `[]` | Global owners; empty defaults to the release's shared `ate-client` ServiceAccount |
| `ateApi.extraArgs` | `[]` | Additional command-line arguments appended to the ateapi defaults |
| `otel.endpoint` | `""` | Set to an OTLP endpoint to export traces, metrics, the actor lifecycle events and the router access log |
| `otel.traces.enabled` | `true` | Set to `false` to export no traces from the Go components and the router |
| `otel.traces.endpoint` | `""` | OTLP endpoint for traces, overriding `otel.endpoint` |
| `otel.traces.samplingRatio` | `0.01` | Fraction of parentless requests that start a trace, applied to the Go components and the router |
| `otel.metrics.enabled` | `true` | Set to `false` to disable OTLP metrics push; Prometheus scraping remains available |
| `otel.metrics.endpoint` | `""` | OTLP endpoint for metrics, overriding `otel.endpoint` |
| `otel.logs.enabled` | `true` | Enable OTLP actor events from ateapi and the ateworkers, plus the router access log. Actor events go to stdout when OTLP logs are disabled |
| `otel.logs.endpoint` | `""` | OTLP endpoint for logs, overriding `otel.endpoint` |

## Router timeouts

The router uses `ResumeActor` to resolve actor assignments, including actors
that already run. `atenetRouter.requestTimeout` sets the HTTP backend request
timeout for those API calls. `atenetRouter.requestParking.budget` bounds the
resume attempts and retry delays. Both apply to direct and CONNECT-tunneled
HTTP/gRPC routes. Both default to `60s`, allowing each attempt to use the
remaining parking budget.

A resume must finish within both the per-call timeout and the remaining parking
budget. A shorter client deadline can still cancel it. Set both values as
duration strings when slow restores need more time, for example:

```yaml
atenetRouter:
  requestParking:
    budget: "120s"
  requestTimeout: "120s"
```

Choose limits using measured restore latency. A parking budget of `0s` uses
agentgateway's default budget; it does not disable the limit. These settings
govern API calls during routing, not the actor's application request duration.

## PostgreSQL setup

The chart does not deploy or initialize PostgreSQL. Prepare the database,
schema, login users, and the configured `readWriteRole` and `ownerRole` before
installing the chart. For development Helm installations in `ate-system`,
run `hack/install-postgres.sh` after installing the separate certificate
controller and its CA pools. It deploys certificate-protected PostgreSQL,
runs the shared bootstrap SQL, and creates both connection Secrets. Use `--kind`
for the smaller Kind deployment; set `KUBECTL_CONTEXT` to select a cluster.
Standalone manifest installations can continue to use `ate-setup`.

When using `hack/install-postgres.sh`, install the application chart with
`--set postgres.clientCertificates.enabled=true`. Its connection Secrets use
the projected owner and runtime certificates. For external databases using
password or provider-managed authentication, leave this setting disabled.
Service-DNS and pod-identity certificates remain enabled for Substrate's
internal connections. Standalone manifest generation enables the PostgreSQL
certificates for the bundled development database.

For an operator-managed database, the operator must provision the identities,
schema, and grants, and create both connection Secrets in the release namespace.

Substrate runs `SET ROLE` for each new connection. It rejects a login without
the required membership.

Create both group roles and all grants before installation, then set
`postgres.readWriteRole` and `postgres.ownerRole` to those names. Use distinct
roles and table schemas for separate installs sharing one database. Give each
install separate logins and grant each login membership only in its install's
roles.
