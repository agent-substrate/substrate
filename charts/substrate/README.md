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

# Install atelet as its own release. object-store.yaml holds the
# objectStorePlugin values for your object store: see "Object store".
kubectl create namespace ate-system
helm upgrade --install substrate-atelet ./charts/substrate-atelet \
  --namespace ate-system \
  -f object-store.yaml

# Create the key pools and the API server authentication config.
# Pass --namespace when the release namespace is not ate-system.
go run ./cmd/ate-setup create actor-id-ca-pool
go run ./cmd/ate-setup create actor-id-ca-certs
go run ./cmd/ate-setup create jwt-authority-pool
go run ./cmd/ate-setup create egress-mitm-ca-pool
go run ./cmd/ate-setup create api-authentication-config

# Install Substrate after creating the database identities and Secrets
helm upgrade --install substrate ./charts/substrate \
  --namespace ate-system \
  -f object-store.yaml \
  --set atenetDataPlane=<envoy|agentgateway> \
  --set ateApi.snapshotLocation=gs://<bucket>/ \
  --set postgres.readWriteConnectionStringSecretRef.name=substrate-postgres-readwrite \
  --set postgres.ownerConnectionStringSecretRef.name=substrate-postgres-owner
```

On a kind cluster, pass the values files in `charts/scenarios/kind-envoy` or
`charts/scenarios/kind-agentgateway`, for the data plane you want, to the
matching releases: `substrate-atelet-values.yaml` and `substrate-values.yaml`.
Run `hack/install-postgres.sh --kind` and `hack/install-rustfs.sh` before the
releases.

Install substrate-atelet before substrate, and upgrade them in that order. The
[version skew policy](../../docs/reference/version-skew-policy.md) upgrades
atelet before ate-api-server.

By default, component images are pulled from `ghcr.io/agent-substrate/substrate`
using the chart `appVersion` as the tag. The charts have `appVersion` 0.1.0,
which names no image, so set `image.tag` to a release tag. Override
`image.registry` and `image.tag` to install from a different image repository
or tag.

## Data plane

`atenetDataPlane` selects the data plane of the router and the egress gateway:
`envoy` or `agentgateway`. It has no default, and the render fails without it.
`ate-setup` installs envoy unless told otherwise. The envoy data plane uses the
`envoy-dataplane` and `atenet` component images.

Some values apply to only one data plane:

- agentgateway only: `atenetRouter.requestParking`, `atenetRouter.requestTimeout`,
  `atenetEgress.agentgateway.logLevel` and the `cacheTtl` of
  `atenetEgress.extraCredentialProviders`. With envoy, a `cacheTtl` fails the
  render.
- envoy only: `atenetRouter.router`, `atenetRouter.envoy`, `atenetEgress.envoy`,
  `atenetEgress.extProc` and `atenetEgress.sdsmint`, which set the resources of
  those containers.

## Object store

The chart does not include an object store. The object-store plugin, a sidecar
of the API server and of atelet, connects to one. Configure the plugin through
its environment, with the same values in the substrate and substrate-atelet
releases:

- `objectStorePlugin.extraEnv`: `ATE_STORAGE_BACKEND` selects `gcs`, the
  default, or `s3`. The S3 backend reads the AWS SDK settings, such as
  `AWS_REGION`, `AWS_ENDPOINT_URL` and `AWS_S3_USE_PATH_STYLE`.
- `objectStorePlugin.credentialsSecret`: a Secret whose keys the plugin gets as
  environment variables, such as `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY`.

`ateApi.snapshotLocation` names the bucket, as `gs://<bucket>/<prefix>` for both
backends.

For workload identity that reads ServiceAccount annotations, such as EKS IAM
roles for service accounts, set `ateApi.serviceAccount.annotations` here and
`atelet.serviceAccount.annotations` in the substrate-atelet release. The pods
get the identity when they start, so restart ate-api-server and atelet after
you change the annotations. GKE Workload Identity Federation needs no
annotations: grant access to the ServiceAccounts as IAM principals. For
workload identity that also needs pod labels, such as Azure Workload Identity,
set `ateApi.podLabels` and `atelet.podLabels`.

To run another plugin, set `objectStorePlugin.image`, and set
`objectStorePlugin.control.args` and `objectStorePlugin.control.startupProbe`
here and `objectStorePlugin.node.args` and `objectStorePlugin.node.startupProbe`
in the substrate-atelet release. Empty args and probes use the ones of the
in-tree plugin. The chart owns the socket: the plugin must serve the
`objectstore.v1` API on `/run/objectstore-plugin/control.sock` in the API
server pods and on `/run/objectstore-plugin/node.sock` in the atelet pods.
`objectStorePlugin.extraVolumes` and `objectStorePlugin.extraVolumeMounts` add
volumes for the plugin.

For development, `hack/install-rustfs.sh` deploys RustFS in `ate-system` from
`manifests/ate-install/kind/rustfs.yaml`, with an `ate-snapshots` bucket and the
`rustfs-credentials` Secret. The kind scenarios in `charts/scenarios` set the
values for it. RustFS keeps its data in the `rustfs-data` PersistentVolumeClaim,
so applying the manifest again keeps the snapshots in an existing claim of that
name.

## microVM sandboxes

The chart installs the class-default SandboxConfigs: `gvisor-default` unless
`sandboxConfigs.gvisor.enabled` is false, and `microvm` when
`sandboxConfigs.microvm.enabled` is true. Nodes for microVM workers need
`/dev/kvm`.

atelet fetches the microVM assets (cloud-hypervisor, virtiofsd, the guest
kernel and the guest image) from `sandboxConfigs.microvm.assetBucket`, under
`microvm-assets/<arch>/<sha256>`. The chart does not upload them. After the
install, run:

```bash
go run ./cmd/ate-setup deploy sandboxconfig --sandbox-classes=microvm
```

It assembles and verifies the assets, uploads them, and applies the same
`microvm` SandboxConfig. On kind it uploads to the RustFS bucket, so set
`assetBucket: s3://ate-snapshots`. Elsewhere it uploads to
`<bucket>-assets` in GCS, where `<bucket>` is its `--storage-bucket-name`, so set
`assetBucket: gs://<bucket>-assets`.

A `microvm` SandboxConfig that `ate-setup` applied before the chart managed it
is not part of the release, and the install refuses to replace it. Let Helm
adopt it with `helm upgrade --take-ownership`, which needs Helm 3.17 or later.

The API server and the egress gateway do not start until the objects from
`ate-setup create` exist. The chart reads them by their fixed names:
`actor-id-ca-pool`, `actor-id-ca-certs`, `actor-id-jwt-pool`,
`egress-mitm-ca-pool` and `ate-api-authentication`.

The chart installs the Kubernetes credential provider and enables HTTPS egress
interception with the `egress-mitm-ca-pool` Secret. Configure actor trust as
described in [egress credential injection](../../docs/guides/egress-credential-injection.md).
Namespace grants default to an empty list, denying credential access.

## Render manifests without applying

```bash
helm template substrate ./charts/substrate --set atenetDataPlane=envoy
```

The separate `substrate-crds` chart mirrors `manifests/ate-install/generated/`.

## Values

See `values.yaml` for the full set; the important keys:

| Key | Default | Notes |
|-----|---------|-------|
| `postgres.readWriteConnectionStringSecretRef` | `substrate-postgres-readwrite` | Read the read/write connection from a pre-created Secret |
| `postgres.ownerConnectionStringSecretRef` | `substrate-postgres-owner` | Read the owner connection from a pre-created Secret |
| `postgres.readWriteRole` | `substrate_readwrite` | Role assumed by read/write connections |
| `postgres.ownerRole` | `substrate_owner` | Role assumed by owner connections |
| `preview` | `[]` | Preview gates for every substrate component in the release; set the same gates in the substrate-atelet release. `[ALL]` enables every gate. Set `[ExternalVolumes]` to enable external volume APIs and mounts |
| `postgres.schema` | `substrate` | Store the Substrate tables in this PostgreSQL schema |
| `postgres.clientCertificates.enabled` | `false` | Project separate owner and runtime login certificates into the API server |
| `<component>.resources` | see `values.yaml` | Requests and limits for each container, e.g. `ateApi.resources` and `atenetRouter.agentgateway.resources` |
| `atenetDataPlane` | none, required | Data plane of the router and the egress gateway: `envoy` or `agentgateway`. See "Data plane" |
| `atenetRouter.requestParking.budget` | `60s` | agentgateway only. Total budget for actor resolution and resume retries |
| `atenetRouter.requestTimeout` | `60s` | agentgateway only. Timeout for each Substrate ingress API call, including actor resolution |
| `atenetEgress.extraCredentialProviders` | `[]` | Credential providers you deploy, added after the bundled `k8s.io` provider, each `{name, address, cacheTtl}`. `name` is the host of the `ate-secret://` URIs it serves; `address` is the `host:port` the egress gateway dials over mTLS. Its serving certificate must chain to the servicedns CA. Optional `cacheTtl`, for agentgateway only, sets how long the gateway reuses a fetched credential (default `5m`; `0s` disables caching). See [egress credential injection](../../docs/guides/egress-credential-injection.md#the-plugin-api) |
| `atenetEgress.agentgateway.logLevel` | `info` | agentgateway only. Egress proxy `RUST_LOG` filter, e.g. `debug` or `info,agentgateway=debug` when troubleshooting |
| `sandboxConfigs.gvisor.enabled` | `true` | Install the class-default gVisor SandboxConfig, `gvisor-default` |
| `sandboxConfigs.microvm.enabled` | `false` | Install the class-default microVM SandboxConfig, `microvm`. See "microVM sandboxes" |
| `sandboxConfigs.microvm.assetBucket` | `""` | Bucket that holds the microVM assets, as `s3://<bucket>` or `gs://<bucket>`. Required with microVM |
| `ateApi.snapshotLocation` | none, required | Cluster-wide snapshot URI, `gs://<bucket>/<prefix>` for both backends. Optional with the `PerTemplateSnapshotLocation` preview gate. See "Object store" |
| `credentialProvider.enabled` | `true` | Deploy the Kubernetes Secret provider and route `k8s.io` credential URIs to it. With no provider, every credential injection fails closed |
| `credentialProvider.namespacePolicies` | `[]` | Default-deny atespace-to-namespace grants; the chart includes get-only Secret RBAC for the provider |
| `ateApi.actorJWTIssuer` | `https://<idp-service>.<namespace>.svc` | Issuer placed in actor JWTs; set a stable URL reachable by relying parties |
| `objectStorePlugin.extraEnv` | `[]` | Environment of the object-store plugin, which selects and configures the object store. Set the same values in the substrate-atelet release. See "Object store" |
| `objectStorePlugin.credentialsSecret` | `""` | Secret whose keys the object-store plugin gets as environment variables. Set the same value in the substrate-atelet release |
| `ateApi.serviceAccount.annotations` | `{}` | Annotations on the ate-api-server ServiceAccount, for workload identity. See "Object store" |
| `ateApi.podLabels` | `{}` | Labels on the ate-api-server pods, for workload identity. See "Object store" |
| `objectStorePlugin.image` | `""` | Image of another object-store plugin, as a full reference. Empty uses the in-tree plugin. `global.imageRegistry` replaces its registry. See "Object store" |
| `objectStorePlugin.control.args` | `[]` | Arguments of the API server's object-store plugin. Empty uses the in-tree plugin's arguments |
| `objectStorePlugin.control.startupProbe` | `{}` | Startup probe of the API server's object-store plugin. Empty uses the in-tree plugin's probe |
| `objectStorePlugin.extraVolumes` | `[]` | Volumes added to the API server pods for the object-store plugin |
| `objectStorePlugin.extraVolumeMounts` | `[]` | Volume mounts added to the object-store plugin container |
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

These settings apply to the agentgateway data plane. The router uses `ResumeActor` to resolve actor assignments, including actors
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

To verify the server certificate of an external database, create the
`postgres-server-ca` Secret in the release namespace with the CA in the
`server-ca.pem` key. The API server mounts it at `/run/postgres-server-ca`. Add
`sslmode=verify-ca&sslrootcert=/run/postgres-server-ca/server-ca.pem` to both
connection strings. For Cloud SQL, get the CA with
`gcloud sql ssl server-ca-certs list --instance=<name> --format="value(cert)"`.

Substrate runs `SET ROLE` for each new connection. It rejects a login without
the required membership.

Create both group roles and all grants before installation, then set
`postgres.readWriteRole` and `postgres.ownerRole` to those names. Use distinct
roles and table schemas for separate installs sharing one database. Give each
install separate logins and grant each login membership only in its install's
roles. With `postgres.clientCertificates.enabled`, the logins are always
`substrate_owner_user` and `substrate_readwrite_user`, so installs that share a
database also share the logins.
