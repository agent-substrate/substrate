# ate-setup commands

Every `ate-setup` command, and the common global flags.
`go run ./cmd/ate-setup --help` lists them all.

```
go run ./cmd/ate-setup [global flags] <command> [flags]
```

`ate-setup` runs exactly one command per invocation.

## Global flags

These configure every command, and may appear before or after it, except
`--version`, which takes no command.
[`docs/guides/operator-install.md`](../../docs/guides/operator-install.md#settings) lists
every setting with its environment variable.

| Flag | Notes |
|---|---|
| `--kind` | Kind overlays, the local registry, and host-architecture image builds. `ATE_INSTALL_KIND=true` when the flag is absent |
| `--atenet-dataplane envoy\|agentgateway` | atenet ingress and egress dataplane (default `envoy`) |
| `--atenet-route-timeout DURATION` | The router's `--route-timeout` (default `5m`). Envoy dataplane only. `ATE_ATENET_ROUTE_TIMEOUT` when the flag is absent |
| `--atenet-parked-request-budget DURATION` | The router's `--parked-request-budget` (default `5s`); the router pod's `terminationGracePeriodSeconds` grows with it. Envoy dataplane only. `ATE_ATENET_PARKED_REQUEST_BUDGET` when the flag is absent |
| `--atenet-extproc-max-requests N` | The router's `--extproc-max-requests`, the circuit breaker on Envoy's ext_proc cluster (default: twice `--parked-request-max`, at least `1024`). Envoy dataplane only. `ATE_ATENET_EXTPROC_MAX_REQUESTS` when the flag is absent |
| `--rollout-timeout DURATION` | Readiness timeout for workloads (default `60s`). It also governs the podcertificate-controller and CSI waits, which stay at their 120s default until it is passed |
| `--podcert-workers-per-signer N` | Concurrent workers per podcertificate-controller signer |
| `--cluster-size size0\|size10` | Footprint profile (default `size0`). `size10` assumes a dedicated PostgreSQL node: it resizes the bundled StatefulSet and its `postgresql.conf`, pins the apiserver's connection pool, and raises the podcertificate-controller's API rate limits. `ATE_INSTALL_CLUSTER_SIZE` when the flag is absent |
| `--ateapi-postgres-storage-class NAME` | StorageClass for the bundled PostgreSQL volume instead of the cluster default, such as `dynamic-rwo` on GKE so C4 and N4 nodes can attach it. An existing install must delete the postgres StatefulSet and its claim first. `ATE_API_POSTGRES_STORAGE_CLASS` when the flag is absent |
| `--cordon-system-infra-components` | Keep the system infra components, such as ate-api-server, atenet-router, atenet-egress and postgres, off the worker nodes. Assumes a small shared pool labeled and tainted `ate.dev/workloadType=ate-system-infra:NoSchedule`, across which each workload's replicas are spread, and a one-node pool labeled and tainted `ate.dev/workloadType=ate-postgres:NoSchedule` for postgres alone. `ATE_INSTALL_CORDON_SYSTEM_INFRA_COMPONENTS=true` when the flag is absent |
| `--fake-data-plane` | Install the fake data plane for control-plane benchmarks: fake-atelet takes atelet's place in its DaemonSet, taking its objectstore-plugin's storage settings, ate-controller is scaled to zero, and fake-workersync honors every WorkerPool with fake Workers on the fake-atelet nodes. Reaches `deploy ate-system`, `deploy atelet`, and `deploy ate-controller`; only `deploy ate-system` switches a cluster between the real and fake data planes, and the other two refuse a data plane other than the installed one. `deploy ate-system --fake-data-plane` refuses a cluster whose WorkerPools have worker pods. fake-atelet runs with its defaults; a benchmark tunes it through the optional `fake-atelet-tuning` ConfigMap, whose `FAKE_ATELET_*` variables set its flags, and which no install creates or resets. Actors do not exist; never use it on a cluster that serves real actors. `ATE_INSTALL_FAKE_DATA_PLANE=true` when the flag is absent |
| `--ingress-allowed-spiffe-ids IDS` | Comma-separated SPIFFE IDs (exact match) of the clients the ingress router lets reach actors; any listed client may reach any actor. Unset, no client may reach actors through the router. Both dataplanes require a Pod Identity CA client certificate on HTTPS and TLS CONNECT. The SPIFFE URI SAN allowlist may be checked during or after TLS, including after CONNECT acceptance, but unlisted clients must be denied before actor routing. The podidentity signer issues `spiffe://cluster.local/ns/<namespace>/sa/<service-account>`. `ATE_INGRESS_ALLOWED_SPIFFE_IDS` when the flag is absent |
| `--egress-gateway-extra-spiffe-ids IDS` | Comma-separated SPIFFE IDs that hold ate-api-server's `egress_gateway` role next to atenet-egress, such as a credential provider that mints actor JWTs. ate-setup passes atenet-egress's ID and then each listed ID as repeated `--egress-gateway-spiffe-id` flags; unset passes none, leaving ate-api-server's default. `ATE_EGRESS_GATEWAY_EXTRA_SPIFFE_IDS` when the flag is absent |
| `--experimental-additional-egress-extproc-service NS/SVC:PORT` | External processor authorization filter |
| `--credential-provider JSON` | Required by `deploy ate-system` and `deploy atenet`. A JSON object: `{"name":"k8s.io"}` deploys and uses the bundled Kubernetes Secrets provider, with a NetworkPolicy that admits only the egress gateway; `{"enabled":false}` turns egress credential injection off; `{"name":"<provider>","address":"<host>:<port>"}` uses a provider you deploy yourself. See [`docs/guides/egress-credential-injection.md`](../../docs/guides/egress-credential-injection.md). `ATE_CREDENTIAL_PROVIDER` when the flag is absent |
| `--ateapi-log-level LEVEL` | ate-api-server log level: `debug`, `info`, `warn`, or `error`. Written to the `ate-api-config` ConfigMap on every deploy when set; unset keeps the installed level (`info` on a new install). The server picks up a change without restarting. `ATE_API_LOG_LEVEL` when the flag is absent |
| `--otlp-endpoint URL` | Send control plane telemetry to `URL` instead of the cluster default (see [`benchmarking/telemetry/README.md`](../../benchmarking/telemetry/README.md)). `ATE_OTLP_ENDPOINT` when the flag is absent |
| `--preview GATES` | Preview gates passed as `--preview` to every substrate component: comma-separated gate names, or `ALL` for all. `ATE_PREVIEW` or `config.preview` when the flag is absent. An explicit `--preview=''` turns them all off, even when `ATE_PREVIEW` is set |
| `--context NAME` | Kubeconfig context. `KUBECTL_CONTEXT` when the flag is absent |
| `--kubeconfig PATH` | Explicit kubeconfig path. `KUBECONFIG` when the flag is absent |
| `--no-dev-env` | Skip `.ate-dev-env.sh` at the repository root. `NO_DEV_ENV=1` when the flag is absent |
| `--version` / `-v` | Print the version |
| `--image-repo REPO` | Install pre-built images from `REPO` instead of building them with `ko` |
| `--image-tag TAG` | The tag those images carry. Each of the two requires the other |

Both have an environment equivalent, read when the flag is absent:
`ATE_IMAGE_REPO` and `ATE_IMAGE_TAG`.

Kind deployments check that Docker is available and the resolved kubeconfig
context exists before doing any install work. If the context is missing, the
error explains how to export kubeconfig for an existing cluster or create the
cluster and local registry with `hack/create-kind-cluster.sh`. That script
replaces an existing cluster with the same name.

Deploy the control plane before deploying demos or benchmarks. These commands
check for `deployment/ate-controller` in the configured system namespace before
creating workload resources. An existing Deployment passes this check; normal
rollout waits still diagnose workloads that cannot become ready.

## Installing pre-built images

Without `--image-repo`, `ate-setup` builds every image from the checkout with
`ko` and pushes it to `KO_DOCKER_REPO`. That is the developer install and is
unchanged.

```
ate-setup deploy ate-system \
  --storage-bucket-name=BUCKET \
  --credential-provider='{"name":"k8s.io"}' \
  --ingress-allowed-spiffe-ids=IDS \
  --image-repo REPO \
  --image-tag TAG
```

installs images already published to `REPO` instead, and never invokes `ko`.
The manifests still come from the checkout, so this needs one; what it removes
is the build and write access to a registry. Releases ship no `ate-setup`
binary, so running it from the checkout still needs Go. On non-Kind clusters,
the default `--sandbox-classes=gvisor,microvm` also downloads the microVM guest
assets from GitHub and uploads them with `gcloud` to `<BUCKET>-assets` (created
with `atelet` read access by `go run ./tools/setup-gcp create bucket` and
`create iam`); pass `--sandbox-classes=gvisor` to install gVisor only without
`--storage-bucket-name` or `gcloud`.

`REPO` has to hold every image the install deploys, all under `TAG`, including
`envoy-dataplane`, which a build from source builds from a Dockerfile with
`docker buildx` (so that build needs `docker` as well as `ko`). An install that
needs an image `REPO` lacks fails naming it.

The images a release publishes to `ghcr.io/agent-substrate/substrate`, with
`publish release-images`, leave out the demos and the benchmarking fakes; see
[`docs/dev/releasing.md`](../../docs/dev/releasing.md#release-images). When an
image releases do not publish is missing from `REPO`, the error says so.

Each reference is pinned to the digest its tag names, which takes one HEAD
request per image, so the installer needs read access to `REPO` and not only the
cluster does.

That read is authenticated with the docker config file and the credential
helpers it names, plus gcloud's own credentials for GCR and Artifact Registry:
Application Default Credentials, falling back to the `gcloud` CLI. Installing
from GCR or Artifact Registry therefore needs no `~/.docker/config.json` entry. Amazon ECR
and Azure Container Registry need credential-helper modules this repository does
not depend on, so those registries need a `docker login` first.

`TAG` may itself carry a digest, as in `--image-tag v0.0.0@sha256:...`. A tag
that already names a manifest is used as written, and is not looked up.

## Deploy

- `deploy ate-system`
- `deploy ate-system --setup-csi=nfs`
- `deploy ate-system --sandbox-classes=gvisor`
- `deploy atelet`
- `deploy apiserver`
- `deploy ate-controller`
- `deploy atenet`
- `deploy podcertificate-controller`
- `deploy sandboxconfig [--sandbox-classes=...]`
- `deploy postgres`

`deploy ate-system` is the whole control plane: CRDs, RBAC, the store, the
apiserver, the controller, atenet, and atelet. By default
(`--sandbox-classes=gvisor,microvm`, also accepted by `deploy sandboxconfig`),
it installs the cluster-wide `gvisor-default` and `microvm` `SandboxConfig`
resources, staging the microVM guest assets into the asset bucket (`rustfs` on
Kind, `<BUCKET_NAME>-assets` on GKE). It creates every `create` resource below
on the way, so those subcommands are only needed to redo one on a running
cluster.

## Publish

- `publish worker-images`
- `publish release-images`
- `publish release-binaries`

`publish worker-images` builds and pushes the worker images for the
checked-out build and prints their refs; a WorkerPool points `spec.workerImage`
to a build to use the ateworker.

`publish release-images` builds and pushes every image a release publishes,
all tagged with the build version; see [Installing pre-built
images](#installing-pre-built-images).

`publish release-binaries` builds `kubectl-ate` for linux and darwin on amd64
and arm64 into `--out` (default `bin/release`), stamped with the build version, commit,
and commit time, each with a `.sha256` file that `shasum -a 256 -c` checks. The
version has to be a release tag. It uploads nothing; the release workflow
attaches the files to the draft release.

## Delete

- `delete ate-system`
- `delete atenet`
- `delete all`

`delete all` removes every registered demo and then the control plane.

## Create

Individual secrets and config that `deploy ate-system` creates automatically.

- `create jwt-authority-pool`
- `create actor-id-ca-pool`
- `create actor-id-ca-certs`
- `create egress-mitm-ca-pool`
- `create podcertificate-controller-cas`
- `create api-server-env-vars`
- `create api-authentication-config`
- `create api-config`

## Setup

- `setup csi [driver]`

`driver` is one of `nfs`, `hostpath`, `both`, or `none`, and defaults to `nfs`. The hostpath is for KIND clusters only. NFS has no such
restriction, but it does need the `nfsd` kernel module loaded on the nodes.


## Benchmarks

- `deploy benchmarks`
- `delete benchmarks`
- `--worker-count N` (default `1`)
- `--sandbox-class gvisor|microvm` (default `gvisor`)
- `--benchmark-actor-memory SIZE` (default `256Mi`)

`--benchmark-actor-memory` is a global flag (`BENCHMARK_ACTOR_MEMORY` when it
is absent); the other two are flags of the benchmark commands. See
[`benchmarking/README.md`](../../benchmarking/README.md).

## Demos

- `deploy demo NAME`
- `delete demo NAME`

`NAME` is one of:

| Command | Description |
|---|---|
| `deploy demo counter` | A counter actor exercising snapshot, resume, and atenet ingress |
| `deploy demo counter --with-external-volume [--storage-class NAME]` | The same, plus an external volume and a pre-seeded file to validate (requires `--preview=ExternalVolumes`). Run `setup csi` first and name the class it created, e.g. `csi-nfs-sc`; defaults to `standard` |
| `deploy demo counter-microvm` | The counter demo on microVM workers |
| `deploy demo egress` | Egress policy enforcement through atenet; the actors trust the egress gateway's CA |
| `deploy demo egress-microvm` | The same on microVM workers |
| `deploy demo jupyter` | A Jupyter notebook server per actor, reached through atenet ingress |
| `deploy demo sandbox` | An on-demand sandbox actor driven by the sandbox client |
| `deploy demo multi-template` | Two ActorTemplates sharing one WorkerPool |
| `deploy demo parking` | Actor parking and unparking on a small WorkerPool |
| `deploy demo claude-code-multiplex` | Several Claude Code agents multiplexed onto one WorkerPool (requires `ANTHROPIC_API_KEY`, `KO_DOCKER_REPO`) |

Each has a matching `delete demo NAME`. Demo flags bind
to the deploy side only; teardown never reads them.

The demo list is not hard-coded here — it is built from the registry in
[`internal/demos`](internal/demos), one package per demo, so
`go run ./cmd/ate-setup deploy demo --help` is authoritative for both the list
and the per-demo flags. A new demo is a new package there, added to
[`internal/demos/all`](internal/demos/all).
