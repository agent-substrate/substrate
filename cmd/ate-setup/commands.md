# ate-setup commands

Every `ate-setup` command alongside.

`ate-setup` is the installer. `hack/install-ate.sh` invokes it with the same
arguments.

```
go run ./cmd/ate-setup [global flags] <command> [flags]
```


## Global flags

`ate-setup` accepts these global flags.

| `ate-setup` | Notes |
|---|---|
| `--kind` | Kind overlays, the local registry, and host-architecture image builds |
| `--atenet-dataplane envoy\| `--atenet-dataplane envoy\|agentgateway` | atenet ingress and egress dataplane (default `envoy`) |
| `--rollout-timeout DURATION` | Readiness timeout for workloads (default `60s`). It also governs the podcertificate-controller and CSI waits |
| `--podcert-workers-per-signer N` | Concurrent workers per podcertificate-controller signer |
| `--cluster-size size0\| `--cluster-size size0\|size10` | Footprint profile (default `size0`). `size10` assumes a dedicated PostgreSQL node: it resizes the bundled StatefulSet and its `postgresql.conf`, pins the apiserver's connection pool, and raises the podcertificate-controller's API rate limits |
| `--ateapi-postgres-storage-class NAME` | StorageClass for the bundled PostgreSQL volume instead of the cluster default, such as `dynamic-rwo` on GKE so C4 and N4 nodes can attach it. An existing install must delete the postgres StatefulSet and its claim first |
| `--cordon-system-infra-components` | Keep the system infra components, such as ate-api-server, atenet-router, atenet-egress and postgres, off the worker nodes. Assumes a small shared pool labeled and tainted `ate.dev/workloadType=ate-system-infra:NoSchedule`, across which each workload's replicas are spread, and a one-node pool labeled and tainted `ate.dev/workloadType=ate-postgres:NoSchedule` for postgres alone |
| `--fake-data-plane` | Install the fake data plane for control-plane benchmarks: fake-atelet takes atelet's place in its DaemonSet, taking its objectstore-plugin's storage settings, ate-controller is scaled to zero, and fake-workersync honors every WorkerPool with fake Workers on the fake-atelet nodes. Reaches `deploy ate-system`, `deploy atelet`, and `deploy ate-controller`; only `deploy ate-system` switches a cluster between the real and fake data planes, and the other two refuse a data plane other than the installed one. `deploy ate-system --fake-data-plane` refuses a cluster whose WorkerPools have worker pods. fake-atelet runs with its defaults; a benchmark tunes it through the optional `fake-atelet-tuning` ConfigMap, whose `FAKE_ATELET_*` variables set its flags, and which no install creates or resets. Actors do not exist; never use it on a cluster that serves real actors |
| `--ingress-allowed-spiffe-ids IDS` | Comma-separated SPIFFE IDs (exact match) of the clients the ingress router lets reach actors; any listed client may reach any actor. Unset, no client may reach actors through the router. Both dataplanes require a Pod Identity CA client certificate on HTTPS and TLS CONNECT. The SPIFFE URI SAN allowlist may be checked during or after TLS, including after CONNECT acceptance, but unlisted clients must be denied before actor routing. The podidentity signer issues `spiffe://cluster.local/ns/<namespace>/sa/<service-account>` |
| `--experimental-additional-egress-extproc-service NS/SVC:PORT` | External processor authorization filter |
| `--credential-provider JSON` | Required by `deploy ate-system` and `deploy atenet`. A JSON object: `{"name":"k8s.io"}` deploys and uses the bundled Kubernetes Secrets provider, with a NetworkPolicy that admits only the egress gateway; `{"enabled":false}` turns egress credential injection off; `{"name":"<provider>","address":"<host>:<port>"}` uses a provider you deploy yourself. See [`docs/egress-credential-injection.md`](../../docs/egress-credential-injection.md) |
| `--ateapi-log-level LEVEL` | ate-api-server log level: `debug`, `info`, `warn`, or `error`. Written to the `ate-api-config` ConfigMap on every deploy when set; unset keeps the installed level (`info` on a new install). The server picks up a change without restarting |
| `--otlp-endpoint URL` | Send control plane telemetry to `URL` instead of the cluster default (see [`benchmarking/telemetry/README.md`](../../benchmarking/telemetry/README.md)) |
| `--preview GATES` | Preview gates passed as `--preview` to every substrate component: comma-separated gate names, or `ALL` for all. `ATE_PREVIEW` or `config.preview` when the flag is absent. An explicit `--preview=''` turns them all off, even when `ATE_PREVIEW` is set |
| `--context NAME` | Kubeconfig context; still defaults to `KUBECTL_CONTEXT` |
| `--kubeconfig PATH` | Explicit kubeconfig path |
| `--no-dev-env` | Skip `.ate-dev-env.sh` at the repository root |
| `--version` / `-v` | Print version information |
| `--image-repo REPO` | Install pre-built images from `REPO` instead of building them with `ko` |
| `--image-tag TAG` | The tag those images carry. Each of the two requires the other |
| `--e2e-tuning` | Apply short atelet snapshot delays for end-to-end test runs. |
| `--e2e-envoy-concurrency N` | Set the Envoy worker-thread count for e2e runs. `0` leaves the manifest unchanged. |

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
  --credential-provider='{"name":"k8s.io"}' \
  --ingress-allowed-spiffe-ids=IDS \
  --image-repo REPO \
  --image-tag TAG
```

installs images already published to `REPO` instead, and never invokes `ko`.
The manifests still come from the checkout, so this needs one; what it removes
is the build and write access to a registry. Releases ship no `ate-setup`
binary, so running it from the checkout still needs Go.

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

The `ate-setup` and `hack/install-ate.sh` deploy with the following subcommands

* `deploy ate-system`
* `deploy ate-system --setup-csi=nfs`
* `deploy atelet`
* `deploy apiserver`
* `deploy ate-controller`
* `deploy atenet`
* `deploy podcertificate-controller`
* `deploy sandboxconfig`
* `deploy postgres`

`deploy ate-system` is the whole control plane: CRDs, RBAC, the store, the
apiserver, the controller, atenet, and atelet. It creates every `create`
resource below on the way, so those subcommands are only needed to redo one on
a running cluster.

## Publish

`ate-setup` publishes with the following subcommands

* `publish worker-images`
* `publish release-images`
* `publish release-binaries`

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

`ate-setup` program cleans up with the following subcommands

* `delete ate-system`
* `delete atenet`
* `delete all`

`delete all` removes every registered demo and then the control plane.

## Create

`ate-setup` program creates resources with the following subcommands.
These are individual secrets and config that `deploy ate-system` creates automatically.

* `create jwt-authority-pool`
* `create actor-id-ca-pool`
* `create actor-id-ca-certs`
* `create egress-mitm-ca-pool`
* `create podcertificate-controller-cas`
* `create api-server-env-vars`
* `create api-authentication-config`
* `create api-config`

## Setup

`ate-setup` sets up CSI drivers with the following subcommands.

* `setup csi [driver]`

`driver` is one of `nfs`, `hostpath`, `both`, or `none`; `setup csi` with `none` as the default option. The hostpath is for KIND clusters only. NFS has no such
restriction, but it does need the `nfsd` kernel module loaded on the nodes.


## Benchmarks

`ate-setup` benchmars with the following subcommands.

* `deploy benchmarks`
* `delete benchmarks`

The `deploy benchmarks` command accepts these flags:

* `--worker-count N (default 1)`
* `--sandbox-class gvisor\|microvm`
* `--benchmark-actor-memory SIZE (default 256Mi)`

The memory limit applies to every benchmark actor.

See
[`benchmarking/README.md`](../../benchmarking/README.md).

## Demos

`ate-setup` deploys demos with the following subcommands.

* `deploy demo NAME`
* `delete demo NAME`

`NAME` is the demo without its `demo-` prefix:

| `ate-setup` | Description |
|---|---|
| `deploy demo counter` | A counter actor exercising snapshot, resume, and atenet ingress |
| `deploy demo counter --with-external-volume [--storage-class NAME]` | The same, plus an external volume and a pre-seeded file to validate (requires `--preview=ExternalVolumes`). Run `setup csi` first and name the class it created, e.g. `csi-nfs-sc`; defaults to `standard` |
| `deploy demo counter-microvm` | The counter demo on microVM workers. Run `hack/install-microvm-deps.sh --install` first |
| `deploy demo egress` | Egress policy enforcement through atenet; the actors trust the egress gateway's CA |
| `deploy demo egress-microvm` | The same on microVM workers. Run `hack/install-microvm-deps.sh --install` first |
| `deploy demo jupyter` | A Jupyter notebook server per actor, reached through atenet ingress |
| `deploy demo sandbox` | An on-demand sandbox actor driven by the sandbox client |
| `deploy demo multi-template` | Two ActorTemplates sharing one WorkerPool |
| `deploy demo parking` | Actor parking and unparking on a small WorkerPool |
| `deploy demo claude-code-multiplex` | Several Claude Code agents multiplexed onto one WorkerPool (requires `ANTHROPIC_API_KEY`, `BUCKET_NAME`, `KO_DOCKER_REPO`) |

Each has a matching `delete demo NAME`. Demo flags bind to the deploy side
only; teardown never reads them.

The demo list is not hard-coded here — it is built from the registry in
[`internal/demos`](internal/demos), one package per demo, so
`go run ./cmd/ate-setup deploy demo --help` is authoritative for both the list
and the per-demo flags. A new demo is a new package there, added to
[`internal/demos/all`](internal/demos/all).
