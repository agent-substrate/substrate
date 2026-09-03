# ate-setup commands

Every `ate-setup` command alongside the equivalent `hack/install-ate.sh` flag.

Both installers work today; `ate-setup` does not yet replace the shell scripts.
Use this table to translate an existing invocation.

```
go run ./cmd/ate-setup [global flags] <command> [flags]
```

`hack/install-ate.sh` accepts its flags in any order and runs one action per
flag, in command line order. `ate-setup` runs exactly one command per
invocation, so a shell line that passed several `--deploy-*` flags becomes
several `ate-setup` calls.

## Global flags

These configure every command. `hack/install-ate.sh` collects its equivalents in
a pre-scan pass, so they may appear anywhere on its command line.

| `ate-setup` | `hack/install-ate.sh` | Notes |
|---|---|---|
| `--kind` | `hack/install-ate-kind.sh`, or `ATE_INSTALL_KIND=true` | Kind overlays, the local registry, and host-architecture image builds |
| `--atenet-router envoy\|agentgateway` | `--atenet-router envoy\|agentgateway` | atenet router dataplane (default `envoy`) |
| `--rollout-timeout DURATION` | `--rollout-timeout DURATION` | Readiness timeout for workloads (default `60s`). Unlike the shell flag it also governs the podcertificate-controller and CSI waits, which stay at their 120s default until it is passed |
| `--podcert-workers-per-signer N` | `--podcert-workers-per-signer N` | Concurrent workers per podcertificate-controller signer |
| `--experimental-use-sdsmint` | `--experimental-use-sdsmint` | Mint TLS certificates on-demand via SDS in atenet egress gateway |
| `--experimental-additional-egress-extproc-service NS/SVC:PORT` | `--experimental-additional-egress-extproc-service NS/SVC:PORT` | External processor authorization filter |
| `--context NAME` | `KUBECTL_CONTEXT=NAME` | Kubeconfig context; still defaults to `KUBECTL_CONTEXT` |
| `--kubeconfig PATH` | `KUBECONFIG=PATH` | Explicit kubeconfig path |
| `--no-dev-env` | `NO_DEV_ENV=1` | Skip `.ate-dev-env.sh` at the repository root |
| `--version` / `-v` | — | New; the shell installer had no version |
| `--image-repo REPO` | — | New. Install pre-built images from `REPO` instead of building them with `ko` |
| `--image-tag TAG` | — | New. The tag those images carry. Each of the two requires the other |

Both have an environment equivalent, read when the flag is absent:
`ATE_IMAGE_REPO` and `ATE_IMAGE_TAG`.

## Installing a release

Without `--image-repo`, `ate-setup` builds every image from the checkout with
`ko` and pushes it to `KO_DOCKER_REPO`. That is the developer install and is
unchanged.

```
ate-setup deploy ate-system \
  --image-repo registry.example.com/substrate \
  --image-tag v0.0.0
```

installs published images instead, and never invokes `ko`. The manifests still
come from the checkout, so this needs one; what it removes is the build, the Go
toolchain, and write access to a registry.

`REPO` has to hold every component image the manifests reference, all under the
same tag, which is how a release publishes them. A release that adds a component
has to publish it alongside the others before a pre-built install can use it.
Each reference is then pinned to the digest its tag names, which takes one HEAD
request per image, so the installer needs read access to `REPO` and not only the
cluster does.

That read is authenticated with the docker config file and the credential
helpers it names, plus gcloud's own credentials for GCR and Artifact Registry:
Application Default Credentials, falling back to the `gcloud` CLI. Installing a
release onto GKE therefore needs no `~/.docker/config.json` entry. Amazon ECR
and Azure Container Registry need credential-helper modules this repository does
not depend on, so those registries need a `docker login` first.

`TAG` may itself carry a digest, as in `--image-tag v0.0.0@sha256:...`. A tag
that already names a manifest is used as written, and is not looked up.

## Deploy

| `ate-setup` | `hack/install-ate.sh` |
|---|---|
| `deploy ate-system` | `--deploy-ate-system` |
| `deploy ate-system --setup-csi=nfs` | `--deploy-ate-system --setup-csi=nfs` |
| `deploy atelet` | `--deploy-atelet` |
| `deploy apiserver` | `--deploy-ate-apiserver` |
| `deploy ate-controller` | (no shell equivalent) |
| `deploy atenet` | `--deploy-atenet` |
| `deploy postgres` | `--deploy-postgres` |

`deploy ate-system` is the whole control plane: CRDs, RBAC, the store, the
apiserver, the controller, atenet, and atelet. It creates every `create`
resource below on the way, so those subcommands are only needed to redo one on
a running cluster.

## Publish

| `ate-setup` | `hack/install-ate.sh` |
|---|---|
| `publish worker-images` | (no shell equivalent) |

Builds and pushes the ateom worker images for the checked-out build and prints
their refs; a WorkerPool points `spec.workerImage` to a build to use the ateom.

## Delete

| `ate-setup` | `hack/install-ate.sh` |
|---|---|
| `delete ate-system` | `--delete-ate-system` |
| `delete atenet` | `--delete-atenet` |
| `delete all` | `--delete-all` |

`delete all` removes every registered demo and then the control plane.

## Create

Individual secrets and config that `deploy ate-system` creates automatically.

| `ate-setup` | `hack/install-ate.sh` |
|---|---|
| `create jwt-authority-pool` | `--create-jwt-authority-pool-secret` |
| `create actor-id-ca-pool` | `--create-actor-id-ca-pool-secret` |
| `create actor-id-ca-certs` | `--create-actor-id-ca-certs-secret` |
| `create egress-mitm-ca-pool` | `--create-egress-mitm-ca-pool-secret` |
| `create podcertificate-controller-cas` | `--create-podcertificate-controller-cas` |
| `create api-server-env-vars` | `--create-api-server-env-vars` |
| `create api-authentication-config` | `--create-api-authentication-config` |

## Setup

| `ate-setup` | `hack/install-ate.sh` |
|---|---|
| `setup csi [driver]` | `--setup-csi[=DRIVER]` |

`driver` is one of `nfs`, `hostpath`, `both`, or `none`; `setup csi` with `none` as the default option. The hostpath is for KIND clusters only. NFS has no such
restriction, but it does need the `nfsd` kernel module loaded on the nodes.


## Benchmarks

| `ate-setup` | `hack/install-ate.sh` |
|---|---|
| `deploy benchmarks` | `--deploy-benchmarks` |
| `delete benchmarks` | `--delete-benchmarks` |
| `--worker-count N` | `--benchmark-worker-count N` (default `1`) |
| `--sandbox-class gvisor\|microvm` | `--benchmark-sandbox-class CLASS` (default `gvisor`) |

The two flags are per-command in `ate-setup` and global in
`hack/install-ate.sh`, which forwards them to whichever benchmark action runs.
See
[`benchmarking/README.md`](../../benchmarking/README.md).

## Demos

| `ate-setup` | `hack/install-ate.sh` |
|---|---|
| `deploy demo NAME` | `--deploy-demo-NAME` |
| `delete demo NAME` | `--delete-demo-NAME` |

`NAME` is the demo without its `demo-` prefix:

| `ate-setup` | `hack/install-ate.sh` | Description |
|---|---|---|
| `deploy demo counter` | `--deploy-demo-counter` | A counter actor exercising snapshot, resume, and atenet ingress |
| `deploy demo counter --with-external-volume [--storage-class NAME]` | `--deploy-demo-counter-with-external-volume` (`STORAGE_CLASS=NAME`) | The same, plus an external volume and a pre-seeded file to validate. Run `setup csi` first and name the class it created, e.g. `csi-nfs-sc`; defaults to `standard` |
| `deploy demo egress` | `--deploy-demo-egress` | Egress policy enforcement through atenet |
| `deploy demo sandbox` | `--deploy-demo-sandbox` | An on-demand sandbox actor driven by the sandbox client |
| `deploy demo multi-template` | `--deploy-demo-multi-template` | Two ActorTemplates sharing one WorkerPool |
| `deploy demo parking` | `--deploy-demo-parking` | Actor parking and unparking on a small WorkerPool |
| `deploy demo autoscaled-workerpool` | `--deploy-demo-autoscaled-workerpool` | A WorkerPool scaled by an HPA over custom metrics (Kind only) |
| `deploy demo claude-code-multiplex` | `--deploy-demo-claude-code-multiplex` | Several Claude Code agents multiplexed onto one WorkerPool (requires `ANTHROPIC_API_KEY`, `BUCKET_NAME`, `KO_DOCKER_REPO`) |

Each has a matching `delete demo NAME` / `--delete-demo-NAME`. Demo flags bind
to the deploy side only; teardown never reads them.

The demo list is not hard-coded here — it is built from the registry in
[`internal/demos`](internal/demos), one package per demo, so
`go run ./cmd/ate-setup deploy demo --help` is authoritative for both the list
and the per-demo flags.

The demos also each have a `hack/install-demo-*.sh`, sourced by
`hack/install-ate.sh`, which registers `--deploy-demo-NAME` /
`--delete-demo-NAME` flags on that installer. ate-setup does not use those
scripts; its demos live in [`internal/demos`](internal/demos).
