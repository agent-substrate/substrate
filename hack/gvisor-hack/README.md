# Running Substrate against a locally-built gVisor runtime

Substrate pins one specific runsc release, but nothing about that pin is baked
into an image. The gVisor runtime fetches runsc at **runtime**: atelet reads a
`url` + `sha256` from a cluster-scoped `SandboxConfig`, downloads the binary,
verifies the digest, and caches it at
`/var/lib/ateom-gvisor/static-files/runsc-<sha256>` (a hostPath on the node).
So integrating an unreleased runsc — one with a feature you're developing — is
just: stage the binary somewhere the cluster can read, and point a
`SandboxConfig` at it.

The helpers here do exactly that. The committed default
(`manifests/ate-install/sandboxconfig-gvisor.yaml`, `SandboxConfig/gvisor-default`)
is left untouched; you get a second, named config that individual WorkerPools
opt into.

| File | Purpose |
| --- | --- |
| `deploy.sh` | One-shot: hash, stage, render + apply the `SandboxConfig`, optionally repoint a WorkerPool |
| `stage-to-rustfs.sh` | Upload the binary to the kind cluster's in-cluster rustfs bucket |
| `stage-to-gcs.sh` | Upload the binary to a GCS bucket (GKE) |
| `sandboxconfig.yaml.tmpl` | The `SandboxConfig` template `deploy.sh` renders |

These are named for the sandbox **class** rather than any one binary. The gVisor
class currently takes a single asset named `runsc`; that name appears in the
`assets` key of `sandboxconfig.yaml.tmpl` and in `runscPathFor`
(`cmd/atelet/sandbox_assets.go`), which is what the backend looks the path up by.
Everything else here is asset-name agnostic.

## Prerequisites

- A running cluster with the control plane installed
  (`hack/create-kind-cluster.sh` + `hack/install-ate-kind.sh`, or
  `hack/install-ate.sh` for GKE).
- `aws` CLI for the kind/rustfs path, or an authenticated `gcloud` for GCS.
- A runsc binary built for the **node's** architecture.

## 1. Build runsc

From your gVisor checkout — see the
[gVisor build docs](https://gvisor.dev/docs/user_guide/install/#build-from-source)
for the current invocation:

```sh
cd ~/src/gvisor
make copy TARGETS=runsc DESTINATION=bin/
```

runsc re-execs itself for the sandbox and gofer processes (`/proc/self/exe`), so
the single binary is the whole asset — there is nothing else to stage.

Copy it somewhere convenient (`bin/` at the repo root is gitignored):

```sh
cp ~/src/gvisor/bin/runsc /path/to/substrate/bin/runsc
```

## 2. Deploy it

### kind

```sh
ATE_INSTALL_KIND=true \
RUNSC=$PWD/bin/runsc \
WORKERPOOL=counter WORKERPOOL_NAMESPACE=ate-demo-counter \
  hack/gvisor-hack/deploy.sh
```

### GKE

```sh
RUNSC=$PWD/bin/runsc \
BUCKET_NAME=my-ate-bucket PROJECT_ID=my-project \
ARCH=amd64 \
WORKERPOOL=counter WORKERPOOL_NAMESPACE=ate-demo-counter \
  hack/gvisor-hack/deploy.sh
```

The bucket must be readable by atelet's service account. atelet tries an
anonymous client first and falls back to its main authenticated client, so the
cluster's own private snapshot bucket works fine.

The script prints the digest and the URL it staged to, uploads the binary to
`<bucket>/gvisor-hack/runsc-<sha256>`, and applies `SandboxConfig/gvisor-hack`.

### Environment reference

| Variable | Default | Meaning |
| --- | --- | --- |
| `RUNSC` | `./bin/runsc` | Binary to deploy |
| `NAME` | `gvisor-hack` | `SandboxConfig` name |
| `ARCH` | host `GOARCH` | Node architecture the binary targets |
| `BUCKET_NAME` | `ate-snapshots` | Object store bucket |
| `PREFIX` | `gvisor-hack` | Object key prefix in the bucket |
| `ATE_INSTALL_KIND` | `false` | `true` → stage to in-cluster rustfs instead of GCS |
| `KUBECTL_CONTEXT` | *(unset)* | Kube context to target |
| `PROJECT_ID` | *(unset)* | GCP project for the GCS upload |
| `MAKE_DEFAULT` | `false` | `true` → make this the cluster-wide gvisor default |
| `WORKERPOOL` / `WORKERPOOL_NAMESPACE` | *(unset)* | Repoint this pool at the new config |
| `SKIP_STAGE` | `false` | `true` → re-apply the config without re-uploading |

## 3. Point workloads at it

`deploy.sh` does this for you when you pass `WORKERPOOL`. Manually:

```sh
kubectl -n ate-demo-counter patch workerpool counter --type=merge \
  -p '{"spec":{"sandboxConfigName":"gvisor-hack"}}'
```

To make it cluster-wide instead, run with `MAKE_DEFAULT=true`. That path also
demotes the incumbent default first — **two** `SandboxConfig`s of the same class
with `spec.default: true` is an error, and every actor launch fails to resolve
its assets until one is demoted.

Assets are resolved when an actor starts, so **actors already running keep the
runsc they booted with**. Create a new actor, or suspend/resume an existing one,
to pick up the change.

## 4. Verify

The staged config and what it pins:

```sh
kubectl get sandboxconfig gvisor-hack -o yaml
```

The binary landed on the node (kind):

```sh
docker exec ate-control-plane ls -l /var/lib/ateom-gvisor/static-files/
docker exec ate-control-plane /var/lib/ateom-gvisor/static-files/runsc-<sha256> --version
```

If the download failed, atelet's log says why — a digest mismatch, an
unreachable bucket, or a permissions error on the object.

## 5. Iterate

Rebuild, then re-run the same command. The digest changes, so the object name
and the on-node cache path change with it; there is no stale-cache case to
reason about, and the previous build stays intact for A/B comparison.

For a really tight loop you can skip the object store entirely: atelet returns
early on a cache hit without ever reading the URL, so preseeding the node works.

```sh
SHA=$(sha256sum bin/runsc | cut -d' ' -f1)
docker cp bin/runsc ate-control-plane:/var/lib/ateom-gvisor/static-files/runsc-$SHA
docker exec ate-control-plane chmod 755 /var/lib/ateom-gvisor/static-files/runsc-$SHA
```

Then apply a config carrying that digest (`SKIP_STAGE=true` does the rest). This
is a dev trick, not a deployment: you must repeat it on every worker node, and
if the file is ever evicted atelet falls back to fetching a URL that has nothing
behind it.

## Gotchas

- **Checkpoints are version-locked.** The snapshot manifest records the exact
  asset set used, and restore refetches that same binary — gVisor cannot restore
  an image written by a different runsc. Snapshots taken with the released runsc
  will not restore under your build, and vice versa. Test with fresh actors.
- **The digest must be lowercase 64-hex.** Enforced by both the CRD schema
  (`pkg/api/v1alpha1/sandboxconfig_types.go`) and `resources.ValidateRunscHash`.
  `sha256sum` output is already in that form.
- **Every architecture listed needs a `runsc` asset.** The
  `sandboxconfig-assets` ValidatingAdmissionPolicy fails the apply otherwise.
  The template emits a single arch block on purpose; atelet only fetches the one
  matching its node's `GOARCH`, so a single-arch config is correct for a
  single-arch cluster.
- **Wrong-arch binaries fail late.** `deploy.sh` warns if `file` disagrees
  with `ARCH`, but the authoritative arch is the *node's*, not your laptop's.
  Set `ARCH` explicitly when they differ.
- **Assets are capped at 8 GiB** (`maxAssetBytes` in
  `cmd/atelet/sandbox_assets.go`). Not a concern for runsc; noted so the guard
  isn't a surprise.
- **Cleanup.** Staged objects accumulate under `<bucket>/gvisor-hack/`, one per
  build. Delete them when you're done; the on-node caches go away with the node.

## Reverting

```sh
kubectl -n ate-demo-counter patch workerpool counter --type=json \
  -p '[{"op":"remove","path":"/spec/sandboxConfigName"}]'
kubectl delete sandboxconfig gvisor-hack
```

If you used `MAKE_DEFAULT=true`, restore the shipped default as well:

```sh
kubectl patch sandboxconfig gvisor-default --type=merge -p '{"spec":{"default":true}}'
```
