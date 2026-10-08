# Azure Blob object-store plugin

> [!NOTE]
> This plugin is hosted in the substrate repository temporarily. It will move
> to a dedicated repository in the near future.

This plugin implements substrate's `objectstore.v1` node and control
services with Azure Blob Storage. It runs in `node` mode beside atelet and in
`control` mode beside ate-api-server. The module imports only substrate's
public `pkg/` packages so it can move to a repository of its own.

The plugin stores each requested snapshot file as one block blob. Uploads use
deterministic staged block IDs and atomically replace the committed block list,
so a retry after a partial upload is safe. Each blob carries a SHA-256 digest
in metadata. Fetch and copy verify that digest before exposing or committing
the destination bytes. Azure also verifies each staged block's MD5 in transit.

## Sidecars

Substrate's install manifests run an `objectstore-plugin` native sidecar next to
atelet and ate-api-server, and the callers reach it over a Unix socket in a
pod-local `emptyDir`:

- node socket: `/run/objectstore-plugin/node.sock`
- control socket: `/run/objectstore-plugin/control.sock`
- node snapshot root: `/var/lib/ate`

The `config/` component replaces those sidecars' image and arguments with this
plugin's, removes the base `ATE_STORAGE_BACKEND`, adds the workload identity
environment and projected token, and points their startup probes at
`azure-blob-objectstore-plugin healthcheck`, which reports whether the plugin at
`--socket` is serving gRPC health. It leaves the caller containers and the
socket volume as the base defines them. The base control sidecar's credential
exchange settings also stay: the `exchange` port, `ATE_API_ACTOR_JWT_ISSUER`,
`POD_NAMESPACE`, and the `servicedns`, `podidentity` and `actor-id-jwt-jwks`
mounts; this plugin serves no exchange and reads none of them. The kind
overlay's own patches run after its components, so they still add its RustFS
`ATE_STORAGE_BACKEND` and `AWS_*` variables to both sidecars; the plugin reads
none of them.

## Snapshot URIs

The plugin accepts:

```text
azblob://<container>/<snapshot-prefix>
```

`<container>` must be one of `--containers`, and `CopySnapshot` may copy
between any two of them. `--prefix`, when set, is prepended to every snapshot
prefix in every container, allowing all substrate data to live below one
directory. A bare container, a query or fragment, `\`, and `.`, `..` or other
path segments made only of dots and spaces are rejected.

## Configuration

| Flag | Environment fallback | Default | Purpose |
|---|---|---|---|
| `--account-url` | `AZURE_STORAGE_ACCOUNT_URL` | required | `https://<account>.blob.core.windows.net` |
| `--containers` | `AZURE_STORAGE_CONTAINERS` | required | Comma-separated blob containers that snapshot URIs may name; a name that breaks Azure's container naming rules fails startup |
| `--prefix` | `AZURE_STORAGE_PREFIX` | empty | Prefix prepended to every snapshot key |
| `--socket` | none | required | Unix socket to serve |
| `--root` | none | `/var/lib/ate` | Node mode's allowed local snapshot tree |

`healthcheck` mode takes `--socket` and `--timeout` (default `5s`) and exits
non-zero unless the plugin at `--socket` reports `SERVING` within the timeout.

The account URL must use `https`. The plugin sends requests, and with them its
storage token, only to the scheme and host of `--account-url`: it refuses a
request to any other endpoint and does not follow redirects. Snapshot URIs
name only a container and prefix, never a host, so no request can choose
where the token goes.

## Authentication

The plugin authenticates to Azure Storage only with Microsoft Entra workload
identity federation: it exchanges the pod's projected service account token for
an Entra token. It does not accept storage account keys, connection strings,
SAS tokens, client secrets or certificates, and never authenticates as the
node's managed identity. It reads the standard `azidentity` variables:

| Variable | Value |
|---|---|
| `AZURE_CLIENT_ID` | Client ID of the user-assigned managed identity whose federated identity credential trusts the cluster's service account token issuer |
| `AZURE_TENANT_ID` | Microsoft Entra tenant ID |
| `AZURE_FEDERATED_TOKEN_FILE` | Path of the projected service account token |

The plugin fails at startup, naming the missing variables, when any of them is
unset or the token file cannot be read. It has no other credential to fall
back to.

Create federated identity credentials with audience
`api://AzureADTokenExchange`, the cluster's service account issuer, and these
subjects:

```text
system:serviceaccount:ate-system:atelet
system:serviceaccount:ate-system:ate-api-server
```

Microsoft Entra ID must be able to fetch the issuer's OIDC discovery document
and signing keys.

Grant each identity `Storage Blob Data Contributor` scoped to each container in
`--containers`:

```bash
CONTAINER_SCOPE="/subscriptions/<subscription>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<account>/blobServices/default/containers/<container>"
az role assignment create \
  --assignee-object-id <principal-object-id> \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope "$CONTAINER_SCOPE"
```

`ServicePrincipal` is the principal type Azure role assignments use for
managed identities as well, so it is correct for a user-assigned managed
identity; no client secret or certificate is involved.

The identity federated to `system:serviceaccount:ate-system:atelet` (the
plugin in `node` mode) needs blob read/write access. The identity federated to
`system:serviceaccount:ate-system:ate-api-server` (the plugin in `control` mode)
needs blob read/write/delete access; the built-in role above covers both.

## Deployment

Build and push the image:

```bash
KO_DOCKER_REPO=<registry> make build-image
```

The `config/` component configures workload identity: it projects a service
account token with audience `api://AzureADTokenExchange` into both sidecars and
sets their `AZURE_*` variables, without a webhook mutating the whole Pod. Edit
`config/kustomization.yaml` to set:

- both `example.invalid/replace-with-azure-blob-objectstore-plugin:latest` image
  fields to the digest reference printed by `make build-image`;
- both client IDs and the tenant ID;
- the storage account URL;
- the containers;
- the optional storage prefix.

`ate-setup` has no option to add a component, so add this one to the overlay
that `ate-setup deploy ate-system` renders: `manifests/ate-install/base` by
default, or `manifests/ate-install/kind` with `--kind`. For example, in
`manifests/ate-install/kind/kustomization.yaml`:

```yaml
components:
- ../../../internal/plugins/azure-blob/config
```

Then install from the repository root:

```bash
go run ./cmd/ate-setup deploy ate-system \
  --kind \
  --context <context> \
  --ko-docker-repo <registry> \
  --ko-default-platforms linux/amd64 \
  --credential-provider='{"name":"k8s.io"}'
```

`--kind` selects the kind overlay. It does not create a kind cluster, but it
defaults several settings to a local kind setup, including the context,
registry, and build platform, so name all three for any other cluster.

Only `deploy ate-system` renders that overlay. `deploy atelet` and
`deploy apiserver` apply their manifests without it, which puts the base
`objectstore-plugin` sidecars back, so redeploy with `deploy ate-system`.

The component changes only the sidecars. atelet and ate-api-server have no
object storage settings of their own. atelet still needs the node's kubelet
image credential provider from the chosen overlay to pull private actor
images: `base` uses GKE's credential provider paths, and the kind overlay pulls
actor images without credentials.

This plugin has been run end to end on AKS with workload identity, the gVisor
sandbox class and one node, through the kind overlay plus an AKS kubelet image
credential provider for atelet: a golden snapshot, an actor restored from it,
suspended, and resumed, then cleaned up. That run did not cover the micro-VM
sandbox class or a restore on a different node from the one that took the
snapshot.

## Actor templates

Every ActorTemplate that uses this plugin must set
`snapshotConfig.storageLocation` to a URI rooted in one of the configured
containers:

```yaml
snapshotConfig:
  storageLocation: azblob://<container>/<snapshot-root>
```

## Error behavior

| Condition | gRPC code |
|---|---|
| Invalid URI, local path, or file list | `InvalidArgument` |
| Requested blob absent during fetch | `NotFound` |
| Configured container absent (cleanup succeeds instead) | `FailedPrecondition` |
| Empty copy source | `FailedPrecondition` |
| Azure Storage returns HTTP 401 or 403 | `PermissionDenied` |
| Azure throttling, timeout, or server failure | `Unavailable` |
| Connection failure, DNS server failure, or DNS timeout after the SDK's retries, or a download that fails after resuming | `Unavailable` |
| Unknown storage account host name | `Internal` |
| Microsoft Entra token request throttled, timed out, failed with a server error, or got no response | `Unavailable` |
| Microsoft Entra rejects the token request | `Internal` |
| Blob replaced while a download was resuming | `Unavailable` |
| Context canceled or deadline exceeded | `Canceled` / `DeadlineExceeded` |
| SHA-256 metadata absent, invalid, or mismatched | `DataLoss` |
| Other local or Azure failures | `Internal` |

## Limitations and open edges

- Substrate currently sends an empty `actor_jwt`. The plugin therefore cannot
  authorize per actor and relies on the plugin sidecar's federated identity and
  configured containers and prefix.
- A failed staged-block upload can leave uncommitted blocks. Azure expires
  uncommitted blocks after seven days. `CleanupSnapshot` deletes committed
  blobs under the caller-selected prefix; Azure does not expose uncommitted
  blocks through prefix listing.
- Upload staging uses at most nine 4 MiB buffers per upload. That bound is per
  RPC, not process-wide, so concurrent uploads multiply the memory use.
- One plugin process is confined to one storage account and the containers in
  `--containers`. Copying snapshots across accounts is not supported.

## Development

```bash
make build
make test
make verify
make fmt
```

The integration tests use the real Azure Blob SDK and the gRPC services over
a Unix socket, against Azurite. They fail rather than skip when their Azurite
configuration is absent. `make test-integration` runs
`hack/test-azurite.sh`, which starts a pinned Azurite image with Docker on
`127.0.0.1:${AZURITE_PORT:-10000}`, runs every test in the module with the
`integration` tag and `-race`, and removes the container; PR CI runs the same
script.

The test uploads and restores a 320 MiB file with distinct per-block patterns,
retries a partial block upload, verifies corrupt data is rejected, exercises
concurrent uploads, and checks both control RPCs.

`go.mod` replaces `go.ate.dev/substrate` with this checkout so
the module tests against the current public API. The import-boundary test
rejects imports of substrate packages outside `pkg/`.
