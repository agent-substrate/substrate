# GCP Setup Tool (`setup-gcp`)

This tool automates the provisioning and configuration of Google Cloud Platform (GCP) resources required to run Agent Substrate. It is designed to be idempotent, meaning it can be run multiple times safely to ensure the environment is correctly configured.

## Overview

The `setup-gcp` tool provisions and configures GCP resources for Agent Substrate.
It uses a hierarchical command structure:

*   `setup-gcp` (root)
    *   `enable apis` - Enable required GCP APIs.
    *   `create` - Parent command for resource creation.
        *   `cluster` - Create GKE cluster.
        *   `bucket` - Create the GCS snapshot and asset buckets.
        *   `iam` - Create IAM policy bindings and grant permissions.
        *   `dashboards` - Create Cloud Monitoring dashboards.
        *   `cloudsql` - Create a Cloud SQL PostgreSQL instance for the ateapi store, with IAM database authentication (see [cloud-sql.md](cloud-sql.md)).
    *   `bootstrap` - Run all setup steps in order.

## Prerequisites

1.  **Go**: Ensure Go is installed (version compatible with the project, see root `go.mod`).
2.  **Google Cloud SDK (`gcloud`)**: Installed and authenticated.
3.  **Application Default Credentials (ADC)**: The tool uses Google Cloud client libraries. You must set up ADC:
    ```bash
    gcloud auth application-default login
    ```
4.  **Target Project**: You must have a GCP project created and have sufficient permissions (typically Owner or Editor) to create resources like GKE clusters, GCS buckets, and IAM bindings.

## Configuration & Defaults

All CLI flags can be configured via environment variables. If an environment variable is set, it will be used as the default value for the corresponding flag. Command-line flags always take precedence over environment variables.

A convenient way to manage these is to copy the example environment file, customize it, and source it before running the tool:

```bash
cp hack/ate-dev-env.sh.example .ate-dev-env.sh
# Edit .ate-dev-env.sh to match your project and preferences
source .ate-dev-env.sh
```

## Global Flags

These flags can be passed to the root command and apply to all subcommands:

| Flag | Description | Default Env Var | Fallback Default |
| :--- | :--- | :--- | :--- |
| `--project-id` | GCP Project ID. | `PROJECT_ID` | None (Required) |
| `--project-number` | GCP Project Number (optional, resolved from `--project-id` if needed). | `PROJECT_NUMBER` | None |
| `--region` | GCP Region for regional resources. | `GCE_REGION` | `us-west1` |

## Subcommands

### 1. Enable APIs

Enables the required GCP APIs for the project.

```bash
go run ./tools/setup-gcp enable apis [flags]
```

**Flags:**
*   (Uses global `--project-id`)

### 2. Create Cluster

Creates a GKE cluster configured for Agent Substrate (with Workload Identity,
required Kubernetes beta APIs, and Managed OpenTelemetry enabled, and the
Filestore CSI driver disabled). An existing cluster keeps its Filestore
add-on setting.

> [!WARNING]
> Agent Substrate requires PodCertificateRequest and ClusterTrustBundle APIs.
> ClusterTrustBundle discovery prefers `certificates.k8s.io/v1`, falling back
> to `certificates.k8s.io/v1beta1` only when the stable resource is not served.
> PodCertificateRequest discovery independently prefers `v1` over `v1beta1`.
> GKE 1.37 and higher serve both under `v1` by default. GKE 1.36 serves them
> only under `v1beta1`, as beta APIs that must be enabled. Versions below 1.36
> are not supported.
>
> `create cluster` enables the beta APIs on every cluster it creates, whatever
> its version. If you bring your own 1.36 cluster, create it with them:
>
> ```bash
> gcloud container clusters create "${CLUSTER_NAME}" ... \
>   --enable-kubernetes-unstable-apis=certificates.k8s.io/v1beta1/podcertificaterequests,certificates.k8s.io/v1beta1/clustertrustbundles
> ```
>
> A 1.36 cluster created without them can be fixed in place: `create cluster`
> and `bootstrap` turn them on for an existing cluster, or run `gcloud
> container clusters update` with the same flag yourself. The APIs are served
> once the update finishes. **That is not enough on its own:** pod
> certificate projection is a kubelet feature, gated through 1.36, and nodes
> that were already running do not pick it up. Pods scheduled onto them fail
> with `MountVolume.SetUp failed: unimplemented`. Replace each such pool:
> create a new one, let workloads move to it, then delete the old one. The
> tool logs both commands for every pool it finds, with the new pool's
> machine type, disk, node count and nested virtualization copied from the
> old one; carry over anything else you customized (taints, service account,
> autoscaling) yourself:
>
> ```bash
> gcloud container node-pools create "${NODE_POOL}-2" --cluster="${CLUSTER_NAME}" \
>   --location="${CLUSTER_LOCATION}" --machine-type="${NODE_MACHINE_TYPE}" ...
> gcloud container node-pools delete "${NODE_POOL}" --cluster="${CLUSTER_NAME}" \
>   --location="${CLUSTER_LOCATION}"
> ```
>
> Upgrading a pool to the version it already runs does **not** help: GKE
> skips a same-version upgrade without recreating any nodes. Node pools on
> 1.37 or higher need nothing further.
>
> The symptom of a cluster without the APIs: the install hangs at "Waiting for
> podcertificate ClusterTrustBundles to be ready" and `kubectl get
> clustertrustbundles` reports the resource type is not served.

```bash
go run ./tools/setup-gcp create cluster [flags]
```

**Flags:**
| Flag | Description | Default Env Var | Fallback Default |
| :--- | :--- | :--- | :--- |
| `--name` | Name of the GKE cluster. | `CLUSTER_NAME` | `substrate-poc` |
| `--location` | Zone or region for the cluster (must be compatible with `--region`). | `CLUSTER_LOCATION` | `us-west1-c` |
| `--version` | Kubernetes version. | `CLUSTER_VERSION` | None |
| `--network` | VPC network name. | `NETWORK` | `default` |
| `--subnetwork` | VPC subnetwork name. | `SUBNETWORK` | `default` |
| `--machine-type` | Machine type for the node pool. | `NODE_MACHINE_TYPE` | `c3-standard-4` |
| `--enable-nested-virtualization` | Create the node pool with nested virtualization, exposing `/dev/kvm` for microVM workers. | `ENABLE_NESTED_VIRTUALIZATION` | `true` |
| `--boot-disk-size` | Boot disk size in GB for the node pool (0 = GKE default). | `BOOT_DISK_SIZE_GB` | None |
| `--boot-disk-type` | Boot disk type for the node pool (empty = GKE default). | `BOOT_DISK_TYPE` | None |

`NODE_MACHINE_TYPE` was named `GVISOR_NODE_MACHINE_TYPE`. The old name still
works and warns; rename it in your `.ate-dev-env.sh`.

**Node sysctls for high sandbox density (>800 sandboxes/node):** each awake
actor sandbox creates its own network namespace, veth/tap pair, and inotify
watches on the host kernel (even in multi-actor worker pods). Default COS
limits (`net.ipv4.neigh.default.gc_thresh3 = 1024`,
`fs.inotify.max_user_instances = 128`, `fs.inotify.max_user_watches = 8192`)
fail around ~800–1,000 live sandboxes per node: in single-node density
benchmarks on `c3-standard-192-metal` (50–100 multi-actor worker pods),
inotify watches exhausted at ~800 live sandboxes and the host ARP cache
overflowed on the 1,000-sandbox wave after passing 850 (`dmesg`: `neighbour:
arp_cache: neighbor table overflow!`), while raising these sysctls sustained
1,500 concurrent sandboxes per node.
`create cluster` sets these sysctls on `substrate-node-pool` **when creating a
new cluster**; existing clusters keep their original node-pool sysctls
(`create cluster` logs a warning if they are missing) and must be updated via
one of the options below.

* **When creating or updating node pools** (e.g., bare-metal pools), pass
  `--system-config-from-file=systemconfig.yaml` to `gcloud container node-pools
  create` or `update`. Note that `node-pools update --system-config-from-file`
  **replaces the pool's entire system config** (so carry over any existing
  sysctls/kubelet/cgroup settings) and **recreates the pool's nodes**:

  ```yaml
  # systemconfig.yaml
  linuxConfig:
    sysctls:
      net.ipv4.neigh.default.gc_thresh1: '4096'
      net.ipv4.neigh.default.gc_thresh2: '16384'
      net.ipv4.neigh.default.gc_thresh3: '32768'
      fs.inotify.max_user_instances: '65536'
      fs.inotify.max_user_watches: '1048576'
  ```

* **On an already-running node pool (without rolling/recreating nodes)**, apply
  a privileged `DaemonSet` to tune the host sysctls in place:

  ```yaml
  apiVersion: apps/v1
  kind: DaemonSet
  metadata:
    name: substrate-sysctl-tuner
    namespace: kube-system
  spec:
    selector:
      matchLabels:
        app: substrate-sysctl-tuner
    template:
      metadata:
        labels:
          app: substrate-sysctl-tuner
      spec:
        hostNetwork: true
        tolerations:
        - operator: Exists
        containers:
        - name: sysctl-tuner
          image: busybox:1.36
          securityContext:
            privileged: true
          command:
          - sh
          - -ce
          - |
            sysctl -w net.ipv4.neigh.default.gc_thresh1=4096
            sysctl -w net.ipv4.neigh.default.gc_thresh2=16384
            sysctl -w net.ipv4.neigh.default.gc_thresh3=32768
            sysctl -w fs.inotify.max_user_instances=65536
            sysctl -w fs.inotify.max_user_watches=1048576
            sleep infinity
  ```

### 3. Create Bucket

Creates the GCS bucket for snapshots and the asset bucket, `NAME-assets`, for
microVM sandbox assets.

```bash
go run ./tools/setup-gcp create bucket [flags]
```

**Flags:**
| Flag | Description | Default Env Var | Fallback Default |
| :--- | :--- | :--- | :--- |
| `--name` | Name of the GCS snapshot bucket. The microVM asset bucket is created beside it as `NAME-assets`. | `BUCKET_NAME` | None (Required*) |

*\*Note: Required unless the `BUCKET_NAME` environment variable is set.*

### 4. Create IAM

Configures IAM permissions and Workload Identity bindings.

```bash
go run ./tools/setup-gcp create iam [flags]
```

**Flags:**
| Flag | Description | Default Env Var | Fallback Default |
| :--- | :--- | :--- | :--- |
| `--bucket` | GCS bucket name. | `BUCKET_NAME` | None (Required for bucket bindings*) |
| `--gke-nodes` | Grant GKE nodes permission to pull images. | - | `true` |
| `--atelet` | Grant atelet project-level permissions. | - | `true` |
| `--transfer-service-account` | Create the snapshot transfer service account and let ate-api-server create tokens for it. | - | `true` |
| `--transfer-gsa-name` | Name of the snapshot transfer service account to create. ate-setup and `hack/teardown.sh` read the same variable. | `SNAPSHOT_TRANSFER_GSA_NAME` | `ate-snapshot-transfer` |
| `--bucket-bindings` | Grant ate-api-server and the snapshot transfer service account access to the snapshot bucket, and atelet read access to the asset bucket. | - | `true` |

*\*Note: Required for bucket bindings unless the `BUCKET_NAME` environment variable is set.*

#### What `create iam` actually grants

On a fresh GCP project these bindings do not exist, and nothing else creates
them. Without them atelet cannot fetch microVM assets, ate-api cannot copy or
collect snapshots or issue per-actor tokens for them (`403` from GCS on the
first suspend/resume), and nodes cannot pull images. `bootstrap` and `create iam`
apply them for you. The table below is the reference for auditing them, or for
applying them by hand in projects where you cannot run the tool with
project-level IAM permissions.

atelet and ate-api authenticate via [GKE Workload Identity
Federation](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity):
their Kubernetes ServiceAccounts (`ate-system/atelet` and
`ate-system/ate-api-server`, created by `install-ate.sh`) are addressed
directly as IAM principals. The one Google service account `create iam`
creates is the snapshot transfer account⁴, which ate-api-server's credential
exchange impersonates and downscopes per actor for every snapshot transfer.
This only works on a cluster with Workload Identity enabled. `create cluster`
enables the `PROJECT_ID.svc.id.goog` pool, and a pre-existing cluster must
have it enabled too, or their GCS calls fail with `401`.

| Member | Role | Resource | Why |
| :--- | :--- | :--- | :--- |
| atelet WI principal¹ | `roles/artifactregistry.reader` | project² | Pull sandbox runtime assets |
| ate-api-server WI principal¹ | `roles/storage.objectAdmin` | snapshot bucket | Copy external snapshots for tags, delete the ones nothing refers to |
| ate-api-server WI principal¹ | `roles/storage.bucketViewer` | snapshot bucket | List/stat the snapshot bucket |
| snapshot transfer SA⁴ | `roles/storage.objectAdmin` | snapshot bucket | Snapshot transfers, on tokens the credential exchange downscopes per actor |
| snapshot transfer SA⁴ | `roles/storage.bucketViewer` | snapshot bucket | List/stat the snapshot bucket |
| ate-api-server WI principal¹ | `roles/iam.serviceAccountTokenCreator` | snapshot transfer SA⁴ | The credential exchange impersonates it |
| Cloud SQL GSA⁵ | `roles/iam.serviceAccountTokenCreator` | snapshot transfer SA⁴ | Granted by `create cloudsql`: with Cloud SQL, ate-api-server's pod runs as this account |
| atelet WI principal¹ | `roles/storage.objectViewer` | asset bucket (`BUCKET_NAME-assets`) | Read microVM sandbox assets |
| default compute SA³ | `roles/artifactregistry.reader` | project² | Nodes pull images from Artifact Registry, which also serves gcr.io |

¹ `principal://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/PROJECT_ID.svc.id.goog/subject/ns/ate-system/sa/SERVICE_ACCOUNT` — note it is keyed to the **project number**, not the ID.
² Project-level today; scoping these down is tracked in the TODOs in `cmd/iam.go`.
³ `PROJECT_NUMBER-compute@developer.gserviceaccount.com`. It gets no storage role, since root on a node can use it. Installs set up before this also granted it `roles/storage.objectViewer` on the project. Remove that with
`gcloud projects remove-iam-policy-binding "${PROJECT_ID}" --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/storage.objectViewer --condition=None`.
⁴ `NAME@PROJECT_ID.iam.gserviceaccount.com`, with `NAME` from `SNAPSHOT_TRANSFER_GSA_NAME`. setup-gcp and `hack/teardown.sh` default it to `ate-snapshot-transfer`. ate-setup has no default and requires it on GKE.
⁵ `ate-api-server@PROJECT_ID.iam.gserviceaccount.com` by default, see [cloud-sql.md](cloud-sql.md).

Manual equivalent:

```bash
WI="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/ate-system/sa"
ATELET="${WI}/atelet"
ATE_API="${WI}/ate-api-server"
TRANSFER_NAME="${SNAPSHOT_TRANSFER_GSA_NAME:-ate-snapshot-transfer}"
TRANSFER="${TRANSFER_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="${ATELET}" --role=roles/artifactregistry.reader
gcloud iam service-accounts create "${TRANSFER_NAME}" --project="${PROJECT_ID}" \
  --display-name="Substrate snapshot transfers"
gcloud iam service-accounts add-iam-policy-binding "${TRANSFER}" --project="${PROJECT_ID}" \
  --member="${ATE_API}" --role=roles/iam.serviceAccountTokenCreator
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="${ATE_API}" --role=roles/storage.objectAdmin
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="${ATE_API}" --role=roles/storage.bucketViewer
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${TRANSFER}" --role=roles/storage.objectAdmin
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${TRANSFER}" --role=roles/storage.bucketViewer
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}-assets" \
  --member="${ATELET}" --role=roles/storage.objectViewer
```

#### Upgrading an existing install

Substrate does not support upgrading a cluster in place yet. A cluster with
microVM workers has to be recreated: each node indexes a sandbox runtime by
the digests of its assets, which do not change when the assets move to the
asset bucket, so a node keeps fetching them from the snapshot bucket. The
commands below still apply when the new cluster reuses the GCP project.

Run `create iam` before deploying a Substrate release whose credential
exchange impersonates the snapshot transfer account. It creates the account
and its grants, and ate-setup refuses to deploy ate-api-server on GKE until
`SNAPSHOT_TRANSFER_GSA_NAME` names it. With Cloud SQL, rerun
`create cloudsql` too, so its account may create the transfer account's
tokens.

setup-gcp only adds bindings. An install set up before the asset bucket
existed still grants atelet object admin on the project and on the snapshot
bucket. Upgrade Substrate first, to a release whose node plugin gets snapshot
credentials from ate-api-server's credential exchange. On an older release,
removing these grants makes every suspend and resume fail with `403`. Remove
those grants after running `create bucket` and `create iam`, and after
rerunning `hack/install-microvm-deps.sh --install` if you use microVM
workers, so the assets are in the asset bucket. `ATELET` is set as in the
manual equivalent above:

```bash
gcloud projects remove-iam-policy-binding "${PROJECT_ID}" \
  --member="${ATELET}" --role=roles/storage.objectAdmin --condition=None
gcloud storage buckets remove-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="${ATELET}" --role=roles/storage.objectAdmin
gcloud storage buckets remove-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="${ATELET}" --role=roles/storage.bucketViewer
```

An install from before the node service account lost its project-wide read
access also still has that grant. Remove it too:

```bash
gcloud projects remove-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role=roles/storage.objectViewer --condition=None
```

### 5. Create Dashboards

Creates or updates Cloud Monitoring dashboards.

```bash
go run ./tools/setup-gcp create dashboards [flags]
```

**Flags:**
| Flag | Description | Default Env Var | Fallback Default |
| :--- | :--- | :--- | :--- |
| `--dir` | Directory containing dashboard JSON files. | `DASHBOARD_DIR` | `tools/setup-gcp/dashboards` |

### 6. Bootstrap (All Steps)

Runs all the setup steps in the correct order to fully bootstrap the environment.

```bash
go run ./tools/setup-gcp bootstrap [flags]
```

**Flags:**
| Flag | Description | Default Env Var | Fallback Default |
| :--- | :--- | :--- | :--- |
| `--cluster-name` | Name of the GKE cluster. | `CLUSTER_NAME` | `substrate-poc` |
| `--cluster-location`| Zone or region for the cluster (must be compatible with `--region`). | `CLUSTER_LOCATION` | `us-west1-c` |
| `--cluster-version` | Kubernetes version. | `CLUSTER_VERSION` | None |
| `--network` | VPC network name. | `NETWORK` | `default` |
| `--subnetwork` | VPC subnetwork name. | `SUBNETWORK` | `default` |
| `--machine-type` | Machine type for the node pool. | `NODE_MACHINE_TYPE` | `c3-standard-4` |
| `--enable-nested-virtualization` | Create the node pool with nested virtualization, exposing `/dev/kvm` for microVM workers. | `ENABLE_NESTED_VIRTUALIZATION` | `true` |
| `--boot-disk-size` | Boot disk size in GB for the node pool (0 = GKE default). | `BOOT_DISK_SIZE_GB` | None |
| `--boot-disk-type` | Boot disk type for the node pool (empty = GKE default). | `BOOT_DISK_TYPE` | None |
| `--bucket-name` | Name of the GCS bucket for snapshots. | `BUCKET_NAME` | None (Required*) |
| `--dashboard-dir` | Directory containing dashboard JSON files. | `DASHBOARD_DIR` | `tools/setup-gcp/dashboards` |
| `--transfer-gsa-name` | Name of the snapshot transfer service account to create. ate-setup and `hack/teardown.sh` read the same variable. | `SNAPSHOT_TRANSFER_GSA_NAME` | `ate-snapshot-transfer` |

*\*Note: Required unless the `BUCKET_NAME` environment variable is set.*

`--enable-nested-virtualization` is on by default and needs a `--machine-type`
that supports the feature; pass `--enable-nested-virtualization=false` to turn
it off. It applies only to a cluster this command creates.

## Examples

Run the tool from the **repository root** to ensure relative paths to dashboard configurations are resolved correctly.

### Bootstrap everything using environment variables

If you have sourced your `.ate-dev-env.sh`:

```bash
go run ./tools/setup-gcp bootstrap
```

### Bootstrap everything overriding some values

```bash
go run ./tools/setup-gcp bootstrap \
  --cluster-name="custom-cluster" \
  --machine-type="n2-standard-8"
```

### Bootstrap a cluster whose nodes cannot run microVM workers

Nested virtualization is on by default, so turning it off is the case that
needs a flag:

```bash
go run ./tools/setup-gcp bootstrap \
  --enable-nested-virtualization=false
```

### Only create the cluster (using env vars for defaults)

```bash
go run ./tools/setup-gcp create cluster
```
