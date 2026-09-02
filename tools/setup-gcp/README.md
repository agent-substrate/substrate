# GCP Setup Tool (`setup-gcp`)

This tool automates the provisioning and configuration of Google Cloud Platform (GCP) resources required to run Agent Substrate. It is designed to be idempotent, meaning it can be run multiple times safely to ensure the environment is correctly configured.

## Overview

The `setup-gcp` tool provisions and configures GCP resources for Agent Substrate.
It uses a hierarchical command structure:

*   `setup-gcp` (root)
    *   `enable apis` - Enable required GCP APIs.
    *   `create` - Parent command for resource creation.
        *   `cluster` - Create GKE cluster.
        *   `bucket` - Create GCS bucket.
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
| `--project-id` | GCP Project ID. | `PROJECT_ID` | None |
| `--project-number` | GCP Project Number (required for IAM). | `PROJECT_NUMBER` | None |
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

Creates a GKE cluster configured for Agent Substrate.

> [!WARNING]
> Agent Substrate requires two Kubernetes beta APIs —
> `certificates.k8s.io/v1beta1/podcertificaterequests` and
> `certificates.k8s.io/v1beta1/clustertrustbundles` — and GKE only honors
> `enableK8sBetaApis` **at cluster creation**. Enabling them later on an
> existing cluster is not recoverable in place: the tool's reconcile path
> issues the update, but the APIs do not become served — the cluster must be
> **recreated** with them enabled. `create cluster` sets them for you; if you
> bring your own cluster, create it with both APIs enabled from the start,
> e.g.:
>
> ```bash
> gcloud container clusters create "${CLUSTER_NAME}" ... \
>   --enable-kubernetes-unstable-apis=certificates.k8s.io/v1beta1/podcertificaterequests,certificates.k8s.io/v1beta1/clustertrustbundles
> ```
>
> The symptom of a cluster without them: the install hangs at "Waiting for
> podcertificate ClusterTrustBundles to be ready" and `kubectl get
> clustertrustbundles` reports the resource type is not served.

```bash
go run ./tools/setup-gcp create cluster [flags]
```

**Flags:**
| Flag | Description | Default Env Var | Fallback Default |
| :--- | :--- | :--- | :--- |
| `--name` | Name of the GKE cluster. | `CLUSTER_NAME` | `substrate-poc` |
| `--location` | Zone or region for the cluster. | `CLUSTER_LOCATION` | `us-west1-c` |
| `--version` | Kubernetes version. | `CLUSTER_VERSION` | None |
| `--network` | VPC network name. | `NETWORK` | `default` |
| `--subnetwork` | VPC subnetwork name. | `SUBNETWORK` | `default` |
| `--machine-type` | Machine type for the gVisor node pool. | `GVISOR_NODE_MACHINE_TYPE` | `c3-standard-4` |

**Node version labels:** pool labels are the birth default for every node GKE
creates later (autoscaling, auto-repair, node upgrades), and `setup-gcp` does
not set `ate.dev/substrate-version` on the pool, so those nodes arrive
unlabeled and run no dataplane pods (see the README's note on node version
labels). Stamp the pool with
`gcloud container node-pools update ... --node-labels=...` (list the existing
labels first and carry them all over; the flag replaces the full set), and
create additional pools with
`--node-labels=ate.dev/substrate-version=<build version>`.

### 3. Create Bucket

Creates a GCS bucket for storing snapshots.

```bash
go run ./tools/setup-gcp create bucket [flags]
```

**Flags:**
| Flag | Description | Default Env Var | Fallback Default |
| :--- | :--- | :--- | :--- |
| `--name` | Name of the GCS bucket. | `BUCKET_NAME` | None (Required*) |

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
| `--bucket-bindings` | Grant atelet access to the snapshot bucket. | - | `true` |

*\*Note: Required for bucket bindings unless the `BUCKET_NAME` environment variable is set.*

#### What `create iam` actually grants

On a fresh GCP project these bindings do not exist, and nothing else creates
them — without them atelet cannot read or write snapshots (`403` from GCS on
the first suspend/resume) and nodes cannot pull images. `bootstrap` and
`create iam` apply them for you; the table below is the reference for auditing
them, or for applying them by hand in projects where you cannot run the tool
with project-level IAM permissions.

atelet authenticates via [GKE Workload Identity
Federation](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity):
its Kubernetes ServiceAccount (`ate-system/atelet`, created by
`install-ate.sh`) is addressed directly as an IAM principal — no Google
service account is created or impersonated. This only works on a cluster with
Workload Identity enabled (`create cluster` enables the
`PROJECT_ID.svc.id.goog` pool; a pre-existing cluster must have it enabled
too, or atelet's GCS calls fail with `401`).

| Member | Role | Resource | Why |
| :--- | :--- | :--- | :--- |
| atelet WI principal¹ | `roles/storage.objectAdmin` | project² | Read/write actor snapshots in GCS |
| atelet WI principal¹ | `roles/artifactregistry.reader` | project² | Pull sandbox runtime assets |
| atelet WI principal¹ | `roles/storage.objectAdmin` | snapshot bucket | Read/write snapshot objects |
| atelet WI principal¹ | `roles/storage.bucketViewer` | snapshot bucket | List/stat the snapshot bucket |
| default compute SA³ | `roles/storage.objectViewer` | project² | Nodes pull images from GCS-backed registries |
| default compute SA³ | `roles/artifactregistry.reader` | project² | Nodes pull images from Artifact Registry |

¹ `principal://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/PROJECT_ID.svc.id.goog/subject/ns/ate-system/sa/atelet` — note it is keyed to the **project number**, not the ID.
² Project-level today; scoping these down is tracked in the TODOs in `cmd/iam.go`.
³ `PROJECT_NUMBER-compute@developer.gserviceaccount.com` (least-privileged node SA is #76).

Manual equivalent:

```bash
ATELET="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/ate-system/sa/atelet"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="${ATELET}" --role=roles/storage.objectAdmin
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="${ATELET}" --role=roles/artifactregistry.reader
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="${ATELET}" --role=roles/storage.objectAdmin
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="${ATELET}" --role=roles/storage.bucketViewer
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
| `--cluster-location`| Zone or region for the cluster. | `CLUSTER_LOCATION` | `us-west1-c` |
| `--cluster-version` | Kubernetes version. | `CLUSTER_VERSION` | None |
| `--network` | VPC network name. | `NETWORK` | `default` |
| `--subnetwork` | VPC subnetwork name. | `SUBNETWORK` | `default` |
| `--machine-type` | Machine type for the gVisor node pool. | `GVISOR_NODE_MACHINE_TYPE` | `c3-standard-4` |
| `--bucket-name` | Name of the GCS bucket for snapshots. | `BUCKET_NAME` | None (Required*) |
| `--dashboard-dir` | Directory containing dashboard JSON files. | `DASHBOARD_DIR` | `tools/setup-gcp/dashboards` |

*\*Note: Required unless the `BUCKET_NAME` environment variable is set.*

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

### Only create the cluster (using env vars for defaults)

```bash
go run ./tools/setup-gcp create cluster
```
