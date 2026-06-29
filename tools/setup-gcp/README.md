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
| `--region` | GCP Region for regional resources. | `GCE_REGION` | `us-central1` |

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

```bash
go run ./tools/setup-gcp create cluster [flags]
```

**Flags:**
| Flag | Description | Default Env Var | Fallback Default |
| :--- | :--- | :--- | :--- |
| `--name` | Name of the GKE cluster. | `CLUSTER_NAME` | `substrate-poc` |
| `--location` | Zone or region for the cluster. | `CLUSTER_LOCATION` | `us-central1-c` |
| `--version` | Kubernetes version. | `CLUSTER_VERSION` | None |
| `--network` | VPC network name. | `NETWORK` | `default` |
| `--subnetwork` | VPC subnetwork name. | `SUBNETWORK` | `default` |
| `--machine-type` | Machine type for the gVisor node pool. | `GVISOR_NODE_MACHINE_TYPE` | `c3-standard-4` |

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
| `--cluster-location`| Zone or region for the cluster. | `CLUSTER_LOCATION` | `us-central1-c` |
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
