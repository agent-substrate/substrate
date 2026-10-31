### GKE Quickstart (Production)

This is the Terraform equivalent of the `go run ./tools/setup-gcp --all`
provisioner described in the [GKE Quickstart (Development)](../../../README.md)
section. It provisions the same GCP resources — a GKE cluster, snapshot bucket,
Artifact Registry repository, and IAM bindings — but does so declaratively and
starting from a **vanilla Google Cloud project**: no APIs enabled, no VPC, and
no subnets are assumed to exist beforehand.

The configuration lives in [`hack/gcp/iac/`](.) and uses resources from the
[Terraform Google Cloud provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
directly (no modules), so every resource is visible and easy to adapt.

What gets created:

- A dedicated `substrate` VPC and subnet (VPC-native, with secondary ranges for pods and services).
- A GKE cluster with Workload Identity and the required Kubernetes beta APIs enabled.
- A single-node default pool (for system and non-gVisor workloads) and a `worker` pool running the gVisor sandbox runtime.
- A GCS bucket for sandbox snapshots.
- An Artifact Registry repository, with Cloud Build granted write access and the cluster granted read access.
- All required Google Cloud APIs.

#### Prerequisites

1. Install [Terraform](https://developer.hashicorp.com/terraform/install) (>= 1.5) and the [`gcloud` CLI](https://cloud.google.com/sdk/docs/install).

2. Create and source your environment file. This single file drives both
   Terraform and the deployment scripts: it exports `TF_VAR_*` variables that
   Terraform picks up automatically, so there is no separate `terraform.tfvars`
   to keep in sync. Source it now, before any of the steps below — everything
   that follows relies on the variables it exports:
   ```bash
   cp hack/gcp/ate-dev-env.sh.gcp .ate-dev-env.sh

   # Edit .ate-dev-env.sh to match your project and preferences, then source it:
   source .ate-dev-env.sh
   ```

3. Authenticate with application-default credentials:
   ```bash
   gcloud auth application-default login --project=${PROJECT_ID}
   ```

4. Bootstrap the two APIs that Terraform itself depends on. Although this
   configuration enables all required APIs via `google_project_service`, that
   resource needs the **Service Usage API** to function, and the
   `google_project` data source read during `terraform plan` needs the **Cloud
   Resource Manager API**. Neither can be enabled by Terraform on a truly
   vanilla project (chicken-and-egg), so enable them once up front with
   `gcloud`:
   ```bash
   gcloud services enable \
     serviceusage.googleapis.com \
     cloudresourcemanager.googleapis.com \
     --project=${PROJECT_ID}
   ```
   Terraform manages the remaining APIs from there.

#### Provisioning

1. Initialize Terraform and review the plan. Terraform reads its inputs from the
   `TF_VAR_*` variables exported by the environment file you sourced in the
   prerequisites, so make sure `.ate-dev-env.sh` is sourced in your current
   shell:
   ```bash
   cd hack/gcp/iac
   terraform init
   terraform plan
   ```

2. Apply to provision all resources:
   ```bash
   terraform apply
   ```

3. Configure `kubectl` to talk to the new cluster. Terraform prints the exact
   command as the `get_credentials_command` output:
   ```bash
   gcloud container clusters get-credentials ${CLUSTER_NAME} --location ${CLUSTER_LOCATION} --project ${PROJECT_ID}
   ```

4. Configure Docker authentication for Artifact Registry. The deployment scripts
   build and push images locally (via `ko`/Docker) to `KO_DOCKER_REPO`, so the
   human or CI principal running the deployment pushes directly — it does **not**
   go through Cloud Build. Authenticate your local Docker client against the
   registry host:
   ```bash
   gcloud auth configure-docker ${GCE_REGION}-docker.pkg.dev
   ```
   This Terraform configuration only grants `roles/artifactregistry.writer` to
   the Cloud Build service account (see [`iam.tf`](iam.tf)). If you deploy with
   the local-push path, make sure the principal running it also has Artifact
   Registry writer access on the repository, for example:
   ```bash
   gcloud artifacts repositories add-iam-policy-binding ${AR_REPOSITORY_ID} \
     --location=${GCE_REGION} \
     --project=${PROJECT_ID} \
     --member="user:$(gcloud config get-value account)" \
     --role="roles/artifactregistry.writer"
   ```

5. Deploy the Agent Substrate system and demos exactly as in the development
   quickstart:
   ```bash
   ./hack/install-ate.sh --deploy-ate-system
   ```

#### Tearing down resources

To delete everything Terraform created:
```bash
cd hack/gcp/iac
terraform destroy
```

The GKE cluster sets `deletion_protection = false`, so `terraform destroy`
removes it without any manual intervention.
