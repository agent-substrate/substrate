# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

##########################################################################
# Enable the required Cloud APIs
##########################################################################
resource "google_project_service" "services" {
  for_each = toset([
    "aiplatform.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudtrace.googleapis.com",
    "compute.googleapis.com",
    "file.googleapis.com",
    "container.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "networkconnectivity.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

##########################################################################
# Fetch defaults
##########################################################################
data "google_project" "project" {
}

data "google_compute_default_service_account" "default" {
  depends_on = [
    google_project_service.services["compute.googleapis.com, iam.googleapis.com"]
  ]
}

##########################################################################
# Set up the VPC and subnet
##########################################################################
resource "google_compute_network" "substrate" {
  name                    = "substrate"
  project                 = var.project_id
  auto_create_subnetworks = false

  depends_on = [
    google_project_service.services["networkingconnectivity.googleapis.com"]
  ]
}

resource "google_compute_subnetwork" "substrate" {
  name          = "substrate"
  project       = var.project_id
  region        = var.region
  network       = google_compute_network.substrate.id
  ip_cidr_range = var.subnet_cidr

  secondary_ip_range {
    range_name    = "substrate-pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "substrate-services"
    ip_cidr_range = var.services_cidr
  }
}

# allow access from health check ranges
resource "google_compute_firewall" "allow_l7_xlb_fw_hc" {
  name          = "allow-l7-xlb-fw-hc"
  direction     = "INGRESS"
  network       = google_compute_network.substrate.id
  source_ranges = ["130.211.0.0/22", "35.191.0.0/16"]
  allow {
    protocol = "tcp"
  }
  target_tags = ["allow-health-check"]
}

# allow ssh ingress from iap
resource "google_compute_firewall" "allow_ssh_ingress_from_iap" {
  name          = "allow-ssh-ingress-from-iap"
  direction     = "INGRESS"
  network       = google_compute_network.substrate.id
  source_ranges = ["35.235.240.0/20"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

##########################################################################
# Set up default service account permissions
##########################################################################
resource "google_project_iam_member" "default_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"
}

resource "google_project_iam_member" "default_service_usage_admin" {
  project = var.project_id
  role    = "roles/serviceusage.serviceUsageAdmin"
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"
}

##########################################################################
# Set up the NAT Router
##########################################################################
resource "google_compute_router" "substrate_router" {
  name    = "substrate-router"
  region  = var.region
  network = google_compute_network.substrate.id

  bgp {
    asn = 64514
  }
}

resource "google_compute_router_nat" "substrate_router_nat" {
  name                               = "substrate-router-nat"
  router                             = google_compute_router.substrate_router.name
  region                             = google_compute_router.substrate_router.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

##########################################################################
# Set up the Artifact Registry 
##########################################################################
resource "google_artifact_registry_repository" "ate_images" {
  location      = var.region
  repository_id = "ate-images"
  description   = "docker repository"
  format        = "DOCKER"
  depends_on = [
    google_project_service.services["artifactregistry.googleapis.com"]
  ]
}

resource "google_artifact_registry_repository_iam_member" "atelet_artifact_reader" {
  location   = google_artifact_registry_repository.ate_images.location
  repository = google_artifact_registry_repository.ate_images.name
  role       = "roles/artifactregistry.reader"
  member     = "principal://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/ate-system/sa/atelet"
  depends_on = [
    google_container_cluster.substrate
  ]
}

resource "google_artifact_registry_repository_iam_member" "default_sa_artifact_reader" {
  location   = google_artifact_registry_repository.ate_images.location
  repository = google_artifact_registry_repository.ate_images.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${data.google_compute_default_service_account.default.email}"
}

##########################################################################
# Set up the Snapshot bucket
##########################################################################
resource "google_storage_bucket" "snapshots" {
  name          = "snapshot-substrate-test-${var.project_id}"
  location      = "US"

  force_destroy               = true
  public_access_prevention    = "enforced"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket_iam_member" "atelet_snapshots_bucket_viewer" {
  bucket     = google_storage_bucket.snapshots.name
  role       = "roles/storage.bucketViewer"
  member     = "principal://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/ate-system/sa/atelet"
  depends_on = [
    google_container_cluster.substrate
  ]
}

resource "google_storage_bucket_iam_member" "atelet_snapshots_object_admin" {
  bucket     = google_storage_bucket.snapshots.name
  role       = "roles/storage.objectAdmin"
  member     = "principal://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/ate-system/sa/atelet"
  depends_on = [
    google_container_cluster.substrate
  ]
}

###########################################################################
# Set up the GKE cluster
##########################################################################
resource "google_container_cluster" "substrate" {
  name     = var.cluster_name
  provider = google-beta
  location = var.cluster_location

  min_master_version = var.cluster_version

  # Required by Google provider 5.0+ — must be false before terraform destroy
  # will succeed. Appropriate for dev quickstart infra.
  deletion_protection = false

  network    = google_compute_network.substrate.name
  subnetwork = google_compute_subnetwork.substrate.name

  # VPC-native: pod and service IPs drawn from the subnet's secondary ranges.
  ip_allocation_policy {
    cluster_secondary_range_name  = "substrate-pods"
    services_secondary_range_name = "substrate-services"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # Beta Kubernetes APIs required by Agent Substrate.
  enable_k8s_beta_apis {
    enabled_apis = [
      "certificates.k8s.io/v1beta1/podcertificaterequests",
      "certificates.k8s.io/v1beta1/clustertrustbundles",
    ]
  }

  # A single default-pool node ensures system and non-gVisor workloads have
  # somewhere to run without being forced onto the gVisor worker pool.
  initial_node_count = var.default_pool_count

  node_config {
    machine_type = var.default_node_machine_type
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
    ]

    tags         = ["default-pool-node", "allow-health-check"]
    metadata     = {
      disable-legacy-endpoints = "true"
    }

    shielded_instance_config {
      enable_secure_boot = true
    }
  }

  addons_config {
    http_load_balancing {
      disabled = false
    }

    gcp_filestore_csi_driver_config {
      enabled = true
    }

    pod_snapshot_config {
      enabled = true
    }
  }

  gateway_api_config {
    channel = "CHANNEL_STANDARD"
  }

  monitoring_config {
    managed_prometheus {
      enabled = true
    }

    advanced_datapath_observability_config {
      enable_metrics = true
      enable_relay   = true
    }
  }

  private_cluster_config {
    enable_private_nodes = true
  }

  datapath_provider = "ADVANCED_DATAPATH"

  enable_shielded_nodes = true

  depends_on = [google_compute_subnetwork.substrate]
}

# Sandbox workloads are scheduled here. gVisor requires the COS_CONTAINERD
# image type; the sandbox_config block activates the runsc runtime.

resource "google_container_node_pool" "worker" {
  name     = "worker"
  provider = google-beta
  cluster  = google_container_cluster.substrate.id
  location = var.cluster_location

  node_count = var.worker_pool_count

  node_config {
    machine_type = var.worker_node_machine_type
    image_type   = "COS_CONTAINERD"

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
    ]

    tags         = ["worker-pool-node", "allow-health-check"]
    metadata     = {
      disable-legacy-endpoints = "true"
    }

    shielded_instance_config {
      enable_secure_boot = true
    }

    sandbox_config {
      type = "GVISOR"
    }
  }
}

###########################################################################
# Set up permissions for Agents to access Vertex AI and Cloud Trace
###########################################################################
resource "google_project_iam_member" "cloud_trace" {
  project = var.project_id
  role    = "roles/cloudtrace.agent"
  member  = "principal://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/ate-system/sa/atelet"
  depends_on = [
    google_container_cluster.substrate
  ]
}

###########################################################################
# Set up permissions for Cloud Build to deploy to GKE and push to AR
###########################################################################
# Explicitly pull the Secret Manager Service Agent identity
resource "google_project_service_identity" "cloudbuild_agent" {
  provider = google-beta
  project  = var.project_id
  service  = "cloudbuild.googleapis.com"
}

resource "google_project_iam_member" "cloudbuild_gke_developer" {
  project = var.project_id
  role    = "roles/container.developer"
  member  = google_project_service_identity.cloudbuild_agent.member
}

resource "google_artifact_registry_repository_iam_member" "cloudbuild_artifactregistry_writer" {
  location   = google_artifact_registry_repository.ate_images.location
  repository = google_artifact_registry_repository.ate_images.name
  role       = "roles/artifactregistry.writer"
  member     = google_project_service_identity.cloudbuild_agent.member
}

##########################################################################
# Set up the Filestore cluster
##########################################################################
resource "google_filestore_instance" "filestore" {
  count    = var.filestore ? 1 : 0
  name     = "filestore"
  location = var.zone
  tier     = "BASIC_SSD"

  file_shares {
    capacity_gb = 2560
    name        = "vol1"

    nfs_export_options {
      ip_ranges   = ["10.0.0.0/16", "10.2.0.0/16"]
      access_mode = "READ_WRITE"
      squash_mode = "NO_ROOT_SQUASH"
    }
  }

  networks {
    network      = google_compute_network.substrate.name
    modes        = ["MODE_IPV4"]
    connect_mode = "DIRECT_PEERING"
  }
}
