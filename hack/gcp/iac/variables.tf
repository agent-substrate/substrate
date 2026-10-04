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

# ── Project ───────────────────────────────────────────────────────────────────

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

# ── Regions & locations ───────────────────────────────────────────────────────

variable "region" {
  description = "region"
  default     = "us-central1"
}

variable "zone" {
  description = "zone"
  default     = "us-central1-c"
}

variable "cluster_location" {
  description = "GKE cluster zone or region (e.g. us-central1-c)"
  type        = string
}

# ── Networking ────────────────────────────────────────────────────────────────

variable "subnet_cidr" {
  description = "Primary IP CIDR range for the 'substrate' subnetwork"
  type        = string
  default     = "10.0.0.0/20"
}

variable "pods_cidr" {
  description = "Secondary CIDR range for GKE pod IPs (VPC-native)"
  type        = string
  default     = "10.1.0.0/16"
}

variable "services_cidr" {
  description = "Secondary CIDR range for GKE service IPs (VPC-native)"
  type        = string
  default     = "10.2.0.0/20"
}

# ── GKE cluster ───────────────────────────────────────────────────────────────

variable "cluster_name" {
  description = "GKE cluster name"
  type        = string
}

variable "cluster_version" {
  description = "GKE cluster version"
  type        = string
}

variable "default_node_machine_type" {
  description = "Machine type for the default (non-gVisor) node pool"
  type        = string
  default     = "e2-standard-2"
}

variable "default_pool_count" {
  description = "number of default nodepool nodes"
  default     = 1
}

variable "worker_node_machine_type" {
  description = "Machine type for the gVisor worker node pool"
  type        = string
  default     = "c3-standard-4"
}

variable "worker_pool_count" {
  description = "number of worker (gvisor) nodepool nodes"
  default     = 2
}

# ── Storage ───────────────────────────────────────────────────────────────────

variable "bucket_name" {
  description = "GCS bucket name for sandbox snapshots"
  type        = string
}

# ── Artifact Registry ─────────────────────────────────────────────────────────

variable "ar_repository_id" {
  description = "Artifact Registry repository ID"
  type        = string
  default     = "substrate"
}

variable "filestore" {
  description = "Flag to add Filestore implementation"
  type        = bool
  default     = false
}
