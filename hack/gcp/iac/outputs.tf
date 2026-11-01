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

output "cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.substrate.name
}

output "cluster_location" {
  description = "GKE cluster location"
  value       = google_container_cluster.substrate.location
}

output "bucket_name" {
  description = "Snapshot GCS bucket name"
  value       = google_storage_bucket.snapshots.name
}

output "artifact_registry_url" {
  description = "Artifact Registry repository URL — use as KO_DOCKER_REPO"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.ate_images.repository_id}"
}

output "get_credentials_command" {
  description = "gcloud command to configure kubectl for this cluster"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.substrate.name} --location ${google_container_cluster.substrate.location} --project ${var.project_id}"
}
