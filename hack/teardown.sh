#!/usr/bin/env bash

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

set -eu

# Source the environment variables
if [ -f .ate-dev-env.sh ]; then
  source .ate-dev-env.sh
else
  echo "Please create .ate-dev-env.sh from the example file in hack"
  exit 1
fi

# Precheck for cluster-admin permissions
kubectl auth can-i delete crd 2>/dev/null | grep -q yes || {
    echo "teardown requires cluster-admin on the GKE cluster" >&2
    exit 1
}

# --- Helper Functions ---
function usage() {
  echo "Usage: $0 [options]"
  echo "Options:"
  echo "  --revoke-gke-node-permissions         Revoke GKE nodes permission to pull images"
  echo "  --delete-iam-policy-bindings          Delete IAM policy bindings for atelet"
  echo "  --delete-snapshot-bucket              Delete snapshot bucket"
  echo "  --delete-gvisor-node-pool             Delete gVisor node pool"
  echo "  --delete-cluster                      Delete GKE cluster"
  echo "  --all                                 Run all teardown steps (reverse order of setup)"
  exit 1
}

# --- Teardown Functions ---

# Revoke GKE Node Permissions (Reverse of grant_gke_node_permissions)
revoke_gke_node_permissions() {
  echo "Revoking GKE node permissions..."
  gcloud projects remove-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/storage.objectViewer" \
    --condition=None \
    --quiet || true
  gcloud projects remove-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/artifactregistry.reader" \
    --condition=None \
    --quiet || true
}

# Delete IAM Policy Bindings for Bucket (Reverse of create_iam_policy_bindings)
delete_iam_policy_bindings() {
  echo "Deleting IAM policy bindings for bucket..."
  gcloud storage buckets remove-iam-policy-binding "gs://${BUCKET_NAME}" \
    --member="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/ate-system/sa/atelet" \
    --role="roles/storage.objectAdmin" \
    --quiet || true
  gcloud storage buckets remove-iam-policy-binding "gs://${BUCKET_NAME}" \
    --member="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/ate-system/sa/atelet" \
    --role="roles/storage.bucketViewer" \
    --quiet || true
}

# Delete Snapshot Bucket (Reverse of create_snapshot_bucket)
delete_snapshot_bucket() {
  echo "Deleting snapshot bucket..."
  gcloud storage rm --recursive "gs://${BUCKET_NAME}/**" --project="${PROJECT_ID}" --quiet || true
  gcloud storage buckets delete "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" --quiet || true
}

# Delete gVisor Node Pool (Reverse of create_gvisor_node_pool)
delete_gvisor_node_pool() {
  echo "Deleting gVisor node pool..."
  gcloud container node-pools delete "${NODE_POOL_NAME}" \
    --cluster="${CLUSTER_NAME}" \
    --location="${CLUSTER_LOCATION}" \
    --project="${PROJECT_ID}" \
    --quiet || true
}

# Delete Cluster (Reverse of create_cluster)
delete_cluster() {
  echo "Deleting GKE cluster..."
  gcloud container clusters delete "${CLUSTER_NAME}" \
    --location="${CLUSTER_LOCATION}" \
    --project="${PROJECT_ID}" \
    --quiet || true
}

# --- Main Logic ---
if [ "$#" -eq 0 ]; then
  usage
fi

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --revoke-gke-node-permissions) revoke_gke_node_permissions ;;
    --delete-iam-policy-bindings) delete_iam_policy_bindings ;;
    --delete-snapshot-bucket) delete_snapshot_bucket ;;
    --delete-gvisor-node-pool) delete_gvisor_node_pool ;;
    --delete-cluster) delete_cluster ;;
    --all)
      revoke_gke_node_permissions
      delete_iam_policy_bindings
      delete_snapshot_bucket
      delete_gvisor_node_pool
      delete_cluster
      ;;
    *) usage ;;
  esac
  shift
done
