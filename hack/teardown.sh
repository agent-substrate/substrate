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

set -o errexit -o nounset -o pipefail

# Source the environment variables. The file is optional: an installer (or a
# user pasting a one-liner) can pass the same variables through the
# environment instead, which also makes teardown possible on a machine that
# never had a dev-env file.
if [ -f .ate-dev-env.sh ]; then
  source .ate-dev-env.sh
fi
# No cluster-admin precheck here: every step below talks to GCP, not to the
# cluster, and requiring a live kubectl context would block tearing down a
# cluster that is already half-gone.

# require checks that each named variable is set, so a step fails up front
# with the variable's name rather than mid-deletion with a gcloud error. Each
# step declares only what it uses: deleting a bucket must not demand a node
# pool name.
require() {
  for var in "$@"; do
    if [ -z "${!var:-}" ]; then
      echo "${var} is not set; export it or create .ate-dev-env.sh from the example file in hack" >&2
      exit 1
    fi
  done
}

# --- Helper Functions ---
function usage() {
  echo "Usage: $0 [options]"
  echo "Options:"
  echo "  --revoke-gke-node-permissions         Revoke GKE nodes permission to pull images"
  echo "  --revoke-atelet-permissions           Revoke atelet's project-level IAM bindings"
  echo "  --delete-iam-policy-bindings          Delete IAM policy bindings for atelet"
  echo "  --delete-snapshot-bucket              Delete snapshot bucket"
  echo "  --delete-gvisor-node-pool             Delete gVisor node pool"
  echo "  --delete-cluster                      Delete GKE cluster"
  echo "  --delete-dashboards                   Delete the Substrate monitoring dashboards"
  echo "  --all                                 Run all teardown steps (reverse order of setup)"
  exit 1
}

# --- Teardown Functions ---

# Revoke GKE Node Permissions (Reverse of grant_gke_node_permissions)
revoke_gke_node_permissions() {
  require PROJECT_ID PROJECT_NUMBER
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

# Revoke Atelet's project-level bindings (Reverse of grant_atelet_permissions)
revoke_atelet_permissions() {
  require PROJECT_ID PROJECT_NUMBER
  echo "Revoking atelet project-level permissions..."
  local member="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/ate-system/sa/atelet"
  gcloud projects remove-iam-policy-binding "${PROJECT_ID}" \
    --member="${member}" \
    --role="roles/storage.objectAdmin" \
    --condition=None \
    --quiet || true
  gcloud projects remove-iam-policy-binding "${PROJECT_ID}" \
    --member="${member}" \
    --role="roles/artifactregistry.reader" \
    --condition=None \
    --quiet || true
}

# Delete Monitoring Dashboards (Reverse of create_monitoring_dashboards)
delete_dashboards() {
  require PROJECT_ID
  echo "Deleting Substrate monitoring dashboards..."
  # Matched by display name, since setup only records names in its JSON.
  local names=(
    "Substrate Snapshot Size & QPS"
    "Substrate Routing & E2E Latency"
    "Substrate gRPC Server — latency / QPS / errors"
  )
  for display_name in "${names[@]}"; do
    for dashboard in $(gcloud monitoring dashboards list \
        --project="${PROJECT_ID}" \
        --filter="displayName=\"${display_name}\"" \
        --format="value(name)" 2>/dev/null); do
      gcloud monitoring dashboards delete "${dashboard}" \
        --project="${PROJECT_ID}" \
        --quiet || true
    done
  done
}

# Delete IAM Policy Bindings for Bucket (Reverse of create_iam_policy_bindings)
delete_iam_policy_bindings() {
  require PROJECT_ID PROJECT_NUMBER BUCKET_NAME
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
  require PROJECT_ID BUCKET_NAME
  echo "Deleting snapshot bucket..."
  gcloud storage rm --recursive "gs://${BUCKET_NAME}/**" --project="${PROJECT_ID}" --quiet || true
  gcloud storage buckets delete "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" --quiet || true
}

# Delete gVisor Node Pool (Reverse of create_gvisor_node_pool)
delete_gvisor_node_pool() {
  require PROJECT_ID CLUSTER_NAME CLUSTER_LOCATION NODE_POOL_NAME
  echo "Deleting gVisor node pool..."
  gcloud container node-pools delete "${NODE_POOL_NAME}" \
    --cluster="${CLUSTER_NAME}" \
    --location="${CLUSTER_LOCATION}" \
    --project="${PROJECT_ID}" \
    --quiet || true
}

# Delete Cluster (Reverse of create_cluster)
delete_cluster() {
  require PROJECT_ID CLUSTER_NAME CLUSTER_LOCATION
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
    --revoke-atelet-permissions) revoke_atelet_permissions ;;
    --delete-iam-policy-bindings) delete_iam_policy_bindings ;;
    --delete-snapshot-bucket) delete_snapshot_bucket ;;
    --delete-gvisor-node-pool) delete_gvisor_node_pool ;;
    --delete-cluster) delete_cluster ;;
    --delete-dashboards) delete_dashboards ;;
    --all)
      delete_dashboards
      delete_iam_policy_bindings
      revoke_atelet_permissions
      revoke_gke_node_permissions
      delete_snapshot_bucket
      # Deleting the cluster removes its node pools, so --all does not insist
      # on a pool name a caller (e.g. an installer) may not track.
      if [ -n "${NODE_POOL_NAME:-}" ]; then
        delete_gvisor_node_pool
      else
        echo "NODE_POOL_NAME not set; skipping node pool deletion (the cluster deletion removes its pools)"
      fi
      delete_cluster
      ;;
    *) usage ;;
  esac
  shift
done
