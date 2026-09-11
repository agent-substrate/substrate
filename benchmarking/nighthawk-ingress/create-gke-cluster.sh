#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Create the fixed six-node GKE rig used by the ingress-capacity benchmark.
# This deliberately creates one node pool per isolated role: a router node,
# a Nighthawk runner node, three actor-worker nodes, and an untainted node for
# the substrate control plane and Kubernetes system workloads.
set -euo pipefail

PROJECT=""
CLUSTER="substrate-nighthawk"
LOCATION="us-east4-a"
CLUSTER_VERSION="1.37"
RELEASE_CHANNEL="rapid"
MACHINE_TYPE="c3-standard-22"
NETWORK="default"
SUBNETWORK="default"
TOPOLOGY_FILE=""
ROLE_KEY="benchmarking.ate.dev/nighthawk-role"

usage() {
  cat <<EOF
Usage: $0 --project PROJECT [options]

Create the reproducible six-node Nighthawk ingress benchmark cluster.

  --project PROJECT          GCP project (required)
  --cluster NAME             GKE cluster name (default: ${CLUSTER})
  --location ZONE            Zonal GKE location (default: ${LOCATION})
  --cluster-version VERSION  GKE version/minor (default: ${CLUSTER_VERSION})
  --release-channel CHANNEL  GKE release channel (default: ${RELEASE_CHANNEL})
  --machine-type TYPE        Node machine type (default: ${MACHINE_TYPE})
  --network NAME             VPC network (default: ${NETWORK})
  --subnetwork NAME          Regional subnetwork (default: ${SUBNETWORK})
  --topology-file PATH       Generated topology env file (default: /tmp/<cluster>-topology.env)
  -h, --help                 Show this help

The script creates these c3-standard-22 pools:
  benchmark-control-plane  1 untainted node for Kubernetes and substrate control-plane pods
  benchmark-router         1 node tainted ${ROLE_KEY}=router:NoSchedule
  benchmark-runner         1 node tainted ${ROLE_KEY}=runner:NoSchedule
  benchmark-workers        3 nodes tainted ${ROLE_KEY}=worker:NoSchedule

It never deletes or mutates an existing cluster. GKE 1.37+ is required because
Substrate uses the GA PodCertificate and ClusterTrustBundle APIs.
EOF
  exit "${1:-1}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2 ;;
    --cluster) CLUSTER="$2"; shift 2 ;;
    --location) LOCATION="$2"; shift 2 ;;
    --cluster-version) CLUSTER_VERSION="$2"; shift 2 ;;
    --release-channel) RELEASE_CHANNEL="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --network) NETWORK="$2"; shift 2 ;;
    --subnetwork) SUBNETWORK="$2"; shift 2 ;;
    --topology-file) TOPOLOGY_FILE="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done

[[ -n "${PROJECT}" ]] || { echo "--project is required" >&2; usage; }
[[ "${CLUSTER_VERSION}" =~ ^1\.(3[7-9]|[4-9][0-9])($|\.) ]] || {
  echo "--cluster-version must be GKE 1.37 or later" >&2
  exit 1
}
TOPOLOGY_FILE="${TOPOLOGY_FILE:-/tmp/${CLUSTER}-nighthawk-topology.env}"

for command in gcloud kubectl; do
  command -v "${command}" >/dev/null || {
    echo "${command} is required" >&2
    exit 1
  }
done

if gcloud container clusters describe "${CLUSTER}" --project "${PROJECT}" \
  --location "${LOCATION}" >/dev/null 2>&1; then
  echo "cluster ${CLUSTER} already exists; refusing to mutate it" >&2
  exit 1
fi

gcloud services enable container.googleapis.com --project "${PROJECT}"

gcloud container clusters create "${CLUSTER}" \
  --project "${PROJECT}" \
  --location "${LOCATION}" \
  --cluster-version "${CLUSTER_VERSION}" \
  --release-channel "${RELEASE_CHANNEL}" \
  --machine-type "${MACHINE_TYPE}" \
  --node-pool "benchmark-control-plane" \
  --num-nodes 1 \
  --node-labels "${ROLE_KEY}=control-plane" \
  --no-enable-autoupgrade \
  --no-enable-autorepair \
  --disk-type pd-balanced \
  --disk-size 100 \
  --enable-ip-alias \
  --enable-dataplane-v2 \
  --addons HttpLoadBalancing,HorizontalPodAutoscaling,NodeLocalDNS,GcePersistentDiskCsiDriver \
  --enable-managed-prometheus \
  --workload-pool "${PROJECT}.svc.id.goog" \
  --network "${NETWORK}" \
  --subnetwork "${SUBNETWORK}"

create_tainted_pool() {
  local name="$1"
  local role="$2"
  local count="$3"
  gcloud container node-pools create "${name}" \
    --project "${PROJECT}" \
    --cluster "${CLUSTER}" \
    --location "${LOCATION}" \
    --machine-type "${MACHINE_TYPE}" \
    --num-nodes "${count}" \
    --node-labels "${ROLE_KEY}=${role}" \
    --node-taints "${ROLE_KEY}=${role}:NoSchedule" \
    --no-enable-autoupgrade \
    --no-enable-autorepair \
    --disk-type pd-balanced \
    --disk-size 100
}

create_tainted_pool benchmark-router router 1
create_tainted_pool benchmark-runner runner 1
create_tainted_pool benchmark-workers worker 3

gcloud container clusters get-credentials "${CLUSTER}" \
  --project "${PROJECT}" --location "${LOCATION}"

router_node="$(kubectl get nodes -l "${ROLE_KEY}=router" -o jsonpath='{.items[0].metadata.name}')"
runner_node="$(kubectl get nodes -l "${ROLE_KEY}=runner" -o jsonpath='{.items[0].metadata.name}')"
control_plane_node="$(kubectl get nodes -l "${ROLE_KEY}=control-plane" -o jsonpath='{.items[0].metadata.name}')"
mapfile -t worker_nodes < <(kubectl get nodes -l "${ROLE_KEY}=worker" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
[[ "${#worker_nodes[@]}" -eq 3 ]] || {
  echo "expected three worker nodes, got ${#worker_nodes[@]}" >&2
  exit 1
}

cat >"${TOPOLOGY_FILE}" <<EOF
# Generated by create-gke-cluster.sh for ${PROJECT}/${CLUSTER} in ${LOCATION}.
ROUTER_NODE="${router_node}"
RUNNER_NODE="${runner_node}"
CONTROL_PLANE_NODE="${control_plane_node}"
WORKER_NODES=(
  "${worker_nodes[0]}"
  "${worker_nodes[1]}"
  "${worker_nodes[2]}"
)
EOF

cat <<EOF
Cluster is ready. The role taints are enforced by the benchmark manifests:
  router  -> patched atenet-router toleration
  runner  -> Nighthawk Job runnerTolerations
  worker  -> WorkerPool template tolerations

Next, provision the substrate dependencies, deploy substrate, then apply the
placement patch and worker pool:
  hack/install-ate.sh --deploy-ate-system
  bash benchmarking/nighthawk-ingress/setup-gke-topology.sh --config ${TOPOLOGY_FILE}
  benchmarking/workloads/deploy.sh --deploy --worker-count 50 --sandbox-class gvisor \\
    --node-selector ${ROLE_KEY}=worker \\
    --toleration ${ROLE_KEY}=worker:NoSchedule
EOF
