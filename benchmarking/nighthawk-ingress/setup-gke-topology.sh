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

# Label the fixed six-node rig and place atenet-router on its router node.
# Invoke with: bash setup-gke-topology.sh --config topology.env
set -euo pipefail

CONFIG=""
usage() {
  echo "Usage: $0 --config topology.env" >&2
  exit "${1:-1}"
}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) CONFIG="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done
[[ -n "${CONFIG}" && -f "${CONFIG}" ]] || usage
# shellcheck disable=SC1090
source "${CONFIG}"
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
for value in "${ROUTER_NODE:-}" "${RUNNER_NODE:-}" "${CONTROL_PLANE_NODE:-}"; do
  [[ -n "${value}" ]] || { echo "missing node in ${CONFIG}" >&2; exit 1; }
  kubectl get node "${value}" >/dev/null
done
[[ "${#WORKER_NODES[@]}" -eq 3 ]] || {
  echo "WORKER_NODES must name exactly three nodes" >&2; exit 1;
}
for node in "${WORKER_NODES[@]}"; do kubectl get node "${node}" >/dev/null; done

require_role() {
  local node="$1"
  local role="$2"
  local tainted="$3"
  local node_json
  node_json="$(kubectl get node "${node}" -o json)"
  [[ "$(jq -r '.metadata.labels["benchmarking.ate.dev/nighthawk-role"]' <<<"${node_json}")" == "${role}" ]] || {
    echo "${node} does not have benchmark role ${role}; create the cluster with create-gke-cluster.sh" >&2
    exit 1
  }
  [[ "${tainted}" == false ]] && return
  jq -e --arg role "${role}" '.spec.taints[]? | select(
    .key == "benchmarking.ate.dev/nighthawk-role" and
    .value == $role and .effect == "NoSchedule")' <<<"${node_json}" >/dev/null || {
      echo "${node} is missing the ${role}:NoSchedule benchmark taint" >&2
      exit 1
    }
}

require_role "${ROUTER_NODE}" router true
require_role "${RUNNER_NODE}" runner true
require_role "${CONTROL_PLANE_NODE}" control-plane false
for node in "${WORKER_NODES[@]}"; do require_role "${node}" worker true; done

# atelet is the node agent that hosts and resumes actors. Restrict it to actor
# workers so it does not consume the isolated router or runner nodes.
for daemonset in $(kubectl -n ate-system get daemonset -l app=atelet -o name); do
  kubectl -n ate-system patch "${daemonset}" --type merge -p \
    '{"spec":{"template":{"spec":{"nodeSelector":{"benchmarking.ate.dev/nighthawk-role":"worker"}}}}}'
  if ! kubectl -n ate-system get "${daemonset}" -o json | jq -e \
    '.spec.template.spec.tolerations[]? | select(
      .key == "benchmarking.ate.dev/nighthawk-role" and
      .operator == "Equal" and .value == "worker" and .effect == "NoSchedule")' >/dev/null; then
    kubectl -n ate-system patch "${daemonset}" --type json -p \
      '[{"op":"add","path":"/spec/template/spec/tolerations/-","value":{"key":"benchmarking.ate.dev/nighthawk-role","operator":"Equal","value":"worker","effect":"NoSchedule"}}]'
  fi
  kubectl -n ate-system rollout status "${daemonset}" --timeout=5m
done

# Hostname, rather than a mutable custom label, makes the router placement
# explicit in the Deployment and prevents scheduler drift during a capacity run.
kubectl -n ate-system patch deployment atenet-router --type merge -p \
  "{\"spec\":{\"template\":{\"spec\":{\"nodeSelector\":{\"kubernetes.io/hostname\":\"${ROUTER_NODE}\"},\"tolerations\":[{\"key\":\"benchmarking.ate.dev/nighthawk-role\",\"operator\":\"Equal\",\"value\":\"router\",\"effect\":\"NoSchedule\"}]}}}}"
kubectl -n ate-system rollout status deployment/atenet-router --timeout=5m

cat <<EOF
Topology applied. Deploy workers and run the benchmark with:
  benchmarking/workloads/deploy.sh --deploy --worker-count 50 --sandbox-class gvisor \\
    --node-selector benchmarking.ate.dev/nighthawk-role=worker \\
    --toleration benchmarking.ate.dev/nighthawk-role=worker:NoSchedule
  ./benchmarking/nighthawk-ingress/run-dev.sh --dataplane agentgateway --proxy-cpu 2 \\
    --runner-node ${RUNNER_NODE}
EOF
