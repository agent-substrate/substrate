#!/usr/bin/env bash
# Copyright 2026 The Agent Substrate Authors
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

# Density scenario: adds actors with external volumes in steps and keeps them
# all running, until a first resume fails, an actor stops running, a worker
# node runs low on memory, or MAX actors are running. It runs wakestorm from
# this machine through a kubectl port-forward.
#
# After each step it writes one row to <out_dir>/steps.csv: actors, first
# resume results, actors RUNNING, NFS mounts and the lowest free memory and
# highest CPU across the worker nodes.
#
# Usage: density.sh <out_dir> <max_actors>
# Env:
#   START (default 40): actors in the first step.
#   STEP (default 20): actors added in each later step.
#   MIN_FREE_MIB (default 1024): stop when a worker node has less free memory.
#   TEMPLATE (default glutton-storage), ATESPACE (default benchmark).
#   WORKER_POOL (default benchmark-worker), WORKER_NAMESPACE (default
#   benchmark-workloads): the WorkerPool whose nodes are sampled.
#   CSI_NAMESPACE (default gcp-filestore-csi-driver), CSI_NODE_CONTAINER
#   (default gcp-filestore-driver): where to count NFS mounts.

set -o errexit -o nounset -o pipefail

out="${1:?usage: density.sh <out_dir> <max_actors>}"
max="${2:?usage: density.sh <out_dir> <max_actors>}"
start="${START:-40}"
step="${STEP:-20}"
min_free="${MIN_FREE_MIB:-1024}"
template="${TEMPLATE:-glutton-storage}"
atespace="${ATESPACE:-benchmark}"
pool="${WORKER_POOL:-benchmark-worker}"
pool_ns="${WORKER_NAMESPACE:-benchmark-workloads}"
csi_ns="${CSI_NAMESPACE:-gcp-filestore-csi-driver}"
csi_container="${CSI_NODE_CONTAINER:-gcp-filestore-driver}"
prefix=density

ROOT="$(git rev-parse --show-toplevel)"
bin="${out}/wakestorm"
mkdir -p "${out}"
go build -o "${bin}" "${ROOT}/benchmarking/storage/cmd/wakestorm"

mapfile -t nodes < <(kubectl get pods -n "${pool_ns}" -l "ate.dev/worker-pool=${pool}" \
  -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u)
if (( ${#nodes[@]} == 0 )); then
  echo "no pods for WorkerPool ${pool_ns}/${pool}" >&2
  exit 1
fi
echo "worker nodes: ${nodes[*]}" | tee "${out}/nodes.txt"

# node_stats prints "lowest free MiB,highest working set MiB,highest CPU millicores"
# across the worker nodes, from the kubelet summary API.
node_stats() {
  local n
  for n in "${nodes[@]}"; do
    kubectl get --raw "/api/v1/nodes/${n}/proxy/stats/summary" |
      jq -r '.node | "\((.memory.availableBytes/1048576)|floor) \((.memory.workingSetBytes/1048576)|floor) \((.cpu.usageNanoCores/1000000)|floor)"'
  done | awk 'NR==1 {f=$1; w=$2; c=$3} {if ($1<f) f=$1; if ($2>w) w=$2; if ($3>c) c=$3} END {printf "%d,%d,%d", f, w, c}'
}

# nfs_mounts prints the NFS mounts summed over the CSI node pods on the
# worker nodes.
nfs_mounts() {
  local n pod total=0 c
  for n in "${nodes[@]}"; do
    pod="$(kubectl get pods -n "${csi_ns}" --field-selector "spec.nodeName=${n}" -o name | awk '/node/ {print; exit}')"
    if [[ -z "${pod}" ]]; then
      echo "no CSI node pod in ${csi_ns} on ${n}; set CSI_NAMESPACE" >&2
      echo "clean up with: go run ./benchmarking/storage/cmd/wakestorm -phase delete -prefix ${prefix} -atespace ${atespace}" >&2
      exit 1
    fi
    c="$(kubectl exec -n "${csi_ns}" "${pod}" -c "${csi_container}" -- sh -c "grep -c ' nfs' /proc/mounts || true")"
    total=$(( total + c ))
  done
  echo "${total}"
}

running_actors() {
  kubectl ate --preview get actors -a "${atespace}" -o json |
    jq --arg p "${prefix}-" '[.. | objects | select(.metadata?.name? // "" | startswith($p)) | select(.status?.state? == "ACTOR_STATE_RUNNING")] | length'
}

printf 'actors,first_resume_ok,first_resume_failed,first_resume_p50_s,first_resume_p90_s,first_resume_max_s,running,nfs_mounts,min_free_mib,max_working_set_mib,max_cpu_m,failure_codes\n' \
  > "${out}/steps.csv"

total=0
target="${start}"
while (( total < max )); do
  target=$(( target > max ? max : target ))
  add=$(( target - total ))
  dir="${out}/step-${target}"
  echo "$(date -u +%T) step ${target}: adding ${add}"
  "${bin}" -phase create -prefix "${prefix}" -start "${total}" -n "${add}" -create-concurrency "${add}" \
    -template "${template}" -atespace "${atespace}" -settle 5s -repair-rounds 0 \
    -out "${dir}" > "${dir}.log" 2>&1 || true
  total="${target}"
  sleep 20

  fr="$(jq -r '[.phases[] | select(.phase == "first-resume")][0] |
    "\(.ok),\(.failed),\(.p50_s),\(.p90_s),\(.max_s),\(.failure_codes // {} | to_entries | map("\(.key)=\(.value)") | join(" "))"' \
    "${dir}/summary.json" 2>/dev/null || echo "0,${add},,,,wakestorm failed; see ${dir}.log")"
  IFS=, read -r ok failed p50 p90 pmax codes <<< "${fr}"
  running="$(running_actors)"
  mounts="$(nfs_mounts)"
  stats="$(node_stats)"
  echo "${total},${ok},${failed},${p50},${p90},${pmax},${running},${mounts},${stats},${codes}" | tee -a "${out}/steps.csv"

  if [[ "${failed}" != "0" || "${running}" -lt "${total}" ]]; then
    echo "$(date -u +%T) stop: ${failed} first resumes failed, ${running} of ${total} running"
    break
  fi
  free="${stats%%,*}"
  if (( free < min_free )); then
    echo "$(date -u +%T) stop: a worker node has ${free} MiB free (limit ${min_free})"
    break
  fi
  target=$(( total + step ))
done
echo "$(date -u +%T) done at ${total} actors; results in ${out}/steps.csv"
echo "clean up with: go run ./benchmarking/storage/cmd/wakestorm -phase delete -prefix ${prefix} -atespace ${atespace}"
