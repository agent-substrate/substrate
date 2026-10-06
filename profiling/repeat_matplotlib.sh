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

# Repeat the matplotlib stream-vs-pull benchmark N times on fresh nodes.
# Run from the substrate repo root after the v3_runbook.md step-0 shell setup
# (CLUSTER, ZONE, NODE_VERSION, IM, SLUG_M, DS, VER must be set).
#
#   N=15 GAP=90 profiling/repeat_matplotlib.sh
#
# Each rep: recreate both node pools (cold node caches) -> WorkerPools ->
# coldboot + lifecycle per arm, arm order alternating per rep.
set -uo pipefail

N="${N:-15}"
GAP="${GAP:-90}"                 # seconds; >= observed ~51 s matplotlib background download
OUT="${OUT:-profiling/results/v3/repeat}"
IM="${IM:-matplotlib__matplotlib-23476}"
SLUG_M="${SLUG_M:-swebench-$(echo "$IM" | tr '_' '-' | tr -s '-')}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
: "${CLUSTER:?}" "${ZONE:?}" "${NODE_VERSION:?}" "${VER:?}" "${DS:?}"

export KO_DOCKER_REPO="${KO_DOCKER_REPO:-gcr.io/${PROJECT_ID:?}/ate-images}"
MACHINE_TYPE="${MACHINE_TYPE:-c3-standard-4}"
DISK_TYPE="${DISK_TYPE:-pd-balanced}"
DISK_SIZE="${DISK_SIZE:-100}"

mkdir -p "$OUT"
cat > "$OUT/config.json" <<EOF
{
  "cluster": "$CLUSTER",
  "zone": "$ZONE",
  "node_version": "$NODE_VERSION",
  "substrate_version": "$VER",
  "machine_type": "$MACHINE_TYPE",
  "disk_type": "$DISK_TYPE",
  "disk_size_gb": $DISK_SIZE,
  "instance": "$IM",
  "n": $N,
  "gap_s": $GAP
}
EOF

create_pool() {  # $1 = pool, $2 = --enable-image-streaming | --no-enable-image-streaming
  gcloud container node-pools create "$1" --cluster "$CLUSTER" --zone "$ZONE" \
    --machine-type "$MACHINE_TYPE" --disk-type "$DISK_TYPE" --disk-size "$DISK_SIZE" \
    --image-type COS_CONTAINERD --node-version "$NODE_VERSION" --num-nodes 2 \
    --workload-metadata GKE_METADATA \
    --node-labels "ate.dev/substrate-version=$VER,bench-arm=$1" \
    --no-enable-autoupgrade --no-enable-autorepair "$2" --quiet
}
mkpool() {  # $1 = WorkerPool, $2 = node pool
  sed -e "s|name: profiling$|name: $1|" -e "s|workload: profiling$|workload: $1|" \
      -e "s|\${SUBSTRATE_VERSION}|$VER|g" \
      -e "s|\${WORKER_COUNT}|3|g; s|\${WORKER_CPU}|1|g; s|\${WORKER_MEMORY}|2Gi|g" \
      -e "s|^\(      ate.dev/substrate-version: .*\)$|\1\n      cloud.google.com/gke-nodepool: $2|" \
      profiling/profiling-pool.yaml.tmpl
}
nodecmd() {  # $1 = node, $2 = shell command run on the host; prints its stdout
  local p
  p=$(kubectl debug "node/$1" --image=busybox --profile=sysadmin -- chroot /host sh -c "$2" 2>&1 \
      | grep -o 'node-debugger-[a-z0-9-]*' | head -1)
  until [[ "$(kubectl get pod "$p" -o jsonpath='{.status.phase}' 2>/dev/null)" =~ Succeeded|Failed ]]; do sleep 2; done
  kubectl logs "$p"; kubectl delete pod "$p" --wait=false >/dev/null
}
wait_gcfsd_idle() {  # $1 = since "YYYY-MM-DD HH:MM:SS" UTC; waits until every pool-stream node has
                     # as many "download/untar done" lines as "download started" lines
  local since="$1" deadline=$((SECONDS + 600)) busy n s d
  while :; do
    busy=0
    for n in $(kubectl get nodes -l cloud.google.com/gke-nodepool=pool-stream -o name | cut -d/ -f2); do
      read -r s d < <(nodecmd "$n" "j=\$(journalctl -u gcfsd --since '$since' --no-pager); \
        echo \$(echo \"\$j\" | grep -c 'Async layer download started') \
             \$(echo \"\$j\" | grep -c 'Layer download/untar done (succeeded=true)')")
      echo "  gcfsd $n: started=$s done=$d"
      (( s > d )) && busy=1
    done
    (( busy == 0 )) && return 0
    (( SECONDS > deadline )) && { echo "  WARNING: gcfsd still busy after 600 s"; return 1; }
    sleep 10
  done
}
run_arm() {  # $1 = stream|pull, $2 = rep dir
  local arm=$1 dir=$2 start
  for a in $(kubectl-ate get actor -a ate-golden 2>/dev/null | awk -v t="ate-prof-$arm/$SLUG_M" '$3 == t {print $2}'); do
    kubectl-ate delete actor "$a" -a ate-golden --any-state || true
  done
  start=$(date -u '+%F %T')
  profiling/profile_coldboot.py --instance "$IM" --atespace "ate-prof-$arm" \
    --worker-namespace ate-profiling --worker-pool "profiling-$arm" \
    --env "WORKLOAD=profiling-$arm" --json "$dir/$arm-coldboot-$IM.json"
  sleep "$GAP"                                   # same spacing on both arms
  [[ $arm == stream ]] && wait_gcfsd_idle "$start" # will need to remove this when running workload benchmarks
  profiling/profile_lifecycle.py --atespace "ate-prof-$arm" --template "ate-prof-$arm/$SLUG_M" \
    --worker-namespace ate-profiling --worker-pool "profiling-$arm" \
    --step-gap "$GAP" --json "$dir/$arm-lifecycle-$IM.json"
}

for i in $(seq 1 "$N"); do
  dir="$OUT/rep-$(printf %02d "$i")"; mkdir -p "$dir"
  echo "=== rep $i/$N -> $dir ($(date -u +%T))"

  kubectl -n ate-profiling delete workerpool profiling-stream profiling-pull --ignore-not-found
  # Sequential: GKE rejects concurrent node-pool operations on one cluster.
  for P in pool-stream pool-pull; do
    gcloud container node-pools delete "$P" --cluster "$CLUSTER" --zone "$ZONE" --quiet || true
  done
  create_pool pool-stream --enable-image-streaming
  create_pool pool-pull --no-enable-image-streaming
  kubectl -n ate-system rollout status ds/"$DS" --timeout=10m
  mkpool profiling-stream pool-stream | hack/run-tool.sh ko apply -f -
  mkpool profiling-pull pool-pull | hack/run-tool.sh ko apply -f -
  kubectl -n ate-profiling wait --for=jsonpath='{.status.readyReplicas}'=3 \
    workerpool/profiling-stream workerpool/profiling-pull --timeout=10m
  kubectl -n ate-profiling get pods -o wide > "$dir/workers.txt"

  if (( i % 2 )); then order="stream pull"; else order="pull stream"; fi
  for arm in $order; do run_arm "$arm" "$dir"; done

  # Flag reps that did not measure what they claim to.
  jq -r '"stream coldboot path: \(.streaming["swebench-verified path"] // "MISSING")"' "$dir/stream-coldboot-$IM.json"
  jq -r '"pull coldboot image: \(.images["swebench-verified"] // "MISSING")"' "$dir/pull-coldboot-$IM.json"
done
echo "done: $OUT"
