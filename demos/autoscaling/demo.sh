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
#
# Demo: WorkerPool autoscaling (issue #198).
#
#   reactive scale-UP   — a resume that finds no free worker emits a
#                         capacity-pressure signal; the autoscaler raises
#                         spec.replicas immediately (not at the next poll).
#   hysteretic scale-DOWN — once the buffer is in surplus, the pool shrinks
#                         back to minReady, but only after a stabilization
#                         window so a brief lull never throws away warm workers.
#
# See README.md for prerequisites. Usage:
#   ./demo.sh [run]      deploy if needed, then run the up + down scenario
#   ./demo.sh deploy     just apply the manifest
#   ./demo.sh cleanup    suspend/delete the demo actors and remove the manifest

set -o errexit -o nounset -o pipefail

ATE="${ATE:-kubectl ate}"                  # override e.g. ATE=./bin/kubectl-ate
KO="${KO:-hack/run-tool.sh ko}"            # how to resolve ko:// images
NS="ate-demo-autoscaling"
POOL="autoscaling"
TEMPLATE="${NS}/${POOL}"
ATESPACE="${ATESPACE:-demo-autoscaling}"   # atespace the demo actors live in
N="${N:-6}"                                # actors to wake (drives the burst)
SCALE_DOWN_WAIT="${SCALE_DOWN_WAIT:-180}"  # seconds to wait for hysteretic shrink
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMPL="${ROOT}/demos/autoscaling/autoscaling.yaml.tmpl"

banner() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
pool()   { kubectl get workerpool "$POOL" -n "$NS"; }
desired() { kubectl get workerpool "$POOL" -n "$NS" -o jsonpath='{.spec.replicas}'; }

render() {
  : "${BUCKET_NAME:?set BUCKET_NAME (GCS bucket for actor snapshots)}"
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" "$TMPL"
}

deploy() {
  banner "Deploying the autoscaled WorkerPool + ActorTemplate"
  render | ( cd "$ROOT" && $KO apply -f - )
  kubectl rollout status "deployment/${POOL}-deployment" -n "$NS" --timeout=300s
  kubectl wait --for=condition=Ready "actortemplate/${POOL}" -n "$NS" --timeout=300s
  # Actors live in an atespace; make sure the demo's exists (idempotent-ish:
  # AlreadyExists is fine).
  $ATE create atespace "$ATESPACE" >/dev/null 2>&1 || true
}

cleanup() {
  banner "Cleanup"
  for i in $(seq 1 "$N"); do
    $ATE suspend actor "demo-${i}" -a "$ATESPACE" >/dev/null 2>&1 || true
    $ATE delete actor "demo-${i}" -a "$ATESPACE" >/dev/null 2>&1 || true
  done
  $ATE delete atespace "$ATESPACE" >/dev/null 2>&1 || true
  render | kubectl delete --ignore-not-found -f -
}

# resume_with_retry tolerates the 503 a resume gets when the buffer is empty:
# that miss is exactly what triggers the scale-up, and the retry lands once a
# fresh worker has booted. Bounded so the demo never hangs on slow cold starts.
resume_with_retry() {
  local id="$1" tries=0
  until $ATE resume actor "$id" -a "$ATESPACE" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 15 ]; then
      echo "  resume ${id}: still no capacity after ${tries} tries (workers may still be booting)"
      return 0
    fi
    echo "  resume ${id}: no free worker (503) — autoscaler reacting; retrying (${tries})..."
    sleep 4
  done
  echo "  resume ${id}: running"
}

main() {
  if ! kubectl get workerpool "$POOL" -n "$NS" >/dev/null 2>&1; then
    deploy
  fi
  $ATE create atespace "$ATESPACE" >/dev/null 2>&1 || true

  banner "Initial state — autoscaler holds the warm buffer (minReady=2, targetBuffer=2)"
  pool
  echo
  echo "Tip: watch live in another terminal:"
  echo "    kubectl get workerpool ${POOL} -n ${NS} -w"
  echo "    kubectl logs -n ate-system deploy/ate-controller -f | grep 'autoscaled WorkerPool'"

  banner "Burst: waking ${N} actors — drains the buffer and triggers reactive scale-up"
  for i in $(seq 1 "$N"); do
    $ATE create actor "demo-${i}" -t "$TEMPLATE" -a "$ATESPACE" >/dev/null 2>&1 || true
  done
  for i in $(seq 1 "$N"); do
    resume_with_retry "demo-${i}"
  done
  sleep 3
  echo
  echo "After the burst (spec.replicas climbed toward occupied + targetBuffer, capped at maxReplicas=8):"
  pool

  banner "Idle: suspending all actors — frees workers, buffer goes into surplus"
  for i in $(seq 1 "$N"); do
    $ATE suspend actor "demo-${i}" -a "$ATESPACE" >/dev/null 2>&1 || true
  done
  pool
  echo
  echo "Scale-down is hysteretic — waiting up to ${SCALE_DOWN_WAIT}s for the stabilization window..."
  local deadline
  deadline=$(( $(date +%s) + SCALE_DOWN_WAIT ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    local d
    d="$(desired)"
    echo "  $(date +%H:%M:%S)  spec.replicas=${d}"
    if [ "${d:-99}" -le 2 ]; then
      echo "  shrunk to the reservation floor (minReady=2)."
      break
    fi
    sleep 10
  done

  banner "Done"
  pool
  echo
  echo "Run './demo.sh cleanup' to remove the demo actors and manifest."
}

case "${1:-run}" in
  run)     main ;;
  deploy)  deploy ;;
  cleanup) cleanup ;;
  *) echo "usage: $0 [run|deploy|cleanup]" >&2; exit 1 ;;
esac
