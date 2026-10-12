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

# Dump the node-side logs a benchmark run needs for phase_report.py: every
# atelet pod (the atelet-side timing breakdowns) and every worker pod
# (ateom-microvm writes its records to the worker pod's stdout). Point
# --since-time (RFC 3339) or --since at the run's start so the report covers
# exactly one run.
#
# kubectl logs returns only a container's current log file: once kubelet
# rotates it under load, earlier records are gone, so collect soon after the
# run. A container that restarted mid-run is dumped twice, its previous log
# under <pod>.previous.log. On GKE, Cloud Logging keeps every record past
# rotation and teardown; phase_report.py reads `gcloud logging read
# --format json` output directly.
#
# Like the other hack scripts, this sources .ate-dev-env.sh from the repo root
# for the cluster settings unless NO_DEV_ENV is set, and respects
# KUBECTL_CONTEXT.
#
# Usage: collect_logs.sh --dest DIR [--since 30m | --since-time 2026-09-24T18:00:00Z]
#                        [--namespace ate-system] [--worker-namespace benchmark-workloads]

set -uo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
if [[ -r "${ROOT}/.ate-dev-env.sh" ]] && [[ -z "${NO_DEV_ENV:-}" ]]; then
  # shellcheck source=/dev/null
  source "${ROOT}/.ate-dev-env.sh"
fi
run_kubectl() { kubectl ${KUBECTL_CONTEXT:+--context=${KUBECTL_CONTEXT}} "$@"; }

DEST=""
SINCE="1h"
SINCE_TIME=""
NAMESPACE="ate-system"
WORKER_NAMESPACE="benchmark-workloads"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dest) DEST="$2"; shift 2 ;;
    --since) SINCE="$2"; shift 2 ;;
    --since-time) SINCE_TIME="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --worker-namespace) WORKER_NAMESPACE="$2"; shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "${DEST}" ]]; then
  echo "usage: $0 --dest DIR [--since 30m | --since-time RFC3339] [--namespace ate-system] [--worker-namespace benchmark-workloads]" >&2
  exit 2
fi
mkdir -p "${DEST}"

if [[ -n "${SINCE_TIME}" ]]; then
  WINDOW=(--since-time="${SINCE_TIME}")
else
  WINDOW=(--since="${SINCE}")
fi

# One pod that cannot be read (still starting, evicted) must not cost the
# others, so every kubectl call warns and moves on rather than aborting.
collect() {
  local ns="$1" selector="$2"
  local pods
  if ! pods=$(run_kubectl get pods -n "${ns}" -l "${selector}" -o name); then
    echo "warn: could not list pods in ${ns} (${selector}); skipping" >&2
    return 0
  fi
  for pod in ${pods}; do
    local name="${pod#pod/}"
    echo "collecting ${ns}/${name} (${WINDOW[*]})"
    run_kubectl logs -n "${ns}" "${name}" "${WINDOW[@]}" --timestamps=false \
      > "${DEST}/${ns}-${name}.log" \
      || { echo "warn: kubectl logs ${ns}/${name} failed; skipping" >&2; rm -f "${DEST}/${ns}-${name}.log"; }
    # --previous is only valid after a restart; kubectl rejects it otherwise.
    local restarts
    restarts=$(run_kubectl get pod -n "${ns}" "${name}" \
      -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0)
    if [[ "${restarts:-0}" -gt 0 ]]; then
      echo "collecting ${ns}/${name} previous container (${restarts} restarts)"
      run_kubectl logs -n "${ns}" "${name}" --previous "${WINDOW[@]}" --timestamps=false \
        > "${DEST}/${ns}-${name}.previous.log" \
        || { echo "warn: kubectl logs --previous ${ns}/${name} failed; skipping" >&2; rm -f "${DEST}/${ns}-${name}.previous.log"; }
    fi
  done
}

collect "${NAMESPACE}" "app=atelet"
# Worker pods carry the pool label whatever the pool's name is.
collect "${WORKER_NAMESPACE}" "ate.dev/worker-pool"

echo "logs in ${DEST}; next:"
echo "  python3 benchmarking/analysis/phase_report.py ${DEST}/*.log"
