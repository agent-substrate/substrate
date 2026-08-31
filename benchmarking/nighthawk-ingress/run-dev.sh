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

# Quick dev run of the Nighthawk ingress-capacity benchmark (~8-10 min).
#
# Runs ONLY the benchmark layer against an already-prepared cluster: pins
# the router if needed, builds the runner image from the WORKING TREE
# (uncommitted changes included), submits the benchmark Job, waits, and
# prints capacity.json. Cluster prerequisites (substrate + workers) are set
# up by the user — see benchmarking/nighthawk-ingress/README.md.
#
#   ./benchmarking/nighthawk-ingress/run-dev.sh --envoy-cpu 2
set -euo pipefail

ENVOY_CPU=2
ACTORS=100
TAIL_LATENCY_SLO_MS=25
ATESPACE="ingress-benchmark"
DEST=""
VENV="${HOME}/.venvs/substrate-bench"
NAMESPACE="benchmarking"

usage() {
  cat <<EOF
Usage: $0 [options]
  --envoy-cpu N             router cpu pin, the independent variable (default: ${ENVOY_CPU})
  --actors N                actor fleet size; needs that many workers Running (default: ${ACTORS})
  --tail-latency-slo-ms N   SLO bound; 0 disables (default: ${TAIL_LATENCY_SLO_MS})
  --atespace NAME           actor namespace (default: ${ATESPACE})
  --dest gs://...           results root (default: gs://\$BUCKET_NAME/nighthawk-ingress-results)
EOF
  exit "${1:-1}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --envoy-cpu) ENVOY_CPU="$2"; shift 2 ;;
    --actors) ACTORS="$2"; shift 2 ;;
    --tail-latency-slo-ms) TAIL_LATENCY_SLO_MS="$2"; shift 2 ;;
    --atespace) ATESPACE="$2"; shift 2 ;;
    --dest) DEST="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

# --- preflight (each failure prints its fix) ---------------------------------
[[ -f .ate-dev-env.sh ]] || {
  echo "ERROR: .ate-dev-env.sh not found at repo root (see GKE Quickstart in the repo README)" >&2
  exit 1
}
# shellcheck disable=SC1091
source .ate-dev-env.sh
for var in PROJECT_ID CLUSTER_NAME CLUSTER_LOCATION KO_DOCKER_REPO BUCKET_NAME; do
  [[ -n "${!var:-}" ]] || { echo "ERROR: ${var} not set by .ate-dev-env.sh" >&2; exit 1; }
done
DEST="${DEST:-gs://${BUCKET_NAME}/nighthawk-ingress-results}"

docker info >/dev/null 2>&1 || {
  echo "ERROR: docker daemon not running (the runner image is built locally)" >&2
  exit 1
}

kubectl get deployment atenet-router -n ate-system >/dev/null 2>&1 || {
  echo "ERROR: substrate is not deployed on ${CLUSTER_NAME}. Run:" >&2
  echo "  hack/install-ate.sh --deploy-ate-system" >&2
  exit 1
}

RUNNING_WORKERS="$(kubectl get pods -n benchmark-workloads --no-headers 2>/dev/null | grep -c ' Running ' || true)"
if (( RUNNING_WORKERS < ACTORS )); then
  echo "ERROR: ${RUNNING_WORKERS} workers Running in benchmark-workloads, need ${ACTORS}. Run:" >&2
  echo "  benchmarking/workloads/deploy.sh --deploy --worker-count ${ACTORS} --sandbox-class gvisor" >&2
  exit 1
fi

# Every value below is used as-is, defaults included — read this before the
# router gets pinned.
echo ">>> run config:"
echo "      cluster:              ${CLUSTER_NAME} (${CLUSTER_LOCATION})"
echo "      envoy_cpu:            ${ENVOY_CPU}"
echo "      actors:               ${ACTORS}"
echo "      workers running:      ${RUNNING_WORKERS}"
echo "      tail_latency_slo_ms:  ${TAIL_LATENCY_SLO_MS}"
echo "      atespace:             ${ATESPACE}"
echo "      dest:                 ${DEST}"

# venv: importing orchestrator.py (defaults/rendering/patch) needs PyYAML.
if ! "${VENV}/bin/python" -c 'import yaml' 2>/dev/null; then
  python3 -m venv "${VENV}"
  "${VENV}/bin/pip" install --quiet pyyaml ||
    "${VENV}/bin/pip" install --quiet --index-url https://pypi.org/simple/ pyyaml
fi
PY="${VENV}/bin/python"

# --- pin the router if its current cpu differs --------------------------------
CURRENT_CPU="$(kubectl get deployment atenet-router -n ate-system \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="envoy")].resources.limits.cpu}')"
if [[ "${CURRENT_CPU}" != "${ENVOY_CPU}" ]]; then
  echo ">>> pinning router: envoy cpu '${CURRENT_CPU:-unset}' -> ${ENVOY_CPU}"
  "${PY}" -c "
import sys
sys.path.insert(0, 'benchmarking/automation')
from testtypes import nighthawk_ingress
nighthawk_ingress.pre_test({'nighthawk-ingress': {'envoyCpu': ${ENVOY_CPU}}})"
fi

# --- runner image from the working tree ---------------------------------------
SHA7="$(git rev-parse --short=7 HEAD)"
DIRTY=""
[[ -n "$(git status --porcelain)" ]] && DIRTY="-dirty"
TAG="quick-${SHA7}${DIRTY}-$(date +%H%M%S)"
IMAGE="${KO_DOCKER_REPO}/nighthawk-ingress-test:${TAG}"
echo ">>> building ${IMAGE} from the working tree"
# --platform is required: the clusters are amd64 and the default target on
# an arm64 host produces an image the nodes cannot pull.
docker build --platform linux/amd64 \
  -f benchmarking/nighthawk-ingress/Dockerfile -t "${IMAGE}" .
docker push "${IMAGE}"

# --- render + submit the Job ---------------------------------------------------
NAME="ingress_routercap_envoy_${ENVOY_CPU}cpu"
JOB="runner-ingress-routercap-${ENVOY_CPU}cpu-quick-$(date +%H%M%S)"
export IMAGE JOB NAME DEST TAG ENVOY_CPU ACTORS TAIL_LATENCY_SLO_MS ATESPACE
"${PY}" - <<'EOF' | kubectl apply -f -
import os
import sys

sys.path.insert(0, "benchmarking/automation")
import orchestrator
from testtypes import nighthawk_ingress

test = {
    "name": os.environ["NAME"],
    "type": "nighthawk-ingress",
    "targetCluster": "dev",
    "duration": "30m",
    "workerCount": int(os.environ["ACTORS"]),
    "nighthawk-ingress": {
        "envoyCpu": int(os.environ["ENVOY_CPU"]),
        "atespace": os.environ["ATESPACE"],
        "tailLatencySloMs": float(os.environ["TAIL_LATENCY_SLO_MS"]),
    },
}
orchestrator.validate_and_normalize_tests([test])
subs = {
    "JOB_NAME": os.environ["JOB"],
    "IMAGE": os.environ["IMAGE"],
    "TAG": os.environ["TAG"],
    "NAME": test["name"],
    "DEST": os.environ["DEST"],
    "ATE_ATEAPI_CLIENT_AUTH": os.environ.get("ATE_ATEAPI_CLIENT_AUTH", "cert"),
    **nighthawk_ingress.job_subs(test),
}
tmpl = nighthawk_ingress.job_tmpl("benchmarking/automation/manifests")
print(orchestrator.render_template(tmpl, subs))
EOF

# --- wait, streaming the runner's own log lines --------------------------------
# [service]/[adaptive] lines are Nighthawk's raw textproto/metric dumps —
# they are in the uploaded logs.txt; too noisy for the terminal.
echo ">>> waiting for job/${JOB} (progress lines only; full logs land in logs.txt)"
( until kubectl logs -f "job/${JOB}" -n "${NAMESPACE}" 2>/dev/null \
    | grep --line-buffered -vE '^\[(service|adaptive)\] '; do sleep 5; done ) &
LOGS_PID=$!
STATUS="timeout"
for _ in $(seq 1 150); do  # 150 x 10s = 25 min
  S="$(kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.succeeded}' 2>/dev/null)"
  F="$(kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.failed}' 2>/dev/null)"
  [[ "${S}" == "1" ]] && { STATUS="complete"; break; }
  [[ "${F}" == "1" ]] && { STATUS="failed"; break; }
  sleep 10
done
kill "${LOGS_PID}" 2>/dev/null || true

# --- verdict --------------------------------------------------------------------
if [[ "${STATUS}" == "complete" ]]; then
  CAPACITY="$(gcloud storage ls "${DEST}/runs/${NAME}/**/run_tag=${TAG}/capacity.json" 2>/dev/null | tail -1)"
  if [[ -n "${CAPACITY}" ]]; then
    echo ">>> ${CAPACITY}"
    gcloud storage cat "${CAPACITY}"
  fi
  kubectl delete job "${JOB}" -n "${NAMESPACE}" >/dev/null
  echo ">>> ${NAME}: complete (tag ${TAG})"
else
  echo ">>> ${NAME}: ${STATUS} — job/${JOB} kept for debugging:" >&2
  echo "      kubectl logs job/${JOB} -n ${NAMESPACE}" >&2
  echo "    (warm-up 503s with TLS errors = expired router pod cert; fix:" >&2
  echo "      kubectl -n ate-system rollout restart deployment/atenet-router)" >&2
  exit 1
fi
