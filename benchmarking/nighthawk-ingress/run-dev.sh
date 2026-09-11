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
#   ./benchmarking/nighthawk-ingress/run-dev.sh --dataplane agentgateway --proxy-cpu 2
set -euo pipefail

ENVOY_CPU=2
DATAPLANE="envoy"
ACTORS=100
TAIL_LATENCY_SLO_MS=25
ATESPACE="ingress-benchmark"
DEST=""
RUNNER_NODE=""
INITIAL_RPS=500
EXP_FACTOR=2.0
MEASURING_PERIOD="10s"
TESTING_STAGE_DURATION="60s"
# Override for a disposable environment on hosts where a previous venv's
# interpreter has disappeared (for example after a system Python upgrade).
VENV="${VENV:-${HOME}/.venvs/substrate-bench}"
NAMESPACE="benchmarking"

usage() {
  cat <<EOF
Usage: $0 [options]
  --envoy-cpu N             router cpu pin, the independent variable (default: ${ENVOY_CPU})
  --proxy-cpu N             alias for --envoy-cpu; use with --dataplane agentgateway
  --dataplane NAME          envoy or agentgateway (default: ${DATAPLANE})
  --actors N                actor fleet size; needs that many workers Running (default: ${ACTORS})
  --tail-latency-slo-ms N   SLO bound; 0 disables (default: ${TAIL_LATENCY_SLO_MS})
  --atespace NAME           actor namespace (default: ${ATESPACE})
  --dest PATH|gs://...      results root (default: /tmp/nighthawk-ingress-results in runner)
  --runner-node NAME        Kubernetes node for the Nighthawk Job
  --initial-rps N           initial total offered RPS (default: ${INITIAL_RPS})
  --exp-factor N            adaptive search multiplier (default: ${EXP_FACTOR})
  --measuring-period D      duration of each load stage (default: ${MEASURING_PERIOD})
  --testing-stage-duration D final stage duration (default: ${TESTING_STAGE_DURATION})
EOF
  exit "${1:-1}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --envoy-cpu) ENVOY_CPU="$2"; shift 2 ;;
    --proxy-cpu) ENVOY_CPU="$2"; shift 2 ;;
    --dataplane) DATAPLANE="$2"; shift 2 ;;
    --actors) ACTORS="$2"; shift 2 ;;
    --tail-latency-slo-ms) TAIL_LATENCY_SLO_MS="$2"; shift 2 ;;
    --atespace) ATESPACE="$2"; shift 2 ;;
    --dest) DEST="$2"; shift 2 ;;
    --runner-node) RUNNER_NODE="$2"; shift 2 ;;
    --initial-rps) INITIAL_RPS="$2"; shift 2 ;;
    --exp-factor) EXP_FACTOR="$2"; shift 2 ;;
    --measuring-period) MEASURING_PERIOD="$2"; shift 2 ;;
    --testing-stage-duration) TESTING_STAGE_DURATION="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done

[[ "${DATAPLANE}" == "envoy" || "${DATAPLANE}" == "agentgateway" ]] || {
  echo "ERROR: --dataplane must be envoy or agentgateway" >&2
  exit 1
}

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

source hack/util/venv.sh

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
DEST="${DEST:-/tmp/nighthawk-ingress-results}"

docker info >/dev/null 2>&1 || {
  echo "ERROR: docker daemon not running (the runner image is built locally)" >&2
  exit 1
}

kubectl get deployment atenet-router -n ate-system >/dev/null 2>&1 || {
  echo "ERROR: substrate is not deployed on ${CLUSTER_NAME}. Run:" >&2
  echo "  hack/install-ate.sh --deploy-ate-system" >&2
  exit 1
}

RUNNING_WORKERS="$(kubectl get pods -n benchmark-workloads --no-headers 2>/dev/null | awk '$3 == "Running" { count++ } END { print count + 0 }')"
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
echo "      dataplane:            ${DATAPLANE}"
echo "      actors:               ${ACTORS}"
echo "      workers running:      ${RUNNING_WORKERS}"
echo "      tail_latency_slo_ms:  ${TAIL_LATENCY_SLO_MS}"
echo "      initial_rps:          ${INITIAL_RPS}"
echo "      exp_factor:           ${EXP_FACTOR}"
echo "      atespace:             ${ATESPACE}"
echo "      dest:                 ${DEST}"
[[ -z "${RUNNER_NODE}" ]] || echo "      runner_node:          ${RUNNER_NODE}"

# Importing orchestrator.py (defaults/rendering/patch) needs PyYAML. Use a
# caller-provided interpreter when the host supplies one; otherwise maintain
# the disposable benchmark venv ourselves.
if [[ -n "${PYTHON:-}" ]]; then
  PY="${PYTHON}"
  "${PY}" -c 'import yaml' 2>/dev/null || {
    echo "ERROR: ${PYTHON} cannot import PyYAML" >&2
    exit 1
  }
else
  ensure_venv "${VENV}"
  if ! "${VENV}/bin/python" -c 'import yaml' 2>/dev/null; then
    "${VENV}/bin/pip" install --quiet pyyaml ||
      "${VENV}/bin/pip" install --quiet --index-url https://pypi.org/simple/ pyyaml
  fi
  PY="${VENV}/bin/python"
fi

# --- pin the router if its current cpu differs --------------------------------
CONTAINER="${DATAPLANE}"
CURRENT_CPU="$(kubectl get deployment atenet-router -n ate-system \
  -o jsonpath="{.spec.template.spec.containers[?(@.name==\"${CONTAINER}\")].resources.limits.cpu}")"
if [[ "${CURRENT_CPU}" != "${ENVOY_CPU}" ]]; then
  echo ">>> pinning router: ${DATAPLANE} cpu '${CURRENT_CPU:-unset}' -> ${ENVOY_CPU}"
  "${PY}" -c "
import sys
sys.path.insert(0, 'benchmarking/automation')
from testtypes import nighthawk_ingress
nighthawk_ingress.pre_test({'nighthawk-ingress': {'dataplane': '${DATAPLANE}', '${DATAPLANE}Cpu': ${ENVOY_CPU}}})"
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
NAME="ingress_routercap_${DATAPLANE}_${ENVOY_CPU}cpu"
JOB="runner-ingress-routercap-${ENVOY_CPU}cpu-quick-$(date +%H%M%S)"
export IMAGE JOB NAME DEST TAG ENVOY_CPU DATAPLANE ACTORS TAIL_LATENCY_SLO_MS ATESPACE RUNNER_NODE INITIAL_RPS EXP_FACTOR MEASURING_PERIOD TESTING_STAGE_DURATION
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
        "dataplane": os.environ["DATAPLANE"],
        "atespace": os.environ["ATESPACE"],
        "initialRps": int(os.environ["INITIAL_RPS"]),
        "exponentialFactor": float(os.environ["EXP_FACTOR"]),
        "measuringPeriod": os.environ["MEASURING_PERIOD"],
        "testingStageDuration": os.environ["TESTING_STAGE_DURATION"],
        "tailLatencySloMs": float(os.environ["TAIL_LATENCY_SLO_MS"]),
        "runnerNodeSelector": (
            {"kubernetes.io/hostname": os.environ["RUNNER_NODE"]}
            if os.environ.get("RUNNER_NODE") else {}
        ),
        "runnerTolerations": (
            [{
                "key": "benchmarking.ate.dev/nighthawk-role",
                "operator": "Equal",
                "value": "runner",
                "effect": "NoSchedule",
            }]
            if os.environ.get("RUNNER_NODE") else []
        ),
    },
}
test["nighthawk-ingress"][f'{os.environ["DATAPLANE"]}Cpu'] = int(os.environ["ENVOY_CPU"])
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
  if [[ "${DEST}" == gs://* ]]; then
    CAPACITY="$(gcloud storage ls "${DEST}/runs/${NAME}/**/run_tag=${TAG}/capacity.json" 2>/dev/null | tail -1)"
    [[ -z "${CAPACITY}" ]] || { echo ">>> ${CAPACITY}"; gcloud storage cat "${CAPACITY}"; }
  else
    RESULT_DIR="benchmarking/nighthawk-ingress/results/${TAG}"
    mkdir -p "${RESULT_DIR}"
    POD="$(kubectl get pods -n "${NAMESPACE}" -l job-name="${JOB}" -o jsonpath='{.items[0].metadata.name}')"
    kubectl cp "${NAMESPACE}/${POD}:${DEST}/runs/${NAME}" "${RESULT_DIR}"
    CAPACITY="$(find "${RESULT_DIR}" -name capacity.json -print -quit)"
    [[ -z "${CAPACITY}" ]] || { echo ">>> ${CAPACITY}"; cat "${CAPACITY}"; }
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
