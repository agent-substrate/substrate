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

# Quick dev run of the Nighthawk egress-gateway capacity benchmark.
#
# Runs the benchmark layer against an already-installed Substrate cluster:
# shapes deployment/atenet-egress, deploys the in-cluster rig unless
# --skip-deploy is passed, builds and pushes the runner image from the working
# tree, submits the runner Job, tails progress logs, and prints capacity.json.
# See benchmarking/nighthawk-egress/README.md for cluster prerequisites.
#
#   ./benchmarking/nighthawk-egress/run-dev.sh --mode http --envoy-cpu 1
set -euo pipefail

MODE="http"
ENVOY_CPU=1
TAIL_LATENCY_SLO_MS=25
MEASURING_PERIOD="20s"
TESTING_STAGE_DURATION="60s"
CONVERGENCE_DEADLINE="600s"
DEST=""
SKIP_DEPLOY=false
VENV="${HOME}/.venvs/substrate-bench"
NAMESPACE="benchmarking"
INVOKER_PRINCIPAL="spiffe://cluster.local/ns/nighthawk/sa/traffic-driver-invoker"

usage() {
  cat <<EOF
Usage: $0 [options]
  --mode MODE                     http | mitm | passthrough (default: ${MODE})
  --envoy-cpu N                   atenet-egress cpu pin, the independent variable (default: ${ENVOY_CPU})
  --tail-latency-slo-ms N         SLO bound in ms (default: ${TAIL_LATENCY_SLO_MS})
  --measuring-period DUR          adaptive step window (default: ${MEASURING_PERIOD})
  --testing-stage-duration DUR    converged confirmation window (default: ${TESTING_STAGE_DURATION})
  --convergence-deadline DUR      search budget (default: ${CONVERGENCE_DEADLINE})
  --dest gs://BUCKET/PREFIX       results root (default: gs://\$BUCKET_NAME/nighthawk-egress-results)
  --skip-deploy                   reuse an already-deployed nighthawk rig (still shapes atenet-egress)
EOF
  exit "${1:-1}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --envoy-cpu) ENVOY_CPU="$2"; shift 2 ;;
    --tail-latency-slo-ms) TAIL_LATENCY_SLO_MS="$2"; shift 2 ;;
    --measuring-period) MEASURING_PERIOD="$2"; shift 2 ;;
    --testing-stage-duration) TESTING_STAGE_DURATION="$2"; shift 2 ;;
    --convergence-deadline) CONVERGENCE_DEADLINE="$2"; shift 2 ;;
    --dest) DEST="$2"; shift 2 ;;
    --skip-deploy) SKIP_DEPLOY=true; shift ;;
    -h|--help) usage 0 ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

# shellcheck source=hack/util/venv.sh disable=SC1091
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
DEST="${DEST:-gs://${BUCKET_NAME}/nighthawk-egress-results}"

docker info >/dev/null 2>&1 || {
  echo "ERROR: docker daemon not running (the runner image is built locally)" >&2
  exit 1
}

if ! kubectl get deployment atenet-egress -n ate-system >/dev/null 2>&1 \
  || ! kubectl get deployment atenet-router -n ate-system >/dev/null 2>&1; then
  echo "ERROR: substrate is not deployed on ${CLUSTER_NAME}. Run:" >&2
  echo "  go run ./cmd/ate-setup deploy ate-system --credential-provider='{\"name\":\"k8s.io\"}' --ingress-allowed-spiffe-ids=${INVOKER_PRINCIPAL}" >&2
  exit 1
fi

ROUTER_ARGS="$(kubectl get deployment atenet-router -n ate-system \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="atenet-router")].args[*]}')"
if [[ "${ROUTER_ARGS}" != *"${INVOKER_PRINCIPAL}"* ]]; then
  echo "ERROR: atenet-router does not allow ${INVOKER_PRINCIPAL}. Redeploy substrate with:" >&2
  echo "  go run ./cmd/ate-setup deploy ate-system --credential-provider='{\"name\":\"k8s.io\"}' --ingress-allowed-spiffe-ids=${INVOKER_PRINCIPAL}" >&2
  exit 1
fi

echo ">>> run config:"
echo "      cluster:                 ${CLUSTER_NAME} (${CLUSTER_LOCATION})"
echo "      mode:                    ${MODE}"
echo "      envoy_cpu:               ${ENVOY_CPU}"
echo "      tail_latency_slo_ms:     ${TAIL_LATENCY_SLO_MS}"
echo "      measuring_period:        ${MEASURING_PERIOD}"
echo "      testing_stage_duration:  ${TESTING_STAGE_DURATION}"
echo "      convergence_deadline:    ${CONVERGENCE_DEADLINE}"
echo "      skip_deploy:             ${SKIP_DEPLOY}"
echo "      dest:                    ${DEST}"

ensure_venv "${VENV}"
if ! "${VENV}/bin/python" -c 'import yaml' 2>/dev/null; then
  "${VENV}/bin/pip" install --quiet pyyaml ||
    "${VENV}/bin/pip" install --quiet --index-url https://pypi.org/simple/ pyyaml
fi
PY="${VENV}/bin/python"

# --- validate knobs before building or touching the cluster ------------------
NAME="egress_gatewaycap_${MODE}_${ENVOY_CPU}cpu"
export NAME MODE ENVOY_CPU TAIL_LATENCY_SLO_MS MEASURING_PERIOD \
  TESTING_STAGE_DURATION CONVERGENCE_DEADLINE SKIP_DEPLOY

TEST_JSON="$("${PY}" - <<'EOF'
import json
import os
import sys

sys.path.insert(0, "benchmarking/automation")
import orchestrator
from util import parse_duration_seconds

wait_s = (
    parse_duration_seconds(os.environ["CONVERGENCE_DEADLINE"])
    + parse_duration_seconds(os.environ["TESTING_STAGE_DURATION"])
    + 300
)
test = {
    "name": os.environ["NAME"],
    "type": "nighthawk-egress",
    "targetCluster": "dev",
    "duration": f"{wait_s}s",
    "nighthawk-egress": {
        "mode": os.environ["MODE"],
        "envoyCpu": int(os.environ["ENVOY_CPU"]),
        "tailLatencySloMs": float(os.environ["TAIL_LATENCY_SLO_MS"]),
        "measuringPeriod": os.environ["MEASURING_PERIOD"],
        "testingStageDuration": os.environ["TESTING_STAGE_DURATION"],
        "convergenceDeadline": os.environ["CONVERGENCE_DEADLINE"],
    },
}
orchestrator.validate_and_normalize_tests([test])
print(json.dumps(test))
EOF
)"
export TEST_JSON
WAIT_S="$("${PY}" -c 'import json, os, sys; sys.path.insert(0, "benchmarking/automation"); from util import parse_duration_seconds; print(parse_duration_seconds(json.loads(os.environ["TEST_JSON"])["duration"]))')"

# --- runner image from the working tree --------------------------------------
SHA7="$(git rev-parse --short=7 HEAD)"
DIRTY=""
[[ -n "$(git status --porcelain)" ]] && DIRTY="-dirty"
TAG="quick-${SHA7}${DIRTY}-$(date +%H%M%S)"
IMAGE="${KO_DOCKER_REPO}/nighthawk-egress-test:${TAG}"
echo ">>> building ${IMAGE} from the working tree"
docker build --platform linux/amd64 \
  -f benchmarking/nighthawk-egress/Dockerfile -t "${IMAGE}" .
docker push "${IMAGE}"

# --- shape atenet-egress and deploy rig (unless --skip-deploy) ---------------
# Reshaping restarts atenet-egress under any benchmark that is still running.
"${PY}" - <<'EOF'
import json
import os
import sys

sys.path.insert(0, "benchmarking/automation")
import orchestrator
from testtypes import nighthawk_egress

test = json.loads(os.environ["TEST_JSON"])
orchestrator.wait_for_no_active_runners()
if os.environ["SKIP_DEPLOY"] == "true":
    nighthawk_egress.shape_atenet_egress(test["nighthawk-egress"]["envoyCpu"])
else:
    nighthawk_egress.pre_test(test)
EOF

# --- render + submit the Job -------------------------------------------------
JOB="runner-egress-${MODE}-${ENVOY_CPU}cpu-quick-$(date +%H%M%S)"
JOB="${JOB:0:63}"
JOB="${JOB%-}"
export IMAGE JOB DEST TAG
"${PY}" - <<'EOF' | kubectl apply -f -
import json
import os
import sys

sys.path.insert(0, "benchmarking/automation")
import orchestrator
from testtypes import nighthawk_egress

test = json.loads(os.environ["TEST_JSON"])
subs = {
    "JOB_NAME": os.environ["JOB"],
    "IMAGE": os.environ["IMAGE"],
    "TAG": os.environ["TAG"],
    "NAME": test["name"],
    "DEST": os.environ["DEST"],
    **nighthawk_egress.job_subs(test),
}
tmpl = nighthawk_egress.job_tmpl("benchmarking/automation/manifests")
print(orchestrator.render_template(tmpl, subs))
EOF

# --- wait, streaming the runner's progress lines -----------------------------
echo ">>> waiting up to ${WAIT_S}s for job/${JOB} (progress lines only; full logs are written to logs.txt)"
( until kubectl logs -f "job/${JOB}" -n "${NAMESPACE}" 2>/dev/null \
    | grep --line-buffered -vE '^\[(service|adaptive)\] '; do sleep 5; done ) &
LOGS_PID=$!
stop_logs() {
  pkill -P "${LOGS_PID}" 2>/dev/null || true
  kill "${LOGS_PID}" 2>/dev/null || true
}
trap stop_logs EXIT
STATUS="timeout"
for _ in $(seq 1 $(( (WAIT_S + 9) / 10 ))); do  # convergence deadline + testing stage + 300s, in 10s steps
  JOB_STATUS="$(kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.succeeded}|{.status.failed}' || true)"
  [[ "${JOB_STATUS%%|*}" == "1" ]] && { STATUS="complete"; break; }
  [[ "${JOB_STATUS##*|}" == "1" ]] && { STATUS="failed"; break; }
  sleep 10
done
stop_logs

# --- verdict -----------------------------------------------------------------
if [[ "${STATUS}" == "complete" ]]; then
  CAPACITY="$(gcloud storage ls "${DEST}/runs/${NAME}/**/run_tag=${TAG}/capacity.json" 2>/dev/null | tail -1 || true)"
  if [[ -n "${CAPACITY}" ]]; then
    echo ">>> ${CAPACITY}"
    gcloud storage cat "${CAPACITY}"
  else
    echo ">>> no capacity.json under ${DEST}/runs/${NAME}/ for tag ${TAG}" >&2
  fi
  kubectl delete job "${JOB}" -n "${NAMESPACE}" >/dev/null
  echo ">>> ${NAME}: complete (tag ${TAG})"
else
  echo ">>> ${NAME}: ${STATUS} (job/${JOB} kept for debugging):" >&2
  echo "      kubectl logs job/${JOB} -n ${NAMESPACE}" >&2
  exit 1
fi
