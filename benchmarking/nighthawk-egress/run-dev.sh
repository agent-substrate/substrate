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

# Build + push the nighthawk-egress runner image and run one adaptive egress
# capacity test against the cluster currently in kubeconfig, without going
# through the full CronJob orchestrator (which redeploys ate-system from
# scratch).
#
# Usage:
#   benchmarking/nighthawk-egress/run-dev.sh [--mode http|mitm|passthrough] [--envoy-cpu N] [--tail-latency-slo-ms N] [--dest gs://BUCKET/PREFIX]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

if [[ -f "${REPO_ROOT}/.ate-dev-env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.ate-dev-env.sh"
fi

MODE="http"
ENVOY_CPU=1
TAIL_SLO_MS=25
DEST=""
MEASURING_PERIOD="20s"
TESTING_DURATION="60s"
CONVERGENCE_DEADLINE="600s"
CONNECTIONS=16
MAX_PENDING=32
SKIP_BUILD=false
SKIP_DEPLOY=

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)                   MODE="$2"; shift 2 ;;
    --envoy-cpu)              ENVOY_CPU="$2"; shift 2 ;;
    --tail-latency-slo-ms)    TAIL_SLO_MS="$2"; shift 2 ;;
    --dest)                   DEST="$2"; shift 2 ;;
    --measuring-period)       MEASURING_PERIOD="$2"; shift 2 ;;
    --testing-stage-duration) TESTING_DURATION="$2"; shift 2 ;;
    --convergence-deadline)   CONVERGENCE_DEADLINE="$2"; shift 2 ;;
    --connections)            CONNECTIONS="$2"; shift 2 ;;
    --max-pending)            MAX_PENDING="$2"; shift 2 ;;
    --skip-build)             SKIP_BUILD=true; shift ;;
    --skip-deploy)            SKIP_DEPLOY=true; shift ;;
    -h|--help)
      sed -n '16,25p' "$0"
      exit 0
      ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "${DEST}" ]]; then
  if [[ -n "${BUCKET_NAME:-}" ]]; then
    DEST="gs://${BUCKET_NAME}/nighthawk-egress-dev"
  elif [[ -n "${PROJECT_ID:-}" ]]; then
    DEST="gs://${PROJECT_ID}-nighthawk-bench-1/nighthawk-egress-dev"
  else
    echo "ERROR: --dest is not set and neither BUCKET_NAME nor PROJECT_ID is set in .ate-dev-env.sh" >&2
    exit 1
  fi
fi

if [[ "${SKIP_BUILD}" == "false" ]] && [[ -z "${KO_DOCKER_REPO:-}" ]]; then
  echo "ERROR: KO_DOCKER_REPO is not set (configure .ate-dev-env.sh or pass --skip-build)" >&2
  exit 1
fi

COMMIT="$(git rev-parse --short HEAD)"
TAG="${COMMIT}-$(date +%H%M%S)"

if [[ "${SKIP_BUILD}" == "false" ]]; then
  IMAGE="${KO_DOCKER_REPO}/nighthawk-egress-runner:${TAG}"
  echo ">>> building and pushing ${IMAGE}"
  docker build --platform linux/amd64 \
    -f benchmarking/nighthawk-egress/Dockerfile \
    -t "${IMAGE}" .
  docker push "${IMAGE}"
else
  IMAGE="${NIGHTHAWK_IMAGE:?--skip-build requires NIGHTHAWK_IMAGE}"
fi

NAME="egress_gatewaycap_${MODE}_${ENVOY_CPU}cpu"
JOB_NAME="runner-egress-${MODE}-${ENVOY_CPU}cpu-${TAG//_/-}"
JOB_NAME="${JOB_NAME:0:63}"

python3 "${SCRIPT_DIR}/cluster_setup.py" run-dev \
  --name "${NAME}" \
  --job-name "${JOB_NAME}" \
  --image "${IMAGE}" \
  --tag "${TAG}" \
  --dest "${DEST}" \
  --mode "${MODE}" \
  --envoy-cpu "${ENVOY_CPU}" \
  --tail-slo-ms "${TAIL_SLO_MS}" \
  --measuring-period "${MEASURING_PERIOD}" \
  --testing-duration "${TESTING_DURATION}" \
  --convergence-deadline "${CONVERGENCE_DEADLINE}" \
  --connections "${CONNECTIONS}" \
  --max-pending "${MAX_PENDING}" \
  ${SKIP_DEPLOY:+--skip-deploy}
