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

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment variables if configured
if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

if [ -z "${PROJECT_ID:-}" ]; then
  echo "Error: PROJECT_ID environment variable must be set." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="${SCRIPT_DIR}/manifests/locust.yaml"

# Substituted into the boomer container's --user-class argument and the master's -f.
BENCHMARK_USER_CLASS=glutton
WORKLOAD_TEMPLATE="${WORKLOAD_TEMPLATE:-swebench-sympy}"
TOTAL_STEPS="${TOTAL_STEPS:-30}"
NUM_CYCLES="${NUM_CYCLES:-4}"

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy                 Deploy the locust workers"
  echo "  --delete                 Delete the locust workers"
  echo "  --user-class NAME        Locust user class, lowercase; runs tests/NAME.py (default: glutton)"
  echo "  --workload-template NAME Workload template name for sweperf (default: swebench-sympy)"
  echo "  --total-steps N          Total steps for sweperf (default: 30)"
  echo "  --num-cycles N           Number of cycles for sweperf (default: 4)"
  echo "  -h|--help                Show this help message"
}

deploy() {
  # The locust manifest targets the `benchmarking` namespace (so prometheus
  # can scrape it when that stack is installed). Ensure it exists either way —
  # benchmarking/monitoring.yaml is otherwise optional.
  echo "Ensuring benchmarking namespace exists..."
  kubectl create namespace benchmarking --dry-run=client -o yaml | kubectl apply -f -
  echo "Deploying Locust load (PROJECT_ID=${PROJECT_ID}, user_class=${BENCHMARK_USER_CLASS}, workload_template=${WORKLOAD_TEMPLATE})..."
  envsubst < "${MANIFEST}" | kubectl apply -f -
}

delete() {
  echo "Deleting Locust load..."
  envsubst < "${MANIFEST}" | kubectl delete --ignore-not-found -f -
}

if [[ "$#" -eq 0 ]]; then
  usage
  exit 1
fi

action=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --deploy) action="deploy" ;;
    --delete) action="delete" ;;
    --user-class) shift; BENCHMARK_USER_CLASS="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" ;;
    --user-class=*) BENCHMARK_USER_CLASS="$(printf '%s' "${1#*=}" | tr '[:upper:]' '[:lower:]')" ;;
    --workload-template) shift; WORKLOAD_TEMPLATE="$1" ;;
    --workload-template=*) WORKLOAD_TEMPLATE="${1#*=}" ;;
    --total-steps) shift; TOTAL_STEPS="$1" ;;
    --total-steps=*) TOTAL_STEPS="${1#*=}" ;;
    --num-cycles) shift; NUM_CYCLES="$1" ;;
    --num-cycles=*) NUM_CYCLES="${1#*=}" ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Error: Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ ! -f "${SCRIPT_DIR}/tests/${BENCHMARK_USER_CLASS}.py" ]]; then
  echo "Error: no tests/${BENCHMARK_USER_CLASS}.py; --user-class must name a test file" >&2
  exit 1
fi
export BENCHMARK_USER_CLASS
export WORKLOAD_TEMPLATE
export TOTAL_STEPS
export NUM_CYCLES

if [[ "${action}" == "deploy" ]]; then
  deploy
elif [[ "${action}" == "delete" ]]; then
  delete
fi
