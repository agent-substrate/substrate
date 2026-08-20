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

if [[ -z "${PROJECT_ID:-}" && -z "${KO_DOCKER_REPO:-}" && -z "${LOCUST_IMAGE:-}" ]]; then
  echo "Error: PROJECT_ID or KO_DOCKER_REPO environment variable must be set." >&2
  exit 1
fi

if [[ -z "${LOCUST_IMAGE:-}" ]]; then
  if [[ -n "${KO_DOCKER_REPO:-}" ]]; then
    LOCUST_IMAGE="${KO_DOCKER_REPO}/locust-test:latest"
  else
    LOCUST_IMAGE="us-docker.pkg.dev/${PROJECT_ID}/gcr.io/ate-images/locust-test:latest"
  fi
fi

if [[ -z "${LOCUST_IMAGE_PULL_POLICY:-}" ]]; then
  if [[ "${LOCUST_IMAGE}" =~ ^localhost: ]]; then
    LOCUST_IMAGE_PULL_POLICY="IfNotPresent"
  else
    LOCUST_IMAGE_PULL_POLICY="Always"
  fi
fi

export LOCUST_IMAGE LOCUST_IMAGE_PULL_POLICY

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="${SCRIPT_DIR}/manifests/locust.yaml"

# Substituted into the boomer container's --user-class argument and the master's -f.
BENCHMARK_USER_CLASS=glutton
# Local YAML to run as the agent-session script, if any. Uploaded as the
# agentsession-script ConfigMap and mounted in the boomer worker at
# AGENTSESSION_SCRIPT_MOUNT; the master serves that path as the default of
# --agentsession-script-file.
AGENTSESSION_SCRIPT=""
AGENTSESSION_SCRIPT_MOUNT=/etc/agentsession/script.yaml

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy           Deploy the locust workers"
  echo "  --delete           Delete the locust workers"
  echo "  --user-class NAME  Locust user class, lowercase; runs tests/NAME.py (default: glutton)"
  echo "  --agentsession-script FILE"
  echo "                     Run this script YAML in the agentsession user class instead of a"
  echo "                     built-in variant. Validated locally, then mounted into the workers."
  echo "  -h|--help          Show this help message"
}

deploy() {
  # The locust manifest targets the `benchmarking` namespace (so prometheus
  # can scrape it when that stack is installed). Ensure it exists either way —
  # benchmarking/monitoring.yaml is otherwise optional.
  echo "Ensuring benchmarking namespace exists..."
  kubectl create namespace benchmarking --dry-run=client -o yaml | kubectl apply -f -
  if [[ -n "${AGENTSESSION_SCRIPT}" ]]; then
    echo "Validating agent-session script ${AGENTSESSION_SCRIPT}..."
    (cd "${SCRIPT_DIR}/../.." && go run ./cmd/benchmarking/boomer-worker --check-agentsession-script "${AGENTSESSION_SCRIPT}")
    echo "Uploading it as the agentsession-script ConfigMap..."
    kubectl -n benchmarking create configmap agentsession-script \
      --from-file="script.yaml=${AGENTSESSION_SCRIPT}" --dry-run=client -o yaml | kubectl apply -f -
  fi
  echo "Deploying Locust load (image=${LOCUST_IMAGE}, pull_policy=${LOCUST_IMAGE_PULL_POLICY}, user_class=${BENCHMARK_USER_CLASS}, agentsession_script=${AGENTSESSION_SCRIPT:-<built-in>})..."
  envsubst < "${MANIFEST}" | kubectl apply -f -
}

delete() {
  echo "Deleting Locust load..."
  envsubst < "${MANIFEST}" | kubectl delete --ignore-not-found -f -
  kubectl -n benchmarking delete configmap agentsession-script --ignore-not-found
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
    --agentsession-script) shift; AGENTSESSION_SCRIPT="$1" ;;
    --agentsession-script=*) AGENTSESSION_SCRIPT="${1#*=}" ;;
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
if [[ -n "${AGENTSESSION_SCRIPT}" ]]; then
  if [[ ! -f "${AGENTSESSION_SCRIPT}" ]]; then
    echo "Error: --agentsession-script ${AGENTSESSION_SCRIPT}: no such file" >&2
    exit 1
  fi
  # Absolute from here on: the validation step below runs from the repo
  # root, and a relative path would resolve against that instead.
  AGENTSESSION_SCRIPT="$(cd "$(dirname "${AGENTSESSION_SCRIPT}")" && pwd)/$(basename "${AGENTSESSION_SCRIPT}")"
fi
export BENCHMARK_USER_CLASS
# Empty leaves the master's --agentsession-script-file default unset, so
# workers run the built-in variant named by --agentsession-script.
AGENTSESSION_SCRIPT_FILE=""
# The script's checksum goes into the boomer container's env, so a redeploy
# with a different script changes the pod template and rolls the pods.
# Without it the manifest would be identical and the old workers would keep
# running until something else restarted them.
AGENTSESSION_SCRIPT_SHA=""
if [[ -n "${AGENTSESSION_SCRIPT}" ]]; then
  AGENTSESSION_SCRIPT_FILE="${AGENTSESSION_SCRIPT_MOUNT}"
  AGENTSESSION_SCRIPT_SHA="$( (sha256sum "${AGENTSESSION_SCRIPT}" 2>/dev/null || shasum -a 256 "${AGENTSESSION_SCRIPT}") | cut -c1-16)"
fi
export AGENTSESSION_SCRIPT_FILE AGENTSESSION_SCRIPT_SHA

if [[ "${action}" == "deploy" ]]; then
  deploy
elif [[ "${action}" == "delete" ]]; then
  delete
fi
