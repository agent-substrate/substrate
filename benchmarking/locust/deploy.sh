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

MANIFEST="benchmarking/locust/manifests/locust.yaml"

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy   Deploy the locust workers"
  echo "  --delete   Delete the locust workers"
  echo "  -h|--help  Show this help message"
}

deploy() {
  # The locust manifest targets the `benchmarking` namespace (so prometheus
  # can scrape it when that stack is installed). Ensure it exists either way —
  # benchmarking/monitoring.yaml is otherwise optional.
  echo "Ensuring benchmarking namespace exists..."
  kubectl create namespace benchmarking --dry-run=client -o yaml | kubectl apply -f -
  echo "Deploying Locust load (PROJECT_ID=${PROJECT_ID})..."
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
    -h|--help) usage; exit 0 ;;
    *)
      echo "Error: Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ "${action}" == "deploy" ]]; then
  deploy
elif [[ "${action}" == "delete" ]]; then
  delete
fi
