#!/usr/bin/env bash
#  Copyright 2026 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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

LOAD_TYPE="${LOAD_TYPE:-all}"

deploy_ate_api() {
  echo "Deploying ate-api load test..."
  envsubst < benchmarking/locust/manifests/ate-api.yaml | kubectl apply -f -
}

deploy_counter() {
  echo "Deploying counter demo load test..."
  envsubst < benchmarking/locust/manifests/counter-demo.yaml | kubectl apply -f -
}

deploy_all() {
  echo "Deploying all load tests with UI..."
  envsubst < benchmarking/locust/manifests/all.yaml | kubectl apply -f -
}

echo "Deploying Locust load with PROJECT_ID=$PROJECT_ID..."

case "$LOAD_TYPE" in
  "ate-api")
    deploy_ate_api
    ;;
  "counter")
    deploy_counter
    ;;
  "all")
    deploy_all
    ;;
  *)
    echo "Error: Invalid LOAD_TYPE '$LOAD_TYPE'. Valid options are 'ate-api', 'counter', or 'all'."
    exit 1
    ;;
esac


