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

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment variables if configured
if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

# Ensure BUCKET_NAME is set
if [[ -z "${BUCKET_NAME:-}" ]]; then
  echo "Error: BUCKET_NAME environment variable is not set." >&2
  exit 1
fi

MANIFEST_TEMPLATE="benchmarking/workloads/manifests/workloads.yaml.tmpl"

if [[ ! -f "${MANIFEST_TEMPLATE}" ]]; then
  echo "Error: ${MANIFEST_TEMPLATE} not found in $(pwd)" >&2
  exit 1
fi

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy    Substitute BUCKET_NAME and deploy workloads to the cluster using ko apply"
  echo "  --delete    Substitute BUCKET_NAME and delete workloads from the cluster using kubectl delete"
  echo "  -h, --help  Show this help message"
}

deploy() {
  echo "Deploying workloads..."
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" "${MANIFEST_TEMPLATE}" \
    | hack/run-tool.sh ko apply -f -
}

delete() {
  echo "Deleting workloads..."
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" "${MANIFEST_TEMPLATE}" \
    | kubectl delete --ignore-not-found -f -
}

if [[ "$#" -eq 0 ]]; then
  usage
  exit 1
fi

action=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --deploy)
      action="deploy"
      ;;
    --delete)
      action="delete"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
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
