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

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
reg_name="kind-registry"

if [ "$#" -gt 0 ]; then
  case "$1" in
    -h|--help)
      echo "Usage: $0"
      echo "Deletes the kind cluster '${KIND_CLUSTER_NAME}' and cleans up the registry container if created by this project."
      exit 0
      ;;
  esac
fi

echo "Deleting kind cluster '${KIND_CLUSTER_NAME}'..."
"${ROOT}"/hack/kind.sh delete cluster --name "${KIND_CLUSTER_NAME}"

if [ "$(docker inspect --format '{{index .Config.Labels "created-by"}}' "${reg_name}" 2>/dev/null)" = "agent-substrate" ]; then
  echo "Deleting registry container '${reg_name}' (created by us)..."
  docker rm -f "${reg_name}" || true
else
  echo "Registry container '${reg_name}' was not created by us, leaving it running."
fi
