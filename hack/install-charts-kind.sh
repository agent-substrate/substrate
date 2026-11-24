#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
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

# Build the chart images into the local registry of a kind cluster that
# hack/create-kind-cluster.sh created, and install the four charts with the
# values of one scenario in charts/scenarios, and optionally an overlay.
set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
usage() {
  echo "Usage: $0 SCENARIO [SUBSTRATE_HELM_FLAGS...]"
  echo "Build the chart images into the kind registry and install the charts with"
  echo "the values in charts/scenarios/SCENARIO. Extra flags go to the substrate release."
  echo
  echo "Configured through the environment:"
  echo "  KIND_CLUSTER_NAME   Name of the cluster (default: kind)."
  echo "  KIND_REGISTRY_PORT  Port of the local registry (default: 5001)."
  echo "  IMAGE_TAG           Tag of the built images (default: dev)."
  echo "  DOCKER_BUILD_FLAGS  Extra docker buildx build flags for envoy-dataplane."
  echo "  VALUES_OVERLAY      Directory of values files applied after the scenario's,"
  echo "                      such as hack/e2e-chart-values (default: none)."
}
if [[ $# -lt 1 || "$1" == -h || "$1" == --help ]]; then
  usage >&2
  exit 1
fi
SCENARIO="${ROOT}/charts/scenarios/$1"
shift
if [[ ! -f "${SCENARIO}/substrate-values.yaml" ]]; then
  echo "error: ${SCENARIO} has no substrate-values.yaml" >&2
  exit 1
fi

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
export KUBECTL_CONTEXT="kind-${KIND_CLUSTER_NAME}"
REPO="localhost:${KIND_REGISTRY_PORT:-5001}/substrate"
TAG="${IMAGE_TAG:-dev}"
PLATFORM="linux/$(go env GOARCH)"
cd "${ROOT}"

# The images that the charts run, under the names a release publishes them by.
echo "Building the chart images into ${REPO}..."
KO_DOCKER_REPO="${REPO}" hack/run-tool.sh ko build --base-import-paths \
  --platform="${PLATFORM}" --tags="${TAG}" \
  ./cmd/ate-idp-server ./cmd/ateapi ./cmd/atecontroller ./cmd/atelet ./cmd/atenet \
  ./cmd/credential-provider/kubernetes-secrets ./cmd/objectstore-plugin ./cmd/podcertcontroller
# shellcheck disable=SC2086 # DOCKER_BUILD_FLAGS is a whitespace-separated list.
docker buildx build --platform="${PLATFORM}" --push ${DOCKER_BUILD_FLAGS:-} \
  -t "${REPO}/envoy-dataplane:${TAG}" cmd/dataplane/envoy

helm() { hack/run-tool.sh helm --kube-context "${KUBECTL_CONTEXT}" "$@"; }
ate_setup() { go run ./cmd/ate-setup --context "${KUBECTL_CONTEXT}" "$@"; }
IMAGE_FLAGS=(--set image.registry="${REPO%/substrate}" --set image.repository=substrate --set image.tag="${TAG}")
# The values files of a chart: the scenario's, then the overlay's.
values() {
  local dir
  for dir in "${SCENARIO}" ${VALUES_OVERLAY:+"${ROOT}/${VALUES_OVERLAY}"}; do
    if [[ -f "${dir}/$1-values.yaml" ]]; then
      echo "-f" "${dir}/$1-values.yaml"
    fi
  done
}

helm upgrade --install substrate-crds ./charts/substrate-crds
ate_setup create podcertificate-controller-cas
# shellcheck disable=SC2046 # values prints whole flags, and paths have no spaces.
helm upgrade --install substrate-podcert ./charts/substrate-podcert \
  --namespace podcertificate-controller-system --create-namespace --wait \
  $(values substrate-podcert) "${IMAGE_FLAGS[@]}"

hack/install-postgres.sh --kind
hack/install-rustfs.sh

# shellcheck disable=SC2046
helm upgrade --install substrate-atelet ./charts/substrate-atelet --namespace ate-system \
  $(values substrate-atelet) "${IMAGE_FLAGS[@]}"

ate_setup create actor-id-ca-pool
ate_setup create actor-id-ca-certs
ate_setup create jwt-authority-pool
ate_setup create egress-mitm-ca-pool
ate_setup create api-authentication-config

# shellcheck disable=SC2046
helm upgrade --install substrate ./charts/substrate --namespace ate-system \
  $(values substrate) "${IMAGE_FLAGS[@]}" \
  --set postgres.readWriteConnectionStringSecretRef.name=substrate-postgres-readwrite \
  --set postgres.ownerConnectionStringSecretRef.name=substrate-postgres-owner \
  "$@"

echo "Waiting for the rollouts..."
kubectl --context "${KUBECTL_CONTEXT}" -n ate-system rollout status daemonset/atelet --timeout=5m
kubectl --context "${KUBECTL_CONTEXT}" -n ate-system wait deployment --all --for=condition=Available --timeout=10m
