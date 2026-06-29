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

# Bring up the counter-microvm demo on a local kind cluster. Wraps
# hack/run-microvm-demo.sh with the same kind-specific environment that
# hack/install-ate-kind.sh uses, so the image repo and snapshot bucket match what
# gets installed in the cluster. All arguments are forwarded to run-microvm-demo.sh.

# shellcheck disable=SC2155 # safe initialization
goarch=$(go env GOARCH)

# override reading dev env -- otherwise .ate-dev-env.sh would point images/assets
# at GKE/GCS instead of the local registry and the in-cluster rustfs.
export NO_DEV_ENV="true"
# images are pushed to the local registry
export KO_DOCKER_REPO="${KO_DOCKER_REPO:-localhost:5001}"
# build for the host architecture
export KO_DEFAULTPLATFORMS="linux/${goarch}"
# use the kind control-plane path (install-ate-kind.sh) + stage assets to rustfs
export ATE_INSTALL_KIND="true"
# default bucket name for local deployment (served by the in-cluster rustfs)
export BUCKET_NAME="${BUCKET_NAME:-ate-snapshots}"
# target the local kind cluster's context
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
export KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME}}"
# unset other env from ate-dev-env.sh in case the developer already sourced them
unset GCE_REGION CLUSTER_LOCATION NETWORK SUBNETWORK MEMORYSTORE_INSTANCE PROJECT_ID

exec "${ROOT}/hack/run-microvm-demo.sh" "$@"
