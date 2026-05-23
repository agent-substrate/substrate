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

# shellcheck disable=SC2155 # safe initialization
goarch=$(go env GOARCH)

# override reading dev env
export NO_DEV_ENV="true"
# we will push images to the local registry
export KO_DOCKER_REPO="${KO_DOCKER_REPO:-localhost:5001}"
# we want to build for the host architecture
export KO_DEFAULTPLATFORMS="linux/${goarch}"
# install resolved manifests using Kustomize overlay for local Kind cluster
export ATE_INSTALL_KIND="true"
# use default bucket name for local deployment
export BUCKET_NAME="ate-snapshots"
# unset other env from ate-dev-env.sh in case the developer already sourced them
unset GCE_REGION CLUSTER_LOCATION NETWORK SUBNETWORK MEMORYSTORE_INSTANCE PROJECT_ID

hack/install-ate.sh "$@"
