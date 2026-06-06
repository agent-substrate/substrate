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

# Runs the E2E tests against a local Kind cluster.
#
# This wraps hack/run-e2e.sh with the same Kind-specific environment that
# hack/install-ate-kind.sh uses, so the snapshot bucket and image repo match
# what was installed in the cluster. All arguments are forwarded to
# hack/run-e2e.sh (run with -h to see them).

# override reading dev env -- otherwise .ate-dev-env.sh would clobber the
# Kind values below (e.g. point snapshots at a GCS bucket that doesn't exist
# in the in-cluster rustfs, causing golden-snapshot uploads to 404).
export NO_DEV_ENV="true"
# images are pushed to the local registry
export KO_DOCKER_REPO="${KO_DOCKER_REPO:-localhost:5001}"
# default bucket name for local deployment (served by in-cluster rustfs)
export BUCKET_NAME="${BUCKET_NAME:-ate-snapshots}"
# target the local Kind cluster's context
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
export KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME}}"

exec "${ROOT}/hack/run-e2e.sh" "$@"
