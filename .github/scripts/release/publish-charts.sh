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

# Publish the tagged checkout's charts beside its release images.
set -o errexit -o nounset -o pipefail

: "${VERSION:?}" "${KO_DOCKER_REPO:?}" "${GITHUB_ACTOR:?}" "${GH_TOKEN:?}"
if [[ ! "${VERSION}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: ${VERSION} is not a release tag" >&2
  exit 1
fi

ROOT="$(git rev-parse --show-toplevel)"
"${ROOT}/.github/scripts/release/refuse-published-release.sh"
package_dir="$(mktemp -d)"
trap 'rm -rf "${package_dir}"' EXIT
version="${VERSION#v}"
charts=(substrate-crds substrate substrate-podcert substrate-atelet)

# Package the complete set before writing to the registry.
for chart in "${charts[@]}"; do
  helm package "${ROOT}/charts/${chart}" --destination "${package_dir}" \
    --version "${version}" --app-version "${VERSION}"
done

helm registry login "${KO_DOCKER_REPO%%/*}" --username "${GITHUB_ACTOR}" \
  --password-stdin <<<"${GH_TOKEN}"
for chart in "${charts[@]}"; do
  helm push "${package_dir}/${chart}-${version}.tgz" "oci://${KO_DOCKER_REPO}/helm"
done
