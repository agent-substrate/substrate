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

# Fails unless GITHUB_SHA, which the attestations name, is TAG's commit.
#
#   TAG=<tag> GITHUB_SHA=<sha> .github/scripts/release/check-release-ref.sh
#
# Used by the images and draft jobs in .github/workflows/release.yaml.

set -o errexit -o nounset -o pipefail

want="$(git rev-parse --verify "refs/tags/${TAG}^{commit}")"
# On a tag push GITHUB_SHA may be the tag object. An object the checkout did
# not fetch is a mismatch.
got="$(git rev-parse --verify --quiet "${GITHUB_SHA}^{commit}" || echo "${GITHUB_SHA}")"
if [[ "${got}" != "${want}" ]]; then
  echo "::error::this run is for ${GITHUB_SHA}, but ${TAG} names ${want}. Start the workflow from the tag: gh workflow run release.yaml --ref ${TAG} -f tag=${TAG}"
  exit 1
fi
echo "${TAG} names ${want}, the commit this run attests."
