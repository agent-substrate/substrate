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

# Fails if the tag VERSION has a published (non-draft) release in
# GITHUB_REPOSITORY. Overwriting a release's images replaces the digests that
# operators install by tag, so it is refused once the release is published.
#
#   GITHUB_REPOSITORY=<owner/repo> VERSION=<tag> GH_TOKEN=<token> \
#     .github/scripts/release/refuse-published-overwrite.sh
#
# A draft is not returned by tag, so a 404 means there is no published
# release. Any other failure stops the script rather than being read as "none".
#
# Used by the images job in .github/workflows/release.yaml.

set -o errexit -o nounset -o pipefail

if out="$(gh api "repos/${GITHUB_REPOSITORY}/releases/tags/${VERSION}" --jq .draft 2>&1)"; then
  if [[ "${out}" == false ]]; then
    echo "::error::${VERSION} has a published release; its images cannot be overwritten"
    exit 1
  fi
elif [[ "${out}" != *"HTTP 404"* ]]; then
  echo "::error::checking for a published ${VERSION} release: ${out}"
  exit 1
fi
