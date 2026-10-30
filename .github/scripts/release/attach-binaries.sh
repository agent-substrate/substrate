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

# Attaches every file in <dir> to the draft release for TAG in
# GITHUB_REPOSITORY, replacing files of the same name. A release that is
# already published is left unchanged, so a rerun never changes its files.
#
#   GITHUB_REPOSITORY=<owner/repo> TAG=<tag> GH_TOKEN=<token> \
#     .github/scripts/release/attach-binaries.sh <dir>
#
# Used by the draft job in .github/workflows/release.yaml.

set -o errexit -o nounset -o pipefail

[[ $# -eq 1 ]] || {
  echo "usage: GITHUB_REPOSITORY=<owner/repo> TAG=<tag> $0 <dir>" >&2
  exit 2
}
dir="$1"

# An assignment, so a failed query (API error, rate limit) stops the script
# under errexit instead of being read as "published" and skipping the upload.
is_draft="$(gh release view "${TAG}" --repo "${GITHUB_REPOSITORY}" --json isDraft --jq .isDraft)"
if [[ "${is_draft}" != true ]]; then
  echo "${TAG} is published; its binaries are left unchanged."
  exit 0
fi
gh release upload "${TAG}" "${dir}"/* --repo "${GITHUB_REPOSITORY}" --clobber
