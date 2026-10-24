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

# Checks that every release image published for VERSION has an attestation
# from this workflow for the tag's commit that names it. Runs when a rerun
# built nothing. The digests land in DIGESTS_FILE for the notes.
#
#   GITHUB_REPOSITORY=<owner/repo> VERSION=<tag> KO_DOCKER_REPO=<registry path> \
#     GH_TOKEN=<token> DIGESTS_FILE=<path> \
#     .github/scripts/release/verify-image-attestations.sh
#
# Needs gh and jq. Used by the images job in .github/workflows/release.yaml.

set -o errexit -o nounset -o pipefail

signer="${GITHUB_REPOSITORY}/.github/workflows/release.yaml"
commit="$(git rev-parse --verify "refs/tags/${VERSION}^{commit}")"
go run ./cmd/ate-setup publish release-images --list-published --digests-file "${DIGESTS_FILE}"
failed=0
# gh checks digest, signer and source; jq checks the statement names this
# image with this digest.
while read -r hex image; do
  if result="$(gh attestation verify "oci://${image}@sha256:${hex}" --repo "${GITHUB_REPOSITORY}" \
      --signer-workflow "${signer}" --source-digest "${commit}" --source-ref "refs/tags/${VERSION}" --format json)" \
    && jq -e --arg name "${image}" --arg hex "${hex}" \
      '[.[].verificationResult.statement.subject[] | select(.name == $name and .digest.sha256 == $hex)] | length > 0' \
      <<<"${result}" >/dev/null; then
    continue
  fi
  echo "::error::${image}@sha256:${hex} has no attestation from ${signer} for ${commit}; rerun with overwrite to rebuild the set"
  failed=1
done <"${DIGESTS_FILE}"
exit "${failed}"
