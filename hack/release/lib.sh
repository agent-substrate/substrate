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

# Definitions shared by the scripts in hack/release. Source this file; do not
# run it.
# shellcheck shell=bash disable=SC2034 # The variables are used by the sourcing scripts.

# Release tags: vMAJOR.MINOR.PATCH for a release, and
# vMAJOR.MINOR.PATCH-PRERELEASE (e.g. v0.3.0-rc.1) for a release candidate.
SEMVER='^v[0-9]+\.[0-9]+\.[0-9]+$'
SEMVER_PRE='^v[0-9]+\.[0-9]+\.[0-9]+-[0-9A-Za-z.-]+$'

# The only repository whose release workflow publishes images, and the
# registry path it publishes them under. Forks never publish: GitHub bills
# package storage to the fork's owner.
UPSTREAM_REPO='agent-substrate/substrate'
RELEASE_IMAGE_REPO='ghcr.io/agent-substrate/substrate'

# The platforms every release image is built for, comma-separated.
RELEASE_PLATFORMS='linux/amd64,linux/arm64'

# release_tag_ok <tag> succeeds if <tag> is a release or release candidate tag.
release_tag_ok() {
  [[ "$1" =~ ${SEMVER} || "$1" =~ ${SEMVER_PRE} ]]
}
