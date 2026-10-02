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

# Builds every release image from the checked-out tag with
# `make build-release-images` and pushes it to REPO, tagged <tag>, for each
# platform in RELEASE_PLATFORMS (hack/release/lib.sh).
#
#   REPO=<registry path> [FORCE=true] hack/release/publish-images.sh <tag>
#
# HEAD must be the commit <tag> names and the work tree must be clean, since
# the images are built from the work tree.
#
# If REPO already holds every image for <tag> on every platform (checked with
# hack/release/verify-images.sh), nothing is built unless FORCE=true. Rerunning
# therefore never replaces published images by accident. An incomplete set,
# left by a failed run, is rebuilt in full.
#
# Needs go, jq, docker with a buildx builder that can build every platform in
# RELEASE_PLATFORMS (QEMU for the non-native ones), and push access to REPO.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
# shellcheck source=hack/release/lib.sh
source "${ROOT}/hack/release/lib.sh"

usage() {
  echo "usage: REPO=<registry path> [FORCE=true] $0 <tag>" >&2
  exit 2
}

[[ $# -eq 1 && -n "${REPO:-}" ]] || usage
tag="$1"
force="${FORCE:-false}"

release_tag_ok "${tag}" || {
  echo "error: ${tag} is not a vMAJOR.MINOR.PATCH[-PRERELEASE] tag" >&2
  exit 1
}
if ! tag_commit="$(git -C "${ROOT}" rev-parse -q --verify "refs/tags/${tag}^{commit}")"; then
  echo "error: tag ${tag} does not exist in this clone" >&2
  exit 1
fi
if [[ "$(git -C "${ROOT}" rev-parse HEAD)" != "${tag_commit}" ]]; then
  echo "error: HEAD is not ${tag} (${tag_commit}); check out the tag first" >&2
  exit 1
fi
if [[ -n "$(git -C "${ROOT}" status --porcelain)" ]]; then
  echo "error: the work tree has changes; images for ${tag} must be built from the tag alone" >&2
  exit 1
fi

if [[ "${force}" != true ]]; then
  # 0: complete, skip. 1: incomplete, build. 2: the registry could not be
  # queried, so whether images exist is unknown; stop.
  status=0
  "${ROOT}/hack/release/verify-images.sh" --quiet "${tag}" || status=$?
  case "${status}" in
    0)
      echo "${REPO} already holds every image for ${tag}; nothing built. Set FORCE=true to rebuild and replace them."
      exit 0
      ;;
    1) ;;
    *) exit "${status}" ;;
  esac
fi

# Variables go on the make command line, not in the environment: the Makefile
# assigns KO_DOCKER_REPO with :=, which overrides the environment. make exports
# command-line variables to the recipes, so ko and the Dockerfile build both
# read KO_DEFAULTPLATFORMS. SKIP_IMAGES= keeps a value in the environment from
# leaving images out of the release.
make -C "${ROOT}" build-release-images \
  KO_DOCKER_REPO="${REPO}" \
  VERSION="${tag}" \
  KO_DEFAULTPLATFORMS="${RELEASE_PLATFORMS}" \
  SKIP_IMAGES=
