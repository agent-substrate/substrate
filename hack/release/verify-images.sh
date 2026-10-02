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

# Checks that every image `make build-release-images` publishes exists as
# REPO/<image>:<tag> for each platform in RELEASE_PLATFORMS (hack/release/lib.sh).
# The image list comes from `make print-release-images`.
#
#   REPO=<registry path> hack/release/verify-images.sh [--quiet] <tag>
#
# Exits 0 if every image is complete, 1 if any is missing or lacks a platform,
# and 2 on a usage error or if the registry could not be queried. --quiet
# prints nothing but errors.
# When GITHUB_STEP_SUMMARY is set, a result table is appended to that file.
#
# Needs docker with buildx, jq, and read access to REPO.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
# shellcheck source=hack/release/lib.sh
source "${ROOT}/hack/release/lib.sh"

usage() {
  echo "usage: REPO=<registry path> $0 [--quiet] <tag>" >&2
  exit 2
}

quiet=false
if [[ "${1:-}" == --quiet ]]; then
  quiet=true
  shift
fi
[[ $# -eq 1 && -n "${REPO:-}" ]] || usage
tag="$1"
release_tag_ok "${tag}" || {
  echo "error: ${tag} is not a vMAJOR.MINOR.PATCH[-PRERELEASE] tag" >&2
  exit 2
}

say() {
  [[ "${quiet}" == true ]] || echo "$@"
}

# SKIP_IMAGES= on the command line, so a value in the environment cannot
# shorten the list being checked.
mapfile -t names < <(make -s -C "${ROOT}" print-release-images SKIP_IMAGES=)
IFS=, read -r -a platforms <<<"${RELEASE_PLATFORMS}"

errfile="$(mktemp)"
trap 'rm -f "${errfile}"' EXIT

rows=()
failed=0
for name in "${names[@]}"; do
  ref="${REPO}/${name}:${tag}"
  # A multi-platform image is an index listing one manifest per platform. A
  # single-platform manifest lists none, and counts as incomplete.
  if raw="$(docker buildx imagetools inspect --raw "${ref}" 2>"${errfile}")"; then
    found="$(jq -r '[.manifests[]?.platform | select(.) | "\(.os)/\(.architecture)"] | join(" ")' <<<"${raw}")"
  elif grep -qiE 'not found|name unknown|manifest unknown' "${errfile}"; then
    found=""
  else
    # Any other error (no network, no credentials) says nothing about whether
    # the image exists. Stop rather than report it missing: publish-images.sh
    # rebuilds missing images, which would replace published ones.
    echo "error: inspecting ${ref}: $(<"${errfile}")" >&2
    exit 2
  fi
  missing=()
  for platform in "${platforms[@]}"; do
    [[ " ${found} " == *" ${platform} "* ]] || missing+=("${platform}")
  done
  if [[ ${#missing[@]} -eq 0 ]]; then
    result="ok"
  elif [[ -z "${found}" ]]; then
    result="missing"
    failed=1
  else
    result="missing ${missing[*]}"
    failed=1
  fi
  say "${ref}: ${result}"
  rows+=("| \`${ref}\` | ${result} |")
done

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "### Release images for ${tag}"
    echo
    echo "| Image | Result |"
    echo "|---|---|"
    printf '%s\n' "${rows[@]}"
  } >>"${GITHUB_STEP_SUMMARY}"
fi

exit "${failed}"
