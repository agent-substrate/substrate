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

# Creates a DRAFT GitHub release for an existing tag, with notes generated
# from .github/release.yml plus a committer list. A maintainer edits and
# publishes the draft (see docs/dev/release-notes.md).
#
#   hack/release/draft-release.sh [--dry-run] <tag>
#
# --dry-run prints the notes and creates nothing. <tag> need not exist: the
# notes then run up to the local HEAD commit, which must already be in the
# repository (pushed to a branch there). If a release for <tag>
# already exists, the script prints the notes and leaves the release alone,
# so rerunning it never overwrites edited notes.
#
# Needs git, jq, and an authenticated gh with write access to the repository
# (the generate-notes API requires it even for --dry-run). GITHUB_REPOSITORY
# selects the repository; it defaults to agent-substrate/substrate.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
# shellcheck source=hack/release/lib.sh
source "${ROOT}/hack/release/lib.sh"
REPO="${GITHUB_REPOSITORY:-${UPSTREAM_REPO}}"

usage() {
  echo "usage: $0 [--dry-run] <tag>" >&2
  exit 2
}

errfile="$(mktemp)"
trap 'rm -f "${errfile}"' EXIT

# api <step> <gh api args...> prints the response. On failure it prints the
# step and gh's error, and returns 1; gh's error stays in ${errfile}.
api() {
  local step="$1"
  shift
  if ! gh api "$@" 2>"${errfile}"; then
    echo "error: ${step}: $(<"${errfile}")" >&2
    return 1
  fi
}

# exists <api-path> returns 0 if the resource exists and 1 if the API answers
# 404, or 422 as the commits endpoint does for an unknown SHA. Any other
# failure (gh not logged in, rate limit) stops the script with gh's error, so
# it is not mistaken for "not found".
exists() {
  if gh api "$1" --silent 2>"${errfile}"; then
    return 0
  fi
  if grep -qE 'HTTP (404|422)' "${errfile}"; then
    return 1
  fi
  echo "error: checking $1: $(<"${errfile}")" >&2
  exit 1
}

dry_run=false
if [[ "${1:-}" == --dry-run ]]; then
  dry_run=true
  shift
fi
[[ $# -eq 1 ]] || usage
tag="$1"

prerelease=false
if [[ "${tag}" =~ ${SEMVER_PRE} ]]; then
  prerelease=true
elif ! [[ "${tag}" =~ ${SEMVER} ]]; then
  echo "error: ${tag} is not a vMAJOR.MINOR.PATCH[-PRERELEASE] tag" >&2
  exit 1
fi

# target is the ref the notes run up to: the tag, or HEAD for a dry run of a
# tag that does not exist yet.
target="${tag}"
if ! exists "repos/${REPO}/git/ref/tags/${tag}"; then
  if [[ "${dry_run}" != true ]]; then
    echo "error: tag ${tag} does not exist in ${REPO}; push it first," \
      "or use --dry-run to preview the notes up to HEAD" >&2
    exit 1
  fi
  target="$(git rev-parse remotes/origin/main)"
  if ! exists "repos/${REPO}/commits/${target}"; then
    echo "error: HEAD (${target}) is not in ${REPO}; push it to a branch there," \
      "or set GITHUB_REPOSITORY to a repository that has it" >&2
    exit 1
  fi
fi

# The notes start at the newest final release below this tag's version. A
# final release therefore diffs against the previous final release, not its
# own release candidates, and every rc of a version shares one starting point.
# Tags come from the API because a local clone may lack them (release tags
# can sit on release-X.Y branches).
base="${tag%%-*}"
tags="$(api "listing tags" "repos/${REPO}/tags" --paginate --jq '.[].name')"
# With no tags at all (a fork, for example), the notes would silently cover
# the whole history. GitHub needs the previous tag in ${REPO} itself.
if ! grep -qE "${SEMVER}" <<<"${tags}"; then
  echo "error: ${REPO} has no vX.Y.Z tags, so there is no previous release" \
    "to start the notes from. Push the release tags to it first" \
    "(git push <remote> v0.1.0 v0.2.0 ...)" >&2
  exit 1
fi
previous="$(
  {
    grep -E "${SEMVER}" <<<"${tags}" | grep -vxF "${base}" || true
    echo "${base}"
  } | sort -V | grep -B1 -xF "${base}" | grep -vxF "${base}" || true
)"

generate_args=(-f "tag_name=${tag}")
if [[ "${target}" != "${tag}" ]]; then
  generate_args+=(-f "target_commitish=${target}")
fi
if [[ -n "${previous}" ]]; then
  generate_args+=(-f "previous_tag_name=${previous}")
fi
if ! notes="$(api "generating notes" "repos/${REPO}/releases/generate-notes" \
  "${generate_args[@]}" --jq .body)"; then
  if grep -q 'HTTP 404' "${errfile}"; then
    echo "GitHub answers 404 here when the token cannot write to ${REPO}." \
      "Generating notes needs write access, even for --dry-run." >&2
  fi
  exit 1
fi

# Only the upstream release workflow publishes images (see
# .github/workflows/release.yaml), so only its notes say where they are.
if [[ "${REPO}" == "${UPSTREAM_REPO}" ]]; then
  notes+=$'\n\n## Install\n\n'
  notes+="Container images for this release are published to \`${RELEASE_IMAGE_REPO}\`."
  notes+=$' From a checkout of the tag:\n\n```sh\n'
  notes+="ate-setup deploy ate-system --image-repo ${RELEASE_IMAGE_REPO} --image-tag ${tag}"
  notes+=$'\n```'
fi

# GitHub only lists first-time contributors; list everyone, as v0.1.0 did.
# get-contributors.sh reads the range from the local clone, so both ends must
# be present locally (the release workflow checks out full history).
if [[ -n "${previous}" ]]; then
  owner="${REPO%%/*}"
  name="${REPO#*/}"
  contributors="$(ORG="${owner}" REPO="${name}" \
    "${ROOT}/hack/util/get-contributors.sh" "${previous}..${target}" | tail -n +2)"
  if [[ -n "${contributors}" ]]; then
    notes+=$'\n\n## Committers in this release\n\n'
    notes+="${contributors}"
  fi
fi

up_to=""
if [[ "${target}" != "${tag}" ]]; then
  up_to=", up to HEAD ${target}"
fi
echo "Release notes for ${tag} (previous release: ${previous:-none}${up_to}):"
echo
echo "${notes}"
echo

if [[ "${dry_run}" == true ]]; then
  echo "Dry run: no release created."
  exit 0
fi
if gh release view "${tag}" --repo "${REPO}" >/dev/null 2>&1; then
  echo "A release for ${tag} already exists; left unchanged."
  exit 0
fi

create_args=(--repo "${REPO}" --draft --verify-tag --title "${tag}" --notes-file -)
if [[ "${prerelease}" == true ]]; then
  create_args+=(--prerelease)
fi
gh release create "${tag}" "${create_args[@]}" <<<"${notes}"
