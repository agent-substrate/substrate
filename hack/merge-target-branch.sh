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

# Merges the pull request's target branch into the checked-out tree, so the jobs
# below test the state that would exist after merging.
#
# actions/checkout already checks out refs/pull/N/merge, which is the PR head
# merged into the target branch AS OF THE MOMENT THE EVENT FIRED. Nothing
# re-triggers the run when the target branch moves on afterwards, so a green
# check can reflect a merge with a target branch that is hours or days stale --
# which is how two individually-green PRs break the branch they land on.
# Merging the live tip here closes that window.
#
# Any conflict fails the job: CI has no business guessing at a resolution, and a
# PR that needs a non-trivial merge needs a human to perform it.
#
# Requires the full history (actions/checkout fetch-depth: 0) to find a merge
# base. Usage: TARGET_BRANCH=main hack/merge-target-branch.sh

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

if [[ -z "${TARGET_BRANCH:-}" ]]; then
  echo "TARGET_BRANCH is unset (expected the branch the PR targets, e.g. main)." >&2
  exit 1
fi

git fetch --no-tags origin \
  "+refs/heads/${TARGET_BRANCH}:refs/remotes/origin/${TARGET_BRANCH}"
TARGET_SHA="$(git rev-parse "refs/remotes/origin/${TARGET_BRANCH}")"

# The common case: the target branch has not moved since the event, so the
# checked-out merge commit already contains it and there is nothing to do.
if git merge-base --is-ancestor "${TARGET_SHA}" HEAD; then
  echo "Already up to date with origin/${TARGET_BRANCH} (${TARGET_SHA})."
  exit 0
fi

echo "Merging origin/${TARGET_BRANCH} (${TARGET_SHA}) into $(git rev-parse HEAD)"
# Commit the merge rather than leaving it staged: hack/verify runs refuse to
# start on a dirty tree, and check HEAD out into a scratch worktree.
if ! git -c user.name='substrate CI' -c user.email='ci@substrate.invalid' \
  merge --no-edit "${TARGET_SHA}"; then
  echo
  echo "Merging ${TARGET_BRANCH} into this pull request conflicts in:" >&2
  git diff --name-only --diff-filter=U >&2
  echo
  echo "Merge or rebase onto ${TARGET_BRANCH} and resolve the conflicts." >&2
  exit 1
fi
