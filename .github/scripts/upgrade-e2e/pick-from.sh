#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
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

# Picks the release to upgrade from and writes it to GITHUB_OUTPUT as tag=.
#
#   FROM=<tag or commit, may be empty> TO=<branch> EVENT=<event name> \
#     .github/scripts/upgrade-e2e/pick-from.sh
#
# FROM wins when set. Otherwise, on a release-X.Y branch, it is the newest
# earlier vX.Y.Z release: a release branch only takes patch releases, and
# users upgrade from a release. A minor with no release yet, such as one
# between its release candidates, starts from its newest pre-release. With
# nothing to upgrade from, a push writes an empty tag and the job skips its
# steps, and a manual run fails.
#
# Runs in the checkout of the release under test. Used by
# .github/workflows/upgrade-e2e.yaml.

set -o errexit -o nounset -o pipefail

if [[ -z "${FROM}" && "${TO}" =~ ^release-[0-9]+\.[0-9]+$ ]]; then
  # The minor's tags, newest first. versionsort.suffix sorts a pre-release
  # (v1.0.0-rc.1) below its release.
  tags="$(git -c versionsort.suffix=- tag -l "v${TO#release-}.*" --merged HEAD --no-contains HEAD --sort=-v:refname)"
  FROM="$(grep -v -e - <<<"${tags}" | head -n 1 || true)"
  if [[ -z "${FROM}" ]]; then
    FROM="$(head -n 1 <<<"${tags}")"
  fi
fi
if [[ -z "${FROM}" ]]; then
  if [[ "${EVENT}" == workflow_dispatch ]]; then
    echo "::error::Nothing to upgrade ${TO} from: set the from input."
    exit 1
  fi
  echo "::notice::No earlier release on ${TO} to upgrade from yet."
  echo "tag=" >> "${GITHUB_OUTPUT}"
  exit 0
fi
echo "Upgrading ${FROM} -> ${TO}"
echo "tag=${FROM}" >> "${GITHUB_OUTPUT}"
