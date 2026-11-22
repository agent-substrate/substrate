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

# Writes code=true to $GITHUB_OUTPUT if HEAD changes any file outside the
# docs-only paths listed in CONTRIBUTING.md, and code=false otherwise. With no
# base commit (push and schedule runs) it writes code=true.
#
# Used by .github/workflows/pr-workflow.yaml.

set -o errexit -o nounset -o pipefail

base="${1:-}"
if [[ -z "${base}" ]]; then
  echo "code=true" >> "${GITHUB_OUTPUT}"
  exit 0
fi

git fetch --quiet --no-tags --depth=1 origin "${base}"

files="$(git diff --name-only --no-renames "${base}" HEAD)"

code=false
while IFS= read -r f; do
  [[ -n "${f}" ]] || continue
  case "${f}" in
    docs/* | site/* | demos/*/README.md) ;;
    */*) code=true ;;
    *.md) ;;
    *) code=true ;;
  esac
  if [[ "${code}" == true ]]; then
    break
  fi
done <<< "${files}"

echo "code=${code}" >> "${GITHUB_OUTPUT}"
