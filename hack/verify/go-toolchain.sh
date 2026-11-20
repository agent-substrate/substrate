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

# The toolchain directive in the root go.mod pins the Go release that builds
# what we ship, and it must stay patched. Plugin modules ship too, so they must
# match it, and CI must set up Go from it. Dev-time tools under hack/tools and
# tools/ are left alone: they need no patches and may not build on a newer Go.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

status=0
fail() {
  echo "$*" >&2
  status=1
}

pin="$(awk '$1 == "toolchain" {print $2; exit}' go.mod)"
if [[ ! "${pin}" =~ ^go1\.[0-9]+\.[0-9]+$ ]]; then
  echo "go.mod: needs a toolchain directive naming a full release, such as 'toolchain go1.27.3'." >&2
  exit 1
fi

while IFS= read -r mod; do
  found="$(awk '$1 == "toolchain" {print $2; exit}' "${mod}")"
  if [[ "${found}" != "${pin}" ]]; then
    fail "${mod}: toolchain ${found:-missing}, want ${pin} from go.mod; run: go mod edit -toolchain=${pin} ${mod}"
  fi
done < <(git ls-files -- 'internal/plugins/*/go.mod')

while IFS= read -r match; do
  fail "${match}: set up Go with 'go-version-file: go.mod' so it follows the pin."
done < <(git grep -nE '^\s*go-version(-file)?:' -- .github \
  | grep -Ev "go-version-file:\s*[\"']?([^\"'[:space:]]*/)?go\.mod[\"']?\s*$" || true)

exit "${status}"
