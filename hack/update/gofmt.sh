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

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# GOTOOLCHAIN has no effect on the version of gofmt.
# We need to find right gofmt, otherwise the one in PATH will be used.
gofmt="$(go env GOROOT)/bin/gofmt"
if [[ ! -x "${gofmt}" ]]; then
  echo "Failed to find $gofmt" >&2
  exit 1
fi

# Find all top-level directories containing Go files, and run gofmt on them.
# shellcheck disable=SC2207 # reading array
dirs=(
    $(git ls-files \
        -cmo \
        --exclude-standard \
        -- \
        ':(glob)**/*.go' \
        ':!:vendor/*' \
        ':!:**/vendor/*' \
        ':!:LICENSES/*' \
        | while read -r FILE; do dirname "${FILE}"; done \
        | sort \
        | uniq)
)

for dir in "${dirs[@]}"; do
  "${gofmt}" -s -w "${dir}"
done
