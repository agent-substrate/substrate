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

# Find clang-format in the PATH, and exit if not found.
tool="clang-format"
clangfmt="$(which "$tool" 2>/dev/null || true)"
if [[ ! -x "${clangfmt}" ]]; then
  echo "Failed to find ${tool}: please make sure it is in your PATH" >&2
  exit 1
fi

# Find all top-level directories containing proto files, and run clangfmt on them.
# shellcheck disable=SC2207 # reading array
files=(
    $(git ls-files \
        -cmo \
        --exclude-standard \
        -- \
        ':(glob)**/*.proto' \
        ':!:vendor/*' \
        ':!:**/vendor/*' \
        ':!:LICENSES/*' \
        | sort \
        | uniq)
)

# Run clang-format on the found files.
#
# Don't reflow long lines, wince those tend to be comments which then require
# re-running the proto generators, which makes "update-all" awkward.
"${clangfmt}" \
    -i \
    --style="{BasedOnStyle: LLVM, ColumnLimit: 0}" \
    "${files[@]}"
