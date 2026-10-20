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

export LC_ALL=C # for sorting to be consistent across locales

# Run every verifier even if one fails, so a single run reports all failures.
FAILED=()
# shellcheck disable=2044 # for-loop over find output is intentional
for F in $(find ./hack/verify -name '*.sh' | sort); do
  echo "Running ${F}"
  if ! "${F}" "$@"; then
    FAILED+=("${F}")
  fi
done

if [[ ${#FAILED[@]} -gt 0 ]]; then
  echo >&2
  echo "FAILED verifiers:" >&2
  printf '  %s\n' "${FAILED[@]}" >&2
  exit 1
fi
