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

# Fails if any base image in .ko.yaml is not pinned by digest. A tag-only base
# makes the build depend on when it runs: the same commit built a week apart
# would produce different images. See the header comment in .ko.yaml.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Image refs are the values of defaultBaseImage and of every entry under
# baseImageOverrides. Both are `key: value` lines; comments and blank lines
# are dropped. defaultPlatforms entries are list items and never match.
refs="$(sed -e 's/#.*$//' .ko.yaml \
  | awk '
      /^defaultBaseImage:/ { print $2; next }
      /^baseImageOverrides:/ { in_overrides = 1; next }
      /^[^[:space:]]/ { in_overrides = 0 }
      in_overrides && NF >= 2 { print $NF }
    ')"

if [[ -z "${refs}" ]]; then
  echo "error: no base image refs found in .ko.yaml" >&2
  exit 1
fi

rc=0
while IFS= read -r ref; do
  if [[ ! "${ref}" =~ @sha256:[0-9a-f]{64}$ ]]; then
    echo "error: .ko.yaml base image is not pinned by digest: ${ref}" >&2
    rc=1
  fi
done <<< "${refs}"

if [[ "${rc}" -ne 0 ]]; then
  echo "Pin every base image as <image>:<tag>@sha256:<digest> (see .ko.yaml)." >&2
fi
exit "${rc}"
