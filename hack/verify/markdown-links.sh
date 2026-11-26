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

# Verifies internal links and heading anchors in tracked Markdown files.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Exclude copied third-party docs and synthetic website test fixtures:
# - vendor/ and _LICENSES/: third-party modules whose READMEs reference files
#   that go mod vendor omits.
# - site/testdata/: synthetic Hugo fixtures used by hack/test-site-templates.sh.
# - .agents/skills/review-crds/references/: copied Kubernetes API conventions
#   doc whose links target paths in kubernetes/community.
files=()
while IFS= read -r -d '' f; do
  files+=("${f}")
done < <(git ls-files -z '*.md' \
  ':(exclude)vendor/' \
  ':(exclude)_LICENSES/' \
  ':(exclude)site/testdata/' \
  ':(exclude).agents/skills/review-crds/references/')

if [[ ${#files[@]} -eq 0 ]]; then
  echo "FAIL: no Markdown files found to check." >&2
  exit 1
fi

go -C tools/mdlinks run . --root "${ROOT}" -- "${files[@]}"
