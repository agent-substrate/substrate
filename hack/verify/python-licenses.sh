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

# Verifies that no Python dependency in any of our requirements.txt files
# (and their transitive closure) carries a disallowed license. Modeled on
# hack/verify/licenses.sh (Go side), but Python deps live in per-project
# venvs that may be ephemeral, so this script creates them on demand.
#
# CNCF allowed third-party licenses:
#   https://github.com/cncf/foundation/blob/main/allowed-third-party-license-policy.md

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Python projects with a requirements.txt to check. Each entry is a directory
# that contains requirements.txt; its venv lives at <dir>/venv. Discovered
# dynamically from tracked/untracked (but not ignored) requirements.txt files,
# excluding vendored trees and the LICENSES/ directory.
mapfile -t PROJECTS < <(
  git ls-files \
    -cmo \
    --exclude-standard \
    -- \
    ':(glob)**/requirements.txt' \
    ':!:vendor/*' \
    ':!:**/vendor/*' \
    ':!:LICENSES/*' \
  | xargs -r -n1 dirname \
  | sort -u
)

# Fail if any installed package's license string contains one of these
# semicolon-separated entries. pip-licenses substring-matches, so single
# substrings like "AGPL" / "GPL" / "SSPL" catch every common spelling.
# Add entries as needed; prefer this denylist over an allowlist because
# pip-licenses license strings vary widely (classifier vs. License field
# vs. PEP-639 expression).
DISALLOWED="AGPL;GPLv2;GPLv3;GPL-2.0;GPL-3.0;GNU General Public License;GNU Affero General Public License;Server Side Public License;SSPL;Commons Clause"

check_project() {
  local dir="$1"
  local venv="${dir}/venv"
  echo "==> ${dir}"
  # Run inside a subshell so `activate` doesn't leak VIRTUAL_ENV/PATH back
  # to the caller; the subshell inherits errexit/nounset/pipefail.
  (
    if [[ ! -d "${venv}" ]]; then
      echo "  Creating venv at ${venv}..."
      python3 -m venv "${venv}" || {
        echo "ERROR: failed to create venv at ${venv}" >&2
        exit 1
      }
      echo "  Activating ${venv}..."
      source "${venv}/bin/activate"
      echo "  Upgrading pip in ${venv}..."
      pip install --quiet --upgrade pip || {
        echo "ERROR: failed to upgrade pip in ${venv}" >&2
        exit 1
      }
      echo "  Installing ${dir}/requirements.txt into ${venv}..."
      pip install --quiet -r "${dir}/requirements.txt" || {
        echo "ERROR: failed to install ${dir}/requirements.txt into ${venv}" >&2
        exit 1
      }
    else
      echo "  Activating ${venv}..."
      source "${venv}/bin/activate"
    fi
    if ! command -v pip-licenses >/dev/null 2>&1; then
      echo "  Installing pip-licenses into ${venv}..."
      pip install --quiet pip-licenses || {
        echo "ERROR: failed to install pip-licenses into ${venv}" >&2
        exit 1
      }
    fi
    echo "  Checking licenses in ${venv}..."
    pip-licenses --fail-on="${DISALLOWED}"
  )
}

fail=0
for proj in "${PROJECTS[@]}"; do
  if ! check_project "${proj}"; then
    echo "ERROR: ${proj} contains disallowed Python license(s)" >&2
    fail=1
  fi
done

if [[ "${fail}" -ne 0 ]]; then
  exit 1
fi

echo "All Python licenses pass."
