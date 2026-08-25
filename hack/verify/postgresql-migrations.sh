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

export LC_ALL=C

MIGRATIONS_DIR="cmd/ateapi/internal/store/atepg/migrations"

if (( $# > 1 )); then
  echo "Usage: $0 [base-commit]" >&2
  exit 2
fi

shopt -s nullglob
migrations=("${MIGRATIONS_DIR}"/*.up.sql)
if (( ${#migrations[@]} == 0 )); then
  echo "Add at least one PostgreSQL migration." >&2
  exit 1
fi

expected=1
for migration in "${migrations[@]}"; do
  name="${migration##*/}"
  if [[ ! "${name}" =~ ^([0-9]{6})_[a-z0-9_]+\.up\.sql$ ]]; then
    echo "Migration file ${name} must match NNNNNN_name.up.sql." >&2
    exit 1
  fi
  version=$((10#${BASH_REMATCH[1]}))
  if (( version != expected )); then
    printf 'Expected PostgreSQL migration version %06d, but found %s.\n' "${expected}" "${name}" >&2
    exit 1
  fi
  expected=$((expected + 1))
done

if compgen -G "${MIGRATIONS_DIR}/*.down.sql" >/dev/null; then
  echo "Do not add PostgreSQL down migrations." >&2
  exit 1
fi

if (( $# == 1 )) && ! git diff --quiet --no-renames --diff-filter=MD "$1" HEAD -- "${MIGRATIONS_DIR}"; then
  echo "Do not change or delete merged PostgreSQL migrations." >&2
  git diff --name-only --no-renames --diff-filter=MD "$1" HEAD -- "${MIGRATIONS_DIR}" >&2
  exit 1
fi
