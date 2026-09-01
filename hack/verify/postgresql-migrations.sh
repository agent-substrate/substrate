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
  echo "Usage: $0 [released-ref]" >&2
  exit 2
fi

shopt -s nullglob
migrations=("${MIGRATIONS_DIR}"/*.sql)
if (( ${#migrations[@]} == 0 )); then
  echo "Add at least one PostgreSQL migration." >&2
  exit 1
fi

expected=1
for migration in "${migrations[@]}"; do
  name="${migration##*/}"
  if [[ ! "${name}" =~ ^([0-9]{6})_[a-z0-9_]+\.sql$ ]]; then
    echo "Migration file ${name} must match NNNNNN_name.sql." >&2
    exit 1
  fi
  version=$((10#${BASH_REMATCH[1]}))
  if (( version != expected )); then
    printf 'Expected PostgreSQL migration version %06d, but found %s.\n' "${expected}" "${name}" >&2
    exit 1
  fi
  if ! grep -Fxq -- '-- +goose Up' "${migration}"; then
    echo "Migration file ${name} must contain a Goose Up annotation." >&2
    exit 1
  fi
  if grep -Fiq -- '-- +goose Down' "${migration}"; then
    echo "Migration file ${name} must not contain a Goose Down migration." >&2
    exit 1
  fi
  if grep -Fiq -- '-- +goose no transaction' "${migration}"; then
    echo "Migration file ${name} must run in a PostgreSQL transaction." >&2
    exit 1
  fi
  if grep -Fiq -- 'IF NOT EXISTS' "${migration}"; then
    echo "Migration file ${name} must not contain an IF NOT EXISTS guard." >&2
    exit 1
  fi
  if grep -Fiq -- '-- +goose ENVSUB' "${migration}"; then
    echo "Migration file ${name} must not use Goose environment substitution." >&2
    exit 1
  fi
  if grep -Eiq -- '^[[:space:]]*(BEGIN|START[[:space:]]+TRANSACTION|COMMIT|ROLLBACK)[[:space:]]*;' "${migration}"; then
    echo "Migration file ${name} must let Goose control the PostgreSQL transaction." >&2
    exit 1
  fi
  expected=$((expected + 1))
done

released_ref="${1:-}"
if [[ -z "${released_ref}" ]]; then
  while read -r tag; do
    if [[ ! "${tag}" =~ ^v[1-9][0-9]*\.[0-9]+\.[0-9]+$ ]]; then
      continue
    fi
    if [[ "$(git ls-tree -r --name-only "${tag}" -- "${MIGRATIONS_DIR}")" == *".sql"* ]]; then
      released_ref="${tag}"
      break
    fi
  done < <(git tag --merged HEAD --sort=-version:refname)
fi

if [[ -n "${released_ref}" ]] && ! git diff --quiet --no-renames --diff-filter=MD "${released_ref}" HEAD -- "${MIGRATIONS_DIR}"; then
  echo "Do not change or delete released PostgreSQL migrations." >&2
  git diff --name-only --no-renames --diff-filter=MD "${released_ref}" HEAD -- "${MIGRATIONS_DIR}" >&2
  exit 1
fi
