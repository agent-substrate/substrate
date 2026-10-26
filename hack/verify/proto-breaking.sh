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

# Rejects proto changes that break binary wire compatibility.
#
# Binary protobuf identifies a field only by its number. ateapi stores
# resources as binary protobuf in bytea columns, and atepg.go unmarshals them
# with DiscardUnknown. A renumbered field loses its stored value, and a reused
# number decodes into the wrong field. The original bytes are dropped on the
# next read-modify-write. The same breaks affect gRPC messages between
# binaries built from different commits.
#
# buf.yaml checks the protos under api/ and internal/proto/ with buf's WIRE
# rules:
#
#   renumber a field                     rejected, unless the old number is reserved
#   delete a field without `reserved`    rejected
#   delete a field with `reserved <n>`   allowed
#   change a field's type or label       rejected, unless wire-compatible
#   rename a field, keeping its number   allowed
#   rename a message used as a field     rejected
#
# buf compares message fields by type name, so it reports a renamed message as
# a type change even though the wire format is the same.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

BASELINE_REF=""
for CANDIDATE in origin/main main; do
  if git rev-parse --verify --quiet "${CANDIDATE}^{commit}" >/dev/null; then
    BASELINE_REF="${CANDIDATE}"
    break
  fi
done
if [ -z "${BASELINE_REF}" ]; then
  echo "proto-breaking: no baseline; fetch main (git fetch origin main)" >&2
  exit 1
fi

# Compare against the merge base, so a breaking change that landed on main
# separately is not reported against this branch.
BASELINE="$(git merge-base HEAD "${BASELINE_REF}")"

# The input is the working tree, so an incompatible edit is reported before it
# is ever committed. --against-config reads the baseline with the current
# buf.yaml, so a baseline commit without one still builds.
BIN="$("${ROOT}"/hack/run-tool.sh --print-bin-path buf)"
"${BIN}" breaking --against ".git#ref=${BASELINE}" --against-config buf.yaml
