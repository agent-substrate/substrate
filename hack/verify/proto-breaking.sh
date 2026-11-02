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

# Rejects breaking proto changes across each proto package.
#
# ateapi stores resources as binary protobuf in bytea columns, and atepg.go
# unmarshals them with DiscardUnknown. kubectl-ate parses and prints resource
# manifests as protojson, and atelet writes ateletpb.Snapshot to disk as
# protojson. External callers and plugins import the generated Go packages
# under api/ and pkg/proto/.
#
# buf.yaml checks the protos under api/, internal/proto/, and pkg/proto/ with
# buf's PACKAGE rules. Within a proto package, declarations may move between
# .proto files, but messages, enums, services, RPCs, fields, enum values, and
# reserved ranges cannot be deleted, renamed, renumbered, or retyped.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

if (( $# > 1 )); then
  echo "Usage: $0 [base-ref]" >&2
  exit 2
fi

# Compare against the merge base, so a breaking change that landed on main
# separately is not reported against this branch. When no ref is given, check
# upstream/main, origin/main, and main, and keep the newest merge base so a
# stale fork or local main does not win.
BASELINE=""
if (( $# == 1 )); then
  BASELINE="$(git merge-base HEAD "$1")"
else
  for CANDIDATE in upstream/main origin/main main; do
    if ! MB="$(git merge-base HEAD "${CANDIDATE}" 2>/dev/null)"; then
      continue
    fi
    if [ -z "${BASELINE}" ] || git merge-base --is-ancestor "${BASELINE}" "${MB}"; then
      BASELINE="${MB}"
    fi
  done
  if [ -z "${BASELINE}" ]; then
    echo "proto-breaking: no baseline; fetch main (git fetch origin main)" >&2
    exit 1
  fi
fi

# The input is the working tree, so an incompatible edit is reported before it
# is ever committed. --against-config reads the baseline with the current
# buf.yaml, so a baseline commit without one still builds.
BIN="$("${ROOT}"/hack/run-tool.sh --print-bin-path buf)"
"${BIN}" breaking --against ".git#ref=${BASELINE}" --against-config buf.yaml
