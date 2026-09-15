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

# Rejects changes to our protos that break binary wire compatibility.
#
# ateapi resources are persisted as binary protobuf in bytea columns, and the
# storage layer decodes them with proto.UnmarshalOptions{DiscardUnknown: true}
# (cmd/ateapi/internal/store/atepg/atepg.go). Binary protobuf keys every field
# on its field number, so reusing or renumbering a field makes a stored row
# decode into the wrong field, and DiscardUnknown then destroys the original
# bytes on the next read-modify-write. Nothing surfaces at runtime. This check
# is where that has to be caught, at review time.
#
# The rules that matter, and what they permit:
#
#   renumber a field                     rejected
#   delete a field without `reserved`    rejected
#   delete a field with `reserved <n>`   allowed  (the correct retirement)
#   change a field's type or label       rejected
#   rename a field, keeping its number   allowed  (names are not on the wire)
#   rename a message                     rejected (see below)
#
# Renaming a message is wire-compatible in fact, but buf compares fields by
# their fully qualified type name and cannot see that the old and new messages
# are structurally identical, so it reports a type change. Renaming one is a
# deliberate act; declare it with the trailer below and say why in the commit.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Compare against the point this branch left main, not main's tip, so a
# breaking change that landed on main independently is not reported here as
# though this branch introduced it.
BASELINE_REF="${PROTO_BREAKING_BASELINE:-}"
if [ -z "${BASELINE_REF}" ]; then
  for CANDIDATE in origin/main main; do
    if git rev-parse --verify --quiet "${CANDIDATE}^{commit}" >/dev/null; then
      BASELINE_REF="${CANDIDATE}"
      break
    fi
  done
fi
if [ -z "${BASELINE_REF}" ]; then
  echo "proto-breaking: no baseline; fetch main (git fetch origin main) or set PROTO_BREAKING_BASELINE" >&2
  exit 1
fi

BASELINE="$(git merge-base HEAD "${BASELINE_REF}")"

# How a deliberate break lands. The commit that makes it carries a trailer in
# the message's last paragraph, naming what becomes unreadable and why that is
# acceptable:
#
#   Proto-Breaking-Change: EgressPolicy.cidrs moves to field 7. Policies
#     written before this decode with no CIDRs and have to be recreated.
#
# A continuation line must be indented, or git stops reading the paragraph as
# trailers and this check sees nothing.
#
# Unlike an environment variable, this is visible in the diff under review,
# stays in the history the break belongs to, and leaves the presubmit green so
# that a red one still means something. It expires on its own: once the commit
# is on main it becomes the baseline and the trailer stops being consulted.
#
# The break has to be committed to be declared -- a trailer cannot describe an
# uncommitted edit -- so the working tree alone is still reported.
TRAILER_KEY="Proto-Breaking-Change"
DECLARED="$(git log --format="%(trailers:key=${TRAILER_KEY},valueonly)" "${BASELINE}..HEAD")"
if [ -n "${DECLARED}" ]; then
  echo "proto-breaking: allowed, ${TRAILER_KEY} declared:" >&2
  echo "${DECLARED}" >&2
  exit 0
fi

# The input is the working tree, so an incompatible edit is reported before it
# is ever committed. --against-config points the baseline at the buf.yaml we
# have now, because commits older than that file carry no buf config of their
# own and would otherwise be read with buf's defaults.
BIN="$("${ROOT}"/hack/run-tool.sh --print-bin-path buf)"
if "${BIN}" breaking --against ".git#ref=${BASELINE}" --against-config buf.yaml; then
  exit 0
fi

cat >&2 <<EOF

proto-breaking: if the break above is intended, commit it with a trailer in the
last paragraph of the commit message saying what stored data it costs:

  ${TRAILER_KEY}: <what becomes unreadable, and why that is acceptable>

Indent any continuation line, or git will not read it as a trailer.
EOF
exit 1
