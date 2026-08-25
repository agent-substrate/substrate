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

# Runs every root-gated test package under sudo. A package is root-gated when
# its tests import internal/roottest (the tests self-skip unless run as root),
# so adding privileged tests anywhere picks them up here with no CI change.
# Extra arguments are passed through to `go test` (e.g. -v, -run).

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

MARKER="github.com/agent-substrate/substrate/internal/roottest"

# Look for the marker in both in-package (TestImports) and external _test
# package (XTestImports) test files.
PKGS="$(go list \
  -f '{{range .TestImports}}{{if eq . "'"${MARKER}"'"}}{{println $.ImportPath}}{{end}}{{end}}{{range .XTestImports}}{{if eq . "'"${MARKER}"'"}}{{println $.ImportPath}}{{end}}{{end}}' \
  ./... | sort -u)"

if [[ -z "${PKGS}" ]]; then
  echo "No root-gated test packages found (nothing imports ${MARKER})."
  exit 0
fi

echo "Root-gated test packages:"
echo "${PKGS}"

# -count=1: the Go test cache does not key on euid, so without it a rerun as
# root replays the unprivileged run's cached skips.
# shellcheck disable=SC2086 # intentional word splitting of the package list
if [[ "$(id -u)" -eq 0 ]]; then
  exec go test -count=1 "$@" ${PKGS}
fi
# -E / env PATH: keep the invoking user's Go toolchain and module caches.
# shellcheck disable=SC2086
exec sudo -E env "PATH=${PATH}" go test -count=1 "$@" ${PKGS}
