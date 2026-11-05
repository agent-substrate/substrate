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

# Builds every pkg/ package that imports internal/ from a module outside
# go.ate.dev/substrate, as an external consumer would, and checks that the
# same module cannot import internal/ directly. Go's internal rule allows the
# first and refuses the second.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Each output is assigned first so errexit stops the script if go list or
# awk fails.
listed="$(go list -mod=vendor -f '{{.ImportPath}} {{join .Imports " "}}' ./pkg/...)"
importers="$(awk '/ go\.ate\.dev\/substrate\/internal\// {print $1}' <<<"${listed}")"
if [[ -z "${importers}" ]]; then
    echo "No pkg/ package imports internal/."
    exit 0
fi
pkgs=()
while IFS= read -r pkg; do
    pkgs+=("${pkg}")
done <<<"${importers}"

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
go_version="$(awk '$1 == "go" {print $2; exit}' go.mod)"
cat >"${tmp}/go.mod" <<EOF
module example.com/substrate-external-import

go ${go_version}

require go.ate.dev/substrate v0.0.0

replace go.ate.dev/substrate => ${ROOT}
EOF
cp go.sum "${tmp}/go.sum"
mkdir "${tmp}/public" "${tmp}/direct"
{
    printf 'package public\n\nimport (\n'
    printf '\t_ "%s"\n' "${pkgs[@]}"
    printf ')\n'
} >"${tmp}/public/public.go"
cat >"${tmp}/direct/direct.go" <<EOF
package direct

import _ "go.ate.dev/substrate/internal/oidcjwt"
EOF

cd "${tmp}"
export GOFLAGS=-mod=mod GOWORK=off
if ! out="$(go build ./public 2>&1)"; then
    echo "${out}" >&2
    echo "A module outside go.ate.dev/substrate cannot build ${pkgs[*]}." >&2
    exit 1
fi
if out="$(go build ./direct 2>&1)"; then
    echo "A module outside go.ate.dev/substrate imported internal/." >&2
    exit 1
fi
if ! grep -Fq "use of internal package go.ate.dev/substrate/internal/oidcjwt not allowed" <<<"${out}"; then
    echo "${out}" >&2
    echo "Building the direct internal/ import failed for an unexpected reason." >&2
    exit 1
fi
echo "Built from an external module: ${pkgs[*]}"
