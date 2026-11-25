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

# Build scenario: ACTORS actors at once, each cloning the Linux kernel from the
# git mirror, building it, editing 10 files and building again, with a suspend
# and resume between steps (build.steps). It runs wakestorm from this machine.
#
# Usage: build.sh <out_dir> [actors]
# Env: TEMPLATE (default coding-build), ATESPACE (default benchmark),
#   CHECKOUT_WORKERS (default 20), JOBS (default 3), IDLE (default 60s).

set -o errexit -o nounset -o pipefail

out="${1:?usage: build.sh <out_dir> [actors]}"
actors="${2:-1}"
S="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(git rev-parse --show-toplevel)"
mirror=git-mirror.benchmarking.svc.cluster.local

mkdir -p "${out}"
# The ate-client token wakestorm mints by default lasts 1 hour, and a run can
# take longer.
token="$(mktemp)"
trap 'rm -f "${token}"' EXIT
kubectl create token ate-client -n ate-system --audience api.ate-system.svc --duration 6h > "${token}"
go run "${ROOT}/benchmarking/storage/cmd/wakestorm" -phase script -n "${actors}" -prefix build \
  -template "${TEMPLATE:-coding-build}" -atespace "${ATESPACE:-benchmark}" \
  -script "${S}/build.steps" -idle "${IDLE:-60s}" -allow-http "${mirror}:80" -retry-contention \
  -env "MIRROR=http://${mirror}" -env "JOBS=${JOBS:-3}" \
  -env GIT_CONFIG_COUNT=1 -env GIT_CONFIG_KEY_0=checkout.workers -env "GIT_CONFIG_VALUE_0=${CHECKOUT_WORKERS:-20}" \
  -token-file "${token}" -out "${out}"
