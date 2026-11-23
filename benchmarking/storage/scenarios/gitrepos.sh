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

# Git repository scenario: one actor clones REPO (kubernetes or linux) with
# full history from the git mirror, checks out an older release, edits and
# commits 10 files and runs git grep, with a suspend and resume between steps
# (gitrepos.steps). It runs wakestorm from this machine.
#
# Usage: gitrepos.sh <out_dir> <kubernetes|linux>
# Env: CHECK_STAT (default default; set minimal for core.checkStat=minimal),
#   TEMPLATE (default coding-git), ATESPACE (default benchmark),
#   CHECKOUT_WORKERS (default 20), IDLE (default 30s).

set -o errexit -o nounset -o pipefail

out="${1:?usage: gitrepos.sh <out_dir> <kubernetes|linux>}"
repo="${2:?usage: gitrepos.sh <out_dir> <kubernetes|linux>}"
S="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(git rev-parse --show-toplevel)"
mirror=git-mirror.benchmarking.svc.cluster.local

case "${repo}" in
  kubernetes) old_ref=origin/release-1.28 glob='*.go' ;;
  linux) old_ref=v6.6 glob='*.c' ;;
  *) echo "repo must be kubernetes or linux, got ${repo}" >&2; exit 1 ;;
esac

git_config=(-env GIT_CONFIG_KEY_0=checkout.workers -env "GIT_CONFIG_VALUE_0=${CHECKOUT_WORKERS:-20}")
if [[ "${CHECK_STAT:-default}" == minimal ]]; then
  git_config+=(-env GIT_CONFIG_COUNT=2 -env GIT_CONFIG_KEY_1=core.checkStat -env GIT_CONFIG_VALUE_1=minimal)
else
  git_config+=(-env GIT_CONFIG_COUNT=1)
fi

mkdir -p "${out}"
# The ate-client token wakestorm mints by default lasts 1 hour, and a run can
# take longer.
token="$(mktemp)"
trap 'rm -f "${token}"' EXIT
kubectl create token ate-client -n ate-system --audience api.ate-system.svc --duration 6h > "${token}"
go run "${ROOT}/benchmarking/storage/cmd/wakestorm" -phase script -n 1 -prefix "git-${repo}" \
  -template "${TEMPLATE:-coding-git}" -atespace "${ATESPACE:-benchmark}" \
  -script "${S}/gitrepos.steps" -idle "${IDLE:-30s}" -allow-http "${mirror}:80" -retry-contention \
  -env "MIRROR=http://${mirror}" -env "REPO=${repo}" -env "OLD_REF=${old_ref}" -env "SRC_GLOB=${glob}" \
  "${git_config[@]}" -token-file "${token}" -out "${out}"
