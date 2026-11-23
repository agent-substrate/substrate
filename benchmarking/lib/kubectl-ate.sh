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

# shellcheck shell=bash
# Sourced by the benchmarking deploy scripts, which run from the repository root.

# build_kubectl_ate builds kubectl-ate from the tree into a temporary directory
# that is removed when the script exits, and points KUBECTL_ATE_BIN at it.
KUBECTL_ATE_BIN=""

build_kubectl_ate() {
  local dir
  dir="$(mktemp -d)"
  trap 'rm -rf '"${dir}" EXIT
  KUBECTL_ATE_BIN="${dir}/kubectl-ate"
  go build -o "${KUBECTL_ATE_BIN}" ./cmd/kubectl-ate
}
