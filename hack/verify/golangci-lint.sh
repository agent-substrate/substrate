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

# Resolve the tool path under the host's native GOOS so the binary is
# executable on the developer's machine, then invoke it with GOOS=linux so
# the linter analyzes the code the same way it would on the Linux CI
# runners. Without the GOOS=linux override, platform-gated packages
# (notably the netlink bindings used by ateom-gvisor) fail to typecheck on
# macOS and golangci-lint's typecheck fail-stop suppresses all other
# findings in the affected files.
BIN="$("${ROOT}"/hack/run-tool.sh --print-bin-path golangci-lint)"
exec env GOOS=linux "${BIN}" run ./... | { grep -v '^0 issues.$' || true; }
