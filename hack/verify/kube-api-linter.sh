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

# Lint the Kubernetes API types against the upstream API conventions.
# Same GOOS=linux override as golangci-lint.sh: analyze the code the same
# way the Linux CI runners do.
BIN="$("${ROOT}"/hack/run-tool.sh --print-bin-path golangci-lint-kube-api-linter)"
exec env GOOS=linux "${BIN}" run --config .golangci-kal.yaml ./pkg/api/... | { grep -v '^0 issues.$' || true; }
