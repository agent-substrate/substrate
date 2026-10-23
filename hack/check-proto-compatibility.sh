#!/usr/bin/env bash

# Copyright 2026 Google LLC
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

set -o errexit -o nounset -o pipefail

cd "$(git rev-parse --show-toplevel)"

# Archive the proto directories instead of cloning vendored dependencies too.
git archive "${1:?Usage: $0 <base-ref>}" api internal/proto |
  buf breaking --against '-#format=tar' --against-config buf.yaml --error-format=github-actions
