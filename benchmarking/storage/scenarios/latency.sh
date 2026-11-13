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

# Latency scenario: one actor with an external volume runs `repeat` cycles of
# create, first resume, suspend, resume and delete, from the wakestorm-runner
# pod (run runner_setup.sh first). `repeat` defaults to 30. Prints P50, P90
# and max per call.
#
# Usage: latency.sh <out_dir> [repeat]
# Env: TEMPLATE (default glutton-storage).

set -o errexit -o nounset -o pipefail

out="${1:?usage: latency.sh <out_dir> [repeat]}"
repeat="${2:-30}"
S="$(cd "$(dirname "$0")" && pwd)"

"${S}/wsr.sh" "${out}" -phase lifecycle -n 1 -repeat "${repeat}" -prefix lat \
  -template "${TEMPLATE:-glutton-storage}" -settle 15s -retry-contention
