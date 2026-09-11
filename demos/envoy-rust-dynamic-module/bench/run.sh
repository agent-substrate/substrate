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
#
# Runs the three arms back to back and reports latency plus the number of
# control-plane calls each one caused.
#
# The load generator runs as a container on the demo's own network rather than
# on the host: the host port forward would add the same overhead to every arm
# and compress the differences being measured.
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION="${DURATION:-20s}"
WARMUP="${WARMUP:-5s}"
CONCURRENCY="${CONCURRENCY:-32}"
ACTORS="${ACTORS:-50}"
NETWORK="ate-rust-dynmod_atenet"
STATS="http://127.0.0.1:21088"

run_arm() {
  local label="$1" target="$2"
  curl -s -X POST "$STATS/stats/reset" >/dev/null
  docker run --rm --network "$NETWORK" -v "$(pwd)/bin:/bin/ate:ro" \
    debian:bookworm-slim \
    /bin/ate/loadgen --target "http://$target" --label "$label" \
    --actors "$ACTORS" --concurrency "$CONCURRENCY" \
    --duration "$DURATION" --warmup "$WARMUP"
  printf '   control-plane calls: %s\n\n' "$(curl -s "$STATS/stats")"
}

echo "duration=$DURATION warmup=$WARMUP concurrency=$CONCURRENCY hot actors=$ACTORS"
echo
run_arm "A  ext_proc (Go)"   "envoy-baseline:8080"
run_arm "B1 rust, no cache"  "envoy-dynmod:8082"
run_arm "B2 rust, cached"    "envoy-dynmod:8080"
run_arm "C  rust + ext_proc" "envoy-coexist:8080"
