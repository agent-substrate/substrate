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

# Generate nighthawk.desc to avoid putting a large number of
# dependencies into the repository for running the benchmark.
#
# Source trees are cloned by the Dockerfile protogen stage:
#   /src/nighthawk   envoyproxy/nighthawk  (pinned commit)
#   /src/envoy       envoyproxy/envoy      (nighthawk's ENVOY_COMMIT pin)
#   /src/xds         cncf/xds              (xds/ + udpa/ import roots)
#   /src/pgv         bufbuild/protoc-gen-validate
#   /src/googleapis  googleapis/googleapis
set -euo pipefail

OUT=${1:-/out/nighthawk.desc}
mkdir -p "$(dirname "$OUT")"

cd /src/nighthawk

# shellcheck disable=SC2046  # word-splitting of the glob expansions is intended
python3 -m grpc_tools.protoc \
  --include_imports \
  --descriptor_set_out="$OUT" \
  -I/src/nighthawk \
  -I/src/envoy/api \
  -I/src/xds \
  -I/src/pgv \
  -I/src/googleapis \
  api/adaptive_load/*.proto \
  api/client/*.proto \
  api/request_source/*.proto

echo "wrote $OUT ($(wc -c <"$OUT") bytes)"
