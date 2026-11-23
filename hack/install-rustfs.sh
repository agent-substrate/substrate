#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
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

# Deploy the development RustFS in ate-system for Helm, with its ate-snapshots
# bucket and the rustfs-credentials Secret for the object-store plugins.
set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [[ $# -ne 0 ]]; then
  echo "Usage: $0" >&2
  echo "Deploy development RustFS in ate-system for Helm." >&2
  echo "KUBECTL_CONTEXT optionally selects the target cluster." >&2
  exit 1
fi

KUBECTL=(kubectl)
if [[ -n "${KUBECTL_CONTEXT:-}" ]]; then
  KUBECTL+=(--context "${KUBECTL_CONTEXT}")
fi
"${KUBECTL[@]}" create namespace ate-system --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
# The access keys match the ones in rustfs.yaml.
"${KUBECTL[@]}" -n ate-system create secret generic rustfs-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=rustfsadmin \
  --from-literal=AWS_SECRET_ACCESS_KEY=rustfsadmin \
  --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
"${KUBECTL[@]}" apply -f "${ROOT}/manifests/ate-install/kind/rustfs.yaml"
"${KUBECTL[@]}" -n ate-system rollout status deployment/rustfs --timeout=5m
"${KUBECTL[@]}" -n ate-system wait job/rustfs-bucket-init --for=condition=Complete --timeout=5m
