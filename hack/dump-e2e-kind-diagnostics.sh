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

set -o nounset -o pipefail

KUBECTL_CONTEXT="kind-kind"
KIND_NODE="kind-control-plane"

kubectl --context "${KUBECTL_CONTEXT}" get workerpool,pods -A -o wide || true

echo "=== node ==="
kubectl --context "${KUBECTL_CONTEXT}" describe node "${KIND_NODE}" 2>/dev/null |
  sed -n '/^Conditions:/,/^Addresses:/p' || true

# An OOM kill and a SIGKILL from a canceled context both surface as
# `signal: killed`; only a dmesg record confirms the former.
echo "=== dmesg: OOM (kind node) ==="
docker exec "${KIND_NODE}" sh -c \
  "dmesg | grep -Ei 'oom|out of memory|killed process|memory cgroup' | tail -50" \
  2>/dev/null || true
echo "=== dmesg: tail (kind node) ==="
docker exec "${KIND_NODE}" sh -c 'dmesg | tail -50' 2>/dev/null || true

echo "=== events ==="
kubectl --context "${KUBECTL_CONTEXT}" get events -A --sort-by=.lastTimestamp 2>/dev/null |
  tail -60 || true

dump() {
  local namespace="$1"
  local pod="$2"
  shift 2

  echo "=== logs: ${namespace}/${pod} $* ==="
  kubectl --context "${KUBECTL_CONTEXT}" logs -n "${namespace}" "${pod}" \
    --all-containers "$@" 2>/dev/null || true
}

while read -r pod; do
  [[ -n "${pod}" ]] || continue
  dump ate-system "${pod}" --tail=300
done < <(kubectl --context "${KUBECTL_CONTEXT}" get pods -n ate-system -o name 2>/dev/null)

# Failed suites retain their namespaces, so worker pods contain the actor's
# ateom logs and, for micro-VM workers, the guest console tail.
while read -r namespace pod; do
  [[ -n "${namespace}" && -n "${pod}" ]] || continue
  dump "${namespace}" "${pod}"
  dump "${namespace}" "${pod}" --previous
  echo "=== describe: ${namespace}/${pod} ==="
  kubectl --context "${KUBECTL_CONTEXT}" describe pod -n "${namespace}" "${pod}" \
    2>/dev/null || true
done < <(
  kubectl --context "${KUBECTL_CONTEXT}" get pods -A -l ate.dev/worker-pool \
    -o 'custom-columns=:.metadata.namespace,:.metadata.name' --no-headers 2>/dev/null
)
