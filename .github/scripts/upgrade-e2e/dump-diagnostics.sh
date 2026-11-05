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

# Prints the state of the kind cluster after a failed upgrade: the worker
# pools and pods, the system workloads, and the logs of the system pods and of
# every worker pod. The actors' worker logs say what happened to each actor.
#
# Used by .github/workflows/upgrade-e2e.yaml. Every command is best effort.

set -o nounset -o pipefail

kubectl --context kind-kind get workerpool,pods -A -o wide || true
kubectl --context kind-kind get ds,deploy -n ate-system -o wide || true
dump() {
  echo "=== logs: $1/$2 ==="
  kubectl --context kind-kind logs -n "$1" "$2" --all-containers --tail=300 2>/dev/null || true
}
for ns in ate-system podcertificate-controller-system; do
  for p in $(kubectl --context kind-kind get pods -n "${ns}" -o name 2>/dev/null); do
    dump "${ns}" "${p}"
  done
done
kubectl --context kind-kind get pods -A -l ate.dev/worker-pool \
  -o 'custom-columns=:.metadata.namespace,:.metadata.name' --no-headers 2>/dev/null \
  | while read -r ns name; do dump "${ns}" "${name}"; done
