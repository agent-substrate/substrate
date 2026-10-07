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

# scales the other sandbox class's demo WorkerPools to zero and restores this
# class's, so an e2e lane has the kind node to itself
#
# Usage: hack/e2e-focus-sandbox-class.sh gvisor|microvm

set -o errexit -o nounset -o pipefail

context="${KUBECTL_CONTEXT:-kind-kind}"
annotation="e2e.ate.dev/replicas"

gvisor_pools=(ate-demo-counter/counter ate-demo-egress/egress)
microvm_pools=(ate-demo-counter-microvm/counter-microvm ate-demo-egress-microvm/egress-microvm)

case "${1:-}" in
    gvisor) keep=("${gvisor_pools[@]}"); drop=("${microvm_pools[@]}") ;;
    microvm) keep=("${microvm_pools[@]}"); drop=("${gvisor_pools[@]}") ;;
    *) echo "usage: $0 gvisor|microvm" >&2; exit 1 ;;
esac

# pool NAMESPACE/NAME VERB [ARGS...]
pool() {
    kubectl --context="${context}" -n "${1%/*}" "$2" "workerpool/${1#*/}" "${@:3}"
}

# stash the replica count in an annotation so we can restore it later
for p in "${drop[@]}"; do
    replicas="$(pool "$p" get -o jsonpath='{.spec.replicas}')"
    if [[ "${replicas}" != "0" ]]; then
        pool "$p" annotate --overwrite "${annotation}=${replicas}"
        pool "$p" scale --replicas=0
    fi
done

for p in "${keep[@]}"; do
    replicas="$(pool "$p" get -o jsonpath="{.metadata.annotations.${annotation//./\\.}}")"
    if [[ -n "${replicas}" ]]; then
        pool "$p" scale --replicas="${replicas}"
        pool "$p" annotate "${annotation}-"
    fi
    replicas="$(pool "$p" get -o jsonpath='{.spec.replicas}')"
    pool "$p" wait --for=jsonpath='{.status.readyReplicas}'="${replicas}" --timeout=5m
done
