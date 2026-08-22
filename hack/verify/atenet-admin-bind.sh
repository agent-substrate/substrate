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

# Dropping ipv4_compat from a gateway's Envoy admin socket fails silently: the
# drain sequence reads the refused IPv4 loopback dial as "Envoy already exited"
# and reports a drain it never performed. No Go test reads these manifests.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

rc=0
for f in manifests/ate-install/atenet-router.yaml manifests/ate-install/atenet-egress.yaml; do
  block="$(grep -A 6 -E '^ *admin:$' "${f}" || true)"
  if [[ -z "${block}" ]]; then
    echo "${f}: no Envoy admin block found; this check needs updating" >&2
    rc=1
  elif ! grep -q '"::"' <<<"${block}"; then
    echo "${f}: Envoy admin socket does not bind \"::\"; an IPv6-primary pod cannot be probed" >&2
    rc=1
  elif ! grep -q 'ipv4_compat: true' <<<"${block}"; then
    echo "${f}: Envoy admin socket binds \"::\" without ipv4_compat; IPv4 loopback dials will be refused" >&2
    rc=1
  fi
done

exit "${rc}"
