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
# Checks the CoreDNS rewrite that create-kind-cluster.sh applies to IPv6-only
# clusters. The offline half needs neither Docker nor a cluster; the live half
# needs `IP_FAMILY=ipv6 hack/create-kind-cluster.sh`.

set -o errexit -o nounset -o pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "${ROOT}"
source "${ROOT}/hack/util/coredns-ipv6.sh"

CTX="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME:-kind}}"
REG_NAME="${REG_NAME:-kind-registry}"
UPSTREAM="${IPV6_DNS_UPSTREAM:-2001:4860:4860::8888 2001:4860:4860::8844}"
OFFLINE_ONLY=false

if [[ $# -gt 0 ]]; then
  case "$1" in
    --offline) OFFLINE_ONLY=true ;;
    -h|--help)
      echo "Usage: $0 [--offline]"
      echo "Verifies the IPv6-only CoreDNS rewrite. Offline checks always run;"
      echo "live checks need a reachable cluster and are skipped without one."
      echo
      echo "  --offline          Skip the live checks entirely."
      echo "  KUBECTL_CONTEXT    Context to check (default: kind-\${KIND_CLUSTER_NAME:-kind})."
      exit 0
      ;;
    *) echo "error: unknown argument '$1'; see --help" >&2; exit 1 ;;
  esac
fi

passed=0
failed=0
ok()  { printf '  ok    %s\n' "$1"; passed=$((passed + 1)); }
bad() { printf '  FAIL  %s\n' "$1"; failed=$((failed + 1)); }

# want <description> <needle> <haystack>
want() {
  if [[ "$3" == *"$2"* ]]; then ok "$1"; else bad "$1 -- expected to find '$2'"; fi
}
# reject <description> <needle> <haystack>
reject() {
  if [[ "$3" != *"$2"* ]]; then ok "$1"; else bad "$1 -- expected NOT to find '$2'"; fi
}

# hosts_block <registry-address> -- the exact block the transform must produce
hosts_block() {
  printf 'hosts {\n       %s %s\n       fallthrough\n    }' "$1" "${REG_NAME}"
}

# A snapshot of what kind installs, so the offline checks run with no cluster.
# The live checks below are what catch it drifting.
STOCK_COREFILE='.:53 {
    errors
    health {
       lameduck 5s
    }
    ready
    kubernetes cluster.local in-addr.arpa ip6.arpa {
       pods insecure
       fallthrough in-addr.arpa ip6.arpa
       ttl 30
    }
    prometheus :9153
    forward . /etc/resolv.conf {
       max_concurrent 1000
    }
    cache 30 {
       disable success cluster.local
       disable denial cluster.local
    }
    loop
    reload
    loadbalance
}'

echo "== offline: the Corefile transform =="
if patched="$(coredns_ipv6_corefile "${STOCK_COREFILE}" "fc00::3" "${REG_NAME}" "${UPSTREAM}")"; then
  ok "transform applies to kind's default Corefile"
  # Matched whole: "fallthrough" alone also appears in the kubernetes plugin.
  want   "hosts block is complete"           "$(hosts_block fc00::3)" "${patched}"
  want   "forwarder points at the upstream"  "forward . ${UPSTREAM}" "${patched}"
  reject "resolv.conf forwarder is gone"     "/etc/resolv.conf"    "${patched}"
  want   "max_concurrent survives"           "max_concurrent 1000" "${patched}"
  want   "the kubernetes plugin is untouched" "kubernetes cluster.local in-addr.arpa ip6.arpa" "${patched}"
else
  bad "transform applies to kind's default Corefile"
  patched=""
fi

custom="$(coredns_ipv6_corefile "${STOCK_COREFILE}" "fc00::3" "${REG_NAME}" "2001:db8::1")"
want "IPV6_DNS_UPSTREAM is honoured" "forward . 2001:db8::1" "${custom}"

echo "== offline: controls that must fail =="
no_search="${STOCK_COREFILE/forward . \/etc\/resolv.conf/forward . 8.8.8.8}"
if coredns_ipv6_corefile "${no_search}" "fc00::3" "${REG_NAME}" "${UPSTREAM}" >/dev/null 2>&1; then
  bad "control: an unrecognised Corefile is rejected"
else
  ok "control: an unrecognised Corefile is rejected"
fi

if [[ -n "${patched}" ]] &&
   coredns_ipv6_corefile "${patched}" "fc00::3" "${REG_NAME}" "${UPSTREAM}" >/dev/null 2>&1; then
  bad "control: re-patching an already-patched Corefile is rejected"
else
  ok "control: re-patching an already-patched Corefile is rejected"
fi

if [[ "${OFFLINE_ONLY}" == "true" ]]; then
  echo
  echo "${passed} passed, ${failed} failed (live checks skipped)"
  [[ "${failed}" -eq 0 ]]
  exit
fi

echo "== live: cluster '${CTX}' =="
K=(kubectl --context "${CTX}")
if ! "${K[@]}" version --request-timeout=10s >/dev/null 2>&1; then
  echo "  skip  no reachable cluster at '${CTX}'; run with --offline to silence this"
  echo
  echo "${passed} passed, ${failed} failed"
  [[ "${failed}" -eq 0 ]]
  exit
fi

cidrs="$("${K[@]}" get nodes -o jsonpath='{.items[0].spec.podCIDRs}')"
live_corefile="$("${K[@]}" -n kube-system get cm coredns -o jsonpath='{.data.Corefile}')"

if [[ "${cidrs}" != *":"* ]]; then
  echo "  (IPv4 cluster -- asserting the rewrite did NOT run)"
  want   "resolv.conf forwarder is intact" "forward . /etc/resolv.conf" "${live_corefile}"
  reject "no hosts block was added"        "hosts {"                    "${live_corefile}"
elif [[ "${cidrs}" == *"."* ]]; then
  echo "  (dual-stack cluster -- asserting the rewrite did NOT run)"
  want   "resolv.conf forwarder is intact" "forward . /etc/resolv.conf" "${live_corefile}"
  reject "no hosts block was added"        "hosts {"                    "${live_corefile}"
else
  echo "  (IPv6-only cluster -- asserting the rewrite ran and works)"
  recorded="$(printf '%s\n' "${live_corefile}" |
    awk '/hosts \{/{h=1} h && /'"${REG_NAME}"'/{print $1; exit}')"
  if [[ -z "${recorded}" ]]; then
    bad "hosts block names ${REG_NAME}"
    recorded="<absent>"
  else
    want "hosts block is complete" "$(hosts_block "${recorded}")" "${live_corefile}"
  fi

  # The hosts entry is a point-in-time snapshot. Recreating the registry moves
  # its address and every older cluster then resolves the name to a dead one --
  # connection refused rather than NXDOMAIN, which reads like a registry outage.
  if ! command -v docker >/dev/null 2>&1; then
    echo "  skip  docker is not on PATH; cannot check the recorded registry address"
  elif ! actual="$(docker inspect "${REG_NAME}" \
      --format '{{.NetworkSettings.Networks.kind.GlobalIPv6Address}}' 2>/dev/null)" ||
       [[ -z "${actual}" ]]; then
    bad "registry '${REG_NAME}' is gone or off the 'kind' network; Corefile still points at ${recorded}"
  elif [[ "${recorded}" == "${actual}" ]]; then
    ok "recorded registry address is current (${actual})"
  else
    bad "recorded registry address is stale: Corefile says ${recorded}, registry is at ${actual}"
  fi

  echo "  (probing from a pod, not the node -- the node is dual-stack)"
  if "${K[@]}" run "verify-ipv6-dns-$$" --rm --attach --quiet --restart=Never \
      --image=busybox:1.36 --command -- \
      sh -c "nslookup storage.googleapis.com >/dev/null" >/dev/null 2>&1; then
    ok "a pod resolves an external name"
  else
    bad "a pod resolves an external name -- IPV6_DNS_UPSTREAM is '${UPSTREAM}'"
  fi

  # Fetched, not resolved: the hosts entry is AAAA-only, so nslookup fails on
  # its A query even though getaddrinfo -- containerd, atelet -- is satisfied.
  if "${K[@]}" run "verify-ipv6-reg-$$" --rm --attach --quiet --restart=Never \
      --image=busybox:1.36 --command -- \
      sh -c "wget -q -T10 -O/dev/null http://${REG_NAME}:5000/v2/" >/dev/null 2>&1; then
    ok "a pod reaches the registry by name"
  else
    bad "a pod reaches the registry by name"
  fi

  # The kubernetes plugin only sees this query if the hosts block declines it.
  if "${K[@]}" run "verify-ipv6-svc-$$" --rm --attach --quiet --restart=Never \
      --image=busybox:1.36 --command -- \
      sh -c "nslookup kubernetes.default.svc.cluster.local >/dev/null" >/dev/null 2>&1; then
    ok "a pod resolves an in-cluster name (fallthrough works)"
  else
    bad "a pod resolves an in-cluster name (fallthrough works)"
  fi
fi

echo
echo "${passed} passed, ${failed} failed"
[[ "${failed}" -eq 0 ]]
