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
# Preconditions:
#   IP_FAMILY=ipv6 hack/create-kind-cluster.sh
#
# Checks the two things the CoreDNS rewrite in create-kind-cluster.sh provides:
# a pod can resolve an external name (a GCS one -- that is where atelet pulls
# sandbox tarballs from) and reach the local registry by name. That script runs
# this last; re-run it by hand after the registry moves (#1049).

set -o errexit -o nounset -o pipefail

KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME:-kind}}"
REG_NAME="${REG_NAME:-kind-registry}"
IPV6_DNS_UPSTREAM="${IPV6_DNS_UPSTREAM:-2001:4860:4860::8888 2001:4860:4860::8844}"

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      echo "Usage: $0"
      echo "Verifies pod DNS on the IPv6-only kind cluster '${KUBECTL_CONTEXT}'."
      echo
      echo "Configured through the environment:"
      echo "  KUBECTL_CONTEXT    Context to check (default: kind-\${KIND_CLUSTER_NAME:-kind})."
      echo "  REG_NAME           Name of the local registry container (default: kind-registry)."
      echo "  IPV6_DNS_UPSTREAM  The resolvers CoreDNS was pointed at; reported on failure only."
      exit 0
      ;;
    *)
      echo "error: unknown argument '$1'; see --help" >&2
      exit 1
      ;;
  esac
fi

# Best-effort: only used to make the registry failure message actionable.
reg_v6="$(docker inspect "${REG_NAME}" \
  --format '{{.NetworkSettings.Networks.kind.GlobalIPv6Address}}' 2>/dev/null || true)"
reg_at="${reg_v6:+ at [${reg_v6}]:5000}"

echo "Verifying DNS from a pod..."
# Probe from a pod, not the node: the node is dual-stack and passes either way.
# The registry leg fetches rather than resolves -- the hosts entry is AAAA-only,
# which fails nslookup's A query but satisfies getaddrinfo.
#
# --attach gives one stream and only the last leg's exit status, so each leg
# reports a marker on stdout and no failure message may contain one; PROBE_RAN
# separates a failed leg from a pod that never ran. Retry the pod, not the
# query: one that asks before CoreDNS settles stays broken for ~30s, while a
# fresh pod 10s later resolves first try.
probe=""
probe_max=4
for ((probe_attempt = 1; probe_attempt <= probe_max; probe_attempt++)); do
  attempt_out="$(kubectl --context="${KUBECTL_CONTEXT}" run "coredns-probe-$$-${probe_attempt}" \
    --rm --attach --quiet --restart=Never --image=busybox:1.36 --command -- \
    sh -c "echo PROBE_RAN
           if out=\$(nslookup storage.googleapis.com 2>&1); then
             echo RESOLVE_OK
           else
             echo \"resolve failed: \$(echo \"\$out\" | tail -2 | tr '\n' ' ')\"
           fi
           if out=\$(wget -T10 -O/dev/null http://${REG_NAME}:5000/v2/ 2>&1); then
             echo REGISTRY_OK
           else
             echo \"registry fetch failed: \$(echo \"\$out\" | tail -1)\"
           fi")" || true
  # A pod that never started must not bury an earlier one's real failure.
  if [[ "${attempt_out}" == *PROBE_RAN* ]]; then probe="${attempt_out}"; fi
  # Only the resolve leg is a settling race; a down registry will not fix itself.
  [[ "${probe}" == *RESOLVE_OK* ]] && break
  if ((probe_attempt < probe_max)); then
    echo "  the cluster is not resolving yet; re-probing (attempt $((probe_attempt + 1)) of ${probe_max})..."
    sleep 10
  fi
done
if [[ "${probe}" != *RESOLVE_OK* || "${probe}" != *REGISTRY_OK* ]]; then
  if [[ "${probe}" != *PROBE_RAN* ]]; then
    echo "error: the probe pod never ran, so CoreDNS is unverified" >&2
    echo "       check that it scheduled and that 'busybox:1.36' pulled" >&2
  elif [[ "${probe}" != *RESOLVE_OK* ]]; then
    echo "error: a pod cannot resolve an external name" >&2
    echo "       IPV6_DNS_UPSTREAM is '${IPV6_DNS_UPSTREAM}'; set it to a reachable resolver" >&2
  else
    echo "error: DNS works but a pod cannot reach '${REG_NAME}'${reg_at}" >&2
    echo "       check the registry container is up and on the 'kind' network" >&2
  fi
  if [[ -n "${probe}" ]]; then
    echo "       probe output was:" >&2
    printf '%s\n' "${probe}" | sed 's/^/         /' >&2
  fi
  exit 1
fi
