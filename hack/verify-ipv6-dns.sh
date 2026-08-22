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
# One stream carries every leg and only the last one's exit status, so each leg
# reports a marker on stdout and no failure message may contain one; PROBE_RAN
# and PROBE_DONE bracket the run so a short read is told apart from a leg that
# failed. Read the log once the pod has terminated rather than attaching to it:
# an attach can drop the tail, and a lost registry marker then reads as an
# unreachable registry. Retry the pod, not the query: one that asks before
# CoreDNS settles stays broken for ~30s, while a fresh pod 10s later resolves
# first try.
probe=""
probe_max=4
for ((probe_attempt = 1; probe_attempt <= probe_max; probe_attempt++)); do
  probe_pod="coredns-probe-$$-${probe_attempt}"
  kubectl --context="${KUBECTL_CONTEXT}" run "${probe_pod}" \
    --restart=Never --image=busybox:1.36 --command -- \
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
           fi
           echo PROBE_DONE" >/dev/null || true
  for ((probe_wait = 0; probe_wait < 120; probe_wait++)); do
    phase="$(kubectl --context="${KUBECTL_CONTEXT}" get pod "${probe_pod}" \
      -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [[ "${phase}" == "Succeeded" || "${phase}" == "Failed" ]] && break
    sleep 1
  done
  attempt_out="$(kubectl --context="${KUBECTL_CONTEXT}" logs "${probe_pod}" 2>/dev/null || true)"
  kubectl --context="${KUBECTL_CONTEXT}" delete pod "${probe_pod}" \
    --now --ignore-not-found --wait=false >/dev/null 2>&1 || true
  # A pod that never started must not bury an earlier one's real failure.
  if [[ "${attempt_out}" == *PROBE_RAN* ]]; then probe="${attempt_out}"; fi
  # Only the resolve leg is a settling race; a down registry will not fix
  # itself, so a finished probe is a verdict either way. An unfinished one
  # reported no registry result at all, which is not the same as a failure.
  if [[ "${probe}" == *RESOLVE_OK* ]] &&
    [[ "${probe}" == *REGISTRY_OK* || "${probe}" == *PROBE_DONE* ]]; then
    break
  fi
  if ((probe_attempt < probe_max)); then
    echo "  the probe did not come back clean; re-probing (attempt $((probe_attempt + 1)) of ${probe_max})..."
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
  elif [[ "${probe}" != *PROBE_DONE* ]]; then
    echo "error: the probe stopped early, so the registry leg is unverified" >&2
    echo "       re-run this script; DNS itself answered" >&2
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
