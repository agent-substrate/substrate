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
#
# Preconditions:
#   IP_FAMILY=ipv4|dual|ipv6 hack/create-kind-cluster.sh
#   hack/install-ate-kind.sh --deploy-ate-system
#
# Checks that both atenet gateways bind and serve on every IP family the
# cluster has. The assertions are read off the cluster, so one run is valid on
# all three families -- worth doing on each, because they fail differently: on
# dual-stack a v4-wildcard bind is a silent data-path gap (the Service has an
# IPv6 ClusterIP nothing listens on), on IPv6-only it is a crashloop, because
# the kubelet probes the pod on its only address.
set -o errexit -o nounset -o pipefail
ROOT="$(git rev-parse --show-toplevel)"; cd "${ROOT}"

CTX="${KUBECTL_CONTEXT:-kind-kind}"
K="kubectl --context ${CTX}"
NS="${NS:-ate-system}"
PROBE_IMAGE="${PROBE_IMAGE:-busybox:1.36}"
PROBE_POD="atenet-ipfamily-probe"

fails=0
pass() { echo "  PASS  $*"; }
fail() { echo "  FAIL  $*"; fails=$((fails + 1)); }

# Here-string, never `... | grep -q`: under pipefail, -q exits at the first
# match and SIGPIPEs the writer, sinking the pipeline, so a match reads as a
# failure -- but only above roughly 100K of input, which looks like a flake.
contains() { grep -q -- "$2" <<<"$1"; }

# The envoy image ships neither curl nor wget, but it has bash, so /dev/tcp is
# the way in -- HTTP/1.1, because the admin listener answers 1.0 with 426.
admin_get() { # deploy loopback port path
  ${K} -n "${NS}" exec "deploy/$1" -c envoy -- bash -c \
    "exec 3<>/dev/tcp/$2/$3; printf 'GET $4 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n' >&3; cat <&3" \
    2>/dev/null | tr -d '\r'
}

echo "== cluster IP families =="
# The Service families are the ground truth for what to assert. PreferDualStack
# resolves to whatever the cluster supports, so this reads back one entry on a
# single-stack cluster and two on dual-stack, in cluster-primary order.
families="$(${K} -n "${NS}" get svc atenet-router -o jsonpath='{.spec.ipFamilies}' \
  | tr -d '[]"' | tr ',' ' ')"
[ -n "${families}" ] || { echo "!! atenet-router Service not found in ${NS}"; exit 1; }
echo "  cluster serves: ${families}"

echo "== gateway pods are up, and stayed up =="
# Restarts and readiness, not phase: a startup-probe refusal leaves the pod
# Running and kills it on a timer, so phase alone reads as healthy.
for app in atenet-router atenet-egress; do
  # Every pod, not .items[0]: mid-rollout the old healthy pod and the new
  # broken one coexist, and whichever sorts first would decide the verdict.
  pods="$(${K} -n "${NS}" get pod -l "app=${app}" -o jsonpath='{.items[*].metadata.name}')"
  [ -n "${pods}" ] || { fail "${app}: no pods"; continue; }
  for pod in ${pods}; do
    # A draining pod from the previous ReplicaSet is not a verdict on this one.
    if [ -n "$(${K} -n "${NS}" get pod "${pod}" -o jsonpath='{.metadata.deletionTimestamp}')" ]; then
      echo "  ....  ${pod}: terminating, skipped"
      continue
    fi
    status="$(${K} -n "${NS}" get pod "${pod}" \
      -o jsonpath='{.status.phase} {range .status.containerStatuses[*]}{.name}={.ready}/{.restartCount} {end}')"
    bad="$(grep -c false <<<"$(${K} -n "${NS}" get pod "${pod}" \
      -o jsonpath='{.status.containerStatuses[*].ready}')" || true)"
    restarts="$(awk '{s+=$1} END {print s+0}' <<<"$(${K} -n "${NS}" get pod "${pod}" \
      -o jsonpath='{range .status.containerStatuses[*]}{.restartCount}{"\n"}{end}')")"
    if [ "${bad}" -eq 0 ] && [ "${restarts}" -eq 0 ]; then
      pass "${pod}: ${status}"
    else
      fail "${pod}: ${status}"
      # The two lines that identify this failure: what Envoy bound, and what
      # the kubelet dialed. Dumping the Envoy log instead buries both.
      ${K} -n "${NS}" logs "${pod}" --all-containers --tail=-1 2>/dev/null \
        | grep "admin address" | sed 's/^/        envoy: /' || true
      ${K} -n "${NS}" get event --field-selector "involvedObject.name=${pod}" \
        -o jsonpath='{range .items[?(@.reason=="Unhealthy")]}{.message}{"\n"}{end}' 2>/dev/null \
        | sort -u | head -3 | sed 's/^/        kubelet: /' || true
    fi
  done
done

echo "== admin sockets bound on :: (from the Envoy startup log) =="
for app in atenet-router:9901 atenet-egress:15000; do
  a="${app%:*}"; port="${app#*:}"
  # --all-containers: the router's Envoy is a child of the atenet-router
  # process, so its startup log lands in that container, not in "envoy".
  logs="$(${K} -n "${NS}" logs -l "app=${a}" --all-containers --tail=-1 2>/dev/null || true)"
  if contains "${logs}" "admin address: \[::\]:${port}"; then
    pass "${a}: admin address: [::]:${port}"
  else
    fail "${a}: no '[::]:${port}' admin line (v4-wildcard bind, or Envoy never started)"
  fi
done

echo "== IPv4 loopback still serves the admin socket =="
# What ipv4_compat buys, and the only check that catches its loss: the router's
# health check (dataplane.go) and the egress drainer (--envoy-admin-address)
# dial 127.0.0.1, as literals in Go code that no manifest edit will follow.
for app in atenet-router:9901 atenet-egress:15000; do
  a="${app%:*}"; port="${app#*:}"
  for loop in 127.0.0.1 ::1; do
    # || true: a refused connection is a result to report, not a reason to die.
    line="$(admin_get "${a}" "${loop}" "${port}" /ready | head -1 || true)"
    case "${line}" in
      "HTTP/1.1 200 OK") pass "${a}: ${loop}:${port}/ready -> 200" ;;
      "") fail "${a}: ${loop}:${port}/ready -> no answer (socket not bound for this family)" ;;
      *) fail "${a}: ${loop}:${port}/ready -> ${line}" ;;
    esac
  done
done

echo "== ipv4_compat, read from /config_dump =="
# Never from /listeners: it reports resolved bound addresses and never emits
# the flag, so a check written against it passes on a manifest that lost it.
for app in atenet-router:9901:1 atenet-egress:15000:2; do
  a="$(echo "${app}" | cut -d: -f1)"
  port="$(echo "${app}" | cut -d: -f2)"
  want="$(echo "${app}" | cut -d: -f3)"
  got="$(grep -c '"ipv4_compat": true' <<<"$(admin_get "${a}" 127.0.0.1 "${port}" /config_dump)" || true)"
  # Router: the admin socket only. Egress: admin plus the :443 listener, which
  # Envoy reports twice (bootstrap and static views), so the floor is 2.
  if [ "${got}" -ge "${want}" ]; then
    pass "${a}: ${got} ipv4_compat socket(s) in /config_dump (want >= ${want})"
  else
    fail "${a}: ${got} ipv4_compat socket(s) in /config_dump, want >= ${want}"
  fi
done

echo "== ingress listeners keep an IPv4 primary and gain a :: address =="
# The asymmetry is deliberate and easy to "clean up" into a regression: these
# are two sockets, so the v4 one stays as-is and ipv4_compat must be false on
# the v6 one. The admin sockets above are one socket, so they need the flag.
rdump="$(admin_get atenet-router 127.0.0.1 9901 /config_dump)"
for l in ingress_http_listener:8080 ingress_https_listener:8443; do
  name="${l%:*}"; port="${l#*:}"
  block="$(sed -n "/\"name\": \"${name}\"/,/last_updated/p" <<<"${rdump}")"
  prim=0; addl=0
  contains "$(grep -A2 '"address": "0.0.0.0"' <<<"${block}" || true)" "\"port_value\": ${port}" && prim=1
  contains "$(sed -n '/additional_addresses/,/]/p' <<<"${block}")" '"address": "::"' && addl=1
  if [ "${prim}" = 1 ] && [ "${addl}" = 1 ]; then
    pass "${name}: 0.0.0.0:${port} primary + :: additional"
  else
    fail "${name}: primary-0.0.0.0=${prim} additional-::=${addl}"
  fi
done

echo "== a pod can reach each gateway on every family the cluster has =="
# Blocking delete: a --wait=false here races its own re-create and the run
# fails with AlreadyExists on any second invocation.
${K} -n "${NS}" delete pod "${PROBE_POD}" --ignore-not-found --timeout=60s >/dev/null 2>&1 || true
${K} -n "${NS}" run "${PROBE_POD}" --image="${PROBE_IMAGE}" --restart=Never \
  --command -- sleep 600 >/dev/null
trap '${K} -n "${NS}" delete pod "${PROBE_POD}" --ignore-not-found --wait=false >/dev/null 2>&1 || true' EXIT
${K} -n "${NS}" wait --for=condition=Ready "pod/${PROBE_POD}" --timeout=120s >/dev/null

# One ClusterIP per family, in the same order as .spec.ipFamilies.
for app in atenet-router:80 atenet-egress:443; do
  a="${app%:*}"; port="${app#*:}"
  ips="$(${K} -n "${NS}" get svc "${a}" -o jsonpath='{.spec.clusterIPs}' | tr -d '[]"' | tr ',' ' ')"
  i=0
  for ip in ${ips}; do
    fam="$(echo "${families}" | cut -d' ' -f$((i + 1)))"; i=$((i + 1))
    # Bracket v6 literals for the URL; a bare colon would read as a port.
    case "${ip}" in *:*) target="[${ip}]";; *) target="${ip}";; esac
    # Connect, don't request: the router 404s without a Host header and egress
    # :443 speaks mTLS CONNECT, so neither answers 200 when it is working.
    if ${K} -n "${NS}" exec "${PROBE_POD}" -- \
      timeout 10 nc -z "${ip}" "${port}" >/dev/null 2>&1; then
      pass "${a} ${fam} ${target}:${port} accepts connections"
    else
      fail "${a} ${fam} ${target}:${port} refused -- Service has the IP, nothing listens on it"
    fi
  done
  [ "${i}" -eq "$(echo "${families}" | wc -w | tr -d ' ')" ] \
    || fail "${a}: ${i} ClusterIP(s) for $(echo "${families}" | wc -w | tr -d ' ') famil(ies) -- ipFamilyPolicy not PreferDualStack?"
done

echo
if [ "${fails}" -eq 0 ]; then
  echo "== PASS: both gateways bind and serve on ${families} =="
else
  echo "== FAIL: ${fails} check(s) failed =="
  exit 1
fi
