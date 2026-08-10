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

# Checks that a cluster created by hack/create-kind-cluster.sh got the address
# families it was asked for, and that node IPs, ClusterIPs, pod IPs, pod-to-pod,
# CoreDNS and registry pulls all work on them.
#
# Whatever is genuinely per-family is checked in *every* family the cluster was
# asked for, not just the primary one. On "dual" the primary is IPv4, so a
# primary-only check passes with IPv6 pod networking entirely dead. What stays
# primary-only is what Kubernetes only ever gives you once: status.podIP is
# defined as podIPs[0], and the "kubernetes" Service is SingleStack.
#
# Usage: KIND_IP_FAMILY=ipv6 hack/verify-kind-networking.sh
#
# KIND_IP_FAMILY must match what the cluster was created with (default ipv4);
# KIND_CLUSTER_NAME (default kind) picks the cluster, as in create-kind-cluster.sh.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
KIND_IP_FAMILY="${KIND_IP_FAMILY:-ipv4}"
KUBECTL_CONTEXT="kind-${KIND_CLUSTER_NAME}"
NS="kind-net-check"
# Pulled through the local registry to exercise the node -> registry path.
REG_IMAGE="localhost:5001/kind-net-check/busybox:1"

case "${KIND_IP_FAMILY}" in
  ipv4) want_families=(ipv4); primary="ipv4" ;;
  ipv6) want_families=(ipv6); primary="ipv6" ;;
  dual) want_families=(ipv4 ipv6); primary="ipv4" ;;
  *)
    echo "error: KIND_IP_FAMILY must be one of ipv4, ipv6, dual (got '${KIND_IP_FAMILY}')" >&2
    exit 1
    ;;
esac

run_kubectl() { kubectl --context="${KUBECTL_CONTEXT}" "$@"; }

log_step() { echo; echo "[kind-net-check]: $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

cleanup() {
  local code=$?
  set +e
  run_kubectl delete namespace "${NS}" --wait=false >/dev/null 2>&1
  if [[ "${code}" -eq 0 ]]; then
    echo
    echo "PASS: ${KIND_IP_FAMILY} cluster networking looks correct."
  fi
  exit "${code}"
}
trap cleanup EXIT

# Kubernetes exposes no field for the family, so go by the colon.
family_of() {
  case "$1" in
    *:*) echo "ipv6" ;;
    *) echo "ipv4" ;;
  esac
}

assert_family() {
  local what="$1" addr="$2" want="$3" got
  got="$(family_of "${addr}")"
  [[ "${got}" == "${want}" ]] || fail "${what}: expected ${want}, got ${addr}"
  echo "  ok: ${what} = ${addr} (${got})"
}

# ip_of_family WANT ADDR... — prints the first ADDR in family WANT, or fails.
ip_of_family() {
  local want="$1" addr
  shift
  for addr in "$@"; do
    if [[ "$(family_of "${addr}")" == "${want}" ]]; then
      echo "${addr}"
      return 0
    fi
  done
  return 1
}

# assert_covers_families WHAT ADDR... — every wanted family appears in ADDR...
assert_covers_families() {
  local what="$1" want found
  shift
  [[ "$#" -gt 0 ]] || fail "${what}: no addresses at all"
  for want in "${want_families[@]}"; do
    found="$(ip_of_family "${want}" "$@")" || fail "${what}: no ${want} address (has: $*)"
    echo "  ok: ${what} ${want} = ${found}"
  done
}

log_step "0. cluster is reachable"
run_kubectl version -o yaml >/dev/null || fail "cannot reach context ${KUBECTL_CONTEXT}"

log_step "1. node InternalIPs cover: ${want_families[*]}"
for node in $(run_kubectl get nodes -o name); do
  # jsonpath joins multiple addresses with a space.
  read -r -a addrs <<<"$(run_kubectl get "${node}" \
    -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')"
  [[ "${#addrs[@]}" -gt 0 ]] || fail "${node} has no InternalIP"
  assert_covers_families "${node} InternalIP" "${addrs[@]}"
done

log_step "2. kubernetes Service ClusterIP is ${primary}"
assert_family "kubernetes ClusterIP" \
  "$(run_kubectl get svc kubernetes -n default -o jsonpath='{.spec.clusterIP}')" "${primary}"

log_step "3. pods pull from the local registry and get podIPs covering: ${want_families[*]}"
run_kubectl create namespace "${NS}" >/dev/null
# Seed the registry so the pull below goes through it rather than a public
# mirror, covering the host -> registry direction on the way.
docker pull --quiet docker.io/library/busybox:1 >/dev/null
docker tag docker.io/library/busybox:1 "${REG_IMAGE}"
docker push --quiet "${REG_IMAGE}" >/dev/null
for name in server client; do
  run_kubectl run "${name}" -n "${NS}" --image="${REG_IMAGE}" --restart=Never \
    --command -- sleep 3600 >/dev/null
done
run_kubectl wait --for=condition=Ready pod/server pod/client -n "${NS}" --timeout=180s >/dev/null
# Ready means containerd pulled the image, which is the step the "kind" network's
# address families actually gate: the node resolves kind-registry through
# Docker's embedded DNS there, not over the published loopback ports.
echo "  ok: pods pulled ${REG_IMAGE} from the local registry"

# podIP is podIPs[0] by definition, so it only ever shows the primary family;
# podIPs is where the second address of a dual-stack pod turns up.
assert_family "server status.podIP" \
  "$(run_kubectl get pod server -n "${NS}" -o jsonpath='{.status.podIP}')" "${primary}"
read -r -a server_ips <<<"$(run_kubectl get pod server -n "${NS}" \
  -o jsonpath='{.status.podIPs[*].ip}')"
[[ "${#server_ips[@]}" -gt 0 ]] || fail "server has no status.podIPs"
assert_family "server status.podIPs[0]" "${server_ips[0]}" "${primary}"
assert_covers_families "server status.podIPs" "${server_ips[@]}"

log_step "4. pod -> pod over: ${want_families[*]}"
# One listener per family on its own port. A single [::] socket would serve IPv4
# too through v4-mapped addresses, so it would pass without the v4 path ever
# being exercised — and it breaks outright wherever bindv6only is on.
# busybox httpd binds 0.0.0.0 given a bare port, which accepts nothing on a
# v6-only pod, so the v6 wildcard is named. URLs bracket a v6 literal, never a v4.
port=8080
for want in "${want_families[@]}"; do
  server_ip="$(ip_of_family "${want}" "${server_ips[@]}")"
  if [[ "${want}" == "ipv6" ]]; then
    listen_spec="[::]:${port}"
    server_host="[${server_ip}]"
  else
    listen_spec="${port}"
    server_host="${server_ip}"
  fi
  # httpd daemonizes without -f, so this exec returns once it is listening.
  run_kubectl exec -n "${NS}" server -- \
    sh -c "mkdir -p /www && echo pong > /www/ping && httpd -p '${listen_spec}' -h /www" >/dev/null
  got="$(run_kubectl exec -n "${NS}" client -- \
    wget -q -T 10 -O- "http://${server_host}:${port}/ping" 2>/dev/null || true)"
  [[ "${got}" == "pong" ]] ||
    fail "pod -> pod to ${server_host}:${port} returned '${got}', want 'pong'"
  echo "  ok: client reached server at ${server_host}:${port}"
  port=$((port + 1))
done

log_step "5. a PreferDualStack Service gets a ClusterIP in each of: ${want_families[*]}"
# Step 2's "kubernetes" Service is SingleStack, so it says nothing about whether
# kube-apiserver got a second Service CIDR. Every dual-stack Service in the tree
# depends on that allocation working.
run_kubectl apply -n "${NS}" -f - >/dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: dual
spec:
  ipFamilyPolicy: PreferDualStack
  selector:
    run: server
  ports:
  - port: 8080
EOF
read -r -a cluster_ips <<<"$(run_kubectl get svc dual -n "${NS}" \
  -o jsonpath='{.spec.clusterIPs[*]}')"
[[ "${#cluster_ips[@]}" -gt 0 ]] || fail "Service dual got no clusterIPs"
assert_covers_families "Service dual clusterIPs" "${cluster_ips[@]}"

log_step "6. CoreDNS resolves kubernetes.default in ${primary}"
# Everything above is ready before CoreDNS is on a fresh cluster, and racing it
# here reports itself as a DNS failure.
run_kubectl -n kube-system rollout status deployment/coredns --timeout=180s >/dev/null
# Primary-only on purpose: kubernetes.default is SingleStack, so an AAAA query
# is a correct NODATA, and busybox nslookup cannot be relied on to ask for one
# type at a time. Per-family DNS belongs in the e2e suite, which has a real
# resolver and can tell NODATA from SERVFAIL.
# Skip to the answer section; everything before "Name:" describes the resolver.
# busybox has written both "Address:" and "Address 1:", so match loosely.
# The trailing "|| true" is load-bearing: errexit plus pipefail would otherwise
# abort the whole script on a failed lookup before reaching the fail() below,
# and a bare non-zero exit from a command substitution prints nothing at all.
resolved="$(run_kubectl exec -n "${NS}" client -- nslookup kubernetes.default.svc.cluster.local 2>/dev/null \
  | awk '/^Name:/{seen=1; next} seen && /^Address/{sub(/^Address[^:]*:[[:space:]]*/, ""); print $1; exit}' \
  || true)"
[[ -n "${resolved}" ]] || fail "kubernetes.default did not resolve"
assert_family "kubernetes.default resolves to" "${resolved}" "${primary}"
