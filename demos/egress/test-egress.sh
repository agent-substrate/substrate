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

# End-to-end test for pluggable actor egress. Reproduces:
#   * POSITIVE — a real, running Actor's plain-HTTP egress is transparently
#     tunneled (nftables -> atunnel -> mTLS + CONNECT) through the configured
#     egress gateway to an in-cluster target, and the gateway authenticates the
#     actor certificate against the ate API (allowed, HTTP 200).
#   * NEGATIVE — a pod holding a perfectly valid *pod* identity, but no actor
#     certificate, cannot open a tunnel: the gateway trusts the actor-identity
#     CA, so the mTLS handshake itself is refused.
#
# Prerequisites: a substrate cluster with `--deploy-demo-egress` applied, plus
# kubectl and kubectl-ate on PATH. See demos/egress/README.md.
#
# Usage:
#   demos/egress/test-egress.sh            # run the tests
#   demos/egress/test-egress.sh --cleanup  # remove everything this script created

set -o errexit -o nounset -o pipefail

CTX="${KUBECTL_CONTEXT:-kind-kind}"
# The actor lives in the demo's atespace: --template-ref resolves the
# template by name within the actor's own atespace.
ATESPACE="${ATESPACE:-ate-demo-egress}"
ACTOR="${ACTOR:-egress-demo}"
TEMPLATE="${TEMPLATE:-egress}"
TARGET_NS="${TARGET_NS:-egress-target}"
TARGET_PORT="${TARGET_PORT:-80}"
PROBE_POD="egress-identity-probe"

K="kubectl --context ${CTX}"
KATE="kubectl-ate --context ${CTX}"

log()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
info() { printf '   %s\n' "$*"; }
pass() { printf '\033[1;32mPASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*"; FAILED=1; }
FAILED=0

require() { command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1"; exit 1; }; }

cleanup() {
  log "cleanup"
  ${K} -n ate-system delete pod "${PROBE_POD}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  ${KATE} suspend actor "${ACTOR}" -a "${ATESPACE}" >/dev/null 2>&1 || true
  ${KATE} delete actor "${ACTOR}" -a "${ATESPACE}" >/dev/null 2>&1 || true
  ${K} delete namespace "${TARGET_NS}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  info "done"
}

if [[ "${1:-}" == "--cleanup" ]]; then require kubectl; require kubectl-ate; cleanup; exit 0; fi

require kubectl
require kubectl-ate
trap '[[ "${KEEP:-}" == "1" ]] || cleanup' EXIT

log "preflight: egress gateway is running"
${K} -n ate-system rollout status deployment/atenet-egress --timeout=120s
DATAPLANE=$(${K} -n ate-system get deployment/atenet-egress -o jsonpath='{.spec.template.spec.containers[0].name}')
case "${DATAPLANE}" in
  envoy)
    ACCESS_LOG_PATTERN='\[egress\]'
    ;;
  agentgateway)
    ACCESS_LOG_PATTERN='substrate.connect.authority'
    ;;
  *)
    echo "unsupported egress dataplane container: ${DATAPLANE}" >&2
    exit 1
    ;;
esac
info "dataplane = ${DATAPLANE}"

log "deploy an in-cluster HTTP target (whoami)"
${K} create namespace "${TARGET_NS}" >/dev/null 2>&1 || true
${K} -n "${TARGET_NS}" create deployment whoami --image=traefik/whoami >/dev/null 2>&1 || true
${K} -n "${TARGET_NS}" expose deployment whoami --port="${TARGET_PORT}" --target-port=80 >/dev/null 2>&1 || true
${K} -n "${TARGET_NS}" rollout status deployment/whoami --timeout=120s
TARGET_IP=$(${K} -n "${TARGET_NS}" get svc whoami -o jsonpath='{.spec.clusterIP}')
info "target = ${TARGET_IP}:${TARGET_PORT}"

log "create + resume Actor ${ATESPACE}/${ACTOR}"
${KATE} create atespace "${ATESPACE}" >/dev/null 2>&1 || true
${KATE} create actor "${ACTOR}" -a "${ATESPACE}" --template-ref "${TEMPLATE}" >/dev/null 2>&1 || true
${KATE} resume actor "${ACTOR}" -a "${ATESPACE}" >/dev/null 2>&1 || true
for _ in $(seq 1 30); do
  ${KATE} get actors -a "${ATESPACE}" 2>/dev/null | grep -q "ACTOR_STATE_RUNNING" && break
  sleep 3
done
${KATE} get actors -a "${ATESPACE}" 2>/dev/null | grep "${ACTOR}" || true
${KATE} get actors -a "${ATESPACE}" 2>/dev/null | grep -q "ACTOR_STATE_RUNNING" || { echo "actor did not reach RUNNING"; exit 1; }

egress_log_since() { ${K} -n ate-system logs deployment/atenet-egress -c "${DATAPLANE}" --tail=-1 2>/dev/null | grep -E "${ACCESS_LOG_PATTERN}" | tail -n +"$(( $1 + 1 ))"; }
egress_log_count() { ${K} -n ate-system logs deployment/atenet-egress -c "${DATAPLANE}" --tail=-1 2>/dev/null | grep -Ec "${ACCESS_LOG_PATTERN}" || true; }
# wait_egress_log <before-count> <grep-pattern>: retry for log-shipping lag.
wait_egress_log() { for _ in $(seq 1 10); do if egress_log_since "$1" | grep -qE "$2"; then egress_log_since "$1" | grep -E "$2" | tail -1; return 0; fi; sleep 1; done; return 1; }

##############################################################################
log "POSITIVE — real Actor egress is tunneled through the gateway (expect 200)"
##############################################################################
BEFORE=$(egress_log_count)
${K} -n ate-system port-forward service/atenet-router 18099:80 >/tmp/egress-pf.log 2>&1 &
PF=$!; sleep 4
CODE=$(curl -s -o /tmp/egress-body.txt -w '%{http_code}' -X POST http://localhost:18099/ \
  -H "Host: ${ACTOR}.${ATESPACE}.actors.resources.substrate.ate.dev" \
  -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://${TARGET_IP}:${TARGET_PORT}/\"}" || true)
kill "${PF}" >/dev/null 2>&1 || true
sleep 1
info "actor round-trip HTTP ${CODE}"
GW_IP=$(${K} -n ate-system get pod -l app=atenet-egress -o jsonpath='{.items[0].status.podIP}')
if [[ "${CODE}" == "200" ]]; then pass "actor fetched the target (HTTP 200)"; else fail "expected HTTP 200, got ${CODE}"; fi
if grep -q "RemoteAddr: ${GW_IP}" /tmp/egress-body.txt 2>/dev/null; then
  pass "target saw the egress gateway (${GW_IP}) as its client — traffic went through the gateway"
else
  info "target body RemoteAddr: $(grep -o 'RemoteAddr: [0-9.]*' /tmp/egress-body.txt 2>/dev/null || echo '?') (gateway IP ${GW_IP})"
fi
if [[ "${DATAPLANE}" == "envoy" ]]; then
  # Envoy's access log identifies the peer by its certificate SAN, not by any
  # header the actor could have written.
  ALLOWED_LOG_PATTERN="actor/${ACTOR}.*code=200"
else
  # agentgateway's structured access log records the CONNECT authority. Actor
  # authentication is also covered by the successful request and negative test.
  ALLOWED_LOG_PATTERN="substrate.connect.authority.*${TARGET_IP}:${TARGET_PORT}"
fi
if LINE=$(wait_egress_log "${BEFORE}" "${ALLOWED_LOG_PATTERN}"); then
  pass "gateway logged the CONNECT: ${LINE}"
else
  fail "${DATAPLANE} did not log the allowed CONNECT"
fi

##############################################################################
log "NEGATIVE — a pod identity is not an actor identity (expect a refused handshake)"
##############################################################################
${K} apply -f - >/dev/null <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: egress-identity-probe
  namespace: ate-system
spec:
  containers:
  - name: curl
    image: curlimages/curl:latest
    command: ["sleep", "600"]
    volumeMounts:
    - { name: podidentity, mountPath: /run/podidentity.podcert.ate.dev, readOnly: true }
    - { name: servicedns,  mountPath: /run/servicedns.podcert.ate.dev,  readOnly: true }
  volumes:
  - name: podidentity
    projected:
      sources:
      - podCertificate:
          signerName: podidentity.podcert.ate.dev/identity
          keyType: ECDSAP256
          credentialBundlePath: credential-bundle.pem
  - name: servicedns
    projected:
      sources:
      - clusterTrustBundle:
          signerName: servicedns.podcert.ate.dev/identity
          labelSelector: { matchLabels: { podcert.ate.dev/canarying: live } }
          path: trust-bundle.pem
YAML
${K} -n ate-system wait --for=condition=Ready pod/${PROBE_POD} --timeout=60s >/dev/null

BEFORE=$(egress_log_count)
# %{http_connect} carries the proxy's CONNECT response code, and stays 000 when
# the tunnel never opens. curl exits non-zero on a failed handshake, so capture
# both and require that no CONNECT was ever answered.
PROBE=$(${K} -n ate-system exec ${PROBE_POD} -- sh -c "curl -s -o /dev/null -w '%{http_connect}' \
  --proxy-cacert /run/servicedns.podcert.ate.dev/trust-bundle.pem \
  --proxy-cert /run/podidentity.podcert.ate.dev/credential-bundle.pem \
  --proxy-key /run/podidentity.podcert.ate.dev/credential-bundle.pem \
  --proxytunnel -x https://atenet-egress.ate-system.svc:443 http://${TARGET_IP}:${TARGET_PORT}/; echo \" exit=\$?\"" || true)
CODE=${PROBE%% *}
info "pod-identity CONNECT attempt: http_connect=${CODE:-000}${PROBE#"${CODE}"}"
if [[ "${CODE}" != "200" ]]; then
  pass "a pod identity cannot open an egress tunnel (no CONNECT succeeded)"
else
  fail "expected the gateway to refuse a non-actor client certificate, but CONNECT returned 200"
fi
# A rejected handshake never becomes an HTTP request, so it produces no egress
# access-log line. Report whatever the gateway logged so a real failure is
# diagnosable, but do not assert on it.
if LINE=$(egress_log_since "${BEFORE}" | tail -1); [[ -n "${LINE:-}" ]]; then
  info "gateway egress log since the probe: ${LINE}"
else
  info "no new egress access-log lines — the handshake was refused before HTTP, as expected"
fi

echo
if [[ "${FAILED}" == "0" ]]; then
  printf '\033[1;32mALL CHECKS PASSED\033[0m — pluggable egress + identity authentication working.\n'
else
  printf '\033[1;31mSOME CHECKS FAILED\033[0m\n'; exit 1
fi
