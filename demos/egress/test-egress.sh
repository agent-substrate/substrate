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
#     tunneled (nftables -> atunnel -> mTLS + CONNECT) through the Envoy egress
#     gateway to an in-cluster target, and the gateway's ext_proc authenticates
#     the actor identity against the ate API (allowed, HTTP 200).
#   * NEGATIVE — a spoofed / unknown actor identity presented directly to the
#     gateway is rejected by ext_proc (HTTP 403).
#
# Prerequisites: a substrate cluster with `--deploy-demo-egress` applied, plus
# kubectl and kubectl-ate on PATH. See demos/egress/README.md.
#
# Usage:
#   demos/egress/test-egress.sh            # run the tests
#   demos/egress/test-egress.sh --cleanup  # remove everything this script created

set -o errexit -o nounset -o pipefail

CTX="${KUBECTL_CONTEXT:-kind-kind}"
ATESPACE="${ATESPACE:-demo}"
ACTOR="${ACTOR:-egress-demo}"
TEMPLATE="${TEMPLATE:-ate-demo-egress/egress}"
TARGET_NS="${TARGET_NS:-egress-target}"
GHOST_ACTOR="ghost-actor-does-not-exist"
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

log "preflight: egress gateway (Envoy + co-located ext_proc) is running"
${K} -n ate-system rollout status deployment/ateway-egress --timeout=120s

log "deploy an in-cluster HTTP target (whoami)"
${K} create namespace "${TARGET_NS}" >/dev/null 2>&1 || true
${K} -n "${TARGET_NS}" create deployment whoami --image=traefik/whoami >/dev/null 2>&1 || true
${K} -n "${TARGET_NS}" expose deployment whoami --port=80 >/dev/null 2>&1 || true
${K} -n "${TARGET_NS}" rollout status deployment/whoami --timeout=120s
TARGET_IP=$(${K} -n "${TARGET_NS}" get svc whoami -o jsonpath='{.spec.clusterIP}')
info "target ClusterIP = ${TARGET_IP}"

log "create + resume Actor ${ATESPACE}/${ACTOR}"
${KATE} create atespace "${ATESPACE}" >/dev/null 2>&1 || true
${KATE} create actor "${ACTOR}" -a "${ATESPACE}" --template "${TEMPLATE}" >/dev/null 2>&1 || true
${KATE} resume actor "${ACTOR}" -a "${ATESPACE}" >/dev/null 2>&1 || true
for _ in $(seq 1 30); do
  ${KATE} get actors -a "${ATESPACE}" 2>/dev/null | grep -q "STATUS_RUNNING" && break
  sleep 3
done
${KATE} get actors -a "${ATESPACE}" 2>/dev/null | grep "${ACTOR}" || true
${KATE} get actors -a "${ATESPACE}" 2>/dev/null | grep -q "STATUS_RUNNING" || { echo "actor did not reach RUNNING"; exit 1; }

egress_log_since() { ${K} -n ate-system logs deployment/ateway-egress --tail=-1 2>/dev/null | grep '\[egress\]' | tail -n +"$(( $1 + 1 ))"; }
egress_log_count() { ${K} -n ate-system logs deployment/ateway-egress --tail=-1 2>/dev/null | grep -c '\[egress\]' || true; }
# wait_egress_log <before-count> <grep-pattern>: retry for log-shipping lag.
wait_egress_log() { local i; for i in $(seq 1 10); do if egress_log_since "$1" | grep -qE "$2"; then egress_log_since "$1" | grep -E "$2" | tail -1; return 0; fi; sleep 1; done; return 1; }

##############################################################################
log "POSITIVE — real Actor egress is tunneled through the gateway (expect 200)"
##############################################################################
BEFORE=$(egress_log_count)
${K} -n ate-system port-forward service/atenet-router 18099:80 >/tmp/egress-pf.log 2>&1 &
PF=$!; sleep 4
CODE=$(curl -s -o /tmp/egress-body.txt -w '%{http_code}' -X POST http://localhost:18099/ \
  -H "Host: ${ACTOR}.${ATESPACE}.actors.resources.substrate.ate.dev" \
  -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://${TARGET_IP}:80/\"}" || true)
kill "${PF}" >/dev/null 2>&1 || true
sleep 1
info "actor round-trip HTTP ${CODE}"
GW_IP=$(${K} -n ate-system get pod -l app=ateway-egress -o jsonpath='{.items[0].status.podIP}')
if [[ "${CODE}" == "200" ]]; then pass "actor fetched the target (HTTP 200)"; else fail "expected HTTP 200, got ${CODE}"; fi
if grep -q "RemoteAddr: ${GW_IP}" /tmp/egress-body.txt 2>/dev/null; then
  pass "target saw the egress gateway (${GW_IP}) as its client — traffic went through the gateway"
else
  info "target body RemoteAddr: $(grep -o 'RemoteAddr: [0-9.]*' /tmp/egress-body.txt 2>/dev/null || echo '?') (gateway IP ${GW_IP})"
fi
if LINE=$(wait_egress_log "${BEFORE}" "actor=${ACTOR}.*code=200"); then
  pass "gateway logged the CONNECT: ${LINE}"
else
  fail "gateway did not log an allowed CONNECT for ${ACTOR}"
fi

##############################################################################
log "NEGATIVE — spoofed/unknown actor identity is rejected by ext_proc (expect 403)"
##############################################################################
# A probe pod with a real pod-identity client cert (so mTLS succeeds) but a
# bogus X-Ate-Actor header. ext_proc's GetActor lookup fails -> 403.
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
# A denied CONNECT does not populate %{http_code}; %{http_connect} carries the
# proxy's CONNECT response code (403). curl exits non-zero on a failed tunnel.
CODE=$(${K} -n ate-system exec ${PROBE_POD} -- sh -c "curl -s -o /dev/null -w '%{http_connect}' \
  --proxy-cacert /run/servicedns.podcert.ate.dev/trust-bundle.pem \
  --proxy-cert /run/podidentity.podcert.ate.dev/credential-bundle.pem \
  --proxy-key /run/podidentity.podcert.ate.dev/credential-bundle.pem \
  --proxy-header 'X-Ate-Atespace: ${ATESPACE}' \
  --proxy-header 'X-Ate-Actor: ${GHOST_ACTOR}' \
  --proxy-header 'X-Ate-Actor-Version: 1' \
  --proxytunnel -x https://ateway-egress.ate-system.svc:443 http://${TARGET_IP}:80/ || true")
info "spoofed-identity CONNECT returned HTTP ${CODE:-000}"
if [[ "${CODE}" == "403" ]]; then
  pass "spoofed identity rejected with 403"
else
  fail "expected HTTP 403 for spoofed identity, got ${CODE:-000}"
fi
if LINE=$(wait_egress_log "${BEFORE}" "actor=${GHOST_ACTOR}.*code=403"); then
  pass "gateway logged the denial: ${LINE}"
else
  info "gateway egress log (recent): $(egress_log_since "${BEFORE}" | tail -1)"
fi

echo
if [[ "${FAILED}" == "0" ]]; then
  printf '\033[1;32mALL CHECKS PASSED\033[0m — pluggable egress + identity authentication working.\n'
else
  printf '\033[1;31mSOME CHECKS FAILED\033[0m\n'; exit 1
fi
