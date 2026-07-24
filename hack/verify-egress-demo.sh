#!/usr/bin/env bash
# POC verification for pluggable egress (guides 2 & 3).
#
# Preconditions:
#   hack/create-kind-cluster.sh
#   hack/install-ate-kind.sh --deploy-demo-egress
#
# The egress demo Actor accepts {"url":"..."} and performs an HTTP GET. With
# egress turned on (atelet --egress-gateway-address), the Actor's outbound TCP
# is nftables-REDIRECTed into atunnel, wrapped in mTLS + HTTP CONNECT, and sent
# to the Envoy egress gateway, which terminates CONNECT and tunnels to the real
# destination. This script drives that path and shows the gateway's access log
# proving the actor identity + CONNECT authority were seen.
set -o errexit -o nounset -o pipefail
ROOT="$(git rev-parse --show-toplevel)"; cd "${ROOT}"

CTX="${KUBECTL_CONTEXT:-kind-kind}"
K="kubectl --context ${CTX}"
ATESPACE="${ATESPACE:-demo}"
ACTOR="${ACTOR:-egress-demo}"
TARGET_URL="${TARGET_URL:-http://example.com/}"

echo "== gateway should be running =="
${K} -n ate-system rollout status deployment/ateway-egress --timeout=120s

echo "== create atespace + actor =="
kubectl-ate --context "${CTX}" create atespace "${ATESPACE}" 2>/dev/null || true
kubectl-ate --context "${CTX}" create actor "${ACTOR}" \
  --atespace "${ATESPACE}" --template ate-demo-egress/egress 2>/dev/null || true
${K} -n ate-system wait --for=condition=Ready "actor/${ACTOR}" 2>/dev/null || sleep 10

echo "== snapshot gateway log offset =="
BEFORE=$(${K} -n ate-system logs deployment/ateway-egress --tail=-1 2>/dev/null | wc -l | tr -d ' ')

echo "== drive actor egress: GET ${TARGET_URL} via the actor =="
${K} -n ate-system port-forward service/atenet-router 18000:80 >/tmp/pf.log 2>&1 &
PF=$!; trap 'kill ${PF} 2>/dev/null || true' EXIT
sleep 3
RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:18000/ \
  -H "Host: ${ACTOR}.${ATESPACE}.actors.resources.substrate.ate.dev" \
  -H 'Content-Type: application/json' \
  -d "{\"url\":\"${TARGET_URL}\"}") || true
echo "actor round-trip HTTP ${RESP} (200 = the actor fetched ${TARGET_URL} through egress)"

echo "== NEW egress gateway access log lines (proof of CONNECT+mTLS+identity) =="
${K} -n ate-system logs deployment/ateway-egress --tail=-1 2>/dev/null \
  | tail -n +"$((BEFORE + 1))" | grep '\[egress\]' || {
    echo "!! no [egress] lines — dumping recent gateway logs:"
    ${K} -n ate-system logs deployment/ateway-egress --tail=20
    exit 1
  }
echo "== PASS: actor egress traversed the Envoy egress gateway =="
