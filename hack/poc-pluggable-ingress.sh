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

# Deploys the basic reference gateway: a standalone external Envoy that calls
# atenet-router's ExtProc endpoint. atenet-router itself is deployed by
# hack/install-ate-kind.sh as part of the normal install (it ships no proxy),
# so this script only needs to add the gateway in front of it.
#
# NOTE: Before running this script, you must have installed the ATE using hack/install-ate-kind.sh
# To test the actors, you can use a demo setup such as hack/install-ate-kind.sh --deploy-demo-counter
#
# For other gateways (agentgateway, Gateway API), see docs/dev/ingress.md and
# manifests/ate-install/examples/ — this script only covers the basic Envoy path.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

PHASE="${1:-deploy}"
NAMESPACE="ate-system"
KO_DOCKER_REPO="${KO_DOCKER_REPO:-localhost:5002}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME}}"
KUBECTL="kubectl --context=${KUBECTL_CONTEXT}"

log() { echo "[poc-pluggable-ingress] $*" >&2; }
die() { log "ERROR: $*"; exit 1; }

deploy() {
  log "=== Deploying pluggable ingress (basic Envoy example) ==="

  # atenet-gateway uses a plain Envoy image (no ko:// reference).
  log "Applying atenet-gateway-envoy.yaml ..."
  ${KUBECTL} apply -f manifests/ate-install/examples/atenet-gateway-envoy.yaml

  log "Waiting for atenet-router to be ready..."
  ${KUBECTL} -n "${NAMESPACE}" rollout status deployment/atenet-router --timeout=120s

  log "Waiting for atenet-gateway to be ready..."
  ${KUBECTL} -n "${NAMESPACE}" rollout status deployment/atenet-gateway --timeout=120s

  log "Applying atenet-dns.yaml so DNS resolves actor hostnames to atenet-gateway ..."
  export KO_DOCKER_REPO
  ko apply -f manifests/ate-install/atenet-dns.yaml -- --context="${KUBECTL_CONTEXT}"

  log "Waiting for DNS controller to be ready..."
  ${KUBECTL} -n "${NAMESPACE}" rollout status deployment/dns --timeout=120s

  log ""
  log "=== Pluggable ingress deployed successfully ==="
  log ""
  log "1. Start a port-forward to atenet-gateway:"
  log "   ${KUBECTL} -n ${NAMESPACE} port-forward svc/atenet-gateway 8080:80 &"
  log ""
  log "2. Pick an actor and its atespace (e.g. from 'kubectl get actors -A'):"
  log "   ATESPACE=<your-atespace>   # e.g. team-a"
  log "   ACTOR_ID=<your-actor-id>   # e.g. 123e4567-e89b-12d3-a456-426614174000"
  log ""
  log "   The actor hostname format is: <actor-id>.<atespace>.actors.resources.substrate.ate.dev"
  log ""
  log "3. Send a request with an explicit Host header (bypasses DNS):"
  log "   curl -v -H \"Host: \${ACTOR_ID}.\${ATESPACE}.actors.resources.substrate.ate.dev\" \\"
  log "        http://localhost:8080/your/path"
  log ""
  log "4. Inspect Envoy access logs to confirm ext_proc was called:"
  log "   ${KUBECTL} -n ${NAMESPACE} logs -l app=atenet-gateway --tail=20"
  log ""
  log "5. Inspect atenet-router logs:"
  log "   ${KUBECTL} -n ${NAMESPACE} logs -l app=atenet-router --tail=20"
  log ""
  log "6. Send a request (inside the cluster) using the actor hostname (DNS should resolve to the gateway):"
  log "   kubectl run -it --rm --restart=Never curltest --image=curlimages/curl -- \\"
  log "     curl -v http://\${ACTOR_ID}.\${ATESPACE}.actors.resources.substrate.ate.dev/your/path"
}

phase_status() {
  log "=== Pluggable ingress status ==="
  echo ""
  echo "--- atenet-router ---"
  ${KUBECTL} -n "${NAMESPACE}" get deploy atenet-router -o wide 2>/dev/null || echo "  (not deployed)"
  echo ""
  echo "--- atenet-gateway ---"
  ${KUBECTL} -n "${NAMESPACE}" get deploy atenet-gateway -o wide 2>/dev/null || echo "  (not deployed)"
  echo ""
  echo "--- Services ---"
  ${KUBECTL} -n "${NAMESPACE}" get svc atenet-router atenet-gateway 2>/dev/null || true
  echo ""
  echo "--- DNS controller args ---"
  ${KUBECTL} -n "${NAMESPACE}" get deploy -l app=dns -o jsonpath='{.items[*].spec.template.spec.containers[0].args}' 2>/dev/null || echo "  (DNS controller not found)"
}

# ---------------------------------------------------------------------------
# teardown: remove the gateway (atenet-router stays; it's part of the base install)
# ---------------------------------------------------------------------------
phase_teardown() {
  log "=== Teardown ==="
  ${KUBECTL} delete -f manifests/ate-install/examples/atenet-gateway-envoy.yaml --ignore-not-found
  log "Gateway removed. atenet-router is still running (it's part of the base install)."
}

case "${PHASE}" in
  status)   phase_status ;;
  teardown) phase_teardown ;;
  deploy)   deploy ;;
  *)        die "Unknown phase: ${PHASE}. Use: status | teardown | deploy" ;;
esac
