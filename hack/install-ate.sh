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

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment variables if configured
# TODO: this pattern makes it difficult to switch environments.
# Developers will likely want to target both cloud and local depending on what they're working on.
if [[ -f .ate-dev-env.sh ]] && [[ -z "${NO_DEV_ENV:-}" ]]; then
  source .ate-dev-env.sh
fi

# If the user has set KUBECTL_CONTEXT, we can assume they already have credentials.
if [[ -z "${KUBECTL_CONTEXT:-}" ]]; then
  # If PROJECT_ID is set, ensure kubeconfig is configured before running any kubectl commands.
  if [[ -n "${PROJECT_ID:-}" ]]; then
    gcloud container clusters get-credentials "${CLUSTER_NAME}" --location "${CLUSTER_LOCATION}" --project="${PROJECT_ID}"
  fi
fi
# otherwise just use the current cluster in KUBECONFIG ...

# ATE_DEMOS is an array that registers the prefix name of the demo functions.
ATE_DEMOS=()

# Include demos.
source "${ROOT}"/hack/install-demo-counter.sh
source "${ROOT}"/hack/install-demo-sandbox.sh
source "${ROOT}"/hack/install-demo-claude-code-multiplex.sh
source "${ROOT}"/hack/install-demo-agent-secret.sh

# ANSI color codes for prettier output
COLOR_CYAN='\033[1;36m'
COLOR_RESET='\033[0m'

function log_step() {
  local step_name="$1"
  echo -e "${COLOR_CYAN}[step]: ${step_name}${COLOR_RESET}"
}

# --- Helper Functions ---
function usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Overall infrastructure (all infrastructure components):"
  echo ""
  echo "  --deploy-ate-system                    Deploy core system (CRDs, atelet, apiserver)"
  echo "  --delete-ate-system                    Delete core system"
  echo "  --delete-all                           Delete core system and all registered demos"
  echo ""
  echo "Infrastructure components:"
  echo ""
  echo "  --deploy-atelet                        Deploy atelet only"
  echo "  --deploy-ate-apiserver                 Deploy ate-api-server only"
  echo "  --deploy-atenet                        Deploy atenet only"
  echo ""
  echo "To create individual resources used by ate-system (Note: These are"
  echo "called automatically by --deploy-ate-system):"
  echo ""
  echo "  --create-jwt-authority-pool-secret     Create JWT authority pool secret"
  echo "  --create-session-id-ca-pool-secret     Create session ID CA pool secret"
  echo "  --create-podcertificate-controller-cas Create podcertificate controller CAs"
  echo "  --create-valkey-ca-certs-secret        Create Valkey CA certs secret"
  echo "  --create-api-server-env-vars           Create ate-api-server env vars"
  echo ""
  for demo_name in "${ATE_DEMOS[@]}"; do
    echo "Demo: ${demo_name}"
    echo ""
    echo "  --deploy-${demo_name}                         Deploy ${demo_name}"
    echo "  --delete-${demo_name}                         Delete ${demo_name}"
    if declare -F "${demo_name}_usage" >/dev/null 2>&1; then
      "${demo_name}_usage"
    fi
  done
}

run_kubectl() {
  kubectl \
    ${KUBECTL_CONTEXT:+--context=${KUBECTL_CONTEXT}} \
    "$@"
}

run_kubectl_ate() {
  go run ./cmd/kubectl-ate \
    ${KUBECTL_CONTEXT:+--context=${KUBECTL_CONTEXT}} \
    "$@"
}

run_ko() {
  # Only ko subcommands that delegate to kubectl (apply, create, delete, run)
  # accept args after `--`. ko build, resolve, deps, login etc. reject
  # `--context=...` as an unknown subcommand and abort the install.
  case "${1:-}" in
    apply|create|delete|run)
      ./hack/run-tool.sh ko "$@" ${KUBECTL_CONTEXT:+-- --context="${KUBECTL_CONTEXT}"}
      ;;
    *)
      ./hack/run-tool.sh ko "$@"
      ;;
  esac
}

create_valkey_ca_certs_secret() {
  log_step "create_valkey_ca_certs_secret"
  local ca_certs=""
  # Extract from in-cluster service-dns-ca-pool secret (base64 json)
  local pool_json=""
  pool_json=$(run_kubectl get secret -n podcertificate-controller-system service-dns-ca-pool -o jsonpath='{.data.pool}' | base64 --decode)
  # Extract RootCertificateDER base64 string
  local der_base64=""
  der_base64=$(echo "${pool_json}" | grep -o '"RootCertificateDER":"[^"]*' | sed 's/"RootCertificateDER":"//')
  # Convert DER to PEM certificate
  ca_certs=$(echo "${der_base64}" | base64 --decode | openssl x509 -inform der -outform pem)

  run_kubectl create secret generic valkey-ca-certs \
    --from-literal=ca.crt="${ca_certs}" \
    -n ate-system \
    --dry-run=client -o yaml \
    | run_kubectl apply -f -
}

create_jwt_authority_pool_secret() {
  log_step "create_jwt_authority_pool_secret"
  run_kubectl_ate admin make-jwt-pool \
    --key-id="1" \
    --name="session-id-jwt-pool" \
    --secret-namespace=ate-system
}

create_session_id_ca_pool_secret() {
  log_step "create_session_id_ca_pool_secret"
  run_kubectl_ate admin make-ca-pool \
    --ca-id="1" \
    --name="session-id-ca-pool" \
    --secret-namespace=ate-system
}

create_podcertificate_controller_cas() {
  log_step "create_podcertificate_controller_cas"
  run_kubectl create namespace podcertificate-controller-system || true
  run_kubectl_ate admin make-ca-pool \
    --ca-id="1" \
    --name="service-dns-ca-pool" \
    --secret-namespace=podcertificate-controller-system
  run_kubectl_ate admin make-ca-pool \
    --ca-id="1" \
    --name="pod-identity-ca-pool" \
    --secret-namespace=podcertificate-controller-system
}

create_api_server_env_vars() {
  log_step "create_api_server_env_vars"
  run_kubectl create namespace ate-system --dry-run=client -o yaml \
    | run_kubectl apply -f -

  local redis_address=""
  local use_iam_auth="true"
  local tls_server_name=""
  local client_cert=""
  redis_address="valkey-cluster.ate-system.svc:6379"
  use_iam_auth="false"
  tls_server_name="valkey-cluster.ate-system.svc"
  client_cert="/run/servicedns.podcert.ate.dev/credential-bundle.pem"

  echo "REDIS_ADDRESS: ${redis_address}"

  local jwt_issuer=""
  if [[ -n "${PROJECT_ID:-}" && -n "${CLUSTER_LOCATION:-}" && -n "${CLUSTER_NAME:-}" ]]; then
    jwt_issuer="https://container.googleapis.com/v1/projects/${PROJECT_ID}/locations/${CLUSTER_LOCATION}/clusters/${CLUSTER_NAME}"
  else
    jwt_issuer=$(run_kubectl get --raw /.well-known/openid-configuration 2>/dev/null | grep -o '"issuer":"[^"]*' | sed 's/"issuer":"//' || true)
    if [[ -z "${jwt_issuer}" ]]; then
      jwt_issuer="https://kubernetes.default.svc"
    fi
  fi

  run_kubectl create configmap -n ate-system ate-api-server-envvars \
    --from-literal=ATE_API_REDIS_ADDRESS="${redis_address}" \
    --from-literal=ATE_API_REDIS_USE_IAM_AUTH="${use_iam_auth}" \
    --from-literal=ATE_API_REDIS_TLS_SERVER_NAME="${tls_server_name}" \
    --from-literal=ATE_API_REDIS_CLIENT_CERT="${client_cert}" \
    --from-literal=ATE_API_K8SJWT_ISSUER="${jwt_issuer}" \
    --dry-run=client -o yaml \
    | run_kubectl apply -f -
}

ensure_crds() {
  log_step "ensure_crds"
  if run_kubectl get crd workerpools.ate.dev actortemplates.ate.dev >/dev/null 2>&1; then
    return
  fi

  deploy_crds
}

deploy_crds() {
  log_step "deploy_crds"
  run_ko apply -f manifests/ate-install/generated
}

deploy_ate_system() {
  log_step "deploy_ate_system"
  ensure_crds

  # Ensure namespace exists
  run_kubectl apply -f manifests/ate-install/ate-system-namespace.yaml \
    && run_kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/ate-system --timeout=60s

  ensure_apiserver_prerequisites

  # Deploy podcertificate-controller first so it starts signing and creating trust bundles immediately
  run_ko apply -f manifests/ate-install/pod-certificate-controller.yaml
  run_kubectl rollout status deployment/podcertificate-controller -n podcertificate-controller-system --timeout=120s

  # Wait for both ClusterTrustBundles to be created by the controller
  echo "Waiting for podcertificate ClusterTrustBundles to be ready..."
  until run_kubectl get clustertrustbundles podidentity.podcert.ate.dev:identity:primary-bundle >/dev/null 2>&1; do
    sleep 1
  done
  until run_kubectl get clustertrustbundles servicedns.podcert.ate.dev:identity:primary-bundle >/dev/null 2>&1; do
    sleep 1
  done

  local manifests=""
  if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
    # Build everything resolved with Kustomize for Kind
    manifests=$(kubectl kustomize manifests/ate-install/kind --load-restrictor LoadRestrictionsNone | run_ko resolve -f -)
  else
    # Build everything resolved with base manifests for GKE
    manifests=$(run_ko resolve -f manifests/ate-install)
  fi
  echo "${manifests}" | run_kubectl apply -f -

  log_step "Waiting for ATE system components to be ready..."
  run_kubectl rollout status deployment/ate-api-server-deployment -n ate-system --timeout=120s
  run_kubectl rollout status deployment/ate-controller -n ate-system --timeout=120s
  run_kubectl rollout status deployment/atenet-router -n ate-system --timeout=120s
  run_kubectl rollout status statefulset/valkey-cluster -n ate-system --timeout=120s
  run_kubectl rollout status daemonset/atelet -n ate-system --timeout=120s
}

# Ensure secrets and configmaps required by ate-apiserver
ensure_apiserver_prerequisites() {
  log_step "ensure_apiserver_prerequisites"
  run_kubectl get secret -n ate-system session-id-jwt-pool >/dev/null 2>&1 \
    || create_jwt_authority_pool_secret
  run_kubectl get secret -n ate-system session-id-ca-pool >/dev/null 2>&1 \
    || create_session_id_ca_pool_secret
  run_kubectl get secret -n podcertificate-controller-system service-dns-ca-pool >/dev/null 2>&1 \
    || create_podcertificate_controller_cas
  run_kubectl get secret -n ate-system valkey-ca-certs >/dev/null 2>&1 \
    || create_valkey_ca_certs_secret
  run_kubectl get configmap -n ate-system ate-api-server-envvars >/dev/null 2>&1 \
    || create_api_server_env_vars
}

# Redeploy only the ate-apiserver
deploy_ate_apiserver() {
  log_step "deploy_ate_apiserver"
  ensure_crds

  # Ensure namespace exists
  run_kubectl apply -f manifests/ate-install/ate-system-namespace.yaml \
    && run_kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/ate-system --timeout=60s

  ensure_apiserver_prerequisites

  run_ko apply -f manifests/ate-install/ate-api-server.yaml
  run_kubectl rollout status deployment/ate-api-server-deployment -n ate-system --timeout=120s
}

deploy_atelet() {
  log_step "deploy_atelet"
  ensure_crds

  # Ensure namespace exists
  run_kubectl apply -f manifests/ate-install/ate-system-namespace.yaml \
    && run_kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/ate-system --timeout=60s

  local manifest=""
  if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
    # Use Kustomize to build and resolve the atelet DaemonSet patch
    manifest=$(kubectl kustomize manifests/ate-install/kind/atelet --load-restrictor LoadRestrictionsNone | run_ko resolve -f -)
  else
    # Use base manifest for GKE
    manifest=$(run_ko resolve -f manifests/ate-install/atelet.yaml)
  fi
  echo "${manifest}" | run_kubectl apply -f -
  run_kubectl rollout status daemonset/atelet -n ate-system --timeout=120s
}

deploy_atenet() {
  log_step "deploy_atenet"
  ensure_crds

  # Ensure namespace exists
  run_kubectl apply -f manifests/ate-install/ate-system-namespace.yaml \
    && run_kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/ate-system --timeout=60s

  run_ko apply -f manifests/ate-install/atenet-router.yaml
  run_ko apply -f manifests/ate-install/atenet-dns.yaml
  run_kubectl rollout status deployment/atenet-router -n ate-system --timeout=120s
  run_kubectl rollout status deployment/atenet-dns -n ate-system --timeout=120s
}

delete_ate_system() {
  log_step "delete_ate_system"
  if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
    kubectl kustomize manifests/ate-install/kind --load-restrictor LoadRestrictionsNone \
      | run_kubectl delete --ignore-not-found -f -
  else
    run_kubectl delete --ignore-not-found -f manifests/ate-install
  fi
  run_kubectl delete --ignore-not-found -f manifests/ate-install/generated
}

delete_atenet() {
  log_step "delete_atenet"
  run_kubectl delete --ignore-not-found -f manifests/ate-install/atenet-router.yaml
}

delete_all() {
  log_step "delete_all"
  for demo_name in "${ATE_DEMOS[@]}"; do
    if declare -F "${demo_name}_delete" >/dev/null 2>&1; then
      "${demo_name}_delete"
    fi
  done
  delete_ate_system
}

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

# If -h or --help appears anywhere in the command line, print the usage and exit.
for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
  esac
done

while [[ "$#" -gt 0 ]]; do
  # Run ${demo}_cmdline if it exists. If it returns 0, then we successfully
  # handled this argument and can continue. Otherwise, fallthrough to check
  # the other arguments.
  for demo_name in "${ATE_DEMOS[@]}"; do
    if declare -F "${demo_name}_cmdline" >/dev/null 2>&1; then
      if "${demo_name}_cmdline" "$1"; then
        shift
        continue 2
      fi
    fi
  done

  case $1 in
    --deploy-ate-system) deploy_ate_system ;;
    --delete-ate-system) delete_ate_system ;;
    --delete-all) delete_all ;;

    --deploy-atelet) deploy_atelet ;;
    --deploy-ate-apiserver) deploy_ate_apiserver ;;

    --deploy-atenet) deploy_atenet ;;
    --delete-atenet) delete_atenet ;;

    --create-jwt-authority-pool-secret) create_jwt_authority_pool_secret ;;
    --create-session-id-ca-pool-secret) create_session_id_ca_pool_secret ;;
    --create-podcertificate-controller-cas) create_podcertificate_controller_cas ;;
    --create-valkey-ca-certs-secret) create_valkey_ca_certs_secret ;;
    --create-api-server-env-vars) create_api_server_env_vars ;;

    *)
      # Invalid option, should usage and exit with an error.
      echo "Error: unknown option: $1" >&2
      echo ""
      usage
      exit 1
      ;;
  esac
  shift
done
