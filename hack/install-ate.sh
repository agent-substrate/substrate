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
source "${ROOT}"/hack/install-demo-multi-template.sh

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
  echo "  --auth-mode=mtls|jwt                   Select ateapi auth mode for --deploy-ate-system (default: mtls)"
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
  echo "Benchmarks (see benchmarking/README.md for details and customization):"
  echo ""
  echo "  --deploy-benchmarks                    Deploy workloads + locust load test stack"
  echo "  --delete-benchmarks                    Delete the locust stack and workloads"
  echo "  --benchmark-worker-count N             Number of WorkerPool replicas (default: 1)"
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
  # Build up a set of ldflags to pass to ko.
  local ldflags=()
  mapfile -t ldflags < <(make ldflags)
  for i in "${!ldflags[@]}"; do
    ldflags[i]="--ldflags=${ldflags[i]}"
  done

  # Only ko subcommands that delegate to kubectl (apply, create, delete, run)
  # accept args after `--`. ko build, resolve, deps, login etc. reject
  # `--context=...` as an unknown subcommand and abort the install.
  case "${1:-}" in
    apply|create|delete|run)
      ./hack/run-tool.sh ko "$@" \
          "${ldflags[@]}" \
          ${KUBECTL_CONTEXT:+-- --context="${KUBECTL_CONTEXT}"}
      ;;
    *)
      ./hack/run-tool.sh ko "$@" \
          "${ldflags[@]}"
      ;;
  esac
}

ate_auth_mode() {
  case "${ATE_API_AUTH_MODE:-mtls}" in
    mtls|jwt)
      echo "${ATE_API_AUTH_MODE:-mtls}"
      ;;
    *)
      echo "Error: ATE_API_AUTH_MODE must be mtls or jwt, got '${ATE_API_AUTH_MODE}'" >&2
      exit 1
      ;;
  esac
}

render_ate_system_manifests() {
  local auth_mode=""
  auth_mode="$(ate_auth_mode)"

  if [[ "${auth_mode}" == "jwt" ]]; then
    local overlay="manifests/ate-install/jwt"
    if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
      overlay="manifests/ate-install/kind-jwt"
    fi
    kubectl kustomize "${overlay}" --load-restrictor LoadRestrictionsNone | run_ko resolve -f -
    return
  fi

  if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
    # Build everything resolved with Kustomize for Kind
    kubectl kustomize manifests/ate-install/kind --load-restrictor LoadRestrictionsNone | run_ko resolve -f -
  else
    # Build everything resolved with base manifests for GKE
    run_ko resolve -f manifests/ate-install
  fi
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
  if run_kubectl get crd workerpools.ate.dev actortemplates.ate.dev sandboxconfigs.ate.dev >/dev/null 2>&1; then
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

  # Enforce per-class SandboxConfig asset requirements (applied before any
  # SandboxConfig so the defaults below are validated too).
  run_kubectl apply -f manifests/ate-install/sandboxconfig-validation.yaml

  # Install the cluster-wide default sandbox config(s). Sandbox binaries live on
  # cluster-scoped SandboxConfigs resolved via each WorkerPool's SandboxClass
  # (decoupled from ActorTemplate). gVisor pools resolve to this default unless
  # they name their own SandboxConfig.
  run_kubectl apply -f manifests/ate-install/sandboxconfig-gvisor.yaml

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
  manifests="$(render_ate_system_manifests)"
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

# get_actor_status echoes the actor's status enum (e.g. STATUS_SUSPENDED).
get_actor_status() {
  local actor_id="$1"
  local atespace="$2"
  local json

  if ! json=$(run_kubectl_ate get actor "${actor_id}" -a "${atespace}" -o json 2>/dev/null); then
    return 1
  fi
  jq -r '.actors[0].status // empty' <<<"${json}"
}

# prepare_actor_for_delete suspends (or resumes then suspends) until DeleteActor
# is allowed. Actors must be STATUS_SUSPENDED before deletion.
prepare_actor_for_delete() {
  local actor_id="$1"
  local atespace="$2"
  local timeout_secs="${3:-120}"
  local deadline=$((SECONDS + timeout_secs))
  local status

  while ((SECONDS < deadline)); do
    if ! status=$(get_actor_status "${actor_id}" "${atespace}"); then
      return 0
    fi

    case "${status}" in
      STATUS_SUSPENDED)
        return 0
        ;;
      STATUS_PAUSED)
        run_kubectl_ate resume actor "${actor_id}" -a "${atespace}" -o json >/dev/null
        ;;
      STATUS_RUNNING)
        run_kubectl_ate suspend actor "${actor_id}" -a "${atespace}" -o json >/dev/null
        ;;
      STATUS_RESUMING | STATUS_SUSPENDING | STATUS_PAUSING)
        ;;
      *)
        echo "cannot delete actor ${actor_id}: unexpected status ${status}" >&2
        return 1
        ;;
    esac
    sleep 2
  done

  echo "timed out waiting for actor ${actor_id} to reach STATUS_SUSPENDED" >&2
  return 1
}

# delete_demo_actors removes all actors for one or more (namespace, template)
# pairs before the demo manifests are deleted. Arguments are alternating
# namespace and template name, e.g.:
#   delete_demo_actors ate-demo-counter counter
#   delete_demo_actors ns-a tmpl-a ns-b tmpl-b
delete_demo_actors() {
  if ! command -v jq &>/dev/null; then
    echo "jq is required to delete demo actors" >&2
    return 1
  fi

  if (($# == 0 || $# % 2 != 0)); then
    echo "delete_demo_actors expects namespace/template pairs" >&2
    return 1
  fi

  if ! run_kubectl get deployment/ate-api-server-deployment -n ate-system >/dev/null 2>&1; then
    log_step "ate-api-server not found; skipping actor cleanup"
    return 0
  fi

  local actors_json
  if ! actors_json=$(run_kubectl_ate get actors -A -o json 2>/dev/null); then
    echo "warning: could not list actors; skipping actor cleanup" >&2
    return 0
  fi

  local ns tmpl atespace actor_id
  while (($# > 0)); do
    ns="$1"
    tmpl="$2"
    shift 2

    log_step "Deleting actors for ${ns}/${tmpl}"
    while IFS=$'\t' read -r atespace actor_id; do
      [[ -z "${actor_id}" ]] && continue
      log_step "  preparing actor ${atespace}/${actor_id} for delete"
      prepare_actor_for_delete "${actor_id}" "${atespace}"
      run_kubectl_ate delete actor "${actor_id}" -a "${atespace}"
    done < <(
      jq -r --arg ns "${ns}" --arg tmpl "${tmpl}" \
        '.actors[]? | select(.actorTemplateNamespace == $ns and .actorTemplateName == $tmpl) | "\(.atespace)\t\(.actorId)"' \
        <<<"${actors_json}"
    )
  done
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

deploy_benchmarks() {
  log_step "deploy_benchmarks (worker_count=${BENCHMARK_WORKER_COUNT})"
  "${ROOT}/benchmarking/deploy_locust.sh" --deploy --worker-count "${BENCHMARK_WORKER_COUNT}"
}

delete_benchmarks() {
  log_step "delete_benchmarks"
  "${ROOT}/benchmarking/deploy_locust.sh" --delete
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

# Pre-scan value-bearing flags so they can appear before or after the action
# flag they configure (e.g. --benchmark-worker-count before/after
# --deploy-benchmarks). The dispatch loop below also accepts these flags but
# treats them as no-ops since the value is already captured here.
BENCHMARK_WORKER_COUNT=1
prescan_args=("$@")
for ((i = 0; i < ${#prescan_args[@]}; i++)); do
  case "${prescan_args[i]}" in
    --auth-mode=*) ATE_API_AUTH_MODE="${prescan_args[i]#*=}" ;;
    --auth-mode)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --auth-mode requires mtls or jwt" >&2
        exit 1
      fi
      ATE_API_AUTH_MODE="${prescan_args[$((i + 1))]}"
      ;;
    --benchmark-worker-count)
      BENCHMARK_WORKER_COUNT="${prescan_args[i+1]:-1}"
      ;;
    --benchmark-worker-count=*)
      BENCHMARK_WORKER_COUNT="${prescan_args[i]#*=}"
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
    --auth-mode=*) ATE_API_AUTH_MODE="${1#*=}" ;;
    --auth-mode)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --auth-mode requires mtls or jwt" >&2
        exit 1
      fi
      ATE_API_AUTH_MODE="$1"
      ;;

    --deploy-ate-system) deploy_ate_system ;;
    --delete-ate-system) delete_ate_system ;;
    --delete-all) delete_all ;;

    --deploy-atelet) deploy_atelet ;;
    --deploy-ate-apiserver) deploy_ate_apiserver ;;

    --deploy-atenet) deploy_atenet ;;
    --delete-atenet) delete_atenet ;;

    --deploy-benchmarks) deploy_benchmarks ;;
    --delete-benchmarks) delete_benchmarks ;;
    # Value captured in the pre-scan above; consume the value arg here so the
    # dispatch loop's `*)` unknown-option branch doesn't reject it.
    --benchmark-worker-count) shift ;;
    --benchmark-worker-count=*) ;;

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
