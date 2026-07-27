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
source "${ROOT}"/hack/install-demo-multi-template.sh
source "${ROOT}"/hack/install-demo-parking.sh
source "${ROOT}"/hack/install-demo-autoscaled-workerpool.sh

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
  echo "  --setup-csi                            Setup CSI hostpath and NFS drivers (Kind only)"
  echo "  --delete-ate-system                    Delete core system"
  echo "  --delete-all                           Delete core system and all registered demos"
  echo "  --ateapi-client-auth=cert|token        Select how in-cluster clients authenticate to ateapi for --deploy-ate-system (default: cert; the server always accepts both)"
  echo "  --atenet-router=envoy|agentgateway     Select the atenet router dataplane (default: envoy)"
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
  echo "  --create-actor-id-ca-pool-secret       Create actor ID CA pool secret"
  echo "  --create-actor-id-ca-certs-secret      Create actor ID CA certs secret"
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
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -n "${line}" ]] && ldflags+=("--ldflags=${line}")
  done < <(make ldflags)

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

ateapi_client_auth() {
  case "${ATE_ATEAPI_CLIENT_AUTH:-cert}" in
    cert|token)
      echo "${ATE_ATEAPI_CLIENT_AUTH:-cert}"
      ;;
    *)
      echo "Error: ATE_ATEAPI_CLIENT_AUTH must be cert or token, got '${ATE_ATEAPI_CLIENT_AUTH}'" >&2
      exit 1
      ;;
  esac
}

atenet_router() {
  case "${ATE_ATENET_ROUTER:-envoy}" in
    envoy|agentgateway)
      echo "${ATE_ATENET_ROUTER:-envoy}"
      ;;
    *)
      echo "Error: --atenet-router must be envoy or agentgateway, got '${ATE_ATENET_ROUTER}'" >&2
      exit 1
      ;;
  esac
}

render_ate_system_manifests() {
  local client_auth=""
  local router=""
  client_auth="$(ateapi_client_auth)"
  router="$(atenet_router)"

  if [[ "${router}" == "agentgateway" ]]; then
    local overlay="manifests/ate-install/agentgateway"
    if [[ "${client_auth}" == "token" ]]; then
      overlay="manifests/ate-install/agentgateway-token-client"
    fi
    if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
      overlay="manifests/ate-install/kind-agentgateway"
      if [[ "${client_auth}" == "token" ]]; then
        overlay="manifests/ate-install/kind-agentgateway-token-client"
      fi
    fi
    kubectl kustomize "${overlay}" --load-restrictor LoadRestrictionsNone | run_ko resolve -f -
    return
  fi

  if [[ "${client_auth}" == "token" ]]; then
    local overlay="manifests/ate-install/token-client"
    if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
      overlay="manifests/ate-install/kind-token-client"
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

render_atenet_router_manifest() {
  if [[ "$(atenet_router)" == "agentgateway" ]]; then
    kubectl kustomize manifests/ate-install/agentgateway-router \
      --load-restrictor LoadRestrictionsNone | run_ko resolve -f -
  else
    run_ko resolve -f manifests/ate-install/atenet-router.yaml
  fi
}

# Apply the ate-otel-config ConfigMap that every control plane component reads
# via envFrom. The full install gets it through render_ate_system_manifests, but
# the targeted single-component redeploys below apply raw manifests with no
# Kustomize, so they have to select the environment's copy themselves. Applying
# the base file unconditionally would overwrite a kind cluster's ConfigMap with
# the GKE endpoint and silently break telemetry for every component at once.
apply_otel_config() {
  if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
    run_kubectl apply -f manifests/ate-install/kind/ate-otel-config.yaml
  else
    run_kubectl apply -f manifests/ate-install/ate-otel-config.yaml
  fi
}

# Extract a CA pool secret's RootCertificateDER and emit it as a PEM certificate.
# The namespace defaults to the podcertificate controller's, where the signer
# CAs live; the actor-identity CA pool is in ate-system, so it passes its own.
ca_pool_root_pem() {
  local secret="$1"
  local namespace="${2:-podcertificate-controller-system}"
  local pool_json=""
  pool_json=$(run_kubectl get secret -n "${namespace}" "${secret}" -o jsonpath='{.data.pool}' | base64 --decode)
  local der_base64=""
  der_base64=$(echo "${pool_json}" | grep -o '"RootCertificateDER":"[^"]*' | sed 's/"RootCertificateDER":"//')
  echo "${der_base64}" | base64 --decode | openssl x509 -inform der -outform pem
}

create_valkey_ca_certs_secret() {
  log_step "create_valkey_ca_certs_secret"
  # valkey requires a single tls-ca-cert-file to verify client and server certs it sees,
  # so it needs both CAs:
  #   - servicedns CA: verifies valkey peers' server certs.
  #   - podidentity CA: verifies the client certs that connect to valkey
  #     (apiserver, the init job, and peers acting as clients).
  # Extract each root into its own variable: errexit cannot see a substitution
  # failing inside printf's argument list, which would silently produce a CA
  # file with a missing root.
  local servicedns_root=""
  servicedns_root=$(ca_pool_root_pem service-dns-ca-pool)
  local podidentity_root=""
  podidentity_root=$(ca_pool_root_pem pod-identity-ca-pool)
  if [[ -z "${servicedns_root}" || -z "${podidentity_root}" ]]; then
    echo "error: failed to extract a CA root for valkey-ca-certs" >&2
    return 1
  fi
  local ca_certs=""
  ca_certs=$(printf '%s\n%s\n' "${servicedns_root}" "${podidentity_root}")

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
    --name="actor-id-jwt-pool" \
    --secret-namespace=ate-system
}

create_actor_id_ca_pool_secret() {
  log_step "create_actor_id_ca_pool_secret"
  run_kubectl_ate admin make-ca-pool \
    --ca-id="1" \
    --name="actor-id-ca-pool" \
    --secret-namespace=ate-system
}

# ---------------------------------------------------------------------------
# SEE(lior): actor-identity CA trust bundle for the egress PEP.
#
# The egress gateway has to verify actor client certificates, which means it
# needs the actor-identity CA *root* — and only the root. The authoritative
# copy today is the actor-id-ca-pool Secret, but its pool.json also carries the
# CA signing key, so mounting that Secret into the gateway would put the key
# that mints every actor identity inside a pod that only ever needs to verify
# them. This derives a cert-only Secret instead, following exactly the pattern
# create_valkey_ca_certs_secret already uses for the signer roots.
#
# TODO(liorlieberman): revisit. The other CAs reach their consumers as
# ClusterTrustBundles published by the podcertificate controller, and the
# actor-identity CA arguably should too — then the gateway would project a
# trust bundle like it already does for servicedns/podidentity and this
# install-time Secret would go away. Doing that needs a signer/controller path
# that does not exist yet, so this is the interim shape.
# ---------------------------------------------------------------------------
create_actor_id_ca_certs_secret() {
  log_step "create_actor_id_ca_certs_secret"
  # Extract into its own variable first: errexit cannot see a substitution fail
  # inside the create-secret argument list, which would silently produce an
  # empty trust bundle and an egress gateway that rejects every actor.
  local actorid_root=""
  actorid_root=$(ca_pool_root_pem actor-id-ca-pool ate-system)
  if [[ -z "${actorid_root}" ]]; then
    echo "error: failed to extract the actor-identity CA root for actor-id-ca-certs" >&2
    return 1
  fi

  run_kubectl create secret generic actor-id-ca-certs \
    --from-literal=ca.crt="${actorid_root}" \
    -n ate-system \
    --dry-run=client -o yaml \
    | run_kubectl apply -f -
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
  # The apiserver dials valkey as a client, so it presents a podidentity
  # (SPIFFE) client cert rather than a servicedns serving cert.
  client_cert="/run/podidentity.podcert.ate.dev/credential-bundle.pem"

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

setup_csi() {
  log_step "setup_csi"
  "${ROOT}/hack/setup-csi-hostpath-kind.sh"
  "${ROOT}/hack/setup-csi-nfs-kind.sh"
}

deploy_ate_system() {
  log_step "deploy_ate_system"
  # Ensure namespace exists before applying RBAC or CRDs
  run_kubectl apply -f manifests/ate-install/ate-system-namespace.yaml \
    && run_kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/ate-system --timeout=60s

  # Not ensure_crds: its existence check skips upgrades, stranding stale CRD
  # schemas and RBAC (role.yaml has no other apply path).
  deploy_crds

  if [[ "${SETUP_CSI:-false}" == "true" ]]; then
    if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
      setup_csi
    else
      echo "Warning: CSI setup is only supported for Kind local installations. Skipping."
    fi
  fi

  # Enforce per-class SandboxConfig asset requirements (applied before any
  # SandboxConfig so the defaults below are validated too).
  run_kubectl apply -f manifests/ate-install/sandboxconfig-validation.yaml

  # Install the cluster-wide default sandbox config(s). Sandbox binaries live on
  # cluster-scoped SandboxConfigs resolved via each WorkerPool's SandboxClass
  # (decoupled from ActorTemplate). gVisor pools resolve to this default unless
  # they name their own SandboxConfig.
  run_kubectl apply -f manifests/ate-install/sandboxconfig-gvisor.yaml

  # Ahead of the bundle below, for the same reason as the namespace: every
  # workload pulls this ConfigMap in via envFrom, and a container whose envFrom
  # target is missing will not start. The bundle contains it, but a raw
  # directory apply orders by filename, so ate-api-server.yaml and
  # ate-controller.yaml would otherwise be created before it and sit in
  # CreateContainerConfigError until it caught up.
  apply_otel_config

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
  run_kubectl rollout status deployment/ate-api-server -n ate-system --timeout=120s
  run_kubectl rollout status deployment/ate-controller -n ate-system --timeout=120s
  run_kubectl rollout status deployment/atenet-router -n ate-system --timeout=120s
  run_kubectl rollout status deployment/ateway-egress -n ate-system --timeout=120s
  run_kubectl rollout status statefulset/valkey-cluster -n ate-system --timeout=120s
  run_kubectl rollout status daemonset/atelet -n ate-system --timeout=120s
}

# Ensure secrets and configmaps required by ate-apiserver
ensure_apiserver_prerequisites() {
  log_step "ensure_apiserver_prerequisites"
  run_kubectl get secret -n ate-system actor-id-jwt-pool >/dev/null 2>&1 \
    || create_jwt_authority_pool_secret
  run_kubectl get secret -n ate-system actor-id-ca-pool >/dev/null 2>&1 \
    || create_actor_id_ca_pool_secret
  # SEE(lior): derived from actor-id-ca-pool above, so it must come after it.
  run_kubectl get secret -n ate-system actor-id-ca-certs >/dev/null 2>&1 \
    || create_actor_id_ca_certs_secret
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
  apply_otel_config

  run_ko apply -f manifests/ate-install/ate-api-server.yaml
  run_kubectl rollout status deployment/ate-api-server -n ate-system --timeout=120s
}

deploy_atelet() {
  log_step "deploy_atelet"
  ensure_crds

  # Ensure namespace exists
  run_kubectl apply -f manifests/ate-install/ate-system-namespace.yaml \
    && run_kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/ate-system --timeout=60s

  apply_otel_config

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

  apply_otel_config

  local router_manifest=""
  router_manifest="$(render_atenet_router_manifest)"
  echo "${router_manifest}" | run_kubectl apply -f -

  run_ko apply -f manifests/ate-install/ateway-egress.yaml
  run_ko apply -f manifests/ate-install/atenet-dns.yaml
  run_kubectl rollout status deployment/atenet-router -n ate-system --timeout=120s
  run_kubectl rollout status deployment/ateway-egress -n ate-system --timeout=120s
  # SEE(lior): this branch also added a `deployment/atenet-dns` wait here, which
  # main has since fixed to `deployment/dns` (the Deployment in atenet-dns.yaml
  # is named "dns"; every other resource in that file is "atenet-dns", so the
  # old name was NotFound on every successful deploy). Dropped this branch's
  # duplicate in favour of main's corrected line.
  run_kubectl rollout status deployment/dns -n ate-system --timeout=120s
}

# get_actor_status echoes the actor's status enum (e.g. STATUS_SUSPENDED).
get_actor_status() {
  local actor_name="$1"
  local atespace="$2"
  local json

  if ! json=$(run_kubectl_ate get actor "${actor_name}" -a "${atespace}" -o json 2>/dev/null); then
    return 1
  fi
  jq -r '.actors[0].status // empty' <<<"${json}"
}

# prepare_actor_for_delete suspends (or resumes then suspends) until DeleteActor
# is allowed. Actors must be STATUS_SUSPENDED before deletion.
prepare_actor_for_delete() {
  local actor_name="$1"
  local atespace="$2"
  local timeout_secs="${3:-120}"
  local deadline=$((SECONDS + timeout_secs))
  local status

  while ((SECONDS < deadline)); do
    if ! status=$(get_actor_status "${actor_name}" "${atespace}"); then
      return 0
    fi

    case "${status}" in
      STATUS_SUSPENDED)
        return 0
        ;;
      STATUS_PAUSED)
        run_kubectl_ate resume actor "${actor_name}" -a "${atespace}" -o json >/dev/null
        ;;
      STATUS_RUNNING)
        run_kubectl_ate suspend actor "${actor_name}" -a "${atespace}" -o json >/dev/null
        ;;
      STATUS_RESUMING | STATUS_SUSPENDING | STATUS_PAUSING)
        ;;
      *)
        echo "cannot delete actor ${actor_name}: unexpected status ${status}" >&2
        return 1
        ;;
    esac
    sleep 2
  done

  echo "timed out waiting for actor ${actor_name} to reach STATUS_SUSPENDED" >&2
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

  if ! run_kubectl get deployment/ate-api-server -n ate-system >/dev/null 2>&1; then
    log_step "ate-api-server not found; skipping actor cleanup"
    return 0
  fi

  local actors_json
  if ! actors_json=$(run_kubectl_ate get actors -A -o json 2>/dev/null); then
    echo "warning: could not list actors; skipping actor cleanup" >&2
    return 0
  fi

  local ns tmpl atespace actor_name
  while (($# > 0)); do
    ns="$1"
    tmpl="$2"
    shift 2

    log_step "Deleting actors for ${ns}/${tmpl}"
    while IFS=$'\t' read -r atespace actor_name; do
      [[ -z "${actor_name}" ]] && continue
      log_step "  preparing actor ${atespace}/${actor_name} for delete"
      prepare_actor_for_delete "${actor_name}" "${atespace}"
      run_kubectl_ate delete actor "${actor_name}" -a "${atespace}"
    done < <(
      jq -r --arg ns "${ns}" --arg tmpl "${tmpl}" \
        '.actors[]? | select(.actorTemplateNamespace == $ns and .actorTemplateName == $tmpl) | "\(.metadata.atespace)\t\(.metadata.name)"' \
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
  run_kubectl delete --ignore-not-found \
    -f manifests/ate-install/components/agentgateway/configmap.yaml
  run_kubectl delete --ignore-not-found -f manifests/ate-install/generated
}

delete_atenet() {
  log_step "delete_atenet"
  run_kubectl delete --ignore-not-found -f manifests/ate-install/atenet-router.yaml
  run_kubectl delete --ignore-not-found \
    -f manifests/ate-install/components/agentgateway/configmap.yaml
  run_kubectl delete --ignore-not-found -f manifests/ate-install/ateway-egress.yaml
  run_kubectl delete --ignore-not-found -f manifests/ate-install/atenet-dns.yaml
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
SETUP_CSI=false
BENCHMARK_WORKER_COUNT=1
prescan_args=("$@")
for ((i = 0; i < ${#prescan_args[@]}; i++)); do
  case "${prescan_args[i]}" in
    --ateapi-client-auth=*) ATE_ATEAPI_CLIENT_AUTH="${prescan_args[i]#*=}" ;;
    --ateapi-client-auth)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --ateapi-client-auth requires cert or token" >&2
        exit 1
      fi
      ATE_ATEAPI_CLIENT_AUTH="${prescan_args[$((i + 1))]}"
      ;;
    --atenet-router=*) ATE_ATENET_ROUTER="${prescan_args[i]#*=}" ;;
    --atenet-router)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --atenet-router requires envoy or agentgateway" >&2
        exit 1
      fi
      ATE_ATENET_ROUTER="${prescan_args[$((i + 1))]}"
      ;;
    --benchmark-worker-count)
      BENCHMARK_WORKER_COUNT="${prescan_args[i+1]:-1}"
      ;;
    --benchmark-worker-count=*)
      BENCHMARK_WORKER_COUNT="${prescan_args[i]#*=}"
      ;;
    --setup-csi)
      SETUP_CSI=true
      ;;
  esac
done
atenet_router >/dev/null

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
    --ateapi-client-auth=*) ATE_ATEAPI_CLIENT_AUTH="${1#*=}" ;;
    --ateapi-client-auth)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --ateapi-client-auth requires cert or token" >&2
        exit 1
      fi
      ATE_ATEAPI_CLIENT_AUTH="$1"
      ;;
    --atenet-router=*) ATE_ATENET_ROUTER="${1#*=}" ;;
    --atenet-router)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --atenet-router requires envoy or agentgateway" >&2
        exit 1
      fi
      ATE_ATENET_ROUTER="$1"
      ;;

    --deploy-ate-system) deploy_ate_system ;;
    --setup-csi)
      if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
        ensure_crds
        setup_csi
      else
        echo "Warning: CSI setup is only supported for Kind local installations. Skipping."
      fi
      ;;
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
    --create-actor-id-ca-pool-secret) create_actor_id_ca_pool_secret ;;
    --create-actor-id-ca-certs-secret) create_actor_id_ca_certs_secret ;;
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
