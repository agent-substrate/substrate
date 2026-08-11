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
source "${ROOT}"/hack/install-demo-egress.sh
source "${ROOT}"/hack/install-demo-sandbox.sh
source "${ROOT}"/hack/install-demo-claude-code-multiplex.sh
source "${ROOT}"/hack/install-demo-multi-template.sh
source "${ROOT}"/hack/install-demo-parking.sh
source "${ROOT}"/hack/install-demo-autoscaled-workerpool.sh

# Include the optional ext_proc filter on the egress gateway's decrypted leg,
# behind --experimental-additional-egress-extproc-service.
source "${ROOT}"/hack/experimental-additional-egress-extproc.sh

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
  echo "  --atenet-router=envoy|agentgateway     Select the ingress and egress dataplane (default: envoy)"
  echo "  --store-backend=redis|postgres         Configure the ateapi store backend (default: redis)"
  echo "  --podcert-workers-per-signer N         Concurrent workers per podcertificate-controller signer (default: 1)"
  echo "  --rollout-timeout DURATION             Per-workload readiness wait timeout, kubectl-style Go duration (default: 60s)"
  echo "  --otlp-endpoint URL                    Send all control plane telemetry to URL, not to the cluster default (see benchmarking/telemetry/README.md)"
  echo ""
  echo "Experiments:"
  echo ""
  echo "  --experimental-use-sdsmint             Deploy the egress gateway with per-SNI certificate minting (experimental)"
  echo "  --experimental-additional-egress-extproc-service NS/SVC:PORT"
  echo "                                         Run an additional ext_proc authorization filter, served by that Service."
  echo "                                         Requires --experimental-use-sdsmint. (experimental)"
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
  echo "  --create-egress-mitm-ca-pool-secret    Create egress MITM CA pool secret"
  echo "  --create-podcertificate-controller-cas Create podcertificate controller CAs"
  echo "  --create-valkey-ca-certs-secret        Create Valkey's combined client/server CA bundle"
  echo "  --create-api-server-env-vars           Create ate-api-server env vars"
  echo "  --create-api-authentication-config     Create the default ate-api-server authentication config"
  echo ""
  echo "PostgreSQL store (standalone operations; normally select it with"
  echo "--deploy-ate-system --store-backend=postgres):"
  echo ""
  echo "  --deploy-postgres                      Deploy the single-replica PostgreSQL StatefulSet"
  echo ""
  echo "Benchmarks (see benchmarking/README.md for details and customization):"
  echo ""
  echo "  --deploy-benchmarks                    Deploy workloads + locust load test stack"
  echo "  --delete-benchmarks                    Delete the locust stack and workloads"
  echo "  --benchmark-worker-count N             Number of WorkerPool replicas (default: 1)"
  echo "  --benchmark-sandbox-class CLASS        Sandbox runtime for the benchmark WorkerPool: gvisor | microvm (default: gvisor)."
  echo "                                         microvm requires hack/install-microvm-deps.sh --install to have run."
  echo "  --benchmark-actor-memory SIZE          Memory limit for the benchmark ActorTemplates (default: 256Mi,"
  echo "                                         the smallest size microvm admits)"
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

# run_kubectl_fatal runs kubectl and aborts the install if it fails. Demo
# handlers need this: the dispatcher below calls them from an `if` condition,
# which suppresses errexit for everything they run, so a plain run_kubectl that
# fails is silently ignored -- a broken wait then costs its whole timeout and
# lets the install "succeed" anyway.
run_kubectl_fatal() {
  if ! run_kubectl "$@"; then
    echo "error: kubectl $* failed" >&2
    exit 1
  fi
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

store_backend() {
  local backend="${ATE_INSTALL_STORE_BACKEND:-${ATE_API_STORE_BACKEND:-redis}}"
  case "${backend}" in
    redis|postgres)
      echo "${backend}"
      ;;
    *)
      echo "Error: store backend must be redis or postgres, got '${backend}'" >&2
      exit 1
      ;;
  esac
}

podcert_workers_per_signer() {
  local workers="${ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER:-1}"
  if ! [[ "${workers}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --podcert-workers-per-signer must be a positive integer, got '${workers}'" >&2
    exit 1
  fi
  echo "${workers}"
}

rollout_timeout() {
  local timeout="${ATE_INSTALL_ROLLOUT_TIMEOUT:-60s}"
  if ! [[ "${timeout}" =~ ^(0|([0-9]+(h|m|s))+)$ ]]; then
    echo "Error: --rollout-timeout must be a Go duration like 300s, 10m, or 1h30m (or 0 for no timeout), got '${timeout}'" >&2
    exit 1
  fi
  echo "${timeout}"
}

default_postgres_connection_string() {
  echo "postgresql://postgres@postgres.ate-system.svc:5432/atepg?sslmode=verify-full&sslrootcert=/run/servicedns.podcert.ate.dev/trust-bundle.pem&sslcert=/run/podidentity.podcert.ate.dev/credential-bundle.pem&sslkey=/run/podidentity.podcert.ate.dev/credential-bundle.pem"
}

render_ate_system_manifests() {
  local router=""
  router="$(atenet_router)"

  if [[ "${router}" == "agentgateway" ]]; then
    local overlay="manifests/ate-install/agentgateway"
    if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
      overlay="manifests/ate-install/kind-agentgateway"
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

# atenet_egress_manifest echoes the path of the egress manifest to deploy:
# the sdsmint variant under --experimental-use-sdsmint, the shipped one
# otherwise. The two are whole files rather than a Kustomize overlay because
# what differs between them is envoy.yaml, which lives as one inline string in
# the atenet-egress ConfigMap; Kustomize can replace that string but cannot
# patch into it, so an overlay would carry a full copy of it anyway.
atenet_egress_manifest() {
  if [[ "${ATE_EXPERIMENTAL_USE_SDSMINT:-false}" == "true" ]]; then
    echo "manifests/ate-install/atenet-egress-with-sdsmint.yaml"
  else
    echo "manifests/ate-install/atenet-egress.yaml"
  fi
}

render_atenet_egress_manifest() {
  if [[ "$(atenet_router)" == "agentgateway" ]]; then
    # The markers live inside Envoy's bootstrap, so there is nowhere here to
    # put the filter. Refuse for the same reason patch_atenet_egress_manifest
    # refuses a non-sdsmint manifest: ignoring the flag would report a
    # successful install of a gateway that has no additional checkpoint on it.
    if additional_egress_extproc_enabled; then
      echo "Error: --experimental-additional-egress-extproc-service requires --atenet-router=envoy" >&2
      return 1
    fi
    kubectl kustomize manifests/ate-install/agentgateway-egress \
      --load-restrictor LoadRestrictionsNone | run_ko resolve -f -
  elif additional_egress_extproc_enabled; then
    patch_atenet_egress_manifest | run_ko resolve -f -
  else
    run_ko resolve -f "$(atenet_egress_manifest)"
  fi
}

# apply_atenet_egress deploys the egress gateway.
apply_atenet_egress() {
  local manifests=""
  manifests="$(render_atenet_egress_manifest)"

  # Whether it is already running has to be settled before the apply: a patched
  # bootstrap arrives as a ConfigMap change, and an otherwise unchanged
  # Deployment will not pick that up on its own.
  local running=false
  if run_kubectl -n ate-system get deployment/atenet-egress >/dev/null 2>&1; then
    running=true
  fi

  echo "${manifests}" | run_kubectl apply -f -

  if [[ "${running}" == "true" ]] && additional_egress_extproc_enabled; then
    run_kubectl -n ate-system rollout restart deployment/atenet-egress
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

# Apply the opt-in PostgreSQL StatefulSet. On kind it goes through an overlay
# that right-sizes the CPU request for a 4-vCPU node; see
# manifests/ate-install/kind/postgres/kustomization.yaml.
apply_postgres() {
  if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
    kubectl kustomize manifests/ate-install/kind/postgres \
      --load-restrictor LoadRestrictionsNone | run_kubectl apply -f -
  else
    run_kubectl apply -f manifests/ate-install/postgres.yaml
  fi
}

# --otlp-endpoint sends all control plane telemetry to a different collector for
# the duration of a measurement. One patch is sufficient: each component reads
# this ConfigMap through envFrom, and ate-controller copies the values to the
# ateom worker pods that it creates. See benchmarking/telemetry/README.md.
#
# Call this AFTER every apply. The ate-system bundle contains its own copy of
# ate-otel-config, thus an apply of the bundle replaces a patch that came
# before it, and the endpoint returns to the cluster default with no error
# message.
#
# A change to a ConfigMap starts no rollout, because the pod template stays the
# same. Thus restart the consumers that read it. Do the restart only when the
# value changes: a restart during the rollout of the bundle makes the two
# rollouts compete, and `kubectl rollout status` can then exceed its timeout.
# An absent workload is not an error, because a deploy of one component has
# only that component.
apply_otel_endpoint_override() {
  if [[ -z "${ATE_OTLP_ENDPOINT:-}" ]]; then
    return 0
  fi

  local current=""
  current="$(run_kubectl -n ate-system get configmap ate-otel-config \
    -o jsonpath='{.data.OTEL_EXPORTER_OTLP_ENDPOINT}' 2>/dev/null || true)"
  if [[ "${current}" == "${ATE_OTLP_ENDPOINT}" ]]; then
    return 0
  fi

  echo "Overriding OTEL_EXPORTER_OTLP_ENDPOINT with ${ATE_OTLP_ENDPOINT}"
  run_kubectl -n ate-system patch configmap ate-otel-config --type=merge \
    -p "{\"data\":{\"OTEL_EXPORTER_OTLP_ENDPOINT\":\"${ATE_OTLP_ENDPOINT}\"}}"

  local workload
  for workload in deployment/ate-api-server deployment/ate-controller \
                  deployment/atenet-router daemonset/atelet; do
    if run_kubectl -n ate-system get "${workload}" >/dev/null 2>&1; then
      run_kubectl -n ate-system rollout restart "${workload}"
    fi
  done
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
  # Valkey uses one CA file to verify certificates in both directions:
  #   - servicedns CA: verifies Valkey peers.
  #   - podidentity CA: verifies clients such as ateapi and Valkey's init job.
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

# deploy_postgres deploys only the experimental single-replica PostgreSQL
# StatefulSet. Full-system installs select it with --store-backend=postgres.
deploy_postgres() {
  log_step "deploy_postgres"
  run_kubectl apply -f manifests/ate-install/ate-system-namespace.yaml \
    && run_kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/ate-system --timeout=60s
  run_kubectl get secret -n podcertificate-controller-system service-dns-ca-pool >/dev/null 2>&1 \
    || create_podcertificate_controller_cas
  run_kubectl get secret -n podcertificate-controller-system pod-identity-ca-pool >/dev/null 2>&1 \
    || create_podcertificate_controller_cas
  # The StatefulSet's projected serving certificate is issued by this
  # controller. Applying it here makes --deploy-postgres usable on a fresh
  # cluster as well as after --deploy-ate-system.
  run_ko apply -f manifests/ate-install/pod-certificate-controller.yaml
  apply_podcert_workers_override
  run_kubectl rollout status deployment/podcertificate-controller \
    -n podcertificate-controller-system --timeout=120s
  wait_for_podcertificate_trust_bundles
  apply_postgres
  run_kubectl rollout status statefulset/postgres -n ate-system --timeout="$(rollout_timeout)"
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

# The egress gateway has to verify actor client certificates, which means it
# needs the actor-identity CA root. actor-id-ca-pool Secret containts both
# root and CA signing key. This derives a cert-only Secret instead, following
# exactly the pattern create_valkey_ca_certs_secret already uses for the
# signer roots.
#
# TODO(liorlieberman): should this be published as ClusterTrustBundles?
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

# The MITM CA the egress gateway's sdsmint sidecar signs per-SNI leaves with.
# ecdsa-p256 rather than the ed25519 default: these leaves are validated by
# arbitrary clients inside actor sandboxes, where Ed25519 support cannot be
# assumed.
create_egress_mitm_ca_pool_secret() {
  log_step "create_egress_mitm_ca_pool_secret"
  run_kubectl_ate admin make-ca-pool \
    --ca-id="mitm" \
    --name="egress-mitm-ca-pool" \
    --secret-namespace=ate-system \
    --key-type=ecdsa-p256 \
    --common-name="substrate egress MITM CA"
}

# Only the sdsmint egress variant mounts this pool.
ensure_egress_mitm_ca_pool_secret() {
  [[ "$(atenet_router)" != "agentgateway" ]] || return 0
  [[ "${ATE_EXPERIMENTAL_USE_SDSMINT:-false}" == "true" ]] || return 0
  run_kubectl get secret -n ate-system egress-mitm-ca-pool >/dev/null 2>&1 \
    || create_egress_mitm_ca_pool_secret
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

wait_for_podcertificate_trust_bundles() {
  echo "Waiting for podcertificate ClusterTrustBundles to be ready..."
  until run_kubectl get clustertrustbundles podidentity.podcert.ate.dev:identity:primary-bundle >/dev/null 2>&1; do
    sleep 1
  done
  until run_kubectl get clustertrustbundles servicedns.podcert.ate.dev:identity:primary-bundle >/dev/null 2>&1; do
    sleep 1
  done
}

create_api_server_env_vars() {
  log_step "create_api_server_env_vars"
  run_kubectl create namespace ate-system --dry-run=client -o yaml \
    | run_kubectl apply -f -

  local backend=""
  local redis_address=""
  local use_iam_auth="true"
  local tls_server_name=""
  local client_cert=""
  local postgres_connection_string="${ATE_API_POSTGRES_CONNECTION_STRING:-}"
  backend="$(store_backend)"
  if [[ "${backend}" == "postgres" && -z "${postgres_connection_string}" ]]; then
    postgres_connection_string="$(default_postgres_connection_string)"
  fi
  redis_address="valkey-cluster.ate-system.svc:6379"
  use_iam_auth="false"
  tls_server_name="valkey-cluster.ate-system.svc"
  # The apiserver dials valkey as a client, so it presents a podidentity
  # (SPIFFE) client cert rather than a servicedns serving cert.
  client_cert="/run/podidentity.podcert.ate.dev/credential-bundle.pem"

  echo "STORE_BACKEND: ${backend}"
  if [[ "${backend}" == "redis" ]]; then
    echo "REDIS_ADDRESS: ${redis_address}"
  fi

  run_kubectl create configmap -n ate-system ate-api-server-envvars \
    --from-literal=ATE_API_REDIS_ADDRESS="${redis_address}" \
    --from-literal=ATE_API_REDIS_USE_IAM_AUTH="${use_iam_auth}" \
    --from-literal=ATE_API_REDIS_TLS_SERVER_NAME="${tls_server_name}" \
    --from-literal=ATE_API_REDIS_CLIENT_CERT="${client_cert}" \
    --from-literal=ATE_API_STORE_BACKEND="${backend}" \
    --from-literal=ATE_API_POSTGRES_CONNECTION_STRING="${postgres_connection_string}" \
    --dry-run=client -o yaml \
    | run_kubectl apply -f -
}

apply_podcert_workers_override() {
  if [[ -z "${ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER:-}" ]]; then
    return 0
  fi

  local workers=""
  workers="$(podcert_workers_per_signer)"

  local current=""
  current="$(run_kubectl -n podcertificate-controller-system get deployment/podcertificate-controller \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="WORKERS_PER_SIGNER")].value}' 2>/dev/null || true)"
  if [[ "${current}" == "${workers}" ]]; then
    return 0
  fi

  echo "Overriding WORKERS_PER_SIGNER with ${workers}"
  run_kubectl -n podcertificate-controller-system set env deployment/podcertificate-controller \
    WORKERS_PER_SIGNER="${workers}"
}

create_api_authentication_config() {
  log_step "create_api_authentication_config"
  run_kubectl create namespace ate-system --dry-run=client -o yaml \
    | run_kubectl apply -f -

  local jwt_issuer=""
  if [[ -n "${PROJECT_ID:-}" && -n "${CLUSTER_LOCATION:-}" && -n "${CLUSTER_NAME:-}" ]]; then
    jwt_issuer="https://container.googleapis.com/v1/projects/${PROJECT_ID}/locations/${CLUSTER_LOCATION}/clusters/${CLUSTER_NAME}"
  else
    jwt_issuer=$(run_kubectl get --raw /.well-known/openid-configuration 2>/dev/null | grep -o '"issuer":"[^"]*' | sed 's/"issuer":"//' || true)
    if [[ -z "${jwt_issuer}" ]]; then
      jwt_issuer="https://kubernetes.default.svc"
    fi
  fi

  local discovery_config=""
  case "${jwt_issuer}" in
    https://kubernetes.default.svc|https://kubernetes.default.svc.cluster.local)
      discovery_config=$'  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\n  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token\n'
      ;;
  esac
  local authentication_config
  authentication_config=$(printf 'actorIdentityJWTProvider: kubernetes\njwtProviders:\n- name: kubernetes\n  issuer: %s\n  audiences: [api.ate-system.svc]\n%s' "${jwt_issuer}" "${discovery_config}")
  run_kubectl create configmap -n ate-system ate-api-authentication \
    --from-literal=authentication.yaml="${authentication_config}" \
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
  apply_podcert_workers_override
  run_kubectl rollout status deployment/podcertificate-controller -n podcertificate-controller-system --timeout=120s

  wait_for_podcertificate_trust_bundles

  # The Kind overlays include Valkey but do not include the opt-in PostgreSQL
  # manifest. Apply PostgreSQL explicitly when
  # selected so backend configuration and deployed resources cannot diverge.
  # Store-specific overlay composition can remove the unused Valkey resources
  # in a separate change.
  if [[ "$(store_backend)" == "postgres" ]]; then
    apply_postgres
  fi

  # CSI setup must run after podcertificate-controller is ready and trust bundles
  # exist. The ghostunnel sidecar uses projected podCertificate and clusterTrustBundle
  # volumes which cannot be fulfilled until podcertcontroller is actively signing,
  # otherwise rollout of csi-hostpath-socat times out.
  if [[ "${SETUP_CSI:-false}" == "true" ]]; then
    if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
      setup_csi
    else
      echo "Warning: CSI setup is only supported for Kind local installations. Skipping."
    fi
  fi

  local manifests=""
  manifests="$(render_ate_system_manifests)"
  echo "${manifests}" | run_kubectl apply -f -

  # Applied on its own rather than through the overlay above, so
  # --experimental-use-sdsmint composes with every overlay instead of needing a
  # variant of each.
  ensure_egress_mitm_ca_pool_secret
  apply_atenet_egress

  log_step "Waiting for ATE system components to be ready..."
  case "$(store_backend)" in
    redis)
      run_kubectl rollout status statefulset/valkey-cluster -n ate-system --timeout="$(rollout_timeout)"
      ;;
    postgres)
      run_kubectl rollout status statefulset/postgres -n ate-system --timeout="$(rollout_timeout)"
      ;;
  esac
  run_kubectl rollout status deployment/ate-api-server -n ate-system --timeout="$(rollout_timeout)"
  run_kubectl rollout status deployment/ate-controller -n ate-system --timeout="$(rollout_timeout)"
  run_kubectl rollout status deployment/atenet-router -n ate-system --timeout="$(rollout_timeout)"
  run_kubectl rollout status deployment/atenet-egress -n ate-system --timeout="$(rollout_timeout)"
  run_kubectl rollout status daemonset/atelet -n ate-system --timeout="$(rollout_timeout)"

  # After the bundle, which carries its own copy of ate-otel-config.
  apply_otel_endpoint_override
}

# Ensure secrets and configmaps required by ate-apiserver
ensure_apiserver_prerequisites() {
  log_step "ensure_apiserver_prerequisites"
  run_kubectl get secret -n ate-system actor-id-jwt-pool >/dev/null 2>&1 \
    || create_jwt_authority_pool_secret
  run_kubectl get secret -n ate-system actor-id-ca-pool >/dev/null 2>&1 \
    || create_actor_id_ca_pool_secret
  # Derived from actor-id-ca-pool above, so it must come after it.
  run_kubectl get secret -n ate-system actor-id-ca-certs >/dev/null 2>&1 \
    || create_actor_id_ca_certs_secret
  run_kubectl get secret -n podcertificate-controller-system service-dns-ca-pool >/dev/null 2>&1 \
    || create_podcertificate_controller_cas
  run_kubectl get secret -n ate-system valkey-ca-certs >/dev/null 2>&1 \
    || create_valkey_ca_certs_secret
  # This ConfigMap carries the selected store backend, so always reconcile it
  # to make switching --store-backend update an existing installation.
  create_api_server_env_vars
  run_kubectl get configmap -n ate-system ate-api-authentication >/dev/null 2>&1 \
    || create_api_authentication_config
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
  apply_otel_endpoint_override

  run_ko apply -f manifests/ate-install/ate-api-server.yaml
  run_kubectl rollout status deployment/ate-api-server -n ate-system --timeout="$(rollout_timeout)"
}

deploy_atelet() {
  log_step "deploy_atelet"
  ensure_crds

  # Ensure namespace exists
  run_kubectl apply -f manifests/ate-install/ate-system-namespace.yaml \
    && run_kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/ate-system --timeout=60s

  apply_otel_config
  apply_otel_endpoint_override

  local manifest=""
  if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
    # Use Kustomize to build and resolve the atelet DaemonSet patch
    manifest=$(kubectl kustomize manifests/ate-install/kind/atelet --load-restrictor LoadRestrictionsNone | run_ko resolve -f -)
  else
    # Use base manifest for GKE
    manifest=$(run_ko resolve -f manifests/ate-install/atelet.yaml)
  fi
  echo "${manifest}" | run_kubectl apply -f -
  run_kubectl rollout status daemonset/atelet -n ate-system --timeout="$(rollout_timeout)"
}

deploy_atenet() {
  log_step "deploy_atenet"
  ensure_crds

  # Ensure namespace exists
  run_kubectl apply -f manifests/ate-install/ate-system-namespace.yaml \
    && run_kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/ate-system --timeout=60s

  apply_otel_config
  apply_otel_endpoint_override

  local router_manifest=""
  router_manifest="$(render_atenet_router_manifest)"
  echo "${router_manifest}" | run_kubectl apply -f -

  ensure_egress_mitm_ca_pool_secret
  apply_atenet_egress
  run_ko apply -f manifests/ate-install/atenet-dns.yaml
  run_kubectl rollout status deployment/atenet-router -n ate-system --timeout="$(rollout_timeout)"
  run_kubectl rollout status deployment/atenet-egress -n ate-system --timeout="$(rollout_timeout)"
  run_kubectl rollout status deployment/dns -n ate-system --timeout="$(rollout_timeout)"
}

# get_actor_state echoes the actor's state enum (e.g. ACTOR_STATE_SUSPENDED).
get_actor_state() {
  local actor_name="$1"
  local atespace="$2"
  local json

  if ! json=$(run_kubectl_ate get actor "${actor_name}" -a "${atespace}" -o json 2>/dev/null); then
    return 1
  fi
  jq -r '.actors[0].status.state // empty' <<<"${json}"
}

# prepare_actor_for_delete suspends (or resumes then suspends) until DeleteActor
# is allowed. Actors must be ACTOR_STATE_SUSPENDED before deletion.
prepare_actor_for_delete() {
  local actor_name="$1"
  local atespace="$2"
  local timeout_secs="${3:-120}"
  local deadline=$((SECONDS + timeout_secs))
  local state

  while ((SECONDS < deadline)); do
    if ! state=$(get_actor_state "${actor_name}" "${atespace}"); then
      return 0
    fi

    case "${state}" in
      ACTOR_STATE_SUSPENDED)
        return 0
        ;;
      ACTOR_STATE_PAUSED)
        run_kubectl_ate resume actor "${actor_name}" -a "${atespace}" -o json >/dev/null
        ;;
      ACTOR_STATE_RUNNING)
        run_kubectl_ate suspend actor "${actor_name}" -a "${atespace}" -o json >/dev/null
        ;;
      ACTOR_STATE_RESUMING | ACTOR_STATE_SUSPENDING | ACTOR_STATE_PAUSING)
        ;;
      *)
        echo "cannot delete actor ${actor_name}: unexpected state ${state}" >&2
        return 1
        ;;
    esac
    sleep 2
  done

  echo "timed out waiting for actor ${actor_name} to reach ACTOR_STATE_SUSPENDED" >&2
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
  run_kubectl delete --ignore-not-found -f manifests/ate-install/valkey.yaml
  run_kubectl delete --ignore-not-found -f manifests/ate-install/postgres.yaml
  run_kubectl delete --ignore-not-found -f manifests/ate-install/generated
}

delete_atenet() {
  log_step "delete_atenet"
  run_kubectl delete --ignore-not-found -f manifests/ate-install/atenet-router.yaml
  run_kubectl delete --ignore-not-found \
    -f manifests/ate-install/components/agentgateway/configmap.yaml
  # Both egress variants, not the selected one: teardown has to clean up an
  # install made with --experimental-use-sdsmint whether or not this invocation
  # passes it, and either file may declare resources the other does not.
  run_kubectl delete --ignore-not-found -f manifests/ate-install/atenet-egress.yaml
  run_kubectl delete --ignore-not-found \
    -f manifests/ate-install/atenet-egress-with-sdsmint.yaml
  run_kubectl delete --ignore-not-found -f manifests/ate-install/atenet-dns.yaml
}

deploy_benchmarks() {
  log_step "deploy_benchmarks (worker_count=${BENCHMARK_WORKER_COUNT}, sandbox_class=${BENCHMARK_SANDBOX_CLASS})"
  # The microvm SandboxConfig lives outside --deploy-ate-system's default set
  # (which only installs gvisor-default); the workloads deploy references it
  # by name and would fail if we skipped this.
  if [[ "${BENCHMARK_SANDBOX_CLASS}" == "microvm" ]]; then
    "${ROOT}/hack/install-microvm-deps.sh" --install
  fi
  # Send the actor telemetry to the same place as the control plane telemetry.
  local benchmark_args=(--deploy
    --worker-count "${BENCHMARK_WORKER_COUNT}"
    --sandbox-class "${BENCHMARK_SANDBOX_CLASS}")
  if [[ -n "${ATE_OTLP_ENDPOINT:-}" ]]; then
    benchmark_args+=(--otlp-endpoint "${ATE_OTLP_ENDPOINT}")
  fi
  if [[ -n "${BENCHMARK_ACTOR_MEMORY}" ]]; then
    benchmark_args+=(--actor-memory "${BENCHMARK_ACTOR_MEMORY}")
  fi
  "${ROOT}/benchmarking/deploy_locust.sh" "${benchmark_args[@]}"
}

delete_benchmarks() {
  log_step "delete_benchmarks (sandbox_class=${BENCHMARK_SANDBOX_CLASS})"
  "${ROOT}/benchmarking/deploy_locust.sh" --delete
  # only tear down the microvm SandboxConfig if the caller opted into microvm.
  if [[ "${BENCHMARK_SANDBOX_CLASS}" == "microvm" ]]; then
    "${ROOT}/hack/install-microvm-deps.sh" --delete
  fi
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
SETUP_CSI="${SETUP_CSI:-false}"
BENCHMARK_WORKER_COUNT=1
BENCHMARK_SANDBOX_CLASS=gvisor
# Empty keeps the default in benchmarking/workloads/deploy.sh (256Mi).
BENCHMARK_ACTOR_MEMORY=""
prescan_args=("$@")
for ((i = 0; i < ${#prescan_args[@]}; i++)); do
  case "${prescan_args[i]}" in
    --atenet-router=*) ATE_ATENET_ROUTER="${prescan_args[i]#*=}" ;;
    --atenet-router)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --atenet-router requires envoy or agentgateway" >&2
        exit 1
      fi
      ATE_ATENET_ROUTER="${prescan_args[$((i + 1))]}"
      ;;
    --experimental-use-sdsmint) ATE_EXPERIMENTAL_USE_SDSMINT=true ;;
    --experimental-additional-egress-extproc-service=*)
      ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE="${prescan_args[i]#*=}"
      ;;
    --experimental-additional-egress-extproc-service)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --experimental-additional-egress-extproc-service requires <namespace>/<service>:<port>" >&2
        exit 1
      fi
      ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE="${prescan_args[$((i + 1))]}"
      ;;
    --store-backend=*) ATE_INSTALL_STORE_BACKEND="${prescan_args[i]#*=}" ;;
    --store-backend)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --store-backend requires redis or postgres" >&2
        exit 1
      fi
      ATE_INSTALL_STORE_BACKEND="${prescan_args[$((i + 1))]}"
      ;;
    --podcert-workers-per-signer=*) ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER="${prescan_args[i]#*=}" ;;
    --podcert-workers-per-signer)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --podcert-workers-per-signer requires a positive integer" >&2
        exit 1
      fi
      ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER="${prescan_args[$((i + 1))]}"
      ;;
    --rollout-timeout=*) ATE_INSTALL_ROLLOUT_TIMEOUT="${prescan_args[i]#*=}" ;;
    --rollout-timeout)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --rollout-timeout requires a Go duration (e.g. 300s, 10m)" >&2
        exit 1
      fi
      ATE_INSTALL_ROLLOUT_TIMEOUT="${prescan_args[$((i + 1))]}"
      ;;
    --benchmark-worker-count)
      BENCHMARK_WORKER_COUNT="${prescan_args[i+1]:-1}"
      ;;
    --benchmark-worker-count=*)
      BENCHMARK_WORKER_COUNT="${prescan_args[i]#*=}"
      ;;
    --benchmark-sandbox-class)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --benchmark-sandbox-class requires gvisor or microvm" >&2
        exit 1
      fi
      BENCHMARK_SANDBOX_CLASS="${prescan_args[$((i + 1))]}"
      ;;
    --benchmark-sandbox-class=*)
      BENCHMARK_SANDBOX_CLASS="${prescan_args[i]#*=}"
      ;;
    --benchmark-actor-memory)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --benchmark-actor-memory requires a size (e.g. 256Mi)" >&2
        exit 1
      fi
      BENCHMARK_ACTOR_MEMORY="${prescan_args[$((i + 1))]}"
      ;;
    --benchmark-actor-memory=*)
      BENCHMARK_ACTOR_MEMORY="${prescan_args[i]#*=}"
      ;;
    --otlp-endpoint)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --otlp-endpoint requires a URL" >&2
        exit 1
      fi
      ATE_OTLP_ENDPOINT="${prescan_args[$((i + 1))]}"
      ;;
    --otlp-endpoint=*) ATE_OTLP_ENDPOINT="${prescan_args[i]#*=}" ;;
    --setup-csi)
      SETUP_CSI=true
      ;;
  esac
done
atenet_router >/dev/null
case "${BENCHMARK_SANDBOX_CLASS}" in
  gvisor|microvm) ;;
  *)
    echo "Error: --benchmark-sandbox-class must be gvisor or microvm, got '${BENCHMARK_SANDBOX_CLASS}'" >&2
    exit 1
    ;;
esac
store_backend >/dev/null
podcert_workers_per_signer >/dev/null
rollout_timeout >/dev/null

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
    --atenet-router=*) ATE_ATENET_ROUTER="${1#*=}" ;;
    --atenet-router)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --atenet-router requires envoy or agentgateway" >&2
        exit 1
      fi
      ATE_ATENET_ROUTER="$1"
      ;;
    # Captured in the pre-scan above; matched here only so the `*)` branch does
    # not reject it as an unknown option.
    --experimental-use-sdsmint) ;;
    --experimental-additional-egress-extproc-service) shift ;;
    --experimental-additional-egress-extproc-service=*) ;;
    --store-backend=*) ATE_INSTALL_STORE_BACKEND="${1#*=}" ;;
    --store-backend)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --store-backend requires redis or postgres" >&2
        exit 1
      fi
      ATE_INSTALL_STORE_BACKEND="$1"
      ;;
    --podcert-workers-per-signer=*) ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER="${1#*=}" ;;
    --podcert-workers-per-signer)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --podcert-workers-per-signer requires a positive integer" >&2
        exit 1
      fi
      ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER="$1"
      ;;
    --rollout-timeout=*) ATE_INSTALL_ROLLOUT_TIMEOUT="${1#*=}" ;;
    --rollout-timeout)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --rollout-timeout requires a Go duration (e.g. 300s, 10m)" >&2
        exit 1
      fi
      ATE_INSTALL_ROLLOUT_TIMEOUT="$1"
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
    --benchmark-sandbox-class) shift ;;
    --benchmark-sandbox-class=*) ;;
    --benchmark-actor-memory) shift ;;
    --benchmark-actor-memory=*) ;;
    --otlp-endpoint) shift ;;
    --otlp-endpoint=*) ;;

    --create-jwt-authority-pool-secret) create_jwt_authority_pool_secret ;;
    --create-actor-id-ca-pool-secret) create_actor_id_ca_pool_secret ;;
    --create-actor-id-ca-certs-secret) create_actor_id_ca_certs_secret ;;
    --create-egress-mitm-ca-pool-secret) create_egress_mitm_ca_pool_secret ;;
    --create-podcertificate-controller-cas) create_podcertificate_controller_cas ;;
    --create-valkey-ca-certs-secret) create_valkey_ca_certs_secret ;;
    --create-api-server-env-vars) create_api_server_env_vars ;;
    --create-api-authentication-config) create_api_authentication_config ;;
    --deploy-postgres) deploy_postgres ;;

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
