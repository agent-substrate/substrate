#!/usr/bin/env bash
# Install Agent Substrate on a kind cluster in JWT auth mode.
#
# Unlike the mTLS install path (hack/install-ate-kind.sh), this works on a
# stock Kubernetes cluster — no ClusterTrustBundle / PodCertificateRequest
# feature gates required. Suitable for a kind cluster created with
# KIND_ENABLE_PODCERT=false hack/create-kind-cluster.sh.
#
# Steps:
#   1. Bootstrap a self-signed Secret/ateapi-tls + ConfigMap/ateapi-ca via
#      openssl (the chart references these but expects them to exist
#      out-of-band).
#   2. Render the chart with auth.mode=jwt + kind-specific values, resolve
#      ko:// image refs against a local registry, and apply.
#   3. Apply the kind-only extras (rustfs storage, OTel collector) from
#      manifests/ate-install/kind/.
set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NS="${NS:-ate-system}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-}"
KO_DOCKER_REPO="${KO_DOCKER_REPO:-localhost:5001}"
KO_DEFAULTPLATFORMS="${KO_DEFAULTPLATFORMS:-linux/$(go env GOARCH)}"

export KO_DOCKER_REPO KO_DEFAULTPLATFORMS

run_kubectl() {
  kubectl ${KUBECTL_CONTEXT:+--context=${KUBECTL_CONTEXT}} "$@"
}

log_step() {
  echo -e "\033[1;36m[step]:\033[0m $1"
}

ensure_namespace() {
  log_step "ensure_namespace ${NS}"
  run_kubectl create namespace "${NS}" --dry-run=client -o yaml | run_kubectl apply -f -
}

# Generate a self-signed CA + server cert pair and install them as the
# Secret/ConfigMap pair the chart references. Idempotent: skips if both
# resources already exist.
bootstrap_jwt_tls() {
  log_step "bootstrap_jwt_tls"
  if run_kubectl get secret -n "${NS}" ateapi-tls >/dev/null 2>&1 \
     && run_kubectl get configmap -n "${NS}" ateapi-ca >/dev/null 2>&1; then
    echo "Secret/ateapi-tls and ConfigMap/ateapi-ca already present — skipping."
    return
  fi

  local tmp
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' RETURN

  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -subj "/CN=ateapi-ca" \
    -keyout "${tmp}/ca.key" -out "${tmp}/ca.crt" >/dev/null 2>&1

  openssl req -newkey rsa:2048 -nodes \
    -subj "/CN=api.${NS}.svc" \
    -keyout "${tmp}/server.key" -out "${tmp}/server.csr" >/dev/null 2>&1

  cat > "${tmp}/server.ext" <<EOF
subjectAltName = DNS:api.${NS}.svc,DNS:api.${NS}.svc.cluster.local,DNS:atenet-router.${NS}.svc
EOF
  openssl x509 -req -in "${tmp}/server.csr" -CA "${tmp}/ca.crt" -CAkey "${tmp}/ca.key" \
    -CAcreateserial -out "${tmp}/server.crt" -days 365 \
    -extfile "${tmp}/server.ext" >/dev/null 2>&1

  run_kubectl -n "${NS}" create secret tls ateapi-tls \
    --cert="${tmp}/server.crt" --key="${tmp}/server.key" \
    --dry-run=client -o yaml | run_kubectl apply -f -

  run_kubectl -n "${NS}" create configmap ateapi-ca \
    --from-file=ca.crt="${tmp}/ca.crt" \
    --dry-run=client -o yaml | run_kubectl apply -f -
}

apply_chart() {
  log_step "apply_chart (helm template | ko resolve | kubectl apply)"
  local rendered
  rendered=$(helm template substrate "${ROOT}/charts/substrate" \
    --namespace "${NS}" \
    -f "${ROOT}/hack/values-kind-jwt.yaml")

  # ko resolve replaces ko:// refs with built+pushed image refs.
  echo "${rendered}" | bash "${ROOT}/hack/run-tool.sh" ko resolve -f - \
    | run_kubectl apply -f -
}

apply_kind_extras() {
  log_step "apply_kind_extras (rustfs + otel-collector)"
  run_kubectl apply -f "${ROOT}/manifests/ate-install/kind/rustfs.yaml"
  run_kubectl apply -f "${ROOT}/manifests/ate-install/kind/otel-collector.yaml"
}

wait_rollouts() {
  log_step "wait_rollouts"
  run_kubectl -n "${NS}" rollout status deployment/ate-api-server-deployment --timeout=180s
  run_kubectl -n "${NS}" rollout status deployment/ate-controller --timeout=180s
  run_kubectl -n "${NS}" rollout status deployment/atenet-router --timeout=180s
  run_kubectl -n "${NS}" rollout status daemonset/atelet --timeout=180s
  run_kubectl -n "${NS}" rollout status statefulset/valkey-cluster --timeout=180s
}

ensure_namespace
bootstrap_jwt_tls
apply_chart
apply_kind_extras
wait_rollouts

echo "Substrate (JWT mode) installed in namespace ${NS}."
