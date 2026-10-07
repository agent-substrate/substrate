#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
STATE="${HOME}/.local/share/substrate-lab"
KUBECONFIG="${STATE}/kubeconfig.yaml"
export KUBECONFIG

if [[ -z "${SUBSTRATE_LAB_POSTGRES_PASSWORD:-}" ]]; then
  echo "SUBSTRATE_LAB_POSTGRES_PASSWORD must be configured as an Amp App secret" >&2
  exit 1
fi
if [[ "${SUBSTRATE_LAB_POSTGRES_PASSWORD}" == *$'\n'* || "${SUBSTRATE_LAB_POSTGRES_PASSWORD}" == *$'\r'* ]]; then
  echo "SUBSTRATE_LAB_POSTGRES_PASSWORD must not contain a newline" >&2
  exit 1
fi

mkdir -p "${STATE}/tls"
chmod 0700 "${STATE}" "${STATE}/tls"

until sudo /usr/local/bin/k3s kubectl get --raw=/readyz >/dev/null 2>&1; do
  sleep 2
done
sudo cat /etc/rancher/k3s/k3s.yaml >"${KUBECONFIG}"
chmod 0600 "${KUBECONFIG}"

kubectl() {
  /usr/local/bin/k3s kubectl --kubeconfig "${KUBECONFIG}" "$@"
}

kubectl create namespace ate-system --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "${ROOT}/manifests/ate-install/generated/ate.dev_workerpools.yaml"
kubectl apply -f "${ROOT}/manifests/ate-install/generated/ate.dev_sandboxconfigs.yaml"
kubectl apply -f "${ROOT}/manifests/ate-install/generated/ate.dev_csidriverconfigs.yaml"
kubectl apply -f "${ROOT}/manifests/ate-install/generated/role.yaml"
kubectl apply -f "${ROOT}/hack/obelisk/substrate-lab.yaml"

if ! kubectl -n ate-system get secret actor-id-jwt-pool >/dev/null 2>&1; then
  "${ROOT}/bin/ate-setup" --no-dev-env --kubeconfig "${KUBECONFIG}" create jwt-authority-pool
fi
if ! kubectl -n ate-system get secret actor-id-ca-pool >/dev/null 2>&1; then
  "${ROOT}/bin/ate-setup" --no-dev-env --kubeconfig "${KUBECONFIG}" create actor-id-ca-pool
fi

if [[ ! -s "${STATE}/tls/ca.crt" ]]; then
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj '/CN=substrate-lab-ca' -days 3650 \
    -keyout "${STATE}/tls/ca.key" -out "${STATE}/tls/ca.crt" >/dev/null 2>&1
  openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj '/CN=api.ate-system.svc' \
    -keyout "${STATE}/tls/server.key" -out "${STATE}/tls/server.csr" >/dev/null 2>&1
  printf '%s\n' 'subjectAltName=DNS:api.ate-system.svc,DNS:api.ate-system.svc.cluster.local' \
    'extendedKeyUsage=serverAuth' >"${STATE}/tls/server.ext"
  openssl x509 -req -in "${STATE}/tls/server.csr" -CA "${STATE}/tls/ca.crt" \
    -CAkey "${STATE}/tls/ca.key" -CAcreateserial -days 825 \
    -extfile "${STATE}/tls/server.ext" -out "${STATE}/tls/server.crt" >/dev/null 2>&1
  openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj '/CN=ate-controller' \
    -keyout "${STATE}/tls/client.key" -out "${STATE}/tls/client.csr" >/dev/null 2>&1
  printf '%s\n' 'subjectAltName=URI:spiffe://cluster.local/ns/ate-system/sa/ate-controller' \
    'extendedKeyUsage=clientAuth' >"${STATE}/tls/client.ext"
  openssl x509 -req -in "${STATE}/tls/client.csr" -CA "${STATE}/tls/ca.crt" \
    -CAkey "${STATE}/tls/ca.key" -CAcreateserial -days 825 \
    -extfile "${STATE}/tls/client.ext" -out "${STATE}/tls/client.crt" >/dev/null 2>&1
  cat "${STATE}/tls/server.crt" "${STATE}/tls/server.key" >"${STATE}/tls/server-bundle.pem"
  cat "${STATE}/tls/client.crt" "${STATE}/tls/client.key" >"${STATE}/tls/client-bundle.pem"
  chmod 0600 "${STATE}/tls/"*.key "${STATE}/tls/"*-bundle.pem
fi

kubectl -n ate-system create secret generic ate-api-tls \
  --from-file=credential-bundle.pem="${STATE}/tls/server-bundle.pem" \
  --from-file=ca.crt="${STATE}/tls/ca.crt" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ate-system create secret generic ate-controller-tls \
  --from-file=credential-bundle.pem="${STATE}/tls/client-bundle.pem" \
  --from-file=ca.crt="${STATE}/tls/ca.crt" \
  --dry-run=client -o yaml | kubectl apply -f -

db_env="$(mktemp "${STATE}/postgres.XXXXXX")"
trap 'rm -f "${db_env}"' EXIT
chmod 0600 "${db_env}"
{
  printf 'POSTGRES_PASSWORD=%s\n' "${SUBSTRATE_LAB_POSTGRES_PASSWORD}"
  printf 'ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING=postgresql://postgres:%s@postgres.ate-system.svc:5432/atepg?sslmode=disable\n' "${SUBSTRATE_LAB_POSTGRES_PASSWORD}"
  printf 'ATE_API_POSTGRES_OWNER_CONNECTION_STRING=postgresql://postgres:%s@postgres.ate-system.svc:5432/atepg?sslmode=disable\n' "${SUBSTRATE_LAB_POSTGRES_PASSWORD}"
} >"${db_env}"
kubectl -n ate-system create secret generic substrate-lab-database \
  --from-env-file="${db_env}" --dry-run=client -o yaml | kubectl apply -f -
rm -f "${db_env}"
trap - EXIT
unset SUBSTRATE_LAB_POSTGRES_PASSWORD

kubectl apply -f "${ROOT}/hack/obelisk/substrate-lab.yaml"
kubectl -n ate-system rollout status statefulset/postgres --timeout=5m
kubectl -n ate-system rollout status deployment/ate-api-server --timeout=5m
kubectl -n ate-system rollout status deployment/ate-controller --timeout=5m

exec python3 "${ROOT}/hack/obelisk/status.py"
