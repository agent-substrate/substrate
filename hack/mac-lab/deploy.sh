#!/bin/bash
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

# Runs only inside the private Linux machine; never on the Mac.
set -euo pipefail
test "$(uname -s)" = Linux
test -f /opt/substrate/mac-lab-marker
cd /opt/substrate
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl() { /usr/local/bin/k3s kubectl "$@"; }
for attempt in $(seq 1 120); do
  if kubectl get --raw=/readyz >/dev/null 2>&1; then break; fi
  sleep 2
done
kubectl get --raw=/readyz
kubectl create namespace ate-system --dry-run=client -o yaml | kubectl apply -f -
kubectl kustomize /opt/substrate/hack/mac-lab --load-restrictor LoadRestrictionsNone > /opt/substrate/rendered.yaml
# The first application creates the CRDs; SandboxConfig needs them established.
for resource in workerpools sandboxconfigs csidriverconfigs; do
  kubectl apply -f "/opt/substrate/manifests/ate-install/generated/ate.dev_${resource}.yaml"
  kubectl wait --for=condition=Established "crd/${resource}.ate.dev" --timeout=60s
done
kubectl apply -f /opt/substrate/rendered.yaml
for pool in jwt-authority-pool actor-id-ca-pool; do
  secret=actor-id-ca-pool
  if [[ "$pool" = jwt-authority-pool ]]; then secret=actor-id-jwt-pool; fi
  if ! kubectl -n ate-system get secret "$secret" >/dev/null 2>&1; then
    /opt/substrate/bin/ate-setup --no-dev-env --kubeconfig "$KUBECONFIG" create "$pool"
  fi
done
for role in api controller; do
  identity=server
  if [[ "$role" = controller ]]; then identity=client; fi
  kubectl -n ate-system create secret generic "ate-${role}-tls" \
    --from-file=credential-bundle.pem="/opt/substrate/tls/${identity}-bundle.pem" \
    --from-file=ca.crt=/opt/substrate/tls/ca.crt \
    --dry-run=client -o yaml | kubectl apply -f -
done
kubectl -n ate-system create secret generic host-runtime-client \
  --from-file=client.crt=/opt/substrate/tls/host-client.crt \
  --from-file=client.key=/opt/substrate/tls/host-client.key \
  --from-file=ca.crt=/opt/substrate/tls/host-ca.crt \
  --dry-run=client -o yaml | kubectl apply -f -
if ! kubectl -n ate-system get secret substrate-lab-database >/dev/null 2>&1; then
  umask 077
  password=$(openssl rand -hex 32)
  envfile=$(mktemp)
  trap 'rm -f "$envfile"' EXIT
  printf 'POSTGRES_PASSWORD=%s\n' "$password" > "$envfile"
  for role in READ_WRITE OWNER; do
    printf 'ATE_API_POSTGRES_%s_CONNECTION_STRING=postgresql://postgres:%s@postgres.ate-system.svc:5432/atepg?sslmode=disable\n' "$role" "$password" >> "$envfile"
  done
  kubectl -n ate-system create secret generic substrate-lab-database --from-env-file="$envfile"
  rm -f "$envfile"
  unset password
  trap - EXIT
fi
# hostPath mounts retain the previous binary inode until pods are replaced.
kubectl -n ate-system rollout restart deployment/ate-api-server deployment/ate-controller
kubectl -n ate-system rollout status statefulset/postgres --timeout=5m
kubectl -n ate-system rollout status deployment/ate-api-server --timeout=5m
kubectl -n ate-system rollout status deployment/ate-controller --timeout=5m
kubectl -n ate-system get pods,pvc
