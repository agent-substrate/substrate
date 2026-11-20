#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
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

# Prepare the bundled development database and connection Secrets for Helm.
# The pod-certificate controller and its CA pools must already be installed.
set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
kind=false
case "${1:-}" in
  --kind) kind=true; shift ;;
  -h|--help)
    echo "Usage: $0 [--kind]"
    echo "Deploy and bootstrap development PostgreSQL in ate-system for Helm."
    echo "Requires the pod-certificate controller and its CA pools."
    echo "KUBECTL_CONTEXT optionally selects the target cluster."
    exit 0
    ;;
esac
if [[ $# -ne 0 ]]; then
  echo "Usage: $0 [--kind]" >&2
  exit 1
fi

KUBECTL=(kubectl)
if [[ -n "${KUBECTL_CONTEXT:-}" ]]; then
  KUBECTL+=(--context "${KUBECTL_CONTEXT}")
fi
"${KUBECTL[@]}" create namespace ate-system --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
if [[ "${kind}" == true ]]; then
  "${KUBECTL[@]}" kustomize --load-restrictor=LoadRestrictionsNone "${ROOT}/manifests/ate-install/kind/postgres" | "${KUBECTL[@]}" apply -f -
else
  "${KUBECTL[@]}" apply -f "${ROOT}/manifests/ate-install/postgres/postgres.yaml"
fi
"${KUBECTL[@]}" -n ate-system rollout status statefulset/postgres --timeout=5m
"${KUBECTL[@]}" -n ate-system exec -i postgres-0 -c postgres -- \
  psql --no-psqlrc --set=ON_ERROR_STOP=1 --single-transaction --username=postgres --dbname=atepg \
  --set=substrate_schema=substrate \
  --set=substrate_owner_role=substrate_owner \
  --set=substrate_owner_user=substrate_owner_user \
  --set=substrate_owner_password="" \
  --set=substrate_readwrite_role=substrate_readwrite \
  --set=substrate_readwrite_user=substrate_readwrite_user \
  --set=substrate_readwrite_password="" < "${ROOT}/pkg/postgressetup/setup.sql"

tls='sslmode=verify-full&sslrootcert=/run/servicedns.podcert.ate.dev/trust-bundle.pem'
"${KUBECTL[@]}" -n ate-system create secret generic substrate-postgres-readwrite \
  --from-literal="readWriteConnectionString=postgresql://substrate_readwrite_user@postgres.ate-system.svc:5432/atepg?${tls}&sslcert=/run/postgres.podcert.ate.dev/substrate_readwrite_user.pem&sslkey=/run/postgres.podcert.ate.dev/substrate_readwrite_user.pem" \
  --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
"${KUBECTL[@]}" -n ate-system create secret generic substrate-postgres-owner \
  --from-literal="ownerConnectionString=postgresql://substrate_owner_user@postgres.ate-system.svc:5432/atepg?${tls}&sslcert=/run/postgres.podcert.ate.dev/substrate_owner_user.pem&sslkey=/run/postgres.podcert.ate.dev/substrate_owner_user.pem" \
  --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
