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

# Stage the assembled micro-VM asset set into the kind cluster's rustfs S3 bucket
# under kata-assets/, where atelet fetches it (per demos/counter/counter-microvm.yaml.tmpl).
# Run after the cluster is up (hack/install-ate-kind.sh) and assemble.sh has produced $OUT.
#
# Requires the `aws` CLI. Env: OUT (asset dir, default ./bin/microvm-assets/arm64),
# BUCKET (default ate-snapshots), NAMESPACE (rustfs namespace, default ate-system),
# KUBECTL_CONTEXT (optional; kube context for port-forward).

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"

OUT="${OUT:-${ROOT}/bin/microvm-assets/arm64}"
BUCKET="${BUCKET:-ate-snapshots}"
NAMESPACE="${NAMESPACE:-ate-system}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-}"

if ! command -v aws >/dev/null 2>&1; then
  echo "error: the 'aws' CLI is required but was not found in PATH" >&2
  exit 1
fi

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

echo ">> Port-forwarding svc/rustfs 9000 in namespace ${NAMESPACE}..."
kubectl ${KUBECTL_CONTEXT:+--context="${KUBECTL_CONTEXT}"} -n "${NAMESPACE}" port-forward svc/rustfs 9000:9000 >/tmp/rustfs-pf.log 2>&1 &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null || true' EXIT
sleep 3

ENDPOINT="http://localhost:9000"
echo ">> Uploading assets to s3://${BUCKET}/kata-assets/ via ${ENDPOINT}..."
for f in cloud-hypervisor vmlinux rootfs.img configuration-clh.toml; do
  echo "   $f"
  aws --endpoint-url "${ENDPOINT}" s3 cp "${OUT}/${f}" "s3://${BUCKET}/kata-assets/${f}"
done

echo ">> Done. Verify:"
aws --endpoint-url "${ENDPOINT}" s3 ls "s3://${BUCKET}/kata-assets/"
