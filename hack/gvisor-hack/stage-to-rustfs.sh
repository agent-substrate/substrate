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

# Stage a locally-built runsc into the kind cluster's rustfs S3 bucket, where
# atelet fetches it (per hack/gvisor-hack/sandboxconfig.yaml.tmpl). The kind
# counterpart of stage-to-gcs.sh; usually invoked by deploy.sh rather than
# directly. Run after the cluster is up (hack/install-ate-kind.sh).
#
# Requires the `aws` CLI. Env: RUNSC (binary to stage, default ./bin/runsc),
# OBJECT (destination object key, default gvisor-hack/runsc), BUCKET (default
# ate-snapshots), NAMESPACE (rustfs namespace, default ate-system),
# KUBECTL_CONTEXT (optional; kube context for the port-forward).

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"

RUNSC="${RUNSC:-${ROOT}/bin/runsc}"
OBJECT="${OBJECT:-gvisor-hack/runsc}"
BUCKET="${BUCKET:-ate-snapshots}"
NAMESPACE="${NAMESPACE:-ate-system}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-}"

if ! command -v aws >/dev/null 2>&1; then
  echo "error: the 'aws' CLI is required but was not found in PATH" >&2
  exit 1
fi

if [[ ! -f "${RUNSC}" ]]; then
  echo "error: runsc binary not found at ${RUNSC} (set RUNSC=/path/to/runsc)" >&2
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
echo ">> Uploading ${RUNSC} to s3://${BUCKET}/${OBJECT} via ${ENDPOINT}..."
aws --endpoint-url "${ENDPOINT}" s3 cp "${RUNSC}" "s3://${BUCKET}/${OBJECT}"

echo ">> Done. Verify:"
aws --endpoint-url "${ENDPOINT}" s3 ls "s3://${BUCKET}/$(dirname "${OBJECT}")/"
