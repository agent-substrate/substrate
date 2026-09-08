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
# under kata-assets/, where atelet fetches it (per manifests/microvm/sandboxconfig-microvm.yaml.tmpl).
# Run after the cluster is up (hack/install-ate-kind.sh) and assemble.sh has produced $OUT.
#
# The S3 client runs in a throwaway container (the same pinned amazon/aws-cli
# image as the rustfs-bucket-init Job), so developers don't need the `aws` CLI
# installed. The container joins the kind node's network namespace, which is what
# makes rustfs's ClusterIP routable: a container merely attached to the `kind`
# docker network can reach the node's own IP but has no route to the service or
# pod CIDRs. That also avoids a `kubectl port-forward`, which silently uploads
# nothing when it targets the wrong cluster, and which a container can't reach at
# all on macOS (the container's localhost is the Docker VM, not the host).
#
# Env: OUT (asset dir, default ./bin/microvm-assets/arm64), BUCKET (default
# ate-snapshots), NAMESPACE (rustfs namespace, default ate-system),
# KUBECTL_CONTEXT (optional; kube context), KIND_CLUSTER_NAME (default: derived
# from KUBECTL_CONTEXT, else "kind").

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"

OUT="${OUT:-${ROOT}/bin/microvm-assets/arm64}"
BUCKET="${BUCKET:-ate-snapshots}"
NAMESPACE="${NAMESPACE:-ate-system}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-}"
# kind contexts are named kind-<cluster>; fall back to kind's own default.
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-${KUBECTL_CONTEXT#kind-}}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"

# Keep in sync with the rustfs-bucket-init Job in
# manifests/ate-install/kind/rustfs.yaml, which creates the bucket we upload into.
# 2.17's botocore rejected a bracketed IPv6 endpoint, which is all this script can build.
AWS_CLI_IMAGE="amazon/aws-cli:2.31.0@sha256:3b018ce74732c98acf6f1de59b3a89587cb7f9eb6ea0d1447d1779091b2bf057"

ASSETS=(cloud-hypervisor virtiofsd vmlinux rootfs.img configuration-clh.toml)

run_kubectl() {
  kubectl ${KUBECTL_CONTEXT:+--context="${KUBECTL_CONTEXT}"} -n "${NAMESPACE}" "$@"
}

if ! command -v docker >/dev/null 2>&1; then
  echo "error: 'docker' is required to run the S3 client but was not found in PATH" >&2
  exit 1
fi

for f in "${ASSETS[@]}"; do
  if [[ ! -f "${OUT}/${f}" ]]; then
    echo "error: missing asset ${OUT}/${f}; run hack/microvm-assets/assemble.sh first" >&2
    exit 1
  fi
done

# rustfs must be serving and the bucket must exist before anything is uploaded.
echo ">> Waiting for rustfs in namespace ${NAMESPACE}..."
run_kubectl rollout status deploy/rustfs --timeout=300s
run_kubectl wait --for=condition=Complete job/rustfs-bucket-init --timeout=300s

NODE="$("${ROOT}/hack/kind.sh" get nodes --name "${KIND_CLUSTER_NAME}" | head -n1)"
if [[ -z "${NODE}" ]]; then
  echo "error: no nodes found for kind cluster '${KIND_CLUSTER_NAME}'" >&2
  exit 1
fi
RUSTFS_IP="$(run_kubectl get svc rustfs -o jsonpath='{.spec.clusterIP}')"
# A v6 ClusterIP needs brackets to be a URL.
if [[ "${RUSTFS_IP}" == *:* ]]; then
  RUSTFS_IP="[${RUSTFS_IP}]"
fi
ENDPOINT="http://${RUSTFS_IP}:9000"

echo ">> Uploading assets to s3://${BUCKET}/kata-assets/ via ${ENDPOINT} (netns of ${NODE})..."
aws_cli() {
  # -i (no -t): a TTY would corrupt the binary stream piped in on stdin.
  docker run --rm -i \
    --network "container:${NODE}" \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}" \
    -e AWS_REGION="${AWS_REGION:-us-east-1}" \
    -e AWS_ENDPOINT_URL="${ENDPOINT}" \
    "${AWS_CLI_IMAGE}" "$@"
}

for f in "${ASSETS[@]}"; do
  echo "   $f"
  aws_cli s3 cp - "s3://${BUCKET}/kata-assets/${f}" < "${OUT}/${f}"
done

echo ">> Done. Verify:"
aws_cli s3 ls "s3://${BUCKET}/kata-assets/" < /dev/null
