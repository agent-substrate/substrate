#!/usr/bin/env bash
# Deploy the dedicated profiling worker pool and atespace.
#
#   profiling/deploy-pool.sh
#
# Run this after hack/install-ate.sh, which is what labels the nodes with
# ate.dev/substrate-version. Without those labels the workers stay Pending.
set -euo pipefail

cd "$(dirname "$0")/.."

MANIFEST="${MANIFEST:-profiling/profiling-pool.yaml.tmpl}"
NAMESPACE="${NAMESPACE:-ate-profiling}"
POOL="${POOL:-profiling}"
ATESPACE="${ATESPACE:-ate-profiling}"
# Sized for two c3-standard-4 nodes (8 vCPU, ~27Gi allocatable in total)
# shared with the ate-system control plane.
WORKER_COUNT="${WORKER_COUNT:-3}"
WORKER_CPU="${WORKER_CPU:-1}"
WORKER_MEMORY="${WORKER_MEMORY:-2Gi}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-10m}"

command -v kubectl >/dev/null || { echo "error: kubectl is required" >&2; exit 1; }

# The same derivation hack/install-ate.sh uses: the build label comes from
# `make ldflags`, and versionlabel turns it into the DNS-safe string that the
# node label and this pool's nodeSelector have to agree on.
version_raw="$(make -s ldflags | grep -o 'internal/version\.Version=[^ ]*' | head -n 1)"
version_raw="${version_raw#*internal/version.Version=}"
[[ -n "${version_raw}" ]] || { echo "error: could not read the build version from 'make ldflags'" >&2; exit 1; }
SUBSTRATE_VERSION="$(go run ./internal/versionlabel/cmd "${version_raw}" | awk '{print $1}')"
echo "substrate version: ${SUBSTRATE_VERSION}"

# ko resolves the ko:// worker image against KO_DOCKER_REPO and pushes it.
sed -e "s|\${SUBSTRATE_VERSION}|${SUBSTRATE_VERSION}|g" \
    -e "s|\${WORKER_COUNT}|${WORKER_COUNT}|g" \
    -e "s|\${WORKER_CPU}|${WORKER_CPU}|g" \
    -e "s|\${WORKER_MEMORY}|${WORKER_MEMORY}|g" \
    "${MANIFEST}" \
  | hack/run-tool.sh ko apply -f -

echo "waiting for the ${POOL} worker pool rollout (timeout ${ROLLOUT_TIMEOUT})..."
kubectl wait --for=jsonpath='{.status.readyReplicas}'="${WORKER_COUNT}" \
  "workerpool/${POOL}" -n "${NAMESPACE}" --timeout="${ROLLOUT_TIMEOUT}"

# The store rejects an ActorTemplate whose atespace does not exist, and the
# atespace is a substrate resource rather than the k8s namespace above.
# Check for it first and only create when missing, so a real failure surfaces
# the server's error instead of being swallowed as "already exists".
if ! kubectl ate get atespace "${ATESPACE}" >/dev/null 2>&1; then
  if ! kubectl ate create atespace "${ATESPACE}"; then
    echo "error: could not create atespace ${ATESPACE}" >&2
    echo "hint: 'missing bearer token' means kubectl-ate is stale." >&2
    echo "      make build-atectl && cp bin/kubectl-ate \"\$(dirname \"\$(command -v kubectl-ate)\")/\"" >&2
    exit 1
  fi
fi

kubectl get workerpool "${POOL}" -n "${NAMESPACE}"
echo "workers are spread over:"
kubectl get pods -n "${NAMESPACE}" -o custom-columns=POD:.metadata.name,NODE:.spec.nodeName --no-headers
