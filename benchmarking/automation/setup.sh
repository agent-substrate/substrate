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

# Interactive wizard:
#  1. Use the repo's .ate-dev-env.sh as the authoritative source for the
#     target cluster's environment. The script snapshots it to
#     scratch/target-clusters/<name>.sh; edit .ate-dev-env.sh and re-run
#     to refresh that snapshot. Re-run with a different name (after editing
#     .ate-dev-env.sh again if needed) to add another target cluster.
#  2. Build & push the orchestrator image to the orchestrator GCP project's
#     registry (the only value not derivable from .ate-dev-env.sh).
#  3. Generate three independent manifests in scratch/ so each can be
#     re-applied without touching the others:
#       - scratch/cronjob.yaml          (Namespace + ServiceAccount + CronJob)
#       - scratch/test-list.yaml        (substrate-benchmark-tests ConfigMap)
#       - scratch/target-clusters.yaml  (substrate-benchmark-target-clusters ConfigMap)
#
# Re-running this script overwrites those three files and overwrites
# scratch/target-clusters/<name>.sh for the prompted name, but leaves other
# files in scratch/target-clusters/ intact.
#
# TODO: drive workload-cluster creation and IAM/Workload Identity bindings
# from tools/setup-gcp (see tools/setup-gcp/cmd/cluster.go) instead of leaving
# them as manual gcloud steps in README.md. That tool already encodes the
# required beta APIs, Workload Identity pool, and node config; this wizard
# should call it (or its equivalent in tools/setup-gcp) once we know the
# values collected below.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
AUTOMATION_DIR="${ROOT}/benchmarking/automation"
SCRATCH_DIR="${AUTOMATION_DIR}/scratch"
TARGET_CLUSTERS_DIR="${SCRATCH_DIR}/target-clusters"
ATE_DEV_ENV="${ROOT}/.ate-dev-env.sh"

prompt() {
  local var_name="$1"
  local description="$2"
  local default="${3:-}"
  local value=""
  if [[ -n "${default}" ]]; then
    read -r -p "${description} [${default}]: " value
    value="${value:-${default}}"
  else
    while [[ -z "${value}" ]]; do
      read -r -p "${description}: " value
    done
  fi
  printf -v "${var_name}" '%s' "${value}"
}

echo "=== Substrate benchmark orchestrator setup ==="
echo
echo "This wizard treats ${ATE_DEV_ENV} as the authoritative source for the"
echo "target cluster's environment. Edit that file before continuing if its"
echo "values need adjusting for the target cluster you're configuring."
echo

if [[ ! -f "${ATE_DEV_ENV}" ]]; then
  echo "ERROR: ${ATE_DEV_ENV} not found." >&2
  echo "Create it (see the example in the repo root) before running this script." >&2
  exit 1
fi

prompt TARGET_CLUSTER_NAME "Target cluster name (e.g. dev, prod) — tests.yaml entries select by this" "dev"
prompt ORCHESTRATOR_PROJECT_ID "GCP project ID hosting the orchestrator image registry"

ORCHESTRATOR_KO_REPO="gcr.io/${ORCHESTRATOR_PROJECT_ID}/ate-images"
IMAGE_TAG="$(git -C "${ROOT}" rev-parse --short HEAD)"
if ! git -C "${ROOT}" diff --quiet HEAD -- benchmarking/automation; then
  IMAGE_TAG="${IMAGE_TAG}-dirty"
fi
ORCHESTRATOR_IMAGE="${ORCHESTRATOR_KO_REPO}/substrate-benchmark-orchestrator:${IMAGE_TAG}"

echo
echo "Will snapshot ${ATE_DEV_ENV} as target cluster '${TARGET_CLUSTER_NAME}'"
echo "and build orchestrator image: ${ORCHESTRATOR_IMAGE}"
echo

mkdir -p "${TARGET_CLUSTERS_DIR}"

# Snapshot the user's .ate-dev-env.sh as the per-cluster env file. The
# orchestrator copies this back into the cloned substrate repo as
# .ate-dev-env.sh at the start of each test that references
# "targetCluster: ${TARGET_CLUSTER_NAME}" in tests.yaml.
cp "${ATE_DEV_ENV}" "${TARGET_CLUSTERS_DIR}/${TARGET_CLUSTER_NAME}.sh"
echo "Snapshotted ${ATE_DEV_ENV} -> ${TARGET_CLUSTERS_DIR}/${TARGET_CLUSTER_NAME}.sh"

echo
echo "Building ${ORCHESTRATOR_IMAGE}..."
docker build -t "${ORCHESTRATOR_IMAGE}" "${AUTOMATION_DIR}"
echo "Pushing ${ORCHESTRATOR_IMAGE}..."
docker push "${ORCHESTRATOR_IMAGE}"

# 1. cronjob.yaml: the durable infrastructure (Namespace + SA + CronJob).
export ORCHESTRATOR_IMAGE  # envsubst reads from the environment
# shellcheck disable=SC2016  # envsubst's allow-list arg must be the literal var name
envsubst '${ORCHESTRATOR_IMAGE}' \
  < "${AUTOMATION_DIR}/manifests/cronjob.yaml.tmpl" \
  > "${SCRATCH_DIR}/cronjob.yaml"
echo "Wrote ${SCRATCH_DIR}/cronjob.yaml"

# 2. test-list.yaml: tests ConfigMap. Update + re-apply this file alone to
# change the test list without touching the CronJob or target clusters.
kubectl create configmap substrate-benchmark-tests \
    --namespace=substrate-benchmark \
    --from-file=tests.yaml="${AUTOMATION_DIR}/tests.yaml" \
    --dry-run=client -o yaml \
    > "${SCRATCH_DIR}/test-list.yaml"
echo "Wrote ${SCRATCH_DIR}/test-list.yaml"

# 3. target-clusters.yaml: target-clusters ConfigMap (one key per
# scratch/target-clusters/*.sh). Tests pick a key by name via the
# `targetCluster` field. Update + re-apply this file alone to change
# cluster targets without touching tests or the CronJob.
target_args=()
for f in "${TARGET_CLUSTERS_DIR}"/*.sh; do
  target_args+=(--from-file="$(basename "${f}")=${f}")
done
kubectl create configmap substrate-benchmark-target-clusters \
    --namespace=substrate-benchmark \
    "${target_args[@]}" \
    --dry-run=client -o yaml \
    > "${SCRATCH_DIR}/target-clusters.yaml"
echo "Wrote ${SCRATCH_DIR}/target-clusters.yaml"

echo
echo "=== Next steps ==="
echo "1. Edit ${SCRATCH_DIR}/cronjob.yaml and fill in the --repo / --branch / --dest args."
echo "2. If anything in ${ATE_DEV_ENV} needs to change for the target cluster,"
echo "   modify it in the config map"
echo "   ${TARGET_CLUSTERS_DIR}/${TARGET_CLUSTER_NAME}.sh and ${SCRATCH_DIR}/target-clusters.yaml."
echo "3. Apply everything to the orchestration cluster:"
echo "     kubectl --context=<orchestration-cluster> apply -f ${SCRATCH_DIR}/cronjob.yaml \\"
echo "                                                       -f ${SCRATCH_DIR}/test-list.yaml \\"
echo "                                                       -f ${SCRATCH_DIR}/target-clusters.yaml"
echo
echo "To update just the test list (no image rebuild):"
echo "  kubectl --context=<orchestration-cluster> apply -f ${SCRATCH_DIR}/test-list.yaml"
echo
echo "To update just the target-cluster configs (no image rebuild):"
echo "  kubectl --context=<orchestration-cluster> apply -f ${SCRATCH_DIR}/target-clusters.yaml"
echo
echo "To add another target cluster, append it to the config map"
