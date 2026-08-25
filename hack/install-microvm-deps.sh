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

# Install (or delete) the cluster-wide micro-VM (kata + cloud-hypervisor)
# dependencies. This is opt-in on top of hack/install-ate.sh --deploy-ate-system,
# which only installs the default gVisor SandboxConfig. Used by:
#   * hack/run-microvm-demo.sh (before applying the counter-microvm demo)
#   * benchmarking/automation/orchestrator.py (before deploying a microvm
#     benchmark workload)
#
# On --install: assembles the asset set (assemble.sh; skipped if OUT already
# has them), stages the assets under kata-assets/ to the cluster's object
# store bucket (rustfs on kind, GCS on GKE), and applies the cluster-wide
# `microvm` SandboxConfig referencing those assets. The virtiofsd
# sha256 is computed from the staged binary and injected at apply time (on
# arm64 the v1.14.0 binary is built from source, so its bytes vary per
# toolchain and cannot be pinned in the manifest).
#
# WorkerPools must reference the SandboxConfig explicitly via
# sandboxConfigName: microvm. This avoids a dirty teardown silently binding
# new pools to a stale config.
#
# On --delete: removes the SandboxConfig from the cluster. Bucket contents
# are left alone (they're inert until a SandboxConfig points at them, and
# re-staging is cheap on next install).
#
# Like the other hack scripts, this sources .ate-dev-env.sh for the cluster /
# registry / bucket settings unless NO_DEV_ENV is set.
#
# Env (most come from .ate-dev-env.sh):
#   BUCKET_NAME      object store bucket for assets/snapshots (default: ate-snapshots).
#   KUBECTL_CONTEXT  (optional) kube context; threaded into kubectl.
#   PROJECT_ID       (optional) GCP project for the GCS asset upload (GKE path).
#   ARCH             target arch (default: from KO_DEFAULTPLATFORMS, else host arch).
#   OUT              asset dir (default: $PWD/bin/microvm-assets/$ARCH, gitignored).
#   ATE_INSTALL_KIND "true" for the kind path (stage assets to rustfs); default
#                    false uploads assets to GCS.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment (cluster, registry, bucket) like the other hack scripts;
# callers set NO_DEV_ENV to skip this and use kind defaults.
if [[ -r .ate-dev-env.sh ]] && [[ -z "${NO_DEV_ENV:-}" ]]; then
  source .ate-dev-env.sh
fi

BUCKET_NAME="${BUCKET_NAME:-ate-snapshots}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-}"
ATE_INSTALL_KIND="${ATE_INSTALL_KIND:-false}"

usage() {
  cat <<EOF
Usage: $0 (--install | --delete)

Options:
  --install   Assemble + stage micro-VM assets and apply the cluster-wide
              microvm SandboxConfig.
  --delete    Remove the microvm SandboxConfig from the cluster.
              (Bucket contents are left alone.)
  -h, --help  Show this message.
EOF
}

action=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --install) action="install" ;;
    --delete)  action="delete"  ;;
    -h|--help) usage; exit 0    ;;
    *) echo "Error: unknown argument $1" >&2; usage; exit 1 ;;
  esac
  shift
done

if [[ -z "${action}" ]]; then
  usage
  exit 1
fi

# kubectl falls back to localhost:8080 when neither --context nor a kubeconfig
# current-context is set, which surfaces mid-install as a confusing "connection
# refused" from the apply -- after the assets have already been assembled and
# staged. Resolve the target cluster up front instead.
if [[ -z "${KUBECTL_CONTEXT}" ]] && ! kubectl config current-context >/dev/null 2>&1; then
  echo "Error: no kube context to target: KUBECTL_CONTEXT is empty and the" >&2
  echo "       kubeconfig has no current-context." >&2
  echo "       Set KUBECTL_CONTEXT (e.g. in .ate-dev-env.sh) or run:" >&2
  echo "         kubectl config use-context <name>" >&2
  exit 1
fi

# ANSI color codes for prettier output (mirrors hack/install-ate.sh).
COLOR_CYAN='\033[1;36m'
COLOR_RESET='\033[0m'
log() {
  echo -e "${COLOR_CYAN}[install-microvm-deps]: $*${COLOR_RESET}"
}

run_kubectl() {
  kubectl ${KUBECTL_CONTEXT:+--context="${KUBECTL_CONTEXT}"} "$@"
}

MANIFEST_TEMPLATE="manifests/microvm/sandboxconfig-microvm.yaml.tmpl"

if [[ "${action}" == "delete" ]]; then
  log "Deleting microvm SandboxConfig..."
  # Delete by name rather than by manifest so a missing/edited template file
  # doesn't block cleanup on an older cluster.
  run_kubectl delete --ignore-not-found sandboxconfig microvm
  log "Done. (Bucket assets at gs://${BUCKET_NAME}/kata-assets/ left in place.)"
  exit 0
fi

# --- install ----------------------------------------------------------------

# Target arch: match the images' platform (KO_DEFAULTPLATFORMS is set by
# .ate-dev-env.sh on GKE and by the kind wrapper); fall back to the host arch.
if [[ -z "${ARCH:-}" ]]; then
  if [[ -n "${KO_DEFAULTPLATFORMS:-}" ]]; then
    ARCH="${KO_DEFAULTPLATFORMS##*/}"
  else
    ARCH="$(go env GOARCH)"
  fi
fi
OUT="${OUT:-${ROOT}/bin/microvm-assets/$ARCH}"

# --- 1. assets: assemble (if missing) --------------------------------------
need_assemble=false
for f in cloud-hypervisor virtiofsd vmlinux rootfs.img configuration-clh.toml; do
  if [[ ! -f "${OUT}/${f}" ]]; then
    need_assemble=true
    break
  fi
done
if [[ "${need_assemble}" == "true" ]]; then
  log "Assembling micro-VM assets into ${OUT} (ARCH=${ARCH})..."
  ARCH="${ARCH}" OUT="${OUT}" hack/microvm-assets/assemble.sh
else
  log "Assets already present in ${OUT}; skipping assemble."
fi

# --- 2. stage assets to rustfs (kind) / GCS (GKE) --------------------------
# Upload the five assets under kata-assets/, where atelet fetches them: the
# in-cluster rustfs (S3 API) on kind, or the GCS bucket on GKE.
if [[ "${ATE_INSTALL_KIND}" == "true" ]]; then
  log "Staging assets to in-cluster rustfs bucket ${BUCKET_NAME} (kata-assets/)..."
  OUT="${OUT}" BUCKET="${BUCKET_NAME}" KUBECTL_CONTEXT="${KUBECTL_CONTEXT}" hack/microvm-assets/stage-to-rustfs.sh
else
  log "Uploading assets to gs://${BUCKET_NAME}/kata-assets/ ..."
  OUT="${OUT}" BUCKET="${BUCKET_NAME}" hack/microvm-assets/stage-to-gcs.sh
fi

# --- 3. apply the cluster-wide microvm SandboxConfig -----------------------
# The arm64 virtiofsd is built from source (release tag in assemble.sh), so
# its binary bytes are not reproducible across toolchains and its sha can't
# be a fixed pin in the manifest. Compute it from the freshly-staged binary
# and inject it, so the deployed SandboxConfig always matches whatever was
# staged. The downloaded assets (cloud-hypervisor/kernel/rootfs/config, plus
# virtiofsd on amd64 where upstream publishes a prebuilt) keep their
# committed, reproducible per-arch shas.
log "Applying microvm SandboxConfig from ${MANIFEST_TEMPLATE}..."
VIRTIOFSD_SHA256="$(sha256sum "${OUT}/virtiofsd" | awk '{print $1}')"
sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
    -e "s|\${VIRTIOFSD_SHA256}|${VIRTIOFSD_SHA256}|g" \
    "${MANIFEST_TEMPLATE}" \
  | run_kubectl apply -f -

log "Done. WorkerPools must reference this SandboxConfig by name (sandboxConfigName: microvm)."
