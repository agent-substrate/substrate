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

# Install the cluster-wide micro-VM (kata + cloud-hypervisor) dependencies.
# This is opt-in on top of ate-setup deploy ate-system, which only installs
# the gVisor SandboxConfig. Used by:
#   * hack/run-microvm-demo.sh (before applying the counter-microvm demo)
#   * benchmarking/automation/orchestrator.py (before deploying a microvm
#     benchmark workload)
#
# On --install: assembles the asset set (assemble.sh; skipped if OUT already
# has them), stages the assets under kata-assets/ to the cluster's object
# store bucket (rustfs on kind, GCS on GKE), and creates this release's
# SandboxConfig, substrate-<version>-microvm, from the template. The version
# is VERSION, else `git describe`, as for the gVisor config `ate-setup`
# creates. Every asset sha256 is pinned in the template, so the apply only
# substitutes the name and the bucket. With --add-default-annotation (the
# default) the config gets the sandboxconfig.ate.dev/is-class-default
# annotation.
#
# Nothing here deletes the configs. Like the gVisor configs, they stay until
# the SandboxConfig CRD is deleted with the control plane.
#
# Like the other hack scripts, this sources .ate-dev-env.sh for the cluster /
# registry / bucket settings unless NO_DEV_ENV is set.
#
# Env (most come from .ate-dev-env.sh):
#   BUCKET_NAME      object store bucket for snapshots (default: ate-snapshots). On
#                    kind the assets share it. On GKE they go to BUCKET_NAME-assets,
#                    which tools/setup-gcp creates, so nodes can read assets
#                    without being able to read snapshots.
#   KUBECTL_CONTEXT  (optional) kube context; threaded into kubectl.
#   PROJECT_ID       (optional) GCP project for the GCS asset upload (GKE path).
#   ARCH             target arch (default: from KO_DEFAULTPLATFORMS, else host arch).
#   OUT              asset dir (default: $PWD/bin/microvm-assets/$ARCH, gitignored).
#   ATE_INSTALL_KIND "true" for the kind path (stage assets to rustfs); default
#                    false uploads assets to GCS.
#   VERSION          (optional) Substrate version in the SandboxConfig name
#                    (default: git describe).

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
Usage: $0 --install [--add-default-annotation=true|false]

Options:
  --install                 Assemble + stage micro-VM assets and create this
                            release's microvm SandboxConfig,
                            substrate-<version>-microvm.
  --add-default-annotation  With --install, add the class-default annotation
                            to the config (default: true).
  -h, --help                Show this message.
EOF
}

action=""
add_default_annotation=true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --install) action="install" ;;
    --add-default-annotation|--add-default-annotation=true) add_default_annotation=true ;;
    --add-default-annotation=false) add_default_annotation=false ;;
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

# ANSI color codes for prettier output.
COLOR_CYAN='\033[1;36m'
COLOR_RESET='\033[0m'
log() {
  echo -e "${COLOR_CYAN}[install-microvm-deps]: $*${COLOR_RESET}"
}

run_kubectl() {
  kubectl ${KUBECTL_CONTEXT:+--context="${KUBECTL_CONTEXT}"} "$@"
}

MANIFEST_TEMPLATE="manifests/microvm/sandboxconfig-microvm.yaml.tmpl"

# The asset URLs' scheme: the node's object-store plugin serves only its own
# backend's, which is S3 (rustfs) on kind and GCS on GKE.
if [[ "${ATE_INSTALL_KIND}" == "true" ]]; then
  BUCKET_SCHEME="s3"
  ASSET_BUCKET_NAME="${BUCKET_NAME}"
else
  BUCKET_SCHEME="gs"
  ASSET_BUCKET_NAME="${BUCKET_NAME}-assets"
fi

DEFAULT_ANNOTATION="sandboxconfig.ate.dev/is-class-default"

# One line per microvm SandboxConfig:
# name|creationTimestamp|default annotation|deletionTimestamp.
# This fails before the slow assemble and stage if the control plane, and
# with it the SandboxConfig CRD, is not installed.
microvm_configs="$(run_kubectl get sandboxconfigs \
  -o jsonpath='{range .items[?(@.spec.sandboxClass=="microvm")]}{.metadata.name}{"|"}{.metadata.creationTimestamp}{"|"}{.metadata.annotations.sandboxconfig\.ate\.dev/is-class-default}{"|"}{.metadata.deletionTimestamp}{"\n"}{end}')"

# --- install ----------------------------------------------------------------

# Name this release's config the way `ate-setup` names the gVisor one when it
# builds from source: VERSION, else `git describe`, lower-cased, with every
# character a name cannot hold replaced by '-'. Checked here so a bad name
# fails before the stage overwrites the bucket's assets.
version="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || true)}"
version="$(tr '[:upper:]' '[:lower:]' <<<"${version:-dev}" | sed 's/[^a-z0-9.-]/-/g')"
SANDBOXCONFIG_NAME="substrate-${version}-microvm"
dns_label='[a-z0-9]([-a-z0-9]*[a-z0-9])?'
if [[ ! "${SANDBOXCONFIG_NAME}" =~ ^${dns_label}(\.${dns_label})*$ || ${#SANDBOXCONFIG_NAME} -gt 253 ]]; then
  echo "Error: version '${version}' gives the invalid SandboxConfig name ${SANDBOXCONFIG_NAME}." >&2
  exit 1
fi

# Check the config before the slow assemble and stage, as
# `ate-setup deploy sandboxconfig` checks the gVisor one:
#   * a config of this name must not be being deleted
#   * with --add-default-annotation, warn when a newer config has the
#     annotation. The newest one is the class default, so this one would not be.
#   * without it, warn when no config of the class has the annotation
# Names compare byte by byte, as atecontroller compares them.
export LC_ALL=C
existing_created=""
newest_name=""
newest_created=""
while IFS='|' read -r name created is_default deleting; do
  [[ -n "${name}" ]] || continue
  if [[ "${name}" == "${SANDBOXCONFIG_NAME}" ]]; then
    existing_created="${created}"
    if [[ -n "${deleting}" ]]; then
      echo "Error: SandboxConfig ${SANDBOXCONFIG_NAME} is being deleted; wait until it" >&2
      echo "       is gone, then run this again." >&2
      exit 1
    fi
  fi
  [[ "${is_default}" == "true" && -z "${deleting}" ]] || continue
  # RFC 3339 timestamps in UTC sort as strings.
  if [[ -z "${newest_name}" || "${created}" > "${newest_created}" ]] ||
    [[ "${created}" == "${newest_created}" && "${name}" < "${newest_name}" ]]; then
    newest_name="${name}"
    newest_created="${created}"
  fi
done <<<"${microvm_configs}"
warning=""
if [[ "${add_default_annotation}" == "false" && -z "${newest_name}" ]]; then
  warning="no microvm SandboxConfig has the ${DEFAULT_ANNOTATION} annotation, so microvm WorkerPools without a configRef resolve no config."
elif [[ "${add_default_annotation}" == "true" && -n "${existing_created}" && -n "${newest_name}" && "${newest_name}" != "${SANDBOXCONFIG_NAME}" ]] &&
  { [[ "${newest_created}" > "${existing_created}" ]] ||
    [[ "${newest_created}" == "${existing_created}" && "${newest_name}" < "${SANDBOXCONFIG_NAME}" ]]; }; then
  warning="SandboxConfig ${SANDBOXCONFIG_NAME} has the ${DEFAULT_ANNOTATION} annotation but is not the microvm class default, because ${newest_name} is newer."
fi

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

# --- 1. assets: assemble (if missing or stale) -----------------------------
need_assemble=false
for f in cloud-hypervisor virtiofsd vmlinux rootfs.img; do
  if [[ ! -f "${OUT}/${f}" ]]; then
    need_assemble=true
    break
  fi
done
# Presence alone is not enough. The filenames don't change when a version pin
# moves, so an asset dir assembled before a bump looks complete while holding the old
# bytes — we'd then stage those against a SandboxConfig pinning the new shas, and the
# mismatch would only surface at runtime as an actor wedged in STATUS_RESUMING while
# atelet rejects the download. Compare the stamp assemble.sh left against the one it
# would write now; a dir predating the stamp, or left by a failed assemble (which clears
# the stamp before overwriting anything), has no file and re-assembles.
if [[ "${need_assemble}" == "false" ]]; then
  want_stamp="$(ARCH="${ARCH}" hack/microvm-assets/assemble.sh --print-stamp)"
  if [[ "$(cat "${OUT}/.asset-versions" 2>/dev/null)" != "${want_stamp}" ]]; then
    log "Asset set in ${OUT} is stale (version stamp mismatch); re-assembling."
    need_assemble=true
  fi
fi
if [[ "${need_assemble}" == "true" ]]; then
  log "Assembling micro-VM assets into ${OUT} (ARCH=${ARCH})..."
  ARCH="${ARCH}" OUT="${OUT}" hack/microvm-assets/assemble.sh
else
  log "Assets already present in ${OUT}; skipping assemble."
fi

# --- 2. stage assets to rustfs (kind) / GCS (GKE) --------------------------
# Upload the four assets under kata-assets/, where atelet fetches them: the
# in-cluster rustfs (S3 API) on kind, or the GCS asset bucket on GKE.
if [[ "${ATE_INSTALL_KIND}" == "true" ]]; then
  log "Staging assets to in-cluster rustfs bucket ${BUCKET_NAME} (kata-assets/)..."
  OUT="${OUT}" BUCKET="${BUCKET_NAME}" KUBECTL_CONTEXT="${KUBECTL_CONTEXT}" hack/microvm-assets/stage-to-rustfs.sh
else
  log "Uploading assets to gs://${ASSET_BUCKET_NAME}/kata-assets/ ..."
  OUT="${OUT}" BUCKET="${ASSET_BUCKET_NAME}" hack/microvm-assets/stage-to-gcs.sh
fi

# --- 3. create this release's microvm SandboxConfig -------------------------
# Every asset is downloaded rather than built, so all four carry committed,
# reproducible per-arch shas and the name and the bucket's scheme and name are
# the only substitutions left.
log "Applying SandboxConfig ${SANDBOXCONFIG_NAME} from ${MANIFEST_TEMPLATE}..."
sed -e "s|\${SANDBOXCONFIG_NAME}|${SANDBOXCONFIG_NAME}|g" \
    -e "s|\${BUCKET_SCHEME}|${BUCKET_SCHEME}|g" \
    -e "s|\${ASSET_BUCKET_NAME}|${ASSET_BUCKET_NAME}|g" \
    "${MANIFEST_TEMPLATE}" \
  | run_kubectl apply -f -

# Annotated outside the template, so a later apply of the template leaves the
# annotation in place.
if [[ "${add_default_annotation}" == "true" ]]; then
  run_kubectl annotate --overwrite sandboxconfig "${SANDBOXCONFIG_NAME}" "${DEFAULT_ANNOTATION}=true"
fi
if [[ -n "${warning}" ]]; then
  echo "Warning: ${warning}" >&2
fi
log "Done. Installed SandboxConfig ${SANDBOXCONFIG_NAME}."
