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

# One-shot bring-up of the counter-microvm demo. GKE/dev-env by default; for a
# local kind cluster use hack/run-microvm-demo-kind.sh (which sets the kind env
# and calls this script), mirroring install-ate.sh / install-ate-kind.sh.
#
# Like the other hack scripts, this sources .ate-dev-env.sh for the cluster /
# registry / bucket settings unless NO_DEV_ENV is set.
#
# The committed .ko.yaml base for cmd/ateom-microvm is debian:stable-slim, which
# lacks mkfs.ext4 (e2fsprogs). The worker needs mkfs.ext4 at runtime to build the
# actor's virtio-blk rootfs, so this script builds hack/ateom-base (debian-slim +
# e2fsprogs) and overrides ONLY that base at build time via a throwaway ko config
# pointed at by KO_CONFIG_PATH — the committed .ko.yaml is never touched.
#
# Env (most come from .ate-dev-env.sh):
#   KO_DOCKER_REPO   (required) image registry, e.g. gcr.io/PROJECT/ate-images for
#                    GKE or localhost:5001 for kind.
#   BUCKET_NAME      object store bucket for assets/snapshots (default: ate-snapshots).
#   KUBECTL_CONTEXT  (optional) kube context; threaded into install + ko apply + kubectl.
#   PROJECT_ID       (optional) GCP project for the GCS asset upload (GKE path).
#   ARCH             target arch (default: from KO_DEFAULTPLATFORMS, else host arch).
#   ATEOM_BASE_TAG   tag for the built ateom-base image (default: e2fsprogs).
#   OUT              asset dir (default: $PWD/bin/microvm-assets/$ARCH, gitignored).
#   ATE_INSTALL_KIND "true" for the kind path (stage assets to rustfs + install-ate-kind.sh);
#                    default false uploads assets to GCS + uses install-ate.sh.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment (cluster, registry, bucket) like the other hack scripts;
# hack/run-microvm-demo-kind.sh sets NO_DEV_ENV to skip this and use kind defaults.
if [[ -r .ate-dev-env.sh ]] && [[ -z "${NO_DEV_ENV:-}" ]]; then
  source .ate-dev-env.sh
fi

# --- env / defaults ---------------------------------------------------------
KO_DOCKER_REPO="${KO_DOCKER_REPO:-}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-}"
BUCKET_NAME="${BUCKET_NAME:-ate-snapshots}"
ATEOM_BASE_TAG="${ATEOM_BASE_TAG:-e2fsprogs}"
ATE_INSTALL_KIND="${ATE_INSTALL_KIND:-false}"

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

if [[ -z "${KO_DOCKER_REPO}" ]]; then
  echo "Error: KO_DOCKER_REPO is required (set it in .ate-dev-env.sh for GKE," >&2
  echo "       or use hack/run-microvm-demo-kind.sh for a local kind cluster)." >&2
  exit 1
fi
export KO_DOCKER_REPO

# ANSI color codes for prettier output (mirrors hack/install-ate.sh).
COLOR_CYAN='\033[1;36m'
COLOR_RESET='\033[0m'
log() {
  echo -e "${COLOR_CYAN}[run-microvm-demo]: $*${COLOR_RESET}"
}

ATEOM_BASE_IMAGE="${KO_DOCKER_REPO}/ateom-base:${ATEOM_BASE_TAG}"

# --- 2. build + push ateom-base (debian-slim + e2fsprogs) for the target arch -
# We build with buildx --load (import into the local docker daemon) and then
# `docker push`, NOT buildx --push: the buildkit builder runs in its own container
# and cannot reach a localhost/kind registry, whereas the docker daemon can. --load
# imports a single-platform image fine even when ARCH != the host arch. For a real
# remote registry (e.g. gcr.io) the same daemon `docker push` works with its creds.
log "Building ateom-base ${ATEOM_BASE_IMAGE} (linux/${ARCH})..."
if docker buildx version >/dev/null 2>&1; then
  log "  using: docker buildx build --load + docker push"
  docker buildx build --platform "linux/${ARCH}" -t "${ATEOM_BASE_IMAGE}" --load hack/ateom-base
else
  log "  using: docker build + docker push (buildx unavailable)"
  docker build -t "${ATEOM_BASE_IMAGE}" hack/ateom-base
fi
docker push "${ATEOM_BASE_IMAGE}"

# --- 3. throwaway ko config overriding ONLY the ateom-microvm base -----------
# KO_CONFIG_PATH points at a FILE that ko parses by extension, so it MUST end in
# .yaml (a bare mktemp file is rejected: "Unsupported Config Type"). Use a temp dir
# with a .yaml-named copy of the repo .ko.yaml and swap the one base line.
KO_CONFIG_DIR="$(mktemp -d)"
KO_CONFIG_TMP="${KO_CONFIG_DIR}/ko-override.yaml"
trap 'rm -rf "${KO_CONFIG_DIR}"' EXIT
cp "${ROOT}/.ko.yaml" "${KO_CONFIG_TMP}"

OVERRIDE_KEY="github.com/agent-substrate/substrate/cmd/ateom-microvm"
if ! grep -q "^  ${OVERRIDE_KEY}:" "${KO_CONFIG_TMP}"; then
  echo "Error: could not find the cmd/ateom-microvm baseImageOverride line in .ko.yaml" >&2
  exit 1
fi
# Replace only the value after the key (use | as the sed delimiter; the value has /).
sed -i.bak "s|^  ${OVERRIDE_KEY}:.*|  ${OVERRIDE_KEY}: ${ATEOM_BASE_IMAGE}|" "${KO_CONFIG_TMP}"
rm -f "${KO_CONFIG_TMP}.bak"
export KO_CONFIG_PATH="${KO_CONFIG_TMP}"
log "Using throwaway KO_CONFIG_PATH=${KO_CONFIG_PATH} (ateom-microvm base -> ${ATEOM_BASE_IMAGE})"

# --- 4. assets: assemble (if missing) then stage to rustfs (kind) / GCS (GKE) --
need_assemble=false
for f in cloud-hypervisor vmlinux rootfs.img configuration-clh.toml; do
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

# Upload the four assets under kata-assets/, where atelet fetches them: the
# in-cluster rustfs (port-forwarded, S3 API) on kind, or the GCS bucket on GKE.
if [[ "${ATE_INSTALL_KIND}" == "true" ]]; then
  log "Staging assets to in-cluster rustfs bucket ${BUCKET_NAME} (kata-assets/)..."
  OUT="${OUT}" BUCKET="${BUCKET_NAME}" KUBECTL_CONTEXT="${KUBECTL_CONTEXT}" hack/microvm-assets/stage-to-rustfs.sh
else
  log "Uploading assets to gs://${BUCKET_NAME}/kata-assets/ ..."
  OUT="${OUT}" BUCKET="${BUCKET_NAME}" hack/microvm-assets/stage-to-gcs.sh
fi

# --- 5. deploy the control plane --------------------------------------------
log "Deploying the ate control plane (--deploy-ate-system)..."
if [[ "${ATE_INSTALL_KIND}" == "true" ]]; then
  # install-ate-kind.sh sets NO_DEV_ENV/KO_DOCKER_REPO/ARCH/ATE_INSTALL_KIND itself.
  KUBECTL_CONTEXT="${KUBECTL_CONTEXT}" hack/install-ate-kind.sh --deploy-ate-system
else
  # GKE path: pass KO_DOCKER_REPO/BUCKET_NAME/KUBECTL_CONTEXT through the env.
  KUBECTL_CONTEXT="${KUBECTL_CONTEXT}" hack/install-ate.sh --deploy-ate-system
fi

# --- 6. apply the demo ------------------------------------------------------
# Use ./hack/run-tool.sh ko so ko honors KO_CONFIG_PATH + KO_DOCKER_REPO. Only
# ko apply/create/delete/run accept args after `--`; thread --context there
# (mirrors the run_ko helper in hack/install-ate.sh).
log "Applying the counter-microvm demo manifest..."
sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/counter/counter-microvm.yaml.tmpl \
  | ./hack/run-tool.sh ko apply -f - ${KUBECTL_CONTEXT:+-- --context="${KUBECTL_CONTEXT}"}

# --- 7. next steps ----------------------------------------------------------
KCTX_FLAG=""
if [[ -n "${KUBECTL_CONTEXT}" ]]; then
  KCTX_FLAG=" --context=${KUBECTL_CONTEXT}"
fi
log "Demo applied. Next steps:"
cat <<EOF

  1. Wait for the ActorTemplate golden snapshot to be Ready:
       kubectl${KCTX_FLAG} wait --for=condition=Ready \\
         actortemplate/counter-microvm -n ate-demo-counter-microvm --timeout=600s

  2. Create + resume an actor (kubectl-ate; install with: go install ./cmd/kubectl-ate):
       kubectl ate${KCTX_FLAG} create actor my-counter-1 \\
         --template ate-demo-counter-microvm/counter-microvm

  3. Port-forward the atenet-router and curl the in-RAM counter:
       kubectl${KCTX_FLAG} port-forward -n ate-system svc/atenet-router 8000:80 &
       curl -X POST -H "Host: my-counter-1.actors.resources.substrate.ate.dev" \\
         http://localhost:8000

     Increment, suspend (kubectl ate suspend actor my-counter-1), resume on another
     worker, and confirm the count continues — the guest memory snapshot round-tripped.
EOF
