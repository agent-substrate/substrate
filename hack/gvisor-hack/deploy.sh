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

# Deploy a locally-built runsc: stage the binary into the cluster's object store,
# then apply a SandboxConfig pinning it by URL + sha256. See README.md in this
# directory for the full walkthrough.
#
# Env:
#   RUNSC                 path to the runsc binary to deploy (default ./bin/runsc)
#   NAME                  SandboxConfig name (default gvisor-hack)
#   ARCH                  node GOARCH the binary targets (default: host GOARCH)
#   BUCKET_NAME           object store bucket (default ate-snapshots)
#   PREFIX                object key prefix within the bucket (default gvisor-hack)
#   ATE_INSTALL_KIND      "true" -> stage to the in-cluster rustfs; else GCS
#   KUBECTL_CONTEXT       kube context (optional)
#   PROJECT_ID            GCP project for the GCS upload (optional)
#   MAKE_DEFAULT          "true" -> make this the cluster-wide gvisor default
#   WORKERPOOL            WorkerPool to point at this config (optional)
#   WORKERPOOL_NAMESPACE  namespace of WORKERPOOL (required with WORKERPOOL)
#   SKIP_STAGE            "true" -> skip the upload, only (re-)apply the config

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

RUNSC="${RUNSC:-${ROOT}/bin/runsc}"
NAME="${NAME:-gvisor-hack}"
ARCH="${ARCH:-$(go env GOARCH)}"
BUCKET_NAME="${BUCKET_NAME:-ate-snapshots}"
PREFIX="${PREFIX:-gvisor-hack}"
ATE_INSTALL_KIND="${ATE_INSTALL_KIND:-false}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-}"
MAKE_DEFAULT="${MAKE_DEFAULT:-false}"
WORKERPOOL="${WORKERPOOL:-}"
WORKERPOOL_NAMESPACE="${WORKERPOOL_NAMESPACE:-}"
SKIP_STAGE="${SKIP_STAGE:-false}"

log() { echo ">> $*"; }

if [[ ! -f "${RUNSC}" ]]; then
  echo "error: runsc binary not found at ${RUNSC} (set RUNSC=/path/to/runsc)" >&2
  exit 1
fi

case "${ARCH}" in
  amd64|arm64) ;;
  *) echo "error: unsupported ARCH=${ARCH} (want amd64 or arm64)" >&2; exit 1 ;;
esac

if [[ -n "${WORKERPOOL}" && -z "${WORKERPOOL_NAMESPACE}" ]]; then
  echo "error: WORKERPOOL_NAMESPACE is required when WORKERPOOL is set" >&2
  exit 1
fi

# NAME becomes a Kubernetes object name. Check it here so a typo costs a second
# rather than an upload, and so it cannot be the thing that fails the apply after
# the incumbent default has already been demoted.
if [[ ${#NAME} -gt 253 ]] || ! [[ "${NAME}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$ ]]; then
  echo "error: NAME=${NAME} is not a valid Kubernetes object name (DNS-1123 subdomain)" >&2
  exit 1
fi

# A quick sanity check that the binary matches the target arch — a runsc built for
# the wrong arch fails deep inside ateom with an exec format error, which is a
# needlessly confusing way to learn about it. Advisory only; `file` may be absent.
if command -v file >/dev/null 2>&1; then
  file_out="$(file -b "${RUNSC}")"
  case "${ARCH}:${file_out}" in
    amd64:*x86-64*|arm64:*aarch64*) ;;
    *) echo "warning: ${RUNSC} looks like '${file_out}', which may not match ARCH=${ARCH}" >&2 ;;
  esac
fi

KUBECTL=(kubectl)
if [[ -n "${KUBECTL_CONTEXT}" ]]; then
  KUBECTL+=(--context="${KUBECTL_CONTEXT}")
fi

# atelet caches assets at static-files/runsc-<sha256> and verifies the download
# against this digest, so the digest is the real version pin; the object name
# carries it too, so a rebuild lands at a new URL instead of silently changing the
# bytes behind an old one.
RUNSC_SHA256="$(sha256sum "${RUNSC}" | awk '{print $1}')"
OBJECT="${PREFIX}/runsc-${RUNSC_SHA256}"
RUNSC_URL="gs://${BUCKET_NAME}/${OBJECT}"

log "runsc:   ${RUNSC}"
log "sha256:  ${RUNSC_SHA256}"
log "url:     ${RUNSC_URL}"
log "arch:    ${ARCH}"
log "config:  SandboxConfig/${NAME} (default=${MAKE_DEFAULT})"

render_config() {
  sed -e "s|\${NAME}|${NAME}|g" \
      -e "s|\${DEFAULT}|${MAKE_DEFAULT}|g" \
      -e "s|\${ARCH}|${ARCH}|g" \
      -e "s|\${RUNSC_URL}|${RUNSC_URL}|g" \
      -e "s|\${RUNSC_SHA256}|${RUNSC_SHA256}|g" \
      hack/gvisor-hack/sandboxconfig.yaml.tmpl
}

# Validate against the live API server before doing anything with side effects.
# The CRD schema and the sandboxconfig-assets ValidatingAdmissionPolicy both run
# under --dry-run=server, so a config the cluster would reject fails here —
# before the upload, and before MAKE_DEFAULT demotes the incumbent default.
log "Validating SandboxConfig/${NAME} (server-side dry run)..."
render_config | "${KUBECTL[@]}" apply --dry-run=server -f - >/dev/null

if [[ "${SKIP_STAGE}" == "true" ]]; then
  log "SKIP_STAGE=true; not uploading"
elif [[ "${ATE_INSTALL_KIND}" == "true" ]]; then
  log "Staging to in-cluster rustfs bucket ${BUCKET_NAME}..."
  RUNSC="${RUNSC}" OBJECT="${OBJECT}" BUCKET="${BUCKET_NAME}" KUBECTL_CONTEXT="${KUBECTL_CONTEXT}" \
    hack/gvisor-hack/stage-to-rustfs.sh
else
  log "Staging to gs://${BUCKET_NAME}/${OBJECT}..."
  RUNSC="${RUNSC}" OBJECT="${OBJECT}" BUCKET="${BUCKET_NAME}" \
    hack/gvisor-hack/stage-to-gcs.sh
fi

restore_demoted() {
  # Guard the expansion: an empty array under `set -u` is an error on bash < 4.4.
  if [[ ${#demoted[@]} -eq 0 ]]; then
    return
  fi
  for sc_name in "${demoted[@]}"; do
    echo "   restoring spec.default on ${sc_name}" >&2
    if ! "${KUBECTL[@]}" patch sandboxconfig "${sc_name}" --type=merge -p '{"spec":{"default":true}}'; then
      echo "error: could not restore ${sc_name} as the default; the cluster has NO default" >&2
      echo "       gvisor SandboxConfig. Fix with: kubectl patch sandboxconfig ${sc_name} \\" >&2
      echo "       --type=merge -p '{\"spec\":{\"default\":true}}'" >&2
    fi
  done
}

on_interrupt() {
  # Ignore further interrupts rather than re-entering this handler: the restore
  # below is the only thing standing between an impatient second Ctrl-C and a
  # cluster with no default SandboxConfig, so let it finish.
  trap '' INT TERM
  echo >&2
  echo "error: interrupted before SandboxConfig/${NAME} was applied" >&2
  restore_demoted
  exit 130
}

# Two defaults for one sandbox class make every actor launch fail to resolve its
# assets, so demote the incumbent BEFORE applying rather than after. Both states
# are broken (see TestResolveSandboxAssetsErrors), but the two-default window
# closes on the next patch, whereas zero defaults persists until someone notices
# — so if the apply fails anyway, put the incumbent back.
demoted=()
if [[ "${MAKE_DEFAULT}" == "true" ]]; then
  # Ctrl-C between the demotion and the apply would otherwise leave zero defaults
  # behind with nothing to put them back. A SIGKILL still escapes this; two object
  # writes cannot be made atomic from a shell script, only narrowed.
  trap on_interrupt INT TERM
  log "Clearing spec.default on other gvisor SandboxConfigs..."
  while read -r sc_name sc_class sc_default; do
    [[ -z "${sc_name}" ]] && continue
    if [[ "${sc_name}" != "${NAME}" && "${sc_class}" == "gvisor" && "${sc_default}" == "true" ]]; then
      log "   demoting ${sc_name}"
      # Record before patching, not after: an interrupt can land after the server
      # has applied the patch but before kubectl returns, and a name missing from
      # this list never gets restored. Restoring one that was never demoted just
      # re-asserts spec.default: true, which is a no-op.
      demoted+=("${sc_name}")
      "${KUBECTL[@]}" patch sandboxconfig "${sc_name}" --type=merge -p '{"spec":{"default":false}}'
    fi
  done < <("${KUBECTL[@]}" get sandboxconfigs -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.sandboxClass}{" "}{.spec.default}{"\n"}{end}')
fi

log "Applying SandboxConfig/${NAME}..."
if ! render_config | "${KUBECTL[@]}" apply -f -; then
  # The dry run above makes this unlikely, but a transient failure here would
  # otherwise leave the cluster with no default at all.
  echo "error: applying SandboxConfig/${NAME} failed" >&2
  restore_demoted
  exit 1
fi
# Past this point the new default exists, so an interrupt is no longer dangerous.
trap - INT TERM

if [[ -n "${WORKERPOOL}" ]]; then
  log "Pointing WorkerPool ${WORKERPOOL_NAMESPACE}/${WORKERPOOL} at SandboxConfig/${NAME}..."
  "${KUBECTL[@]}" -n "${WORKERPOOL_NAMESPACE}" patch workerpool "${WORKERPOOL}" \
    --type=merge -p "{\"spec\":{\"sandboxConfigName\":\"${NAME}\"}}"
fi

cat <<EOF

>> Deployed. SandboxConfig/${NAME} pins runsc ${RUNSC_SHA256}.

   Sandbox assets are resolved when an actor starts, so actors already running
   keep the runsc they booted with — create a new actor (or suspend/resume an
   existing one) to pick this up.
EOF

if [[ -z "${WORKERPOOL}" && "${MAKE_DEFAULT}" != "true" ]]; then
  cat <<EOF
   Nothing references it yet. Point a pool at it with:

     kubectl -n <namespace> patch workerpool <name> --type=merge \\
       -p '{"spec":{"sandboxConfigName":"${NAME}"}}'
EOF
fi

# Say plainly what was changed outside the config we own. The demotion is logged
# as it happens, but that scrolls past; this is the line you come back to when
# you want the cluster the way it was.
if [[ ${#demoted[@]} -gt 0 ]]; then
  cat <<EOF

   This cleared spec.default on: ${demoted[*]}
   To undo, delete SandboxConfig/${NAME} and run:

     kubectl patch sandboxconfig ${demoted[0]} --type=merge -p '{"spec":{"default":true}}'
EOF
fi
