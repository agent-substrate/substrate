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

# Shared settings and helpers for the atenet-router capacity harness.
# Sourced by provision.sh and run.sh; not executable on its own.

# shellcheck disable=SC2034  # RC_* settings are consumed by the scripts that source this file

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
RC_DIR="${ROOT}/benchmarking/routercap"

# The cluster this harness is allowed to touch; every destructive step checks
# it, so an inherited KUBECONFIG cannot point it at someone else's work.
RC_CLUSTER="${ROUTERCAP_CLUSTER:-substrate-routercap}"
RC_LOCATION="${ROUTERCAP_CLUSTER_LOCATION:-us-central1-a}"

# A stockout surfaces as a raw gcloud error rather than a silent fallback to
# a smaller node, and whatever type is actually used is recorded in run.json.
# The router node is deliberately oversized so the only scarce resource on it
# is the one under test; provision.sh asserts the largest arm fits.
RC_MACHINE_TYPE="${ROUTERCAP_MACHINE_TYPE:-c3-standard-88}"
RC_SYSTEM_MACHINE_TYPE="${ROUTERCAP_SYSTEM_MACHINE_TYPE:-c3-standard-88}"
RC_WORKER_NODES="${ROUTERCAP_WORKER_NODES:-2}"

# Its own kubeconfig, never the caller's. provision.sh and run.sh both export
# this, so neither can move anyone else's current context.
RC_KUBECONFIG="${ROUTERCAP_KUBECONFIG:-${HOME}/.kube/substrate-routercap.config}"

# Namespaces and selectors, shared between the scripts and the binary's flag
# defaults.
RC_ROUTER_NS="ate-system"
RC_ROUTER_SELECTOR="app=atenet-router"
RC_WORKER_NS="benchmark-workloads"
RC_WORKER_POOL="benchmark-ateom"
RC_WORKER_SELECTOR="ate.dev/worker-pool"
RC_JOB_NS="benchmarking"
RC_JOB_SA="benchmark-runner"

# Node pool roles. The label and the taint carry the same key and value: the
# label attracts the pods that belong, the taint repels everything else.
RC_ROLE_KEY="ate.dev/role"
RC_POOL_ROUTER="router"
RC_POOL_SYSTEM="system"
RC_POOL_WORKERS="workers"
RC_POOL_LOADGEN="loadgen"

# The system pool's taint is soft; the other three are hard. A hard taint on
# system deadlocks the install: ate-system pods carry no toleration until
# provision.sh's pinning patch lands, so they need somewhere to schedule
# first.
#
# router, workers and loadgen stay NoSchedule: an uninvited pod on those nodes
# is exactly the contamination this harness exists to exclude.
RC_TAINT_SYSTEM="PreferNoSchedule"
RC_TAINT_HARD="NoSchedule"

# The default arms: three Envoy CPU sizes, each running the same ladder.
# 2/4/8 doubles across the CPU-bound region — 2 well inside, 8 at the edge
# where cores stop being the binding constraint; see RESULTS.md for the
# sweeps that located it.
RC_ARMS_DEFAULT="2 4 8"

# The Go sidecar's CPU, pinned across every arm so only the envoy container
# varies. Lives here because provision.sh needs the same number for its fit
# check, and two copies would drift.
RC_SIDECAR_CORES="${ROUTERCAP_SIDECAR_CORES:-8}"

COLOR_CYAN='\033[1;36m'
COLOR_RED='\033[1;31m'
COLOR_RESET='\033[0m'

rc::step() { echo -e "${COLOR_CYAN}[routercap] $*${COLOR_RESET}" >&2; }
rc::warn() { echo -e "${COLOR_RED}[routercap] $*${COLOR_RESET}" >&2; }
rc::die() {
  rc::warn "$*"
  exit 4
}

# rc::need checks for a binary on PATH. Checked up front rather than three
# minutes into a provision.
rc::need() {
  local missing=()
  for bin in "$@"; do
    command -v "${bin}" >/dev/null 2>&1 || missing+=("${bin}")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    rc::die "missing required tools: ${missing[*]}"
  fi
}

# rc::kubectl runs kubectl against this harness's kubeconfig only.
rc::kubectl() { KUBECONFIG="${RC_KUBECONFIG}" kubectl "$@"; }

# rc::assert_cluster refuses to act on anything but the harness's own cluster.
# The check is against the kubeconfig's current context, not against an
# environment variable, so exporting the right name at the wrong cluster does
# not get past it.
rc::assert_cluster() {
  local ctx=""
  ctx="$(KUBECONFIG="${RC_KUBECONFIG}" kubectl config current-context 2>/dev/null || true)"
  if [[ -z "${ctx}" ]]; then
    rc::die "no current context in ${RC_KUBECONFIG}; run provision.sh first"
  fi
  if [[ "${ctx}" != *"${RC_CLUSTER}"* ]]; then
    rc::die "kubeconfig ${RC_KUBECONFIG} points at '${ctx}', which is not ${RC_CLUSTER}; refusing to touch it"
  fi
}

# rc::env sources the repo's dev env for PROJECT_ID / KO_DOCKER_REPO /
# BUCKET_NAME, then overrides the cluster coordinates with this harness's own.
# The install scripts read CLUSTER_NAME and CLUSTER_LOCATION from the
# environment; a developer's usual values would install substrate elsewhere.
rc::env() {
  if [[ -f "${ROOT}/.ate-dev-env.sh" ]]; then
    # shellcheck disable=SC1091
    source "${ROOT}/.ate-dev-env.sh"
  fi
  : "${PROJECT_ID:?PROJECT_ID must be set (put it in .ate-dev-env.sh)}"
  : "${KO_DOCKER_REPO:?KO_DOCKER_REPO must be set (put it in .ate-dev-env.sh)}"
  export CLUSTER_NAME="${RC_CLUSTER}"
  export CLUSTER_LOCATION="${RC_LOCATION}"
  export KUBECONFIG="${RC_KUBECONFIG}"
  unset KUBECTL_CONTEXT || true
}

# rc::tolerations emits the tolerations JSON for one role.
rc::tolerations() {
  local role="$1"
  # No "effect" field, deliberately: an empty effect matches every effect, so
  # one toleration covers both the hard-tainted pools and the soft-tainted
  # system pool.
  printf '[{"key":"%s","operator":"Equal","value":"%s"}]' "${RC_ROLE_KEY}" "${role}"
}

# rc::pin_workload pins one Deployment/DaemonSet/StatefulSet to a node pool:
# the nodeSelector puts the pod on the right pool, the toleration lets it past
# that pool's taint. Applied as a patch because manifests/ate-install is the
# product's and this placement is the experiment's.
rc::pin_workload() {
  local kind="$1" ns="$2" name="$3" role="$4"
  rc::kubectl -n "${ns}" patch "${kind}" "${name}" --type=strategic -p "$(cat <<EOF
{"spec":{"template":{"spec":{
  "nodeSelector":{"${RC_ROLE_KEY}":"${role}"},
  "tolerations":$(rc::tolerations "${role}")
}}}}
EOF
)" >/dev/null
}

# rc::router_port_range reads the router pod's real ephemeral source-port
# range through an ephemeral busybox container targeting envoy specifically —
# the image is distroless, and /proc/sys is per network namespace. Prints
# "low high" on success and nothing on failure; an unreadable range is
# recorded as assumed rather than aborting the arm.
rc::router_port_range() {
  local pod="$1" ns="$2" dbg="portrange-$$-${RANDOM}"

  # Created detached and read back from its logs, rather than attached:
  # kubectl debug only attaches when given -i or -t, so the attached form
  # returns nothing when run headless.
  rc::kubectl -n "${ns}" debug "${pod}" \
    --target=envoy --image=busybox:1.36 --container="${dbg}" \
    --attach=false --quiet -- \
    cat /proc/sys/net/ipv4/ip_local_port_range >/dev/null 2>&1 || return 0

  # Polled rather than slept on: the container exits in milliseconds once the
  # image is local, but the first pull on a fresh node is not instant.
  local out="" i
  for (( i = 0; i < 60; i++ )); do
    out="$(rc::kubectl -n "${ns}" logs "${pod}" -c "${dbg}" 2>/dev/null | tr -d '\r' | head -1 || true)"
    [[ -n "${out}" ]] && break
    sleep 1
  done

  # An ephemeral container cannot be removed, only its output read; the pod is
  # replaced at the next arm anyway.
  if [[ "${out}" =~ ^[0-9]+[[:space:]]+[0-9]+$ ]]; then
    echo "${out}"
  fi
}
