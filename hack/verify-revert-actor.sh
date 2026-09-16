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
#
# Exercises RevertActor against a live cluster. Not part of `make verify`: it
# needs a running control plane, a worker pool with spare capacity, and an
# ActorTemplate whose actors can be resumed and suspended.
#
# The interesting case is revert from CRASHED. A real crash is produced by
# deleting the worker pod the actor is running on: the control plane notices
# the pod is gone, marks the actor CRASHED, and clears its worker assignment.
# That leaves an actor with no worker to talk to, which is exactly the wreck
# revert exists to clean up.
#
#   hack/verify-revert-actor.sh --atespace my-space --template my-template
#
# Everything the script creates is torn down on exit unless --keep is passed.

set -o errexit
set -o nounset
set -o pipefail

ATE="${ATE:-bin/kubectl-ate}"
ATESPACE=""
TEMPLATE=""
ACTOR="revert-check-$$"
ENDPOINT="${ATE_ENDPOINT:-}"
KEEP="false"
# Deleting a pod, noticing it is gone, and rescheduling all take real time on a
# real cluster; a laptop-speed timeout only produces confusing failures.
TIMEOUT_SECONDS="${ATE_VERIFY_TIMEOUT:-180}"

usage() {
  cat <<'EOF'
Usage: hack/verify-revert-actor.sh --atespace <name> --template <name> [options]

Required:
  --atespace <name>   Atespace to create the test actor in.
  --template <name>   ActorTemplate to derive it from. Must already exist, and
                      its actors must be resumable on an available worker.

Options:
  --actor <name>      Name for the test actor. Default: revert-check-<pid>.
  --endpoint <addr>   gRPC target, e.g. localhost:8080. Without this the CLI
                      port-forwards on every call, which makes the run slow.
  --keep              Leave the actor behind for inspection.
  --timeout <secs>    Per-wait timeout. Default: 180.

Environment:
  ATE                 Path to the kubectl-ate binary. Default: bin/kubectl-ate.
  ATE_ENDPOINT        Same as --endpoint.
  ATE_VERIFY_TIMEOUT  Same as --timeout.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --atespace) ATESPACE="$2"; shift 2 ;;
    --template) TEMPLATE="$2"; shift 2 ;;
    --actor) ACTOR="$2"; shift 2 ;;
    --endpoint) ENDPOINT="$2"; shift 2 ;;
    --timeout) TIMEOUT_SECONDS="$2"; shift 2 ;;
    --keep) KEEP="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "${ATESPACE}" || -z "${TEMPLATE}" ]]; then
  echo "--atespace and --template are required" >&2
  usage
  exit 2
fi

FAILURES=0

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }
pass() { printf '    \033[32mPASS\033[0m %s\n' "$*"; }
fail() { printf '    \033[31mFAIL\033[0m %s\n' "$*"; FAILURES=$((FAILURES + 1)); }
die()  { printf '\n\033[31m%s\033[0m\n' "$*" >&2; exit 1; }

ate() {
  if [[ -n "${ENDPOINT}" ]]; then
    "${ATE}" "$@" --endpoint "${ENDPOINT}"
  else
    "${ATE}" "$@"
  fi
}

actor_json() {
  ate get actor "${ACTOR}" -a "${ATESPACE}" -o json
}

# actor_field prints one jq path from the actor, empty when protojson omitted
# it. A failed read is also reported as empty: these are called from polling
# loops, where a single blip should cost a retry rather than the whole run.
actor_field() {
  local raw
  raw="$(actor_json 2>/dev/null)" || return 0
  jq -r "$1 // \"\"" <<<"${raw}"
}

actor_state() {
  actor_field '.status.state'
}

expect_state() {
  local want="$1" context="$2" got
  got="$(actor_state)"
  if [[ "${got}" == "${want}" ]]; then
    pass "${context}: state is ${want}"
  else
    fail "${context}: state is ${got}, want ${want}"
  fi
}

wait_for_state() {
  local want="$1" deadline=$((SECONDS + TIMEOUT_SECONDS)) got
  while (( SECONDS < deadline )); do
    got="$(actor_state)"
    if [[ "${got}" == "${want}" ]]; then
      return 0
    fi
    sleep 2
  done
  fail "timed out after ${TIMEOUT_SECONDS}s waiting for ${want}, last state ${got:-unknown}"
  return 1
}

cleanup() {
  local code=$?
  if [[ "${KEEP}" == "true" ]]; then
    printf '\n--keep set; leaving actor %s/%s in place\n' "${ATESPACE}" "${ACTOR}"
  else
    step "Cleanup"
    # --any-state: a failed run can leave the actor anywhere, including REVERTING.
    if ate delete actor "${ACTOR}" -a "${ATESPACE}" --any-state >/dev/null 2>&1; then
      info "deleted actor ${ACTOR}"
    else
      info "no actor to delete (or delete failed; check manually)"
    fi
  fi
  exit "${code}"
}
trap cleanup EXIT

# --- Preflight -------------------------------------------------------------

step "Preflight"
command -v jq >/dev/null || die "jq is required"
command -v kubectl >/dev/null || die "kubectl is required"
[[ -x "${ATE}" ]] || die "${ATE} not found or not executable; run 'make build-atectl' or set ATE"
ate get atespaces >/dev/null || die "cannot reach the control plane"
info "atespace=${ATESPACE} template=${TEMPLATE} actor=${ACTOR}"
[[ -n "${ENDPOINT}" ]] || info "no --endpoint: each CLI call will port-forward, expect a slow run"

# --- Establish a snapshot for revert to return to --------------------------

step "Create and run the actor"
ate create actor "${ACTOR}" -a "${ATESPACE}" --template "${TEMPLATE}" >/dev/null
ate resume actor "${ACTOR}" -a "${ATESPACE}" >/dev/null
wait_for_state "ACTOR_STATE_RUNNING" || die "actor never reached RUNNING"
pass "actor is RUNNING"

step "Suspend once, so there is an external snapshot to revert to"
ate suspend actor "${ACTOR}" -a "${ATESPACE}" >/dev/null
expect_state "ACTOR_STATE_SUSPENDED" "after suspend"
SNAPSHOT_URI="$(actor_field '.status.externalSnapshot.snapshotUri')"
[[ -n "${SNAPSHOT_URI}" ]] || die "suspend wrote no external snapshot; cannot verify revert"
info "snapshot: ${SNAPSHOT_URI}"

# --- Scenario 1: revert from CRASHED ---------------------------------------

step "Scenario 1: revert from CRASHED"
ate resume actor "${ACTOR}" -a "${ATESPACE}" >/dev/null
wait_for_state "ACTOR_STATE_RUNNING" || die "actor never got back to RUNNING"

WORKER_POD="$(actor_field '.status.workerAssignment.workerPod')"
WORKER_NS="$(actor_field '.status.workerAssignment.workerNamespace')"
[[ -n "${WORKER_POD}" && -n "${WORKER_NS}" ]] || die "running actor has no worker pod recorded"
info "running on ${WORKER_NS}/${WORKER_POD}"

info "deleting the worker pod to crash the actor"
kubectl delete pod "${WORKER_POD}" -n "${WORKER_NS}" --wait=false >/dev/null
wait_for_state "ACTOR_STATE_CRASHED" || die "actor never reached CRASHED after its pod was deleted"
pass "actor is CRASHED"

# The crash clears the assignment, so revert has no worker to terminate
# through. This is the path that needs no live worker to succeed.
if [[ -n "$(actor_field '.status.workerAssignment.workerPod')" ]]; then
  info "note: the crashed actor still records a worker assignment"
fi

ate revert actor "${ACTOR}" -a "${ATESPACE}" >/dev/null || fail "RevertActor from CRASHED returned an error"
expect_state "ACTOR_STATE_SUSPENDED" "after revert from CRASHED"

GOT_URI="$(actor_field '.status.externalSnapshot.snapshotUri')"
if [[ "${GOT_URI}" == "${SNAPSHOT_URI}" ]]; then
  pass "external snapshot untouched"
else
  fail "external snapshot is ${GOT_URI:-<empty>}, want it untouched at ${SNAPSHOT_URI}"
fi

if [[ -z "$(actor_field '.status.workerAssignment.workerPod')" ]]; then
  pass "worker assignment cleared"
else
  fail "worker assignment survived the revert"
fi

if [[ -z "$(actor_field '.status.localSnapshotInfo.snapshotName')" ]]; then
  pass "local snapshot pointer cleared"
else
  fail "local snapshot pointer survived the revert; the next resume would restore the discarded execution"
fi

step "The reverted actor is usable: resume it"
ate resume actor "${ACTOR}" -a "${ATESPACE}" >/dev/null || fail "resume after revert returned an error"
wait_for_state "ACTOR_STATE_RUNNING" || fail "actor did not come back up after revert"
pass "actor resumed from its preserved snapshot"

# --- Scenario 2: revert from RUNNING ---------------------------------------

step "Scenario 2: revert from RUNNING"
ate revert actor "${ACTOR}" -a "${ATESPACE}" >/dev/null || fail "RevertActor from RUNNING returned an error"
expect_state "ACTOR_STATE_SUSPENDED" "after revert from RUNNING"
GOT_URI="$(actor_field '.status.externalSnapshot.snapshotUri')"
if [[ "${GOT_URI}" == "${SNAPSHOT_URI}" ]]; then
  pass "external snapshot still the original one, no new snapshot taken"
else
  fail "external snapshot is ${GOT_URI:-<empty>}, want the original ${SNAPSHOT_URI}"
fi

# --- Scenario 3: revert from PAUSED ----------------------------------------

step "Scenario 3: revert from PAUSED"
ate resume actor "${ACTOR}" -a "${ATESPACE}" >/dev/null
wait_for_state "ACTOR_STATE_RUNNING" || die "actor never reached RUNNING"
if ate pause actor "${ACTOR}" -a "${ATESPACE}" >/dev/null 2>&1; then
  wait_for_state "ACTOR_STATE_PAUSED" || die "actor never reached PAUSED"
  PAUSE_NODE="$(actor_field '.status.localSnapshotInfo.nodeVmsWithLocalSnapshots[0]')"
  info "paused with a node-local snapshot on ${PAUSE_NODE:-<none recorded>}"

  ate revert actor "${ACTOR}" -a "${ATESPACE}" >/dev/null || fail "RevertActor from PAUSED returned an error"
  expect_state "ACTOR_STATE_SUSPENDED" "after revert from PAUSED"
  if [[ -z "$(actor_field '.status.localSnapshotInfo.snapshotName')" ]]; then
    pass "pause snapshot pointer discarded"
  else
    fail "pause snapshot pointer survived; resume would restore the discarded execution"
  fi
  if [[ -n "${PAUSE_NODE}" ]]; then
    info "NOTE: the checkpoint bytes on ${PAUSE_NODE} are not pruned yet (see the"
    info "      commented-out ensureLocalCheckpointsPruned call in workflow_revert.go)."
    info "      Check with: kubectl debug node/${PAUSE_NODE} -- ls /host/var/lib/ateom-gvisor/actors"
  fi
else
  info "pause is unavailable for this template; skipping"
fi

# --- Scenario 4: SUSPENDED is rejected -------------------------------------

step "Scenario 4: reverting a SUSPENDED actor is rejected"
expect_state "ACTOR_STATE_SUSPENDED" "before the rejection check"
if REJECT_OUT="$(ate revert actor "${ACTOR}" -a "${ATESPACE}" 2>&1)"; then
  fail "RevertActor succeeded on a SUSPENDED actor, want FailedPrecondition"
elif grep -qi 'FailedPrecondition' <<<"${REJECT_OUT}"; then
  pass "rejected with FailedPrecondition"
else
  fail "rejected, but not with FailedPrecondition: ${REJECT_OUT}"
fi

# --- Result ----------------------------------------------------------------

step "Result"
if (( FAILURES == 0 )); then
  printf '    \033[32mall checks passed\033[0m\n'
else
  printf '    \033[31m%d check(s) failed\033[0m\n' "${FAILURES}"
  exit 1
fi
