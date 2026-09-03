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

# Verifies the atenet-router graceful drain end to end on a live cluster:
#
#   1. Pins the counter demo's worker pool to a single worker and
#      oversubscribes it with two actors, so a request for the second actor
#      parks on the router.
#   2. Deletes the router pod WHILE the request is parked, then frees the
#      worker — the parked request must complete (200) through the
#      Terminating pod: park -> resume -> route rides out the shutdown.
#   3. Asserts /readyz flips to 503 while /healthz stays 200, the pod
#      terminates via the drain sequence (after the drain-delay, well under
#      terminationGracePeriodSeconds — i.e. the drain-complete marker
#      released Envoy's preStop, not the SIGKILL path), and the router logs
#      show the ordered drain sequence.
#
# This lives in hack/ rather than the e2e suites because it deletes the
# shared router pod, which breaks port-forward tunnels held by other suites —
# the e2e runner executes suites in parallel, so this check must run alone.
#
# Prerequisites: a cluster with the ate system and the counter demo installed
# (hack/install-ate-kind.sh --deploy-ate-system --deploy-demo-counter), and no
# actors currently running on the counter demo pool (the pool is scaled to 1
# for the duration of the check and restored afterwards).
#
# Respects KUBECTL_CONTEXT like the other hack scripts.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

ROUTER_NS="ate-system"
DEMO_NS="ate-demo-counter"
DEMO_POOL="counter"
# The demo's atespace: a template reference resolves in the actor's
# atespace, so the check's actors live next to the demo's. Their names are
# timestamped, so they cannot collide with anything already there.
ATESPACE="ate-demo-counter"
SUFFIX="$(date +%s)"
ACTOR_BUSY="busy-${SUFFIX}"    # occupies the only worker
ACTOR_PARKED="parked-${SUFFIX}" # its request parks, then survives the drain
LOCAL_HTTP_PORT="${LOCAL_HTTP_PORT:-18080}"
LOCAL_METRICS_PORT="${LOCAL_METRICS_PORT:-19090}"
# Xs must be the trailing characters: BSD (macOS) mktemp rejects suffixes.
LOG_FILE="$(mktemp /tmp/atenet-drain-check-log.XXXXXX)"
CURL_OUT="$(mktemp /tmp/atenet-drain-check-out.XXXXXX)"

run_kubectl() { kubectl ${KUBECTL_CONTEXT:+--context=${KUBECTL_CONTEXT}} "$@"; }
run_kubectl_ate() { go run ./cmd/kubectl-ate ${KUBECTL_CONTEXT:+--context=${KUBECTL_CONTEXT}} "$@"; }

log_step() { echo; echo "[drain-check]: $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

BG_PIDS=()
ORIG_REPLICAS=""
cleanup() {
  local code=$?
  set +e
  for pid in ${BG_PIDS[@]+"${BG_PIDS[@]}"}; do kill "${pid}" 2>/dev/null; done
  log_step "cleanup: actors and pool"
  # Deletion requires suspended actors; suspend both best-effort first (the
  # busy actor is RUNNING on any failure before the drain step).
  run_kubectl_ate suspend actor "${ACTOR_PARKED}" -a "${ATESPACE}" >/dev/null 2>&1
  run_kubectl_ate suspend actor "${ACTOR_BUSY}" -a "${ATESPACE}" >/dev/null 2>&1
  run_kubectl_ate delete actor "${ACTOR_PARKED}" -a "${ATESPACE}" >/dev/null 2>&1
  run_kubectl_ate delete actor "${ACTOR_BUSY}" -a "${ATESPACE}" >/dev/null 2>&1
  if [[ -n "${ORIG_REPLICAS}" ]]; then
    run_kubectl patch workerpool -n "${DEMO_NS}" "${DEMO_POOL}" --type merge \
      -p "{\"spec\":{\"replicas\":${ORIG_REPLICAS}}}" >/dev/null 2>&1
    run_kubectl rollout status "deploy/${DEMO_POOL}" -n "${DEMO_NS}" --timeout=180s >/dev/null 2>&1
  fi
  exit "${code}"
}
trap cleanup EXIT

# --- Preflight -------------------------------------------------------------

log_step "preflight"
# Ready means the template's golden snapshot exists; protojson omits empty
# fields, so the key is only present once it is set.
run_kubectl_ate get actor-template "${DEMO_POOL}" -a "${ATESPACE}" -o json 2>/dev/null \
  | grep -q '"goldenSnapshot"' \
  || fail "actor template ${ATESPACE}/${DEMO_POOL} has no golden snapshot; install the counter demo first"
# Column 4 is STATUS; the header row's "ASSIGNED ACTOR" column name must not
# trip the check.
if run_kubectl_ate get workers 2>/dev/null | awk 'NR>1 && $4=="ASSIGNED"' | grep -q .; then
  fail "workers on the ${DEMO_POOL} pool are ASSIGNED; the check scales the pool to 1 and would crash running actors — suspend them first"
fi
for port in "${LOCAL_HTTP_PORT}" "${LOCAL_METRICS_PORT}"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
    exec 3>&- 3<&-
    fail "local port ${port} is already in use (a stale port-forward?); free it or set LOCAL_HTTP_PORT/LOCAL_METRICS_PORT"
  fi
done

# --- Fixture: 1 worker, two actors, worker occupied --------------------------

log_step "pinning ${DEMO_NS}/${DEMO_POOL} to 1 worker"
ORIG_REPLICAS="$(run_kubectl get workerpool -n "${DEMO_NS}" "${DEMO_POOL}" -o jsonpath='{.spec.replicas}')"
run_kubectl patch workerpool -n "${DEMO_NS}" "${DEMO_POOL}" --type merge -p '{"spec":{"replicas":1}}' >/dev/null
run_kubectl rollout status "deploy/${DEMO_POOL}" -n "${DEMO_NS}" --timeout=180s >/dev/null

log_step "creating actors ${ACTOR_BUSY} + ${ACTOR_PARKED} in atespace ${ATESPACE}"
run_kubectl_ate create actor "${ACTOR_BUSY}" -a "${ATESPACE}" --template-ref "${DEMO_POOL}" >/dev/null
run_kubectl_ate create actor "${ACTOR_PARKED}" -a "${ATESPACE}" --template-ref "${DEMO_POOL}" >/dev/null

log_step "occupying the only worker with ${ACTOR_BUSY}"
for i in $(seq 1 20); do
  run_kubectl_ate resume actor "${ACTOR_BUSY}" -a "${ATESPACE}" >/dev/null 2>&1 && break
  [[ "$i" == 20 ]] && fail "could not resume ${ACTOR_BUSY} (worker never became available)"
  sleep 3
done

# --- Observers ---------------------------------------------------------------

POD="$(run_kubectl get pods -n "${ROUTER_NS}" -l app=atenet-router --no-headers | awk '$3=="Running"{print $1}')"
# wc -w pads its output on macOS; compare numerically.
(( $(wc -w <<<"${POD}") == 1 )) || fail "expected exactly 1 running router pod, got: ${POD:-none}"
log_step "router pod under test: ${POD}"

run_kubectl logs -n "${ROUTER_NS}" "${POD}" -c atenet-router -f > "${LOG_FILE}" 2>&1 &
BG_PIDS+=($!)
run_kubectl port-forward -n "${ROUTER_NS}" svc/atenet-router "${LOCAL_HTTP_PORT}:80" >/dev/null 2>&1 &
BG_PIDS+=($!)
run_kubectl port-forward -n "${ROUTER_NS}" "${POD}" "${LOCAL_METRICS_PORT}:9090" >/dev/null 2>&1 &
BG_PIDS+=($!)
sleep 3

readyz() { curl -s -o /dev/null -w '%{http_code}' "localhost:${LOCAL_METRICS_PORT}/readyz"; }
healthz() { curl -s -o /dev/null -w '%{http_code}' "localhost:${LOCAL_METRICS_PORT}/healthz"; }

# The port-forwards need a moment to come up; retry before judging.
for i in $(seq 1 20); do
  [[ "$(readyz)" == "200" ]] && break
  [[ "$i" == 20 ]] && fail "steady-state /readyz != 200"
  sleep 0.5
done
[[ "$(healthz)" == "200" ]] || fail "steady-state /healthz != 200"
echo "steady state: /readyz=200 /healthz=200"

# --- The drain: park -> delete pod -> free worker ----------------------------

log_step "firing the request that will park (single worker is busy)"
( curl -s --max-time 30 -w '\nHTTP=%{http_code}\n' \
    -H "Host: ${ACTOR_PARKED}.${ATESPACE}.actors.resources.substrate.ate.dev" \
    "http://localhost:${LOCAL_HTTP_PORT}/" > "${CURL_OUT}" 2>&1 ) &
CURL_PID=$!
sleep 1  # inside the 5s park budget; the request is parked on the router

log_step "deleting the router pod with the request parked on it"
DELETE_T="$(date +%s)"
run_kubectl delete pod -n "${ROUTER_NS}" "${POD}" --wait=false >/dev/null
until [[ -n "$(run_kubectl get pod -n "${ROUTER_NS}" "${POD}" -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null)" ]]; do
  sleep 0.2
done
[[ "$(readyz)" == "503" ]] || fail "/readyz != 503 while Terminating"
[[ "$(healthz)" == "200" ]] || fail "/healthz != 200 while Terminating"
echo "Terminating: /readyz=503 /healthz=200"

log_step "freeing the worker (suspend ${ACTOR_BUSY}) — the parked request must now complete"
run_kubectl_ate suspend actor "${ACTOR_BUSY}" -a "${ATESPACE}" >/dev/null

wait "${CURL_PID}" || true
grep -q "HTTP=200" "${CURL_OUT}" || fail "parked request did not return 200 through the Terminating pod: $(cat "${CURL_OUT}")"
grep -q "hello from" "${CURL_OUT}" || fail "parked request body missing the counter greeting: $(cat "${CURL_OUT}")"
echo "parked request served by the Terminating pod:"
sed 's/^/  /' "${CURL_OUT}"

# --- Termination window -------------------------------------------------------

log_step "waiting for the pod to terminate"
until ! run_kubectl get pod -n "${ROUTER_NS}" "${POD}" >/dev/null 2>&1; do sleep 1; done
ELAPSED=$(( $(date +%s) - DELETE_T ))
echo "pod terminated ${ELAPSED}s after deletion"
(( ELAPSED >= 10 )) || fail "terminated in ${ELAPSED}s — before the 13s drain-delay could run; the drain sequence was skipped"
(( ELAPSED <= 55 )) || fail "terminated in ${ELAPSED}s — at the grace period; SIGKILL path, the drain-complete handshake did not release Envoy"

log_step "drain log sequence"
for marker in "Shutdown signal received; draining" "Draining dataplane" "Starting ext_proc drain" "Drain-complete marker written" "Shutdown complete"; do
  grep -q "${marker}" "${LOG_FILE}" || fail "router log missing \"${marker}\" (see ${LOG_FILE})"
done
grep -E "Shutdown signal|Draining dataplane|Dataplane drain|ext_proc drain|marker written|Shutdown complete" "${LOG_FILE}" | sed 's/^/  /'

log_step "waiting for the replacement router pod"
run_kubectl rollout status deploy/atenet-router -n "${ROUTER_NS}" --timeout=120s >/dev/null

echo
echo "PASS: atenet-router graceful drain verified (parked request served by the"
echo "Terminating pod; readiness flipped; terminated in ${ELAPSED}s, inside the"
echo "drain window; log sequence complete; replacement pod Ready)."
