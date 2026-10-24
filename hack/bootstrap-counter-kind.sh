#!/usr/bin/env bash
#
# bootstrap-counter-kind.sh
#
# End-to-end bootstrap (or recover) of the counter demo on a local kind
# cluster: kind cluster + local registry -> Agent Substrate -> counter demo
# -> running counter actor reachable at http://localhost:8000.
#
# The script is idempotent: it skips steps that are already done and
# repairs what broke (Docker Desktop cycles, node restarts, crashed actors,
# stale atunnel tunnels). Safe to re-run after any outage.
#
# Usage:
#   hack/bootstrap-counter-kind.sh            # full bootstrap or repair
#   hack/bootstrap-counter-kind.sh --teardown # delete cluster + demo, exit
#
# Known host failure mode handled here: when the host data volume runs out
# of space, the Docker Desktop VM's shared volume goes read-only and the
# kind node's kube-apiserver dies (kubectl EOFs, "lost connection to pod").
# Freeing host disk and cycling Docker Desktop restores the cluster.
#
set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Default target; override via env (KIND_CLUSTER_NAME is honored by the
# cluster-creation scripts too).
CLUSTER="${KIND_CLUSTER_NAME:-kind}"
CTX="kind-${CLUSTER}"
CTX_FLAG=(--context "${CTX}")
NS_DEMO="ate-demo-counter"
NS_SYS="ate-system"
ACTOR="${COUNTER_ACTOR_NAME:-my-counter-1}"
REGISTRY_PORT="${KIND_REGISTRY_PORT:-5001}"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

die() {
  log "FATAL: $*"
  exit 1
}

# shellcheck disable=SC2016
docker_cycle() {
  log "cycling Docker Desktop (pkill VM + relaunch)..."
  pkill -9 -f "com.docker" 2>/dev/null || true
  pkill -9 -f "Docker.app" 2>/dev/null || true
  pkill -9 -f "vpnkit" 2>/dev/null || true
  sleep 8
  open -a Docker
}

wait_docker() {
  log "waiting for the Docker API..."
  local i
  for i in $(seq 1 48); do
    docker info --format '{{.ServerVersion}}' >/dev/null 2>&1 && break
    sleep 5
  done
  docker info --format '{{.ServerVersion}}' >/dev/null 2>&1 \
    || die "Docker API never came up after the cycle"
  log "docker $(docker info --format '{{.ServerVersion}}')"
}

wait_node_ready() {
  log "waiting for node ${CLUSTER} to be Ready..."
  kubectl "${CTX_FLAG[@]}" wait --for=condition=Ready node/kind-control-plane \
    --timeout=240s >/dev/null \
    || die "node never became Ready"
  log "node Ready"
}

wait_demo_deployments() {
  log "waiting for data-plane deployments..."
  kubectl "${CTX_FLAG[@]}" rollout status deploy/counter -n "${NS_DEMO}" --timeout=300s >/dev/null
  kubectl "${CTX_FLAG[@]}" rollout status deploy/atenet-router -n "${NS_SYS}" --timeout=240s >/dev/null
  kubectl "${CTX_FLAG[@]}" rollout status deploy/atenet-egress -n "${NS_SYS}" --timeout=240s >/dev/null
}

recycle_workers() {
  log "recycling worker pods (stale atunnel tunnels after node restart)..."
  local p
  for p in $(kubectl "${CTX_FLAG[@]}" get pods -n "${NS_DEMO}" \
      -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    kubectl "${CTX_FLAG[@]}" delete pod -n "${NS_DEMO}" "${p}" >/dev/null
  done
  kubectl "${CTX_FLAG[@]}" rollout status deploy/counter -n "${NS_DEMO}" --timeout=300s >/dev/null
}

recover_actor() {
  local state
  state="$(kubectl ate get actor "${ACTOR}" -a "${NS_DEMO}" "${CTX_FLAG[@]}" -o json 2>/dev/null \
    | grep -o '"state": *"ACTOR_STATE_[A-Z]*"' | head -1 || true)"
  case "${state}" in
    *RUNNING*)
      log "actor ${ACTOR} RUNNING — nothing to do"
      ;;
    *CRASHED*)
      log "actor ${ACTOR} CRASHED — reverting to last snapshot"
      kubectl ate revert actor "${ACTOR}" -a "${NS_DEMO}" "${CTX_FLAG[@]}" >/dev/null
      ;;
    *SUSPENDED*|*PAUSED*)
      log "actor ${ACTOR} ${state} — leaving as is"
      ;;
    *)
      log "actor ${ACTOR} not usable (${state:-<absent>}) — (re)creating from template"
      kubectl ate delete actor "${ACTOR}" -a "${NS_DEMO}" "${CTX_FLAG[@]}" >/dev/null 2>&1 || true
      kubectl ate create actor "${ACTOR}" -a "${NS_DEMO}" --template counter \
        "${CTX_FLAG[@]}" >/dev/null
      ;;
  esac
}

start_port_forward() {
  log "starting atenet-router port-forward on :8000..."
  kill "$(lsof -ti tcp:8000 -sTCP:LISTEN 2>/dev/null)" 2>/dev/null || true
  sleep 2
  nohup kubectl "${CTX_FLAG[@]}" port-forward -n "${NS_SYS}" svc/atenet-router 8000:80 \
    > /tmp/atenet-pf.log 2>&1 &
  echo "port-forward pid: $! (log: /tmp/atenet-pf.log)"
}

teardown() {
  log "tearing down the counter demo..."
  kill "$(lsof -ti tcp:8000 -sTCP:LISTEN 2>/dev/null)" 2>/dev/null || true
  hack/install-ate-kind.sh --delete-demo-counter >/dev/null
  log "deleting kind cluster ${CLUSTER}..."
  KIND_CLUSTER_NAME="${CLUSTER}" hack/delete-kind-cluster.sh >/dev/null
  log "teardown complete (kind cluster + registry removed)"
}

case "${1:-}" in
  --teardown)
    teardown
    exit 0
    ;;
  "")
    : # default: bootstrap/repair
    ;;
  *)
    die "unknown argument: $1 (usage: bootstrap-counter-kind.sh [--teardown])"
    ;;
esac

# ---------------------------------------------------------------------------
# 0. Prerequisites
# ---------------------------------------------------------------------------
command -v kind >/dev/null || die "kind not found in PATH"
command -v kubectl >/dev/null || die "kubectl not found in PATH"
command -v docker >/dev/null || die "docker not found in PATH"
command -v go >/dev/null || die "go not found in PATH (needed for ko builds)"

# Host disk headroom. On this host the recurring outage is: data volume
# fills -> Docker VM shared volume goes read-only -> apiserver dies.
avail_kb="$(df -k /System/Volumes/Data 2>/dev/null | awk 'NR==2{print $4}')"
if [[ -n "${avail_kb}" ]] && (( avail_kb < 5 * 1024 * 1024 )); then
  log "host data volume has <5GiB free — the likely root cause of VM read-only outages"
  log "free up space first, then re-run; continuing (the cluster may still be healthy)"
fi

# ---------------------------------------------------------------------------
# 1. Cluster + registry
# ---------------------------------------------------------------------------
if docker ps --format '{{.Names}}' | grep -qx "${CLUSTER}-control-plane" \
   || [[ -n "$(kind get clusters 2>/dev/null | grep -x "${CLUSTER}")" ]]; then
  log "kind cluster ${CLUSTER} exists — skipping creation"
else
  log "creating kind cluster ${CLUSTER} + local registry on :${REGISTRY_PORT}..."
  KIND_CLUSTER_NAME="${CLUSTER}" KIND_REGISTRY_PORT="${REGISTRY_PORT}" \
    hack/create-kind-cluster.sh >/dev/null
fi

# ---------------------------------------------------------------------------
# 2. Docker API + node health (the recurring outage repair)
# ---------------------------------------------------------------------------
if ! docker info >/dev/null 2>&1; then
  docker_cycle
  wait_docker
fi

cluster_ok=0
for _ in 1 2 3; do
  if kubectl "${CTX_FLAG[@]}" get nodes >/dev/null 2>&1; then
    cluster_ok=1
    break
  fi
  log "cluster unreachable — attempting node recovery"
  docker_cycle
  wait_docker
  if docker exec kind-control-plane sh -c 'touch /var/lib/kubelet/.wtest' \
      >/dev/null 2>&1; then
    log "node fs writable — restarting node container"
    docker restart kind-control-plane >/dev/null
  else
    log "node fs read-only — Docker VM needs a clean restart; done"
    docker restart kind-control-plane >/dev/null
  fi
  sleep 5
done
[[ "${cluster_ok}" -eq 1 ]] || {
  log "cluster still unreachable — waiting for the node to come Ready..."
}
wait_node_ready

# ---------------------------------------------------------------------------
# 3. Substrate system + counter demo
# ---------------------------------------------------------------------------
demo_ns="$(kubectl "${CTX_FLAG[@]}" get ns "${NS_DEMO}" --no-headers 2>/dev/null | awk '{print $1}')"
if [[ -z "${demo_ns}" ]]; then
  log "deploying Agent Substrate + counter demo on kind (first run: builds via ko)..."
  KIND_CLUSTER_NAME="${CLUSTER}" hack/install-ate-kind.sh --deploy-demo-counter
else
  log "counter demo namespace exists — repairing data plane if needed"
  kubectl "${CTX_FLAG[@]}" rollout restart deploy/atenet-router deploy/atenet-egress -n "${NS_SYS}" \
    >/dev/null 2>&1 || true
  wait_demo_deployments
  recycle_workers
  recover_actor
fi

# ---------------------------------------------------------------------------
# 4. Make the counter reachable
# ---------------------------------------------------------------------------
start_port_forward

# ---------------------------------------------------------------------------
# 5. Verify
# ---------------------------------------------------------------------------
log "verifying the counter end-to-end..."
ok=0
for _ in 1 2 3 4 5 6; do
  out="$(curl -s --max-time 60 \
    -H "ate-target-actor: ${NS_DEMO}/${ACTOR}" http://localhost:8000 || true)"
  if [[ "${out}" == *"preserved memory count"* ]]; then
    ok=1
    log "verified: ${out}"
    break
  fi
  log "counter not ready yet — activating on a fresh worker, retrying"
  sleep 10
done
[[ "${ok}" -eq 1 ]] || die "counter did not respond on :8000 after retries"

log "done. The counter is live:"
echo "  curl -X POST -H \"ate-target-actor: ${NS_DEMO}/${ACTOR}\" http://localhost:8000"
echo "  status: kubectl ate get actor ${ACTOR} -a ${NS_DEMO} --context ${CTX}"
echo "  teardown: hack/bootstrap-counter-kind.sh --teardown"
