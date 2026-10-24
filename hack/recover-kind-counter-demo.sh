#!/usr/bin/env bash
# Recovers the kind "kind" cluster + counter demo after the recurring
# Docker Desktop VM read-only outage (host disk pressure -> VM shared
# volume goes read-only -> apiserver dies -> kubectl EOFs).
#
# Usage: ./recover-kind-counter-demo.sh [port-forward]
set -o errexit -o nounset -o pipefail

CTX="kind-kind"
CTX_FLAG=(--context "${CTX}")
ACTOR="my-counter-1"
ATESPACE="ate-demo-counter"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# 0. Prerequisite: host disk headroom. The VM shared volume cannot go
# read-only if the host has space; this is the actual root cause.
avail_kb=$(df -k /System/Volumes/Data | awk 'NR==2{print $4}')
if (( avail_kb < 5 * 1024 * 1024 )); then
  log "host data volume has <5GiB free (this is the root cause) — free space first, then re-run"
  exit 1
fi

# 1. Cycle Docker Desktop if the cluster is unreachable
if kubectl "${CTX_FLAG[@]}" get nodes >/dev/null 2>&1; then
  log "cluster reachable — skipping Docker cycle"
else
  log "cluster unreachable — cycling Docker Desktop"
  pkill -9 -f "com.docker" 2>/dev/null || true
  pkill -9 -f "Docker.app" 2>/dev/null || true
  pkill -9 -f vpnkit 2>/dev/null || true
  sleep 8
  open -a Docker
fi

# 2. Wait for the Docker API
log "waiting for Docker API..."
for _ in $(seq 1 48); do
  docker info --format '{{.ServerVersion}}' >/dev/null 2>&1 && break
  sleep 5
done
docker info --format 'docker {{.ServerVersion}}'

# 3. Heal the node fs if still read-only, then restart the node
if docker exec kind-control-plane sh -c 'touch /var/lib/kubelet/.wtest' 2>/dev/null; then
  log "node fs writable"
else
  log "node fs read-only — restarting node container"
  docker restart kind-control-plane
fi
log "waiting for node Ready..."
kubectl "${CTX_FLAG[@]}" wait --for=condition=Ready node/kind-control-plane --timeout=180s

# 4. Wait for data-plane deployments
log "waiting for deployments..."
kubectl "${CTX_FLAG[@]}" rollout status deploy/counter -n ate-demo-counter --timeout=300s
kubectl "${CTX_FLAG[@]}" rollout status deploy/atenet-router -n ate-system --timeout=180s
kubectl "${CTX_FLAG[@]}" rollout status deploy/atenet-egress -n ate-system --timeout=180s

# 5. Recover the actor: CRASHED -> SUSPENDED at last snapshot
state="$(kubectl ate get actor "${ACTOR}" -a "${ATESPACE}" "${CTX_FLAG[@]}" -o json 2>/dev/null | grep -o '"state": *"ACTOR_STATE_[A-Z]*"' | head -1 || true)"
case "${state}" in
  *RUNNING*)
    log "actor ${ACTOR} RUNNING — no revert needed"
    ;;
  *CRASHED*)
    log "actor ${ACTOR} CRASHED -> reverting to last snapshot"
    kubectl ate revert actor "${ACTOR}" -a "${ATESPACE}" "${CTX_FLAG[@]}" >/dev/null
    ;;
  *)
    log "actor ${ACTOR} state: ${state:-<none>} — recreating"
    kubectl ate delete actor "${ACTOR}" -a "${ATESPACE}" "${CTX_FLAG[@]}" 2>/dev/null || true
    kubectl ate create actor "${ACTOR}" -a "${ATESPACE}" --template counter "${CTX_FLAG[@]}" >/dev/null
    ;;
esac

# 6. Recycle worker pods: their atunnel tunnels/credentials are stale
# after a node restart and block the router's return path (requests
# reach the actor, responses never come back).
log "recycling worker pods..."
for p in $(kubectl "${CTX_FLAG[@]}" get pods -n ate-demo-counter -o jsonpath='{.items[*].metadata.name}'); do
  kubectl "${CTX_FLAG[@]}" delete pod -n ate-demo-counter "${p}"
done
kubectl "${CTX_FLAG[@]}" rollout status deploy/counter -n ate-demo-counter --timeout=300s

# 7. Restart the router port-forward if requested
if [[ "${1:-}" == "port-forward" ]]; then
  log "restarting atenet router port-forward on :8000"
  kill "$(lsof -ti tcp:8000 -sTCP:LISTEN 2>/dev/null)" 2>/dev/null || true
  sleep 2
  nohup kubectl port-forward -n ate-system svc/atenet-router 8000:80 >/tmp/atenet-pf.log 2>&1 &
  echo "port-forward pid: $! (log: /tmp/atenet-pf.log)"
fi

log "done — verify:"
echo "  curl -X POST -H \"ate-target-actor: ${ATESPACE}/${ACTOR}\" http://localhost:8000"
