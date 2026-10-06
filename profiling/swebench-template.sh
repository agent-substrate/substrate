#!/usr/bin/env bash
# Create an ActorTemplate whose container is a SWE-bench Verified image from
# Artifact Registry, so a profiling run pulls ~1-3 GB over the network instead
# of the few MB a ko-built demo image costs.
#
#   ./swebench-template.sh pytest-dev__pytest-7205
#
# ActorTemplate is not a Kubernetes CRD. It is a substrate resource served by
# ate-api-server, so the manifest below is a protojson-shaped
# ateapipb.ActorTemplate (no apiVersion/kind, metadata.atespace rather than
# metadata.namespace, proto enum spellings) fed to `kubectl ate create
# actor-template`. Parsing is strict: an unknown field is an error, not a
# silent drop.
#
# The image is pinned by digest: atelet's node-local layer cache is keyed by
# reference, so a tag that moves would silently turn a cold pull into a warm
# one. Pick a different instance to get a pull that is cold on every node.
set -euo pipefail

INSTANCE="${1:-pytest-dev__pytest-7205}"
NAME="${2:-swebench-$(echo "$INSTANCE" | tr '_' '-' | tr -s '-')}"
# Defaults to the dedicated profiling atespace and worker pool, which
# profiling/deploy-pool.sh creates. Kept off the demo pools so a demo
# redeploy cannot move the ground under a profiling run.
ATESPACE="${ATESPACE:-ate-profiling}"
WORKLOAD="${WORKLOAD:-profiling}"
REPO="${REPO:-us-west1-docker.pkg.dev/amywxu-gke-dev/swebench-mirror/swebench-verified}" # TODO: set general defaults
BUCKET_NAME="${BUCKET_NAME:-snapshot-substrate-test-amywxu-gke-dev}" # TODO: set general defaults
STORAGE_LOCATION="${STORAGE_LOCATION:-gs://${BUCKET_NAME}/${ATESPACE}/}"
SANDBOX_CONFIG_NAME="${SANDBOX_CONFIG_NAME:-gvisor-default}"
# An actor occupies its whole worker, so these must stay at or below the
# pool's per-worker limits (see internal/sizing) or the actor never places.
# deploy-pool.sh gives each worker 1 CPU / 2Gi by default, and these
# containers only sleep: the 1-3 GB is rootfs on disk, not resident memory.
ACTOR_CPU="${ACTOR_CPU:-1}"
ACTOR_MEMORY="${ACTOR_MEMORY:-2Gi}"
# FULL captures process memory, so a resume is a true restore. DATA captures
# only durable volumes, so the process cold-boots on every resume -- which is
# how to profile a cold boot that still emits an atelet phase breakdown.
SNAPSHOT_SCOPE="${SNAPSHOT_SCOPE:-SNAPSHOT_CONTENT_SCOPE_FULL}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"
# Templates are immutable: the server has no update verb. Re-running with a
# changed image is a no-op unless the old template is removed first.
RECREATE="${RECREATE:-0}"

for tool in crane jq kubectl; do
  command -v "$tool" >/dev/null || { echo "error: $tool is required" >&2; exit 1; }
done

ate() { kubectl ate "$@"; }

DIGEST="$(crane digest "${REPO}:sweb.eval.x86_64.${INSTANCE}")"
echo "${ATESPACE}/${NAME} -> ${REPO}@${DIGEST}"

# The store rejects a template whose atespace does not exist. Creating one
# that is already there is not an error worth surfacing.
ate create atespace "${ATESPACE}" >/dev/null 2>&1 || \
  ate get atespace "${ATESPACE}" >/dev/null 2>&1 || {
    echo "error: could not create or find atespace ${ATESPACE}" >&2; exit 1; }

if ate get actor-template "${NAME}" -a "${ATESPACE}" >/dev/null 2>&1; then
  if [[ "${RECREATE}" == "1" ]]; then
    # Deleting a template also deletes its golden actor and golden snapshot.
    echo "deleting existing ${ATESPACE}/${NAME}"
    ate delete actor-template "${NAME}" -a "${ATESPACE}"
    until ! ate get actor-template "${NAME}" -a "${ATESPACE}" >/dev/null 2>&1; do sleep 2; done
  else
    echo "${ATESPACE}/${NAME} already exists; keeping it (RECREATE=1 to replace)"
  fi
fi

if ! ate get actor-template "${NAME}" -a "${ATESPACE}" >/dev/null 2>&1; then
  ate create actor-template -f - >/dev/null <<EOF
metadata:
  atespace: ${ATESPACE}
  name: ${NAME}
workerSelector:
  matchLabels:
    workload: ${WORKLOAD}
containers:
# The image declares CMD ["/bin/bash"], which exits at once without a tty.
# Idling keeps the actor alive; the point here is the pull, not the workload.
# No readyz probe, so the reconciler treats the actor as ready as soon as it
# runs rather than waiting on an HTTP endpoint that does not exist.
- name: testbed
  image: ${REPO}@${DIGEST}
  command: ["/bin/sleep"]
  args: ["infinity"]
resources:
  limits:
  - name: cpu
    quantity: "${ACTOR_CPU}"
  - name: memory
    quantity: ${ACTOR_MEMORY}
snapshotsConfig:
  # FULL on both by default, so a suspend captures process memory and a resume
  # is a real restore. DATA scope makes every "resume" a cold boot in disguise,
  # which is exactly what SNAPSHOT_SCOPE=SNAPSHOT_CONTENT_SCOPE_DATA is for.
  onPause: ${SNAPSHOT_SCOPE}
  onCommit: ${SNAPSHOT_SCOPE}
  storageLocation: ${STORAGE_LOCATION}
sandboxConfig:
  sandboxClass: SANDBOX_CLASS_GVISOR
  configName: ${SANDBOX_CONFIG_NAME}
EOF
fi

# Creating the template makes the control plane boot a golden actor at once,
# which pulls this same image. A profiling run started before that finishes
# does not get its own pull: atelet dedupes concurrent requests for a digest
# through a singleflight group, so the run blocks on the golden actor's fetch
# and reports a cache miss with no pull duration under it, timing only the
# tail it happened to wait for. Waiting for the golden snapshot keeps the two
# apart.
#
# There is no `kubectl wait` for substrate resources, so this polls the way
# wait_actortemplate_ready() in benchmarking/workloads/deploy.sh does.
#
# kubectl-ate versions differ in two ways, so both are accepted: `get -o json`
# may wrap the template in {"actorTemplates": [...]}, and readiness is either
# a golden tag or a golden snapshot URI.
STATUS_JQ='(.actorTemplates[0] // .) | .status.goldenSnapshotStatus'
echo "waiting for the ${ATESPACE}/${NAME} golden snapshot (timeout ${READY_TIMEOUT}s)..."
deadline=$((SECONDS + READY_TIMEOUT))
while ((SECONDS < deadline)); do
  if json="$(ate get actor-template "${NAME}" -a "${ATESPACE}" -o json 2>/dev/null)"; then
    if tag="$(jq -r "${STATUS_JQ} | .goldenTag.name // .goldenSnapshot.snapshotUri // empty" <<<"${json}")" \
       && [[ -n "${tag}" ]]; then
      echo "golden snapshot ready: ${tag}"
      exit 0
    fi
    # A golden actor that crashes leaves the template unready forever, so the
    # timeout alone does not say whether this is slow or broken.
    if msg="$(jq -r "${STATUS_JQ} | .errorMessage // empty" <<<"${json}")" \
       && [[ -n "${msg}" ]]; then
      echo "error: ${ATESPACE}/${NAME} failed to build its golden snapshot: ${msg}" >&2
      exit 1
    fi
  fi
  sleep 5
done

echo "error: timed out after ${READY_TIMEOUT}s waiting for the ${ATESPACE}/${NAME} golden snapshot" >&2
ate get actor-template "${NAME}" -a "${ATESPACE}" -o json 2>/dev/null \
  | jq "${STATUS_JQ}" >&2 || true
exit 1
