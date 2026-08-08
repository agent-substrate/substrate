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

# Drives the atenet-router capacity sweep: one Envoy CPU size per arm, each
# running the same ladder of offered load, one Job per arm. Unattended by
# design; everything that writes to the cluster lives here, and load
# generation (which does not) lives in the binary.
#
#   0  clean       1  failed       2  interrupted
#   3  rig-limited 4  preflight/provisioning

# shellcheck source=benchmarking/routercap/common.sh
source "$(git rev-parse --show-toplevel)/benchmarking/routercap/common.sh"

ARMS="${RC_ARMS_DEFAULT}"
ACTORS=100
START_QPS=1000
STEP_QPS=1000
RUNGS=16
HOLD_S=45
WARMUP_S=10
# Not a knob: this is the denominator of the generator's own CPU guard, which
# trips at 80% of it. 80, not 88, because the loadgen node is one
# c3-standard-88 and a request for all of it would sit Pending; change only
# alongside the loadgen pool's machine type.
LOADGEN_CPU=80
LOADGEN_MEMORY=64Gi
SIDECAR_CORES="${RC_SIDECAR_CORES}"
OUTPUT_DIR=""
TAG=""
IMAGE="${ROUTERCAP_IMAGE:-}"
SKIP_CHARTS=false
SMOKE=false

usage() {
  cat <<EOF
Usage: $0 [options]

  --arms "2 4 8"        Envoy CPU sizes to sweep (default: ${ARMS}).
  --actors N            Actors to warm, one per worker pod (default: ${ACTORS}).
  --start-qps N         First rung (default: ${START_QPS}).
  --step-qps N          Added by each rung (default: ${STEP_QPS}).
  --rungs N             Rungs per ladder (default: ${RUNGS}).
  --hold N              Seconds per rung (default: ${HOLD_S}).
  --warmup N            Leading seconds of each rung marked warmup (default: ${WARMUP_S}).
  --output-dir DIR      Where the run lands (default: benchmarking/routercap/runs/<utc timestamp>).
  --tag T               Run tag; defaults to the short commit, with -dirty if the tree is.
  --image REF           Skip the ko build and use this image.
  --smoke               One arm, 2 actors, 3 short rungs. Proves the rig, measures nothing.
  --skip-charts         Leave charts.py unrun.
  -h, --help            This.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --arms) shift; ARMS="$1" ;;
    --arms=*) ARMS="${1#*=}" ;;
    --actors) shift; ACTORS="$1" ;;
    --actors=*) ACTORS="${1#*=}" ;;
    --start-qps) shift; START_QPS="$1" ;;
    --start-qps=*) START_QPS="${1#*=}" ;;
    --step-qps) shift; STEP_QPS="$1" ;;
    --step-qps=*) STEP_QPS="${1#*=}" ;;
    --rungs) shift; RUNGS="$1" ;;
    --rungs=*) RUNGS="${1#*=}" ;;
    --hold) shift; HOLD_S="$1" ;;
    --hold=*) HOLD_S="${1#*=}" ;;
    --warmup) shift; WARMUP_S="$1" ;;
    --warmup=*) WARMUP_S="${1#*=}" ;;
    --output-dir) shift; OUTPUT_DIR="$1" ;;
    --output-dir=*) OUTPUT_DIR="${1#*=}" ;;
    --tag) shift; TAG="$1" ;;
    --tag=*) TAG="${1#*=}" ;;
    --image) shift; IMAGE="$1" ;;
    --image=*) IMAGE="${1#*=}" ;;
    --smoke) SMOKE=true ;;
    --skip-charts) SKIP_CHARTS=true ;;
    -h|--help) usage; exit 0 ;;
    *) rc::die "unknown option: $1" ;;
  esac
  shift
done

if [[ "${SMOKE}" == "true" ]]; then
  # Proves the rig end to end; measures nothing. Three rungs is the fewest
  # that gives the ladder a slope.
  ARMS="${ARMS%% *}"
  ACTORS=2
  RUNGS=3
  HOLD_S=30
  WARMUP_S=5
  START_QPS=200
  STEP_QPS=200
fi

rc::need kubectl git go python3
rc::env
rc::assert_cluster

GIT_SHA="$(git -C "${ROOT}" rev-parse HEAD)"
if [[ -z "${TAG}" ]]; then
  TAG="$(git -C "${ROOT}" rev-parse --short HEAD)"
  if [[ -n "$(git -C "${ROOT}" status --porcelain)" ]]; then
    # A run tagged with a clean commit that was not the code that ran is worse
    # than no tag at all.
    TAG="${TAG}-dirty"
  fi
fi
if [[ -z "${OUTPUT_DIR}" ]]; then
  OUTPUT_DIR="${RC_DIR}/runs/$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "${OUTPUT_DIR}"

MACHINE_TYPE="$(rc::kubectl get nodes -l "${RC_ROLE_KEY}=${RC_POOL_ROUTER}" \
  -o jsonpath='{.items[0].metadata.labels.node\.kubernetes\.io/instance-type}' 2>/dev/null || true)"
: "${MACHINE_TYPE:=${RC_MACHINE_TYPE}}"

# --- preflight ---------------------------------------------------------------

rc::step "preflight"
worker_pods="$(rc::kubectl -n "${RC_WORKER_NS}" get pods -l "${RC_WORKER_SELECTOR}" \
  --field-selector=status.phase=Running -o name | wc -l | tr -d ' ')"
if (( worker_pods < ACTORS )); then
  rc::die "${worker_pods} worker pods are Running but ${ACTORS} actors were asked for; one actor per pod is what keeps the per-worker connection-rate limit from binding before the concurrency limit"
fi
rc::step "${worker_pods} worker pods running"

# A generator that does not fit its node sits Pending until the sweep times
# out. Checked here rather than in provision.sh because LOADGEN_CPU lives
# here and the loadgen pool can be resized between a provision and a run.
loadgen_alloc="$(rc::kubectl get node -l "${RC_ROLE_KEY}=${RC_POOL_LOADGEN}" \
  -o jsonpath='{.items[0].status.allocatable.cpu}' 2>/dev/null || true)"
if [[ -n "${loadgen_alloc}" ]]; then
  if [[ "${loadgen_alloc}" == *m ]]; then lg_m="${loadgen_alloc%m}"; else lg_m=$(( loadgen_alloc * 1000 )); fi
  # 3 cores for atelet and GKE's own DaemonSets, same allowance provision.sh uses.
  if (( (LOADGEN_CPU + 3) * 1000 > lg_m )); then
    rc::die "the generator's ${LOADGEN_CPU} cores do not fit the loadgen node (${lg_m}m allocatable, 3 cores reserved for DaemonSets); the Job would sit Pending. Lower LOADGEN_CPU in run.sh or grow the pool's machine type"
  fi
fi

rc::kubectl apply -f "${RC_DIR}/manifests/rbac.yaml" >/dev/null

if [[ -z "${IMAGE}" ]]; then
  rc::step "building the generator image"
  ldflags=()
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -n "${line}" ]] && ldflags+=("--ldflags=${line}")
  done < <(make -C "${ROOT}" ldflags)
  # In a subshell at the repo root: ko resolves ./cmd/... against its own
  # working directory.
  IMAGE="$(cd "${ROOT}" && "${ROOT}/hack/run-tool.sh" ko build --platform=linux/amd64 \
    "${ldflags[@]}" ./cmd/benchmarking/routercap | tail -1)"
fi
rc::step "generator image: ${IMAGE}"

# --- one arm -----------------------------------------------------------------

# patch_arm resizes the Envoy container and matches its --concurrency; the
# binary scrapes envoy_server_concurrency back and refuses a mismatch.
# RC_CONCURRENCY overrides only --concurrency (diagnostic runs);
# RC_ROUTER_PODS scales the replicas, and the next run scales back to 1.
# Recreate strategy = fresh pod per arm; the sidecar stays pinned so the two
# containers remain separable.
patch_arm() {
  local arm="$1" patch="" threads="${RC_CONCURRENCY:-$1}" replicas="${RC_ROUTER_PODS:-1}"
  patch="$(rc::kubectl -n "${RC_ROUTER_NS}" get deployment atenet-router -o json | python3 -c '
import json, sys

arm, sidecar, threads, replicas = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
spec = json.load(sys.stdin)["spec"]["template"]["spec"]
# Replaces the whole strategy object, so any rollingUpdate block goes with it;
# leaving one behind alongside type Recreate is rejected by the API server.
ops = [{"op": "replace", "path": "/spec/strategy", "value": {"type": "Recreate"}},
       {"op": "replace", "path": "/spec/replicas", "value": replicas}]
seen = False
for i, c in enumerate(spec["containers"]):
    envoy = c["name"] == "envoy"
    cores = arm if envoy else sidecar
    for field in ("requests", "limits"):
        ops.append({"op": "replace",
                    "path": "/spec/template/spec/containers/%d/resources/%s/cpu" % (i, field),
                    "value": cores})
    if not envoy:
        continue
    seen = True
    for key in ("command", "args"):
        argv = c.get(key) or []
        if "--concurrency" in argv:
            ops.append({"op": "replace",
                        "path": "/spec/template/spec/containers/%d/%s/%d" % (i, key, argv.index("--concurrency") + 1),
                        "value": threads})
            break
    else:
        sys.exit("the envoy container passes no --concurrency, so its worker threads cannot be kept in step with its CPU limit")
if not seen:
    sys.exit("no container named envoy in the atenet-router Deployment")
json.dump(ops, sys.stdout)
' "${arm}" "${SIDECAR_CORES}" "${threads}" "${replicas}")" || return 4
  rc::kubectl -n "${RC_ROUTER_NS}" patch deployment atenet-router --type=json -p "${patch}" >/dev/null
  rc::kubectl -n "${RC_ROUTER_NS}" rollout status deployment/atenet-router --timeout=10m
}

# wait_pod_started blocks until the Job's pod is past Pending, so the log
# stream that follows starts at the first line rather than erroring out.
wait_pod_started() {
  local job="$1" deadline=$((SECONDS + 600))
  while (( SECONDS < deadline )); do
    local phase=""
    phase="$(rc::kubectl -n "${RC_JOB_NS}" get pod -l "job-name=${job}" \
      -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)"
    case "${phase}" in
      Running|Succeeded|Failed) return 0 ;;
    esac
    sleep 2
  done
  return 1
}

# job_exit_code reads the generator's own exit status. The binary distinguishes
# rig-limited from failed from interrupted, and collapsing that to "the Job
# failed" would throw away the only bit that says whether the number is usable.
job_exit_code() {
  local job="$1" deadline=$((SECONDS + 300))
  while (( SECONDS < deadline )); do
    local code=""
    code="$(rc::kubectl -n "${RC_JOB_NS}" get pod -l "job-name=${job}" \
      -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || true)"
    if [[ -n "${code}" ]]; then
      echo "${code}"
      return 0
    fi
    sleep 2
  done
  echo 1
}

run_arm() {
  local arm="$1"
  local dir="${OUTPUT_DIR}/arm-${arm}c"
  mkdir -p "${dir}"

  rc::step "arm ${arm}c: patching the router"
  # Checked explicitly: the sweep runs each arm with errexit off so one bad arm
  # does not take the rest of the sweep with it, which means a failing call
  # inside here does not unwind on its own.
  if ! patch_arm "${arm}"; then
    rc::warn "arm ${arm}c: could not resize the router; skipping rather than measuring the previous arm again under this arm's label"
    return 4
  fi

  local pod
  pod="$(rc::kubectl -n "${RC_ROUTER_NS}" get pod -l "${RC_ROUTER_SELECTOR}" \
    -o jsonpath='{.items[0].metadata.name}')"

  # Read per arm, not once: the rollout replaces the pod, and a range read from
  # the previous pod is a number about a container that no longer exists.
  local range=""
  range="$(rc::router_port_range "${pod}" "${RC_ROUTER_NS}")"
  if [[ -z "${range}" ]]; then
    rc::warn "could not read ip_local_port_range from ${pod}; the header will say the default was assumed"
  fi
  local port_range="${range// /-}"

  local job
  job="routercap-${arm}c-$(date -u +%H%M%S)"
  local deadline=$(( RUNGS * HOLD_S + 900 ))

  rc::step "arm ${arm}c: launching ${job} (deadline ${deadline}s)"
  sed \
    -e "s|\${JOB_NAME}|${job}|g" \
    -e "s|\${IMAGE}|${IMAGE}|g" \
    -e "s|\${ARM}|${arm}|g" \
    -e "s|\${CONCURRENCY}|${RC_CONCURRENCY:-${arm}}|g" \
    -e "s|\${ROUTER_PODS}|${RC_ROUTER_PODS:-1}|g" \
    -e "s|\${PASS}|1|g" \
    -e "s|\${ACTORS}|${ACTORS}|g" \
    -e "s|\${START_QPS}|${START_QPS}|g" \
    -e "s|\${STEP_QPS}|${STEP_QPS}|g" \
    -e "s|\${RUNGS}|${RUNGS}|g" \
    -e "s|\${HOLD}|${HOLD_S}s|g" \
    -e "s|\${WARMUP}|${WARMUP_S}s|g" \
    -e "s|\${PORT_RANGE}|${port_range}|g" \
    -e "s|\${NAME}|routercap|g" \
    -e "s|\${TAG}|${TAG}|g" \
    -e "s|\${GIT_SHA}|${GIT_SHA}|g" \
    -e "s|\${CLUSTER}|${RC_CLUSTER}|g" \
    -e "s|\${LOCATION}|${RC_LOCATION}|g" \
    -e "s|\${MACHINE_TYPE}|${MACHINE_TYPE}|g" \
    -e "s|\${LOADGEN_CPU}|${LOADGEN_CPU}|g" \
    -e "s|\${LOADGEN_MEMORY}|${LOADGEN_MEMORY}|g" \
    -e "s|\${DEADLINE}|${deadline}|g" \
    -e "s|\${ROLE_KEY}|${RC_ROLE_KEY}|g" \
    "${RC_DIR}/manifests/job.yaml.tmpl" > "${dir}/job.yaml"

  # A placeholder added to the template but not to the sed list above would
  # otherwise be applied verbatim — a run labelled with something it did not
  # do.
  local unrendered
  # shellcheck disable=SC2016  # literal ${VAR} placeholders are the search target
  unrendered="$(grep -o '\${[A-Z_]\+}' "${dir}/job.yaml" | sort -u | tr '\n' ' ')"
  if [[ -n "${unrendered}" ]]; then
    rc::warn "arm ${arm}c: job.yaml still contains ${unrendered}— add it to the substitution list in run.sh"
    return 4
  fi

  rc::kubectl apply -f "${dir}/job.yaml" >/dev/null

  if ! wait_pod_started "${job}"; then
    rc::kubectl -n "${RC_JOB_NS}" describe job "${job}" >"${dir}/job.describe" 2>&1 || true
    rc::warn "arm ${arm}c: pod never started; see ${dir}/job.describe"
    rc::kubectl -n "${RC_JOB_NS}" delete job "${job}" --wait=false >/dev/null 2>&1 || true
    return 4
  fi

  # Per-thread CPU sampler, backgrounded for the arm's whole run. Best-effort:
  # the arm is complete without it, so its failures go only to threads.err.
  local threads_pid=""
  "${RC_DIR}/threads.sh" "${pod}" "${RC_ROUTER_NS}" 5 \
    > "${dir}/threads.log" 2> "${dir}/threads.err" &
  threads_pid=$!

  # Streamed, not collected at the end: nothing can read files back out of the
  # distroless container, and streaming keeps every line an interrupted arm
  # already emitted.
  # errexit is saved and restored, never forced on: errexit would turn a
  # rig-limited arm into killing the whole sweep.
  local errexit_was="off"
  [[ $- == *e* ]] && errexit_was="on"
  set +o errexit
  rc::kubectl -n "${RC_JOB_NS}" logs -f "job/${job}" --tail=-1 \
    | python3 "${RC_DIR}/demux.py" "${dir}"
  [[ "${errexit_was}" == "on" ]] && set -o errexit

  # Stop the sampler and reduce its stream to per-worker cores.
  if [[ -n "${threads_pid}" ]]; then
    kill "${threads_pid}" >/dev/null 2>&1 || true
    wait "${threads_pid}" 2>/dev/null || true
    python3 "${RC_DIR}/threads.py" "${dir}/threads.log" > "${dir}/worker-cpu.jsonl" 2>>"${dir}/threads.err" || true
  fi

  local code
  code="$(job_exit_code "${job}")"
  rc::kubectl -n "${RC_JOB_NS}" delete job "${job}" --cascade=foreground --wait=true >/dev/null 2>&1 || true

  # A clean arm keeps only what downstream reads: samples.jsonl, run.json and
  # worker-cpu.jsonl. Debugging material survives only when the arm did not
  # exit clean, which is exactly when someone will want it.
  if [[ "${code}" -eq 0 ]]; then
    rm -f "${dir}/job.yaml" "${dir}/job.log" "${dir}/threads.log" "${dir}/threads.err"
  fi
  return "${code}"
}

# --- the sweep ---------------------------------------------------------------

# Ctrl-C deletes the Job with a foreground cascade, which SIGTERMs the
# generator, which suspends and deletes every actor on the way out. Without
# this a hundred actors survive the run that created them.
# shellcheck disable=SC2317,SC2329  # invoked via the trap below, not by call
cleanup() {
  local jobs=""
  jobs="$(rc::kubectl -n "${RC_JOB_NS}" get jobs -l app=routercap -o name 2>/dev/null || true)"
  if [[ -n "${jobs}" ]]; then
    rc::warn "interrupted; deleting ${jobs}"
    # shellcheck disable=SC2086
    rc::kubectl -n "${RC_JOB_NS}" delete ${jobs} --cascade=foreground --wait=true >/dev/null 2>&1 || true
  fi
  exit 2
}
trap cleanup INT TERM

rc::step "sweep: arms [${ARMS}] · ${RUNGS} rungs · ${HOLD_S}s each · ${ACTORS} actors · tag ${TAG}"
rc::step "output: ${OUTPUT_DIR}"

worst=0
for arm in ${ARMS}; do
  set +o errexit
  run_arm "${arm}"
  code=$?
  set -o errexit
  case "${code}" in
    0) rc::step "arm ${arm}c: complete" ;;
    2) rc::warn "arm ${arm}c: interrupted"; exit 2 ;;
    3) rc::warn "arm ${arm}c: RIG-LIMITED — the rig ran out, not the router; this arm's numbers are not about the router" ;;
    *) rc::warn "arm ${arm}c: failed (exit ${code})" ;;
  esac
  # The sweep continues past a failed arm. Two good arms and one bad one is a
  # partial answer; two unrun arms is none.
  (( code > worst )) && worst="${code}"
done

trap - INT TERM

# --- charts ------------------------------------------------------------------

if [[ "${SKIP_CHARTS}" != "true" ]]; then
  rc::step "rendering charts"
  set +o errexit
  python3 "${RC_DIR}/charts.py" "${OUTPUT_DIR}"
  charts_code=$?
  set -o errexit
  if (( charts_code != 0 )); then
    # charts.py reads only the run directory, so a charting failure costs a
    # rerun of charts.py, not of the sweep.
    rc::warn "charts.py failed (exit ${charts_code}); samples are intact, re-run: python3 ${RC_DIR}/charts.py ${OUTPUT_DIR}"
  fi
fi

rc::step "done: ${OUTPUT_DIR}"
if (( worst != 0 )); then
  rc::warn "at least one arm did not finish clean (worst exit ${worst})"
fi
exit "${worst}"
