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

# Runs ONE disk-contention arm on an already-deployed cluster and collects
# node-level evidence for it.
#
# Usage:
#   run_arm.sh <name> <runtime> <arm> <users> <duration> [extra runner flags...]
#
# Required env: RUNNER_IMAGE, DEST (gs://bucket/prefix), OUT_DIR (local).
# Optional env: JOB_TMPL (runner Job template path), WARMUP_SECS (default 180), PROM_URL (default
#   http://localhost:19090, a port-forward of svc/prometheus), DEVICE
#   (default auto: the disk mounted at /var/lib/ate/actors or /var/lib/ate, else the boot disk),
#   GCE_INSTANCE_ID, PROJECT_ID, CLEANUP_TIMEOUT_SECS (default 300).
#
# Results: ${OUT_DIR}/<name>/<tag>/{stats.jsonl,traces.txt,...,
#   meta.json, collect.json}

set -o errexit -o nounset -o pipefail

if [[ $# -lt 5 ]]; then
  sed -n '16,/^[^#]/{/^#/p;}' "$0"
  exit 1
fi
NAME="$1" RUNTIME="$2" ARM="$3" USERS="$4" DURATION="$5"
shift 5
EXTRA=("$@")

: "${RUNNER_IMAGE:?}" "${DEST:?}" "${OUT_DIR:?}"
WARMUP_SECS="${WARMUP_SECS:-180}"
PROM_URL="${PROM_URL:-http://localhost:19090}"
DEVICE="${DEVICE:-auto}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

TAG="dc-$(date +%s)"
JOB="runner-${TAG}"
RUN_DIR="${OUT_DIR}/${NAME}/${TAG}"
mkdir -p "${RUN_DIR}"

# Leftover actors from an earlier run skew results; record how many exist in the
# benchmark atespace (where boomer-worker creates probe and noise actors).
ATESPACE="${ATESPACE:-benchmark}"
KUBECTL_ATE="${KUBECTL_ATE:-${OUT_DIR}/.kubectl-ate}"
if [[ ! -x "${KUBECTL_ATE}" ]] && command -v go >/dev/null && [[ -d "${ROOT}/cmd/kubectl-ate" ]]; then
  (cd "${ROOT}" && go build -o "${KUBECTL_ATE}" ./cmd/kubectl-ate)
fi
if [[ -x "${KUBECTL_ATE}" ]]; then
  ACTIVE_JSON=$("${KUBECTL_ATE}" get actors -a "${ATESPACE}" -o json 2>/dev/null || true)
  ACTIVE=$(printf '%s' "${ACTIVE_JSON}" | python3 -c 'import json,sys
d=json.load(sys.stdin); a=d if isinstance(d,list) else (d.get("actors") or d.get("items") or [])
print(len(a))' 2>/dev/null || echo "?")
else
  # Actors running on the nodes, from atelet's stats poller.
  ACTIVE=$(python3 -c 'import json,sys,urllib.request
u=sys.argv[1]+"/api/v1/query?query=sum(ate_actor_stats_sampled_actors)"
r=json.loads(urllib.request.urlopen(u,timeout=5).read())["data"]["result"]
print(int(float(r[0]["value"][1])) if r else "?")' "${PROM_URL}" 2>/dev/null || echo "?")
fi
echo "[run_arm] ${NAME} tag=${TAG} actors_before=${ACTIVE}"

job_yaml="${RUN_DIR}/job.yaml"
sed -e "s|\${JOB_NAME}|${JOB}|" -e "s|\${IMAGE}|${RUNNER_IMAGE}|" \
    -e "s|\${TEST_FILE}|/app/tests/diskcontention.py|" -e "s|\${DURATION}|${DURATION}|" \
    -e "s|\${USERS}|${USERS}|" -e "s|\${TAG}|${TAG}|" -e "s|\${NAME}|${NAME}|" \
    -e "s|\${DEST}|${DEST}|" -e "s|\${RUNNER_CPU}|2|" -e "s|\${RUNNER_MEMORY}|2Gi|" \
    "${JOB_TMPL:-${ROOT}/benchmarking/automation/manifests/runner-job.yaml.tmpl}" > "${job_yaml}"
# Append the extra flags to the runner container's args list. The args list
# ends right before `env:` in the template.
python3 - "${job_yaml}" "${EXTRA[@]}" <<'EOF'
import sys
path, extra = sys.argv[1], sys.argv[2:]
lines = open(path).read().split("\n")
out = []
inserted = False
for i, line in enumerate(lines):
    if not inserted and line.strip() == "env:" and out and out[-1].strip().startswith('- "'):
        indent = out[-1][: len(out[-1]) - len(out[-1].lstrip())]
        out += [f'{indent}- "{e}"' for e in extra]
        inserted = True
    out.append(line)
if not inserted:
    sys.exit("could not find the runner args list")
open(path, "w").write("\n".join(out))
EOF

kubectl apply -f "${job_yaml}" >/dev/null
echo "[run_arm] submitted job ${JOB}; waiting (duration ${DURATION})"
# The job runs for its duration plus ~1.5 min of telemetry and upload. Poll
# for success or failure: `kubectl wait --for=condition=complete` never
# returns for a failed Job.
deadline=$((SECONDS + 3600))
while :; do
  state=$(kubectl -n benchmarking get "job/${JOB}" -o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null || true)
  case "${state}" in
    [1-9]*/*) break ;;
    */[1-9]*) echo "[run_arm] job ${JOB} failed"; kubectl -n benchmarking logs "job/${JOB}" --tail=80; exit 1 ;;
  esac
  if ((SECONDS >= deadline)); then
    echo "[run_arm] job ${JOB} did not finish within 60m"; kubectl -n benchmarking logs "job/${JOB}" --tail=80; exit 1
  fi
  sleep 15
done
kubectl -n benchmarking logs "job/${JOB}" --timestamps > "${RUN_DIR}/runner.log"

# The measurement window is the locust run itself, from the runner's own log.
read -r START END < <(python3 - "${RUN_DIR}/runner.log" "${SCRIPT_DIR}" <<'EOF'
import sys
sys.path.insert(0, sys.argv[2])
from report import parse_ts
start = end = None
for line in open(sys.argv[1]):
    ts, _, msg = line.partition(" ")
    t = parse_ts(ts)
    if t is None:
        continue
    if start is None and "Running:" in msg and "locust" in msg:
        start = t
    if "Locust exited with code" in msg:
        end = t
if start is None or end is None:
    sys.exit(f"could not parse locust window from {sys.argv[1]}: start={start} end={end}")
print(f"{start:.0f} {end:.0f}")
EOF
)
echo "[run_arm] locust window ${START}..${END} ($((END - START))s)"

gcloud storage cp "${DEST}/runs/${NAME}/**/run_tag=${TAG}/*" "${RUN_DIR}/" >/dev/null 2>&1 \
  || echo "[run_arm] WARNING: could not download results from ${DEST}"

cat > "${RUN_DIR}/meta.json" <<EOF
{"name": "${NAME}", "tag": "${TAG}", "runtime": "${RUNTIME}", "arm": "${ARM}",
 "users": ${USERS}, "duration": "${DURATION}", "start": ${START}, "end": ${END},
 "warmup": ${WARMUP_SECS}, "active_actors_before": "${ACTIVE}",
 "runner_image": "${RUNNER_IMAGE}", "flags": "$(printf '%s ' "${EXTRA[@]}")"}
EOF

collect_args=(--prom "${PROM_URL}" --start "${START}" --end "${END}" --warmup "${WARMUP_SECS}"
  --device "${DEVICE}" --out "${RUN_DIR}/collect.json")
if [[ -n "${GCE_INSTANCE_ID:-}" && -n "${PROJECT_ID:-}" ]]; then
  collect_args+=(--project "${PROJECT_ID}" --instance-id "${GCE_INSTANCE_ID}")
fi
python3 "${SCRIPT_DIR}/collect.py" "${collect_args[@]}" > /dev/null

# Store the window, node evidence and runner log next to the runner's own
# results, so report.py can be run from GCS alone.
GCS_RUN=$(gcloud storage ls "${DEST}/runs/${NAME}/**/run_tag=${TAG}/stats.csv" 2>/dev/null | head -1 || true)
GCS_RUN="${GCS_RUN%stats.csv}"
if [[ -n "${GCS_RUN}" ]]; then
  gcloud storage cp "${RUN_DIR}/meta.json" "${RUN_DIR}/collect.json" "${RUN_DIR}/runner.log" \
    "${GCS_RUN}" >/dev/null 2>&1 || echo "[run_arm] WARNING: upload of collect.json failed"
else
  echo "[run_arm] WARNING: runner results not found in ${DEST}; meta/collect kept locally only"
fi

# Delete the actors the run left behind so they don't skew the next arm. An
# actor wedged mid-transition can survive the delete
# (agent-substrate/substrate#2011), so give up after CLEANUP_TIMEOUT_SECS.
if [[ -x "${KUBECTL_ATE}" ]]; then
  cleanup_deadline=$((SECONDS + ${CLEANUP_TIMEOUT_SECS:-300}))
  while :; do
    if ! left=$("${KUBECTL_ATE}" get actors -a "${ATESPACE}" -o json 2>/dev/null | python3 -c 'import json,sys
for a in json.load(sys.stdin).get("actors") or []:
    print(a["metadata"]["name"], a.get("status", {}).get("state", ""))' 2>/dev/null); then
      echo "[run_arm] WARNING: could not list actors in ${ATESPACE}; skipping cleanup"
      break
    fi
    [[ -z "${left}" ]] && break
    if ((SECONDS >= cleanup_deadline)); then
      echo "[run_arm] WARNING: $(wc -l <<<"${left}") actors left in ${ATESPACE} after cleanup"
      break
    fi
    awk '$2 != "ACTOR_STATE_DELETING" {print $1}' <<<"${left}" \
      | xargs -r -P 8 -I{} "${KUBECTL_ATE}" delete actor {} -a "${ATESPACE}" --any-state >/dev/null 2>&1 || true
    sleep 10
  done
else
  echo "[run_arm] no kubectl ate; actors left in ${ATESPACE} are not deleted"
fi
echo "[run_arm] done: ${RUN_DIR}"
