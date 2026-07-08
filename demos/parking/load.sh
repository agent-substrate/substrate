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

# load.sh -- oversubscription load generator for the request-parking demo.
#
# It drives one concurrent request->suspend loop per actor against a small
# WorkerPool. Because there are more actors than workers, the pool is constantly
# saturated; the suspend at the end of each loop is what frees a worker for a
# competitor (standing in for an actor going idle, since auto-suspend-on-idle
# isn't implemented yet). The result tally shows how parking turns transient
# saturation into (slightly slower) 200s instead of 503s.
#
# Compare two runs:
#   * parking ON  (default router config)        -> ~all 200, some elevated latency
#   * parking OFF (--parking-max-parked=0)       -> a burst of 503s
# See README.md for how to flip the router flag.
#
# Usage:
#   ./load.sh [-d duration_secs] [-r router_url] [-a atespace] [actor_id ...]
#
# Examples:
#   ./load.sh                      # 30s, actors p1 p2 p3 p4, http://localhost:8000
#   ./load.sh -d 60 p1 p2 p3 p4 p5 p6
#
# Prerequisites:
#   * `kubectl ate` plugin installed (go install ./cmd/kubectl-ate)
#   * router port-forwarded:  kubectl port-forward -n ate-system svc/atenet-router 8000:80

set -uo pipefail

DURATION=30
ROUTER="http://localhost:8000"
TEMPLATE="ate-demo-parking/parking"
ATESPACE="parking"
SUFFIX="actors.resources.substrate.ate.dev"

usage() {
  cat <<'EOF'
load.sh -- oversubscription load generator for the request-parking demo.

Usage: ./load.sh [-d duration_secs] [-r router_url] [-a atespace] [actor_id ...]
  -d   load duration in seconds (default 30)
  -r   router base URL          (default http://localhost:8000)
  -a   atespace for the actors  (default parking)
  args actor IDs                (default: p1 p2 p3 p4)

Prereqs: `kubectl ate` installed and the router port-forwarded
         (kubectl port-forward -n ate-system svc/atenet-router 8000:80).
EOF
}

while getopts ":d:r:a:h" opt; do
  case "${opt}" in
    d) DURATION="${OPTARG}" ;;
    r) ROUTER="${OPTARG}" ;;
    a) ATESPACE="${OPTARG}" ;;
    h) usage; exit 0 ;;
    *) echo "unknown option -${OPTARG}; use -h for help" >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

ACTORS=("$@")
if [[ ${#ACTORS[@]} -eq 0 ]]; then
  ACTORS=(p1 p2 p3 p4)
fi

TMP="$(mktemp -d)"
pids=()
cleanup() { rm -rf "${TMP}"; }
abort() { [[ ${#pids[@]} -gt 0 ]] && kill "${pids[@]}" 2>/dev/null; cleanup; exit 130; }
trap cleanup EXIT
trap abort INT TERM

echo "==> request-parking load test"
echo "    router:   ${ROUTER}"
echo "    duration: ${DURATION}s"
echo "    actors:   ${ACTORS[*]} (${#ACTORS[@]}) vs a 2-worker pool -> oversubscribed"
echo

# Preflight: make sure the atespace and actors exist (idempotent; ignore
# "already exists").
echo "==> ensuring atespace ${ATESPACE} and actors exist (template ${TEMPLATE})"
kubectl ate create atespace "${ATESPACE}" >/dev/null 2>&1 || true
for a in "${ACTORS[@]}"; do
  kubectl ate create actor "${a}" --atespace "${ATESPACE}" --template "${TEMPLATE}" >/dev/null 2>&1 || true
done

# One worker per actor: hammer it with request->suspend until the deadline.
worker() {
  local actor="$1" host="$1.${ATESPACE}.${SUFFIX}" log="${TMP}/$1.log"
  local deadline=$(( $(date +%s) + DURATION ))
  while [[ $(date +%s) -lt ${deadline} ]]; do
    # %{http_code} lets us tally outcomes; %{time_total} reveals parking waits.
    curl -s -o /dev/null -w '%{http_code} %{time_total}\n' \
      -H "Host: ${host}" "${ROUTER}" >>"${log}" 2>/dev/null
    # Free the worker so a parked competitor can proceed (simulate going idle).
    kubectl ate suspend actor "${actor}" --atespace "${ATESPACE}" >/dev/null 2>&1 || true
  done
}

echo "==> generating load for ${DURATION}s ..."
for a in "${ACTORS[@]}"; do
  worker "${a}" &
  pids+=($!)
done
wait

# ---- aggregate -------------------------------------------------------------
echo
echo "==> results"
cat "${TMP}"/*.log 2>/dev/null >"${TMP}/all" || true
total=$(wc -l <"${TMP}/all" | tr -d ' ')
if [[ "${total}" -eq 0 ]]; then
  echo "    no responses recorded -- is the router port-forwarded at ${ROUTER}?"
  exit 1
fi

ok=$(awk '$1==200' "${TMP}/all" | wc -l | tr -d ' ')
busy=$(awk '$1==503' "${TMP}/all" | wc -l | tr -d ' ')
other=$(awk '$1!=200 && $1!=503' "${TMP}/all" | wc -l | tr -d ' ')
slowest_ok=$(awk '$1==200{print $2}' "${TMP}/all" | sort -rn | head -1)
avg_ok=$(awk '$1==200{s+=$2;n++} END{if(n)printf "%.3f", s/n; else print "n/a"}' "${TMP}/all")

printf "    total requests : %s\n" "${total}"
printf "    200 OK         : %s\n" "${ok}"
printf "    503 unavailable: %s\n" "${busy}"
printf "    other          : %s\n" "${other}"
printf "    200 latency    : avg %ss, slowest %ss  <- parked requests sit here\n" "${avg_ok}" "${slowest_ok:-n/a}"
echo
if [[ "${busy}" -eq 0 ]]; then
  echo "    => 0 failures under saturation: parking absorbed the contention."
  echo "       Re-run with the router started --parking-max-parked=0 to see 503s."
else
  echo "    => ${busy} requests were shed with 503 (parking off, or lot full / budget exceeded)."
fi
