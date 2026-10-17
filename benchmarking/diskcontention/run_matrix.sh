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

# Runs the disk-contention matrix for one runtime on an already-deployed
# cluster: arms are interleaved within each repetition so slow drift hits all
# arms equally.
#
#   A     3 probe actors only (baseline)
#   B<x>  3 probes + 8 glutton actors writing fsync'd 64 MiB chunks to their
#         durable dir, capped at x% of the boot disk's provisioned MB/s
#         (Bmax = uncapped)
#   B<x>nf  as B<x> but without fsync (page cache + writeback only)
#   C<n>  3 probes + n sweperf actors (C = C8), back to back (no think time;
#         the probes' think time is --dc-probe-*-wait-time)
#   C     3 probes + 8 sweperf (SWE-bench replay) actors
#
# Usage: run_matrix.sh <gvisor|microvm>
# Env: as run_arm.sh, plus REPS (default 3), DURATION (default 10m),
#      DISK_MBPS (default 290), SWEEP_ONCE (default "B25 B50 B100": arms run in
#      rep 1 only), EVERY_REP (default "A Bmax C"), NAME_PREFIX (default
#      sweperf_dc; test names are <prefix>_<runtime>_<arm>), PROBES (default
#      3), NOISE (default 8 noise VUs), PROBE_MEM (default 512Mi), PROBE_WAIT_MIN
#      and PROBE_WAIT_MAX (default 20 and 30 s), MIN_ROOTFS_FREE (default 0.30:
#      skip an arm when the boot disk is fuller than that), LIFECYCLE_MODE
#      (default suspend; pause hibernates the probes with local snapshots).

set -o errexit -o nounset -o pipefail
RUNTIME="${1:?runtime}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPS="${REPS:-3}"
DURATION="${DURATION:-10m}"
DISK_MBPS="${DISK_MBPS:-290}"
SWEEP_ONCE="${SWEEP_ONCE-B25 B50 B100}"
EVERY_REP="${EVERY_REP-A Bmax C}"
PROBES="${PROBES:-3}"
NOISE="${NOISE:-8}"
PROBE_MEM="${PROBE_MEM:-512Mi}"
PROBE_WAIT_MIN="${PROBE_WAIT_MIN:-20}"
PROBE_WAIT_MAX="${PROBE_WAIT_MAX:-30}"
LIFECYCLE_MODE="${LIFECYCLE_MODE:-suspend}"

# Probe actors: glutton suspend/resume cycles (cf. glutton_mem_*gi in
# benchmarking/automation/tests.yaml), sized and paced so that the probes
# alone keep the disk well below saturation. Back-to-back 1 GiB probes
# (the tests.yaml config, 1 s think time) saturate a 290 MB/s boot disk by
# themselves, which leaves no clean baseline.
PROBE_FLAGS=(--dc-probe-users "${PROBES}" --mem-target "${PROBE_MEM}" --mem-churn 32Mi
  --mem-read all --dc-probe-min-wait-time "${PROBE_WAIT_MIN}"
  --dc-probe-max-wait-time "${PROBE_WAIT_MAX}"
  --trace-probability 1.0 --spawn-rate 4 --lifecycle-mode "${LIFECYCLE_MODE}")

arm_flags() {
  local arm="$1"
  case "${arm}" in
    A) echo "--dc-noise-mode none" ;;
    Bmax) echo "--dc-noise-mode disk --dc-noise-rate-mbps 0" ;;
    B*nf)
      # Same rate, no fsync: data reaches the disk only through writeback.
      local pct="${arm#B}"; pct="${pct%nf}"
      echo "--dc-noise-mode disk --dc-noise-no-fsync 1 --dc-noise-rate-mbps $(python3 -c "print(round(${DISK_MBPS}*${pct}/100/${NOISE}, 2))")"
      ;;
    B*)
      local pct="${arm#B}"
      echo "--dc-noise-mode disk --dc-noise-rate-mbps $(python3 -c "print(round(${DISK_MBPS}*${pct}/100/${NOISE}, 2))")"
      ;;
    C|C[0-9]*) echo "--dc-noise-mode sweperf --sweperf-template sweperf-astropy-7336" ;;
    *)
      echo "[matrix] ERROR: unknown arm: ${arm}" >&2
      return 1
      ;;
  esac
}

# Boot-disk headroom guard: microVM sweperf arms can fill the 100 GB boot disk,
# which triggers kubelet evictions of atelet/workers and poisons later arms.
MIN_ROOTFS_FREE="${MIN_ROOTFS_FREE:-0.30}"
rootfs_free() {
  curl -s "${PROM_URL:-http://localhost:19090}/api/v1/query" --data-urlencode \
    'query=min(node_filesystem_avail_bytes{mountpoint="/mnt/stateful_partition"} / node_filesystem_size_bytes{mountpoint="/mnt/stateful_partition"})' \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else 1)' 2>/dev/null || echo 1
}

run_one() {
  local arm="$1" rep="$2" users=$((PROBES + NOISE))
  local free
  free=$(rootfs_free)
  if python3 -c "import sys; sys.exit(0 if float('${free}') < float('${MIN_ROOTFS_FREE}') else 1)"; then
    echo "[matrix] SKIP ${arm} rep ${rep}: boot disk free ${free} < ${MIN_ROOTFS_FREE}"
    return 0
  fi
  [[ "${arm}" == "A" ]] && users=${PROBES}
  # C<n>: n sweperf actors instead of NOISE.
  [[ "${arm}" =~ ^C([0-9]+)$ ]] && users=$((PROBES + BASH_REMATCH[1]))
  local name="${NAME_PREFIX:-sweperf_dc}_${RUNTIME}_${arm}"
  # shellcheck disable=SC2046
  "${SCRIPT_DIR}/run_arm.sh" "${name}" "${RUNTIME}" "${arm}" "${users}" "${DURATION}" \
    "${PROBE_FLAGS[@]}" $(arm_flags "${arm}") \
    || echo "[matrix] ${name} rep ${rep} FAILED"
  # Let noise actors finish deleting and dirty pages drain before the next arm.
  sleep 60
}

# Refuse unknown or malformed arms before running anything.
for arm in ${EVERY_REP} ${SWEEP_ONCE}; do
  flags=$(arm_flags "${arm}") || exit 1
  if [[ "${flags}" == *--dc-noise-rate-mbps* ]] && ! [[ "${flags##*--dc-noise-rate-mbps }" =~ ^[0-9.]+$ ]]; then
    echo "[matrix] ERROR: arm ${arm} has no valid noise rate" >&2
    exit 1
  fi
done

for rep in $(seq 1 "${REPS}"); do
  arms="${EVERY_REP}"
  [[ "${rep}" == 1 ]] && arms="${EVERY_REP} ${SWEEP_ONCE}"
  for arm in ${arms}; do
    echo "[matrix] $(date -u +%H:%M:%S) ${RUNTIME} rep ${rep} arm ${arm}"
    run_one "${arm}" "${rep}"
  done
done
echo "[matrix] done"
