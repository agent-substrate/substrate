#!/bin/sh
# cuda-checkpoint wrapper invoked by runsc via --save-restore-exec-argv.
# Toggles every CUDA-touching PID inside the sandbox. Idempotent, so runs
# fine on both pre-save and post-restore invocations.
set -e

CB=/usr/local/bin/cuda-checkpoint
[ -x "$CB" ] || { echo "wrapper: $CB missing" >&2; exit 1; }

pids=""
for d in /proc/[0-9]*; do
  pid=${d#/proc/}
  [ "$pid" = "$$" ] && continue
  [ -r "$d/maps" ] && grep -qE '(/dev/nvidia|libcuda\.so|libcudart\.so|libnvidia-ml\.so)' "$d/maps" 2>/dev/null && pids="$pids $pid"
done
[ -z "$pids" ] && { echo "wrapper: no CUDA pids" >&2; exit 0; }
for p in $pids; do
  echo "wrapper: --toggle pid=$p" >&2
  "$CB" --toggle --pid "$p"
done
