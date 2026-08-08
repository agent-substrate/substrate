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

# Samples per-thread CPU of the router pod's envoy container to stdout, one
# block per interval (default 5s): a "T <epoch>" line, then every thread's
# /proc stat line. threads.py turns the stream into per-worker cores.
#
# Per-thread CPU exists nowhere else (cAdvisor and Envoy only export sums), so
# it is read from /proc via an ephemeral debug container sharing envoy's PID
# namespace. Best-effort: run.sh backgrounds it and the arm is complete
# without it.
#
# Usage: threads.sh <pod> <namespace> [interval-seconds]

set -o errexit -o nounset -o pipefail

POD="${1:?router pod name}"
NS="${2:?namespace}"
INTERVAL="${3:-5}"

# The debug container joins the envoy container's PID namespace, but pid 1 is
# only envoy if the runtime set it up that way — so find it by comm instead of
# assuming. Everything below runs inside busybox sh on the node.
# shellcheck disable=SC2016  # the single-quoted script must reach busybox unexpanded
exec kubectl -n "${NS}" debug "${POD}" -q --profile=general \
  --image=busybox:1.36 --target=envoy --attach=true -- sh -c '
pid=""
for p in /proc/[0-9]*; do
  if [ "$(cat "$p/comm" 2>/dev/null)" = "envoy" ]; then pid="${p#/proc/}"; break; fi
done
if [ -z "$pid" ]; then echo "no envoy process visible" >&2; exit 1; fi
while true; do
  echo "T $(date +%s)"
  cat /proc/"$pid"/task/*/stat 2>/dev/null
  sleep '"${INTERVAL}"'
done'
