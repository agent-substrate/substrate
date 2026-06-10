#!/usr/bin/env bash

#  Copyright 2026 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Demo workload entrypoint: periodically invokes Antigravity with a task and
# idles between intervals. The idle window is what substrate uses to suspend
# this actor and multiplex its worker onto another actor.
#
# Env vars:
#   TASK                — the prompt available to ANTIGRAVITY_COMMAND each tick
#   INTERVAL_SECONDS  — sleep length between ticks (longer = more multiplex headroom)
#   ANTIGRAVITY_COMMAND — command to run once per tick; TASK is exported

set -u

if [ -z "${ANTIGRAVITY_COMMAND:-}" ]; then
  echo "[demo-actor] ERROR: ANTIGRAVITY_COMMAND not set; refusing to start" >&2
  exit 1
fi

ACTOR_NAME="${ACTOR_NAME:-$(hostname)}"
TICK=0

export TASK

echo "[demo-actor:${ACTOR_NAME}] starting; task=\"${TASK}\" interval=${INTERVAL_SECONDS}s command=\"${ANTIGRAVITY_COMMAND}\""

while true; do
  TICK=$((TICK + 1))
  echo ""
  echo "[demo-actor:${ACTOR_NAME}] === tick ${TICK} at $(date -u +%H:%M:%SZ) ==="
  echo "[demo-actor:${ACTOR_NAME}] running: ${TASK}"
  echo "---"
  # Output streams to stdout so kubectl logs picks it up live. Keep the
  # Antigravity invocation configurable until its official headless prompt
  # flags are documented.
  sh -c "${ANTIGRAVITY_COMMAND}" 2>&1 || echo "[demo-actor:${ACTOR_NAME}] antigravity command exited non-zero"
  echo "---"
  echo "[demo-actor:${ACTOR_NAME}] tick ${TICK} done; sleeping ${INTERVAL_SECONDS}s"
  sleep "${INTERVAL_SECONDS}"
done
