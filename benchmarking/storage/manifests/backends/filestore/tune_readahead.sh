#!/bin/sh
# Copyright 2026 The Agent Substrate Authors
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

# Sets a 16 MiB read ahead (read_ahead_kb=16384) on every NFS backing device
# (0:*) on the node. Each actor resume creates a new NFS mount with the kernel
# default of 128 KiB, so this loop rechecks every 0.1 s using shell builtins.

set -eu

target="${READ_AHEAD_KB:-16384}"

while true; do
  for f in /sys/class/bdi/0:*/read_ahead_kb; do
    [ -f "${f}" ] || continue
    cur=""
    if read -r cur < "${f}" 2>/dev/null && [ "${cur}" != "${target}" ]; then
      printf '%s\n' "${target}" > "${f}" 2>/dev/null || true
    fi
  done
  sleep 0.1
done
