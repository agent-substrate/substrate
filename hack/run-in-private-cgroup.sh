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


# Runs a command, as root, in a cgroup of its own that is the root of a new
# cgroup namespace and a new mount namespace, with cgroup2 mounted at
# /sys/fs/cgroup. That is what a worker pod has, so code that makes cgroup
# leaves under /sys/fs/cgroup can run on a host without making them in the
# host's own tree. Anything left in the cgroup after the command is reported.
#
# Usage: run-in-private-cgroup.sh <name> <command> [args...]

set -o errexit -o nounset -o pipefail

if [[ "$#" -lt 2 ]]; then
  echo "usage: $0 <name> <command> [args...]" >&2
  exit 2
fi
if [[ "$(id -u)" -ne 0 ]]; then
  echo "$0: must run as root" >&2
  exit 2
fi
if [[ ! -e /sys/fs/cgroup/cgroup.controllers ]]; then
  echo "$0: needs cgroup v2 mounted at /sys/fs/cgroup" >&2
  exit 2
fi

name="$1"
shift

original="$(cut -d: -f3 /proc/self/cgroup)"
cg="/sys/fs/cgroup/${name}-$$"
mkdir "${cg}"
echo $$ >"${cg}/cgroup.procs"

status=0
unshare --cgroup --mount --propagation private -- \
  bash -c 'mount -t cgroup2 none /sys/fs/cgroup && exec "$@"' bash "$@" || status=$?

echo $$ >"/sys/fs/cgroup${original%/}/cgroup.procs"
# The command removes its own leaves. What is left is a leak.
find "${cg}" -mindepth 1 -depth -type d -exec rmdir {} + 2>/dev/null || true
if ! rmdir "${cg}"; then
  echo "$0: ${cg} is left behind" >&2
  status=1
fi
exit "${status}"
