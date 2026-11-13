#!/usr/bin/env bash
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

# Runs wakestorm in the wakestorm-runner pod (see runner_setup.sh), waits for
# it to finish, and copies its output to <out_dir>. The run is started in its
# own session, so it keeps going if the kubectl exec connection drops.
#
# Usage: wsr.sh <out_dir> <wakestorm args...>
# wsr.sh adds -endpoint, -token-file and -out.

set -o errexit -o nounset -o pipefail

out="${1:?usage: wsr.sh <out_dir> <wakestorm args...>}"
shift
name="$(basename "${out}")-$(date +%s)"
dir="/tmp/out/${name}"

X() { kubectl exec -n benchmarking wakestorm-runner -c runner -- "$@"; }

args="$(printf '%q ' "$@")"
X sh -c "mkdir -p ${dir} && setsid sh -c '/tmp/wakestorm -endpoint api.ate-system.svc:443 \
  -token-file /var/run/ate-token/token -out ${dir} ${args} > ${dir}/run.log 2>&1; echo \$? > ${dir}/exitcode' \
  < /dev/null > /dev/null 2>&1 &"

until X test -f "${dir}/exitcode" 2>/dev/null; do
  if ! kubectl get pod wakestorm-runner -n benchmarking -o name > /dev/null; then
    echo "wakestorm-runner is gone; the run in ${dir} was lost" >&2
    exit 1
  fi
  sleep 15
done
mkdir -p "${out}"
kubectl cp -c runner "benchmarking/wakestorm-runner:${dir}/." "${out}" > /dev/null
code="$(cat "${out}/exitcode")"
sed -n '/^| Op/,$p;/^| Phase/,$p' "${out}/run.log"
echo "exit code ${code}, results in ${out}"
exit "${code}"
