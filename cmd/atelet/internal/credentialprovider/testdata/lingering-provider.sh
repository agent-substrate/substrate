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

# On its first run this plugin touches "$0.entered" and then blocks. A
# background sleep inherits stdout, so once the plugin is killed the pipe
# stays open until exec.Cmd.WaitDelay expires, keeping the first flight alive
# while a second caller arrives. Later runs answer immediately.

set -o errexit -o nounset -o pipefail

cat > /dev/null

n=0
if [[ -f "$0.runs" ]]; then
  n=$(cat "$0.runs")
fi
n=$((n + 1))
printf '%d' "${n}" > "$0.runs"

if [[ "${n}" -eq 1 ]]; then
  touch "$0.entered"
  sleep 10 &
  wait "$!"
fi

cat << 'JSON'
{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Registry",
  "auth": {"gcr.io": {"username": "_token", "password": "fresh-flight-token"}}
}
JSON
