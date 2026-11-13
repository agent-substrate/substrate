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

set -o errexit -o nounset -o pipefail

cat >> "$0.requests"
echo >> "$0.requests"
count=$(wc -l < "$0.requests")
if (( count < 3 )); then
  echo "HTTP 429: quota_exceeded on 'Token exchange requests'" >&2
  cat <<'RESPONSE'
{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{}}
RESPONSE
  exit 0
fi
cat "$0.response"
