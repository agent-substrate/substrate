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

# Runs the plugin's Azurite integration tests against a throwaway Azurite
# container. Arguments replace the default test command, e.g. a gotestsum
# wrapper in CI. The tests fail, not skip, without Azurite.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

image="mcr.microsoft.com/azure-storage/azurite:3.37.0@sha256:830430c1da1a2d537e08f3e6764dd1f5ae00cf0346bcaf625b968ec3f0971fd5"
name="azure-blob-azurite-$$"
port="${AZURITE_PORT:-10000}"
account=substrate
# Azurite accepts any base64 key; this one exists only in the container.
key=c3Vic3RyYXRlLWF6dXJpdGUtaW50ZWdyYXRpb24ta2V5IQ==

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run -d --name "$name" -p "127.0.0.1:${port}:10000" \
	-e "AZURITE_ACCOUNTS=${account}:${key}" \
	"$image" azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck >/dev/null

ready=false
for _ in $(seq 1 60); do
	if curl -s -o /dev/null --connect-timeout 2 --max-time 5 "http://127.0.0.1:${port}/"; then
		ready=true
		break
	fi
	sleep 1
done
if [[ $ready != true ]]; then
	docker logs "$name" >&2 || true
	echo "Azurite did not start on 127.0.0.1:${port}" >&2
	exit 1
fi

export AZURITE_BLOB_ENDPOINT="http://127.0.0.1:${port}/${account}"
export AZURITE_ACCOUNT_NAME="$account"
export AZURITE_ACCOUNT_KEY="$key"
if [[ $# -eq 0 ]]; then
	set -- go test -race -count=1 -tags=integration -v ./...
fi
"$@"
