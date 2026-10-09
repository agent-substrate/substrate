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

# ate-api-server moves snapshots through the snapshot plugin sidecar, so it
# must not link the object-store SDKs.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

banned=(
  cloud.google.com/go/storage
  github.com/aws/aws-sdk-go-v2/service/s3
)

# ko ships ate-api-server for every platform in .ko.yaml.
mapfile -t platforms < <(awk '/^defaultPlatforms:/ {in_list = 1; next} in_list && $1 == "-" {print $2; next} {in_list = 0}' .ko.yaml)
if [[ ${#platforms[@]} -eq 0 ]]; then
  echo "no defaultPlatforms in .ko.yaml." >&2
  exit 1
fi

status=0
for platform in "${platforms[@]}"; do
  deps="$(GOOS="${platform%/*}" GOARCH="${platform#*/}" CGO_ENABLED=0 go list -deps ./cmd/ateapi)"
  for pkg in "${banned[@]}"; do
    if grep -Fxq -- "${pkg}" <<<"${deps}"; then
      echo "cmd/ateapi (${platform}) must not depend on ${pkg}." >&2
      status=1
    fi
  done
done
exit "${status}"
