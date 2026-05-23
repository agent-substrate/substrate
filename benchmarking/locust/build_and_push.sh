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

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment variables if configured
if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

if [ -z "${PROJECT_ID:-}" ]; then
  echo "Error: PROJECT_ID environment variable must be set." >&2
  exit 1
fi

IMAGE="us-docker.pkg.dev/${PROJECT_ID}/gcr.io/ate-images/locust-test:latest"

echo "Building Docker image: $IMAGE"
docker build -t "$IMAGE" -f benchmarking/locust/Dockerfile benchmarking/locust/

echo "Pushing Docker image..."
docker push "$IMAGE"

echo "Done!"
