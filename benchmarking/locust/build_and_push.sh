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

if [[ -z "${PROJECT_ID:-}" && -z "${KO_DOCKER_REPO:-}" && -z "${LOCUST_IMAGE:-}" ]]; then
  echo "Error: PROJECT_ID or KO_DOCKER_REPO environment variable must be set." >&2
  exit 1
fi

if [[ -n "${LOCUST_IMAGE:-}" ]]; then
  IMAGE="${LOCUST_IMAGE}"
elif [[ -n "${KO_DOCKER_REPO:-}" ]]; then
  IMAGE="${KO_DOCKER_REPO}/locust-test:latest"
else
  IMAGE="us-docker.pkg.dev/${PROJECT_ID}/gcr.io/ate-images/locust-test:latest"
fi

# Target platform must match the cluster's nodes, not the build host.
PLATFORM="${LOCUST_IMAGE_PLATFORM:-linux/amd64}"

echo "Building Docker image: $IMAGE (platform: $PLATFORM)"
# Build context is the monorepo root because the Dockerfile compiles the
# boomer-glutton Go binary alongside the Python install (see Dockerfile).
docker build --platform "$PLATFORM" -t "$IMAGE" -f benchmarking/locust/Dockerfile .

echo "Pushing Docker image..."
docker push "$IMAGE"

echo "Done!"
