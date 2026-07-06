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

# End-to-end deploy/uninstall of the locust benchmarking stack:
#   --deploy:  workloads/deploy.sh --deploy -> locust/build_and_push.sh -> locust/deploy.sh --deploy
#   --delete:  locust/deploy.sh --delete -> workloads/deploy.sh --delete (reverse order)
#
# Worker count is forwarded to workloads/deploy.sh.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
BENCHMARKING_DIR="${ROOT}/benchmarking"

WORKER_COUNT=1
SKIP_BUILD=0

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy             Deploy workloads, build/push locust image, then deploy locust"
  echo "  --delete             Delete locust and then workloads"
  echo "  --worker-count N     Number of WorkerPool replicas (default: 1)"
  echo "  --skip-build         Skip locust image build/push (use the existing :latest image)"
  echo "  -h|--help            Show this help message"
  echo ""
  echo "Environment:"
  echo "  PROJECT_ID, BUCKET_NAME, etc. are read from .ate-dev-env.sh by the"
  echo "  scripts this wrapper invokes."
}

if [[ "$#" -eq 0 ]]; then
  usage
  exit 1
fi

action=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --deploy) action="deploy" ;;
    --delete) action="delete" ;;
    --worker-count) shift; WORKER_COUNT="$1" ;;
    --worker-count=*) WORKER_COUNT="${1#*=}" ;;
    --skip-build) SKIP_BUILD=1 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Error: Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ "${action}" == "deploy" ]]; then
  echo "=== Deploying benchmark workloads (worker_count=${WORKER_COUNT}) ==="
  "${BENCHMARKING_DIR}/workloads/deploy.sh" --deploy --worker-count "${WORKER_COUNT}"

  if [[ "${SKIP_BUILD}" -eq 0 ]]; then
    echo
    echo "=== Building and pushing locust image ==="
    "${BENCHMARKING_DIR}/locust/build_and_push.sh"
  else
    echo
    echo "=== Skipping locust image build/push (--skip-build) ==="
  fi

  echo
  echo "=== Deploying locust ==="
  "${BENCHMARKING_DIR}/locust/deploy.sh" --deploy
elif [[ "${action}" == "delete" ]]; then
  echo "=== Deleting locust ==="
  "${BENCHMARKING_DIR}/locust/deploy.sh" --delete

  echo
  echo "=== Deleting benchmark workloads ==="
  "${BENCHMARKING_DIR}/workloads/deploy.sh" --delete
else
  usage
  exit 1
fi
