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
# Worker count and sandbox class are forwarded to workloads/deploy.sh.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
BENCHMARKING_DIR="${ROOT}/benchmarking"

WORKER_COUNT=1
# Forwarded to both halves of the stack, which must agree on the pools: one
# creates them, the other tells the boomer workers which to use. Empty keeps
# the single unpinned pool.
WORKER_POOLS=""
SANDBOX_CLASS=gvisor
SKIP_BUILD=0
OTLP_ENDPOINT=""
# Empty keeps the default in workloads/deploy.sh (256Mi, the microvm minimum).
ACTOR_MEMORY=""
WAIT_TIMEOUT_SECS=""

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy                Deploy workloads, build/push locust image, then deploy locust"
  echo "  --delete                Delete locust and then workloads"
  echo "  --worker-count N        Total number of WorkerPool replicas across all pools (default: 1)"
  echo "  --worker-pools LIST     Comma-separated name:weight[:nodeSelectorKey=value] entries."
  echo "                          One WorkerPool per entry, --worker-count split between them by"
  echo "                          weight, each actor pinned to one pool. Default: one pool,"
  echo "                          actors unpinned. See benchmarking/README.md."
  echo "  --sandbox-class CLASS   Sandbox runtime for the WorkerPool: gvisor | microvm (default: gvisor)."
  echo "                          microvm requires hack/install-microvm-deps.sh --install to have run."
  echo "  --otlp-endpoint URL     Forwarded to workloads/deploy.sh. The address to which an"
  echo "                          instrumented actor container sends telemetry."
  echo "  --actor-memory SIZE     Forwarded to workloads/deploy.sh. Memory limit for the"
  echo "                          benchmark ActorTemplates (default: 256Mi, the microvm minimum)."
  echo "  --wait-timeout SECONDS  Forwarded to workloads/deploy.sh. The timeout in seconds for"
  echo "                          waiting for the ateom workers to be ready (default: 300)"
  echo "  --skip-build            Skip locust image build/push (use the existing :latest image)"
  echo "  -h|--help               Show this help message"
  echo ""
  echo "Environment:"
  echo "  PROJECT_ID, BUCKET_NAME, etc. are read from .ate-dev-env.sh by the"
  echo "  scripts this wrapper invokes."
}

# boomer_worker_pools drops the optional node selector from each WORKER_POOLS
# entry: it says where a pool's worker pods run, which the boomer workers have
# no use for and reject as a third field.
boomer_worker_pools() {
  local entry name weight rest out=""
  local IFS=,
  for entry in ${WORKER_POOLS}; do
    [[ -z "${entry}" ]] && continue
    name="${entry%%:*}"
    rest="${entry#*:}"
    weight="${rest%%:*}"
    out+="${out:+,}${name}:${weight}"
  done
  printf '%s' "${out}"
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
    --worker-pools) shift; WORKER_POOLS="$1" ;;
    --worker-pools=*) WORKER_POOLS="${1#*=}" ;;
    --sandbox-class) shift; SANDBOX_CLASS="$1" ;;
    --sandbox-class=*) SANDBOX_CLASS="${1#*=}" ;;
    --otlp-endpoint) shift; OTLP_ENDPOINT="$1" ;;
    --otlp-endpoint=*) OTLP_ENDPOINT="${1#*=}" ;;
    --actor-memory) shift; ACTOR_MEMORY="$1" ;;
    --actor-memory=*) ACTOR_MEMORY="${1#*=}" ;;
    --wait-timeout) shift; WAIT_TIMEOUT_SECS="$1" ;;
    --wait-timeout=*) WAIT_TIMEOUT_SECS="${1#*=}" ;;
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

case "${SANDBOX_CLASS}" in
  gvisor|microvm) ;;
  *)
    echo "Error: --sandbox-class must be gvisor or microvm, got '${SANDBOX_CLASS}'" >&2
    exit 1
    ;;
esac

if [[ -n "${WAIT_TIMEOUT_SECS}" ]] && ! [[ "${WAIT_TIMEOUT_SECS}" =~ ^[0-9]+$ ]]; then
  echo "Error: --wait-timeout must be a whole number of seconds like 300, got '${WAIT_TIMEOUT_SECS}'" >&2
  exit 1
fi

if [[ "${action}" == "deploy" ]]; then
  echo "=== Deploying benchmark workloads (worker_count=${WORKER_COUNT}, worker_pools=${WORKER_POOLS:-none}, sandbox_class=${SANDBOX_CLASS}) ==="
  # An empty OTLP_ENDPOINT must not become an empty --otlp-endpoint argument,
  # which would overwrite the default in workloads/deploy.sh with an empty
  # string and send the actor telemetry nowhere.
  workload_args=(--deploy --worker-count "${WORKER_COUNT}" --sandbox-class "${SANDBOX_CLASS}")
  if [[ -n "${WORKER_POOLS}" ]]; then
    workload_args+=(--worker-pools "${WORKER_POOLS}")
  fi
  if [[ -n "${OTLP_ENDPOINT}" ]]; then
    workload_args+=(--otlp-endpoint "${OTLP_ENDPOINT}")
  fi
  if [[ -n "${ACTOR_MEMORY}" ]]; then
    workload_args+=(--actor-memory "${ACTOR_MEMORY}")
  fi
  if [[ -n "${WAIT_TIMEOUT_SECS}" ]]; then
    workload_args+=(--wait-timeout "${WAIT_TIMEOUT_SECS}")
  fi
  "${BENCHMARKING_DIR}/workloads/deploy.sh" "${workload_args[@]}"

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
  locust_args=(--deploy)
  if [[ -n "${WORKER_POOLS}" ]]; then
    locust_args+=(--worker-pools "$(boomer_worker_pools)")
  fi
  "${BENCHMARKING_DIR}/locust/deploy.sh" "${locust_args[@]}"
elif [[ "${action}" == "delete" ]]; then
  echo "=== Deleting locust ==="
  "${BENCHMARKING_DIR}/locust/deploy.sh" --delete

  echo
  echo "=== Deleting benchmark workloads ==="
  # workloads/deploy.sh renders one manifest per pool to delete it, so the
  # teardown needs the same list the deploy ran with.
  workload_args=(--delete)
  if [[ -n "${WORKER_POOLS}" ]]; then
    workload_args+=(--worker-pools "${WORKER_POOLS}")
  fi
  "${BENCHMARKING_DIR}/workloads/deploy.sh" "${workload_args[@]}"
else
  usage
  exit 1
fi
