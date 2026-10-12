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

set -o errexit -o nounset -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_MANIFEST="${SCRIPT_DIR}/nighthawk-server.yaml"
INVOKER_MANIFEST="${SCRIPT_DIR}/traffic-driver-invoker.yaml"
WAIT_TIMEOUT_SECS=120
CONTEXT="${KUBECTL_CONTEXT:-}"

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy                Deploy nighthawk-server and traffic-driver-invoker in ate-system (default)"
  echo "  --delete                Delete nighthawk-server and traffic-driver-invoker from ate-system"
  echo "  --context CONTEXT       kubectl context to use (default: \$KUBECTL_CONTEXT or current context)"
  echo "  --wait-timeout SECONDS  Rollout wait timeout in seconds (default: ${WAIT_TIMEOUT_SECS})"
  echo "  -h, --help              Show this help message"
}

action="deploy"
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --deploy)
      action="deploy"
      ;;
    --delete)
      action="delete"
      ;;
    --context)
      shift
      CONTEXT="$1"
      ;;
    --context=*)
      CONTEXT="${1#*=}"
      ;;
    --wait-timeout)
      shift
      WAIT_TIMEOUT_SECS="$1"
      ;;
    --wait-timeout=*)
      WAIT_TIMEOUT_SECS="${1#*=}"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if ! [[ "${WAIT_TIMEOUT_SECS}" =~ ^[0-9]+$ ]]; then
  echo "Error: --wait-timeout must be a whole number of seconds like 120, got '${WAIT_TIMEOUT_SECS}'" >&2
  exit 1
fi

kubectl_cmd=(kubectl)
if [[ -n "${CONTEXT}" ]]; then
  kubectl_cmd+=(--context "${CONTEXT}")
fi

if [[ "${action}" == "deploy" ]]; then
  echo "Deploying Nighthawk server from ${SERVER_MANIFEST}..."
  "${kubectl_cmd[@]}" apply -f "${SERVER_MANIFEST}"
  echo "Deploying traffic-driver-invoker from ${INVOKER_MANIFEST}..."
  "${kubectl_cmd[@]}" apply -f "${INVOKER_MANIFEST}"
  echo "Waiting for deployment/nighthawk-server in ate-system to be ready (timeout: ${WAIT_TIMEOUT_SECS}s)..."
  "${kubectl_cmd[@]}" -n ate-system rollout status deployment/nighthawk-server --timeout="${WAIT_TIMEOUT_SECS}s"
  echo "Waiting for deployment/traffic-driver-invoker in ate-system to be ready (timeout: ${WAIT_TIMEOUT_SECS}s)..."
  "${kubectl_cmd[@]}" -n ate-system rollout status deployment/traffic-driver-invoker --timeout="${WAIT_TIMEOUT_SECS}s"
elif [[ "${action}" == "delete" ]]; then
  echo "Deleting Nighthawk server and traffic-driver-invoker..."
  "${kubectl_cmd[@]}" delete --ignore-not-found -f "${INVOKER_MANIFEST}" -f "${SERVER_MANIFEST}"
fi
