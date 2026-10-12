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
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

if [[ -f "${REPO_ROOT}/.ate-dev-env.sh" ]] && [[ -z "${NO_DEV_ENV:-}" ]]; then
  # shellcheck source=/dev/null
  source "${REPO_ROOT}/.ate-dev-env.sh"
fi

ATESPACE="nighthawk"
TEMPLATE_NAME="traffic-driver"
ACTOR_NAME="tester"
POOL_MANIFEST="${SCRIPT_DIR}/traffic-driver.yaml.tmpl"
MANIFEST="${SCRIPT_DIR}/traffic-driver-template.yaml.tmpl"
EGRESS_POLICY_MANIFEST="${SCRIPT_DIR}/egress-policy.yaml"
KUBECTL_ATE_BIN="kubectl-ate"
WAIT_TIMEOUT_SECS=300
CONTEXT="${KUBECTL_CONTEXT:-}"
BUCKET_NAME="${BUCKET_NAME:-ate-snapshots}"
KO_DOCKER_REPO="${KO_DOCKER_REPO:-localhost:5001}"

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy                Deploy the ${ATESPACE}/${TEMPLATE_NAME} worker pool, actor template, and ${ACTOR_NAME} actor (default)"
  echo "  --delete                Delete the ${ACTOR_NAME} actor, template, atespace, and worker pool"
  echo "  --bucket-name BUCKET    GCS bucket name for actor snapshots (default: \$BUCKET_NAME)"
  echo "  --ko-docker-repo REPO   Container registry for ko image builds (default: \$KO_DOCKER_REPO)"
  echo "  --context CONTEXT       kubectl context to use (default: \$KUBECTL_CONTEXT or current context)"
  echo "  --wait-timeout SECONDS  Timeout waiting for worker pool and golden snapshot readiness (default: ${WAIT_TIMEOUT_SECS})"
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
    --bucket-name)
      shift
      BUCKET_NAME="$1"
      ;;
    --bucket-name=*)
      BUCKET_NAME="${1#*=}"
      ;;
    --ko-docker-repo)
      shift
      KO_DOCKER_REPO="$1"
      ;;
    --ko-docker-repo=*)
      KO_DOCKER_REPO="${1#*=}"
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
  echo "Error: --wait-timeout must be a whole number of seconds like 300, got '${WAIT_TIMEOUT_SECS}'" >&2
  exit 1
fi

run_kubectl() {
  local args=()
  if [[ -n "${CONTEXT}" ]]; then
    args+=(--context "${CONTEXT}")
  fi
  kubectl "${args[@]}" "$@"
}

run_kubectl_ate() {
  local args=()
  if [[ -n "${CONTEXT}" ]]; then
    args+=(--context "${CONTEXT}")
  fi
  "${KUBECTL_ATE_BIN}" "${args[@]}" "$@"
}

wait_template_ready() {
  echo "Waiting for actor template ${ATESPACE}/${TEMPLATE_NAME} golden snapshot (timeout: ${WAIT_TIMEOUT_SECS}s)..."
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECS))
  while true; do
    local json
    json="$(run_kubectl_ate get actor-template "${TEMPLATE_NAME}" -a "${ATESPACE}" -o json)"
    local err_msg
    err_msg="$(jq -r '.status.goldenSnapshotStatus.errorMessage // empty' <<<"${json}")"
    if [[ -n "${err_msg}" ]]; then
      echo "Error: ActorTemplate ${ATESPACE}/${TEMPLATE_NAME} golden snapshot failed: ${err_msg}" >&2
      return 1
    fi
    local tag
    tag="$(jq -r '.status.goldenSnapshotStatus.goldenTag.name // empty' <<<"${json}")"
    if [[ -n "${tag}" ]]; then
      echo "  ${ATESPACE}/${TEMPLATE_NAME} ready (golden tag: ${tag})"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "Error: Timed out waiting for ActorTemplate ${ATESPACE}/${TEMPLATE_NAME} golden snapshot" >&2
      echo "${json}" >&2
      return 1
    fi
    sleep 2
  done
}

deploy() {
  if [[ -z "${BUCKET_NAME}" ]]; then
    echo "Error: BUCKET_NAME is not set. Pass --bucket-name or configure .ate-dev-env.sh." >&2
    exit 1
  fi
  if [[ -z "${KO_DOCKER_REPO}" ]]; then
    echo "Error: KO_DOCKER_REPO is not set. Pass --ko-docker-repo or configure .ate-dev-env.sh." >&2
    exit 1
  fi
  export KO_DOCKER_REPO

  echo "Deploying worker pool from ${POOL_MANIFEST}..."
  hack/run-tool.sh ko resolve -f "${POOL_MANIFEST}" | run_kubectl apply -f -
  echo "Waiting for worker pool ${ATESPACE}/${TEMPLATE_NAME} to be ready (timeout: ${WAIT_TIMEOUT_SECS}s)..."
  run_kubectl wait --for=create "deployment/${TEMPLATE_NAME}" \
    --namespace="${ATESPACE}" --timeout="${WAIT_TIMEOUT_SECS}s"
  run_kubectl rollout status "deployment/${TEMPLATE_NAME}" \
    --namespace="${ATESPACE}" --timeout="${WAIT_TIMEOUT_SECS}s"

  echo "Ensuring atespace ${ATESPACE} exists..."
  run_kubectl_ate create atespace "${ATESPACE}" >/dev/null 2>&1 \
    || run_kubectl_ate get atespace "${ATESPACE}" >/dev/null

  # Remove any existing actor and template before redeploying.
  run_kubectl_ate suspend actor "${ACTOR_NAME}" -a "${ATESPACE}" >/dev/null 2>&1 || true
  run_kubectl_ate delete actor "${ACTOR_NAME}" -a "${ATESPACE}" >/dev/null 2>&1 || true
  run_kubectl_ate delete actor-template "${TEMPLATE_NAME}" -a "${ATESPACE}" >/dev/null 2>&1 || true

  echo "Creating actor template ${ATESPACE}/${TEMPLATE_NAME} from ${MANIFEST}..."
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" "${MANIFEST}" \
    | run_kubectl_ate create actor-template -f -

  wait_template_ready

  echo "Creating actor ${ATESPACE}/${ACTOR_NAME} from template ${TEMPLATE_NAME}..."
  run_kubectl_ate create actor "${ACTOR_NAME}" -a "${ATESPACE}" --template "${TEMPLATE_NAME}"

  echo "Applying egress policy to actor ${ATESPACE}/${ACTOR_NAME} from ${EGRESS_POLICY_MANIFEST}..."
  run_kubectl_ate create egress-policy "${ACTOR_NAME}" -a "${ATESPACE}" -f "${EGRESS_POLICY_MANIFEST}"

  echo "Resuming actor ${ATESPACE}/${ACTOR_NAME}..."
  run_kubectl_ate resume actor "${ACTOR_NAME}" -a "${ATESPACE}"
}

delete() {
  echo "Suspending and deleting actor ${ATESPACE}/${ACTOR_NAME}..."
  run_kubectl_ate suspend actor "${ACTOR_NAME}" -a "${ATESPACE}" >/dev/null 2>&1 || true
  run_kubectl_ate delete actor "${ACTOR_NAME}" -a "${ATESPACE}" >/dev/null 2>&1 || true

  echo "Deleting actor template ${ATESPACE}/${TEMPLATE_NAME}..."
  run_kubectl_ate delete actor-template "${TEMPLATE_NAME}" -a "${ATESPACE}" >/dev/null 2>&1 || true
  run_kubectl_ate delete atespace "${ATESPACE}" >/dev/null 2>&1 \
    || echo "atespace ${ATESPACE} not deleted (may not exist or is not empty)"

  echo "Deleting worker pool from ${POOL_MANIFEST}..."
  run_kubectl delete --ignore-not-found -f "${POOL_MANIFEST}"
}

"${action}"
