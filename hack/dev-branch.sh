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

GIT_COMMON_DIR="$(git rev-parse --git-common-dir)"
MAIN_ROOT="$(cd "${GIT_COMMON_DIR}/.." && pwd)"
cd "${MAIN_ROOT}"

sanitize_name() {
  local raw_name="$1"
  local cleaned
  cleaned="$(echo "${raw_name}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9-]+/-/g' | sed -E 's/^-+|-+$//g')"
  if [[ -z "${cleaned}" ]]; then
    cleaned="dev"
  fi
  echo "${cleaned}"
}

show_help() {
  cat <<EOF
Usage: $0 <command> [args]

Manage self-contained development environments (Git worktree + Kind cluster).

Commands:
  setup <branch-name> [worktree-path]
      Creates a Git worktree, provisions an isolated Kind cluster, deploys the
      Substrate control plane, and writes a .dev-env.sh file.

  teardown <branch-name> [worktree-path]
      Deletes the isolated Kind cluster and removes the Git worktree.

  env <branch-name>
      Outputs environment variable export statements for testing against the branch's
      Kind cluster. Usage: eval "\$($0 env <branch-name>)"

  exec <branch-name> -- <command...>
      Executes the command inside the branch's worktree directory with environment
      variables set for its Kind cluster.

  list
      Lists active Git worktrees and Kind clusters.

  help, -h, --help
      Displays this help message.

Examples:
  $0 setup feature-router
  $0 exec feature-router -- hack/run-e2e-kind.sh
  $0 teardown feature-router
EOF
}

setup_env() {
  local branch="${1:-}"
  local target_path="${2:-}"

  if [[ -z "${branch}" ]]; then
    echo "Error: Branch name required for setup." >&2
    echo "Usage: $0 setup <branch-name> [worktree-path]" >&2
    exit 1
  fi

  local s_name
  s_name="$(sanitize_name "${branch}")"
  local cluster_name="kind-dev-${s_name}"

  if [[ -z "${target_path}" ]]; then
    target_path="${MAIN_ROOT}/.worktrees/${s_name}"
  fi

  echo "==> Setting up development environment for branch '${branch}'..."
  echo "    Worktree path: ${target_path}"
  echo "    Kind cluster:  ${cluster_name}"

  mkdir -p "$(dirname "${target_path}")"
  if [[ -d "${target_path}" ]]; then
    echo "    Worktree directory '${target_path}' already exists."
  else
    if git rev-parse --verify "${branch}" >/dev/null 2>&1; then
      echo "    Adding worktree for existing branch '${branch}'..."
      git worktree add "${target_path}" "${branch}"
    elif git rev-parse --verify "origin/${branch}" >/dev/null 2>&1; then
      echo "    Adding worktree tracking 'origin/${branch}'..."
      git worktree add -b "${branch}" "${target_path}" "origin/${branch}"
    else
      echo "    Creating new branch '${branch}' and adding worktree..."
      git worktree add -b "${branch}" "${target_path}" HEAD
    fi
  fi

  echo "==> Provisioning Kind cluster '${cluster_name}'..."
  KIND_CLUSTER_NAME="${cluster_name}" "${MAIN_ROOT}/hack/create-kind-cluster.sh"

  echo "==> Deploying Agent Substrate control plane to '${cluster_name}'..."
  KIND_CLUSTER_NAME="${cluster_name}" "${MAIN_ROOT}/hack/install-ate-kind.sh" --deploy-ate-system

  echo "==> Writing environment configuration file '${target_path}/.dev-env.sh'..."
  cat <<EOF > "${target_path}/.dev-env.sh"
# Environment configuration for dev branch '${branch}'
export NO_DEV_ENV="true"
export KIND_CLUSTER_NAME="${cluster_name}"
export KUBECTL_CONTEXT="kind-${cluster_name}"
export KO_DOCKER_REPO="localhost:5001"
export BUCKET_NAME="ate-snapshots"
EOF

  echo ""
  echo "========================================================================="
  echo "Development environment setup complete!"
  echo ""
  echo "To start working in this environment:"
  echo "  cd ${target_path}"
  echo "  eval \"\$(${MAIN_ROOT}/hack/dev-branch.sh env ${branch})\""
  echo "  # or: source .dev-env.sh"
  echo ""
  echo "To run E2E tests against this isolated environment:"
  echo "  ${MAIN_ROOT}/hack/dev-branch.sh exec ${branch} -- ./hack/run-e2e-kind.sh"
  echo ""
  echo "To teardown this environment when finished:"
  echo "  ${MAIN_ROOT}/hack/dev-branch.sh teardown ${branch} ${target_path}"
  echo "========================================================================="
}

teardown_env() {
  local branch="${1:-}"
  local target_path="${2:-}"

  if [[ -z "${branch}" ]]; then
    echo "Error: Branch name required for teardown." >&2
    echo "Usage: $0 teardown <branch-name> [worktree-path]" >&2
    exit 1
  fi

  local s_name
  s_name="$(sanitize_name "${branch}")"
  local cluster_name="kind-dev-${s_name}"

  if [[ -z "${target_path}" ]]; then
    target_path="${MAIN_ROOT}/.worktrees/${s_name}"
  fi

  echo "==> Tearing down development environment for branch '${branch}'..."

  echo "==> Deleting Kind cluster '${cluster_name}'..."
  KIND_CLUSTER_NAME="${cluster_name}" "${MAIN_ROOT}/hack/delete-kind-cluster.sh" || true

  if git worktree list | grep -q "${target_path}"; then
    echo "==> Removing Git worktree at '${target_path}'..."
    git worktree remove --force "${target_path}" || true
  elif [[ -d "${target_path}" ]]; then
    echo "==> Removing directory '${target_path}'..."
    rm -rf "${target_path}"
  fi

  echo "Teardown complete for '${branch}'."
}

print_env() {
  local branch="${1:-}"

  if [[ -z "${branch}" ]]; then
    echo "# Error: Branch name required for env." >&2
    echo "# Usage: eval \"\$($0 env <branch-name>)\"" >&2
    exit 1
  fi

  local s_name
  s_name="$(sanitize_name "${branch}")"
  local cluster_name="kind-dev-${s_name}"

  cat <<EOF
export NO_DEV_ENV="true"
export KIND_CLUSTER_NAME="${cluster_name}"
export KUBECTL_CONTEXT="kind-${cluster_name}"
export KO_DOCKER_REPO="localhost:5001"
export BUCKET_NAME="ate-snapshots"
EOF
}

exec_env() {
  local branch="${1:-}"
  if [[ -z "${branch}" ]]; then
    echo "Error: Branch name required for exec." >&2
    echo "Usage: $0 exec <branch-name> -- <command...>" >&2
    exit 1
  fi
  shift

  if [[ "$#" -gt 0 && "$1" == "--" ]]; then
    shift
  fi

  if [[ "$#" -eq 0 ]]; then
    echo "Error: No command specified to execute." >&2
    echo "Usage: $0 exec <branch-name> -- <command...>" >&2
    exit 1
  fi

  local s_name
  s_name="$(sanitize_name "${branch}")"
  local cluster_name="kind-dev-${s_name}"
  local target_path="${MAIN_ROOT}/.worktrees/${s_name}"

  if [[ ! -d "${target_path}" ]]; then
    echo "Error: Worktree directory '${target_path}' does not exist. Run setup first." >&2
    exit 1
  fi

  export NO_DEV_ENV="true"
  export KIND_CLUSTER_NAME="${cluster_name}"
  export KUBECTL_CONTEXT="kind-${cluster_name}"
  export KO_DOCKER_REPO="localhost:5001"
  export BUCKET_NAME="ate-snapshots"

  cd "${target_path}"
  exec "$@"
}

list_envs() {
  echo "Active Git Worktrees:"
  git worktree list
  echo ""
  echo "Active Kind Clusters:"
  "${MAIN_ROOT}/hack/kind.sh" get clusters 2>/dev/null || echo "No kind clusters found."
}

main() {
  if [[ "$#" -eq 0 ]]; then
    show_help
    exit 0
  fi

  local cmd="$1"
  shift

  case "${cmd}" in
    setup|create)
      setup_env "$@"
      ;;
    teardown|delete|destroy)
      teardown_env "$@"
      ;;
    env)
      print_env "$@"
      ;;
    exec)
      exec_env "$@"
      ;;
    list|status)
      list_envs "$@"
      ;;
    help|-h|--help)
      show_help
      ;;
    *)
      echo "Error: Unknown command '${cmd}'" >&2
      show_help
      exit 1
      ;;
  esac
}

main "$@"
