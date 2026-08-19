#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
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

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Error: ${command_name} is required but was not found in PATH." >&2
    return 1
  fi
}

check_git() {
  require_command git
  if ! git --version >/dev/null 2>&1; then
    echo "Error: git is installed but not working (failed to run 'git --version')." >&2
    return 1
  fi
}

check_git_worktree() {
  local status=""
  status="$(git status --porcelain --untracked-files=normal)"
  if [[ -n "${status}" ]]; then
    echo "Error: this operation requires a clean Git worktree, but local changes were found:" >&2
    echo "${status}" >&2
    echo "Hint: commit, stash, or remove these changes before running the installer." >&2
    return 1
  fi
}

check_go() {
  require_command go

  if ! go version >/dev/null 2>&1; then
    echo "Error: go is installed but not working (failed to run 'go version')." >&2
    return 1
  fi
}

check_go_tool_ko() {
  if ! ./hack/run-tool.sh ko version >/dev/null 2>&1; then
    echo "Error: ko is required but not working through hack/run-tool.sh." >&2
    echo "Hint: run './hack/run-tool.sh ko version' to verify your setup." >&2
    return 1
  fi
}

check_kubectl() {
  require_command kubectl
  if ! kubectl version --client >/dev/null 2>&1; then
    echo "Error: kubectl is installed but not working (failed to get its client version)." >&2
    return 1
  fi
}

check_make() {
  require_command make
  if ! printf 'prerequisite-check:\n\t@:\n' | make -f - prerequisite-check >/dev/null 2>&1; then
    echo "Error: make is installed but not working." >&2
    return 1
  fi
}

check_openssl() {
  require_command openssl
  if ! openssl version >/dev/null 2>&1; then
    echo "Error: openssl is installed but not working (failed to run 'openssl version')." >&2
    return 1
  fi
}

check_jq() {
  require_command jq
  if ! jq -n 'true' >/dev/null 2>&1; then
    echo "Error: jq is installed but not working." >&2
    return 1
  fi
}

check_docker() {
  require_command docker
  if ! docker version >/dev/null 2>&1; then
    echo "Error: docker is installed but not working or its daemon is unavailable." >&2
    return 1
  fi
}

check_docker_buildx() {
  check_docker
  if ! docker buildx version >/dev/null 2>&1; then
    echo "Error: 'docker buildx' is required but not working." >&2
    return 1
  fi
}

check_envsubst() {
  require_command envsubst
  # shellcheck disable=SC2016 # Verify that envsubst expands literal input.
  if [[ "$(ATE_PREREQUISITE_CHECK=working envsubst '${ATE_PREREQUISITE_CHECK}' <<< '${ATE_PREREQUISITE_CHECK}')" != "working" ]]; then
    echo "Error: envsubst is installed but not working." >&2
    return 1
  fi
}

check_cluster_admin() {
  local current_context=""
  current_context="${KUBECTL_CONTEXT:-$(kubectl config current-context 2>/dev/null || true)}"
  if [[ -z "${current_context}" ]]; then
    current_context="<unknown-context>"
  fi

  if ! run_kubectl get --raw=/version >/dev/null 2>&1; then
    echo "Error: unable to connect to the Kubernetes cluster on context '${current_context}'." >&2
    echo "Hint: verify your kubeconfig and cluster credentials." >&2
    return 1
  fi

  if ! run_kubectl auth can-i '*' '*' --all-namespaces --quiet >/dev/null 2>&1; then
    echo "Error: current Kubernetes user is not cluster-admin on context '${current_context}'." >&2
    echo "Hint: this installer applies cluster-scoped resources and needs full cluster-admin privileges." >&2
    return 1
  fi
}

check_prerequisites() {
  log_step "check_prerequisites"
  check_git_worktree
  check_go
  check_go_tool_ko
  check_kubectl
  check_make
  check_cluster_admin
}
