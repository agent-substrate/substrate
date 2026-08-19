#!/usr/bin/env bash

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

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

export NO_DEV_ENV=true
# shellcheck disable=SC1090
source <(sed '/^if \[ "\$#" -eq 0 \]; then$/,$d' "${ROOT}/hack/install-ate.sh")

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  [[ "${haystack}" == *"${needle}"* ]] || fail "expected output to contain: ${needle}"
}

assert_not_contains() {
  local haystack="$1"
  local needle="$2"
  [[ "${haystack}" != *"${needle}"* ]] || fail "expected output not to contain: ${needle}"
}

render_pull_secret_template() {
  local template="$1"
  sed -e "$(workerpool_pull_secret_sed_expr)" \
      -e "$(workerpool_pull_secret_in_template_sed_expr)" \
      "${template}"
}

test_workerpool_pull_secret_template_injection() {
  local template="demos/counter/counter.yaml.tmpl"
  local rendered=""

  rendered="$(INSTALL_DOCKER_PULL_SECRET=false render_pull_secret_template "${template}")"
  assert_not_contains "${rendered}" '${WORKERPOOL_PULL_SECRETS}'
  assert_not_contains "${rendered}" '${WORKERPOOL_PULL_SECRETS_IN_TEMPLATE}'
  assert_not_contains "${rendered}" 'imagePullSecrets:'

  rendered="$(INSTALL_DOCKER_PULL_SECRET=true ATE_DOCKER_PULL_SECRET_NAME=test-pull-secret render_pull_secret_template "${template}")"
  assert_not_contains "${rendered}" '${WORKERPOOL_PULL_SECRETS}'
  assert_not_contains "${rendered}" '${WORKERPOOL_PULL_SECRETS_IN_TEMPLATE}'
  assert_contains "${rendered}" $'replicas: 3\n  ateomImage: ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor\n  template:\n    imagePullSecrets:\n    - name: test-pull-secret'
}

test_all_workerpool_templates_accept_pull_secret_injection() {
  local template=""
  local rendered=""
  local templates=(
    benchmarking/workloads/manifests/full_workloads.yaml.tmpl
    benchmarking/workloads/manifests/workloads.yaml.tmpl
    demos/autoscaled-workerpool/autoscaled-workerpool.yaml.tmpl
    demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl
    demos/counter/counter-microvm.yaml.tmpl
    demos/counter/counter.yaml.tmpl
    demos/multi-template/multi-template.yaml.tmpl
    demos/parking/parking.yaml.tmpl
    demos/sandbox/sandbox.yaml.tmpl
  )

  for template in "${templates[@]}"; do
    rendered="$(INSTALL_DOCKER_PULL_SECRET=true ATE_DOCKER_PULL_SECRET_NAME=test-pull-secret render_pull_secret_template "${template}")"
    assert_not_contains "${rendered}" '${WORKERPOOL_PULL_SECRETS}'
    assert_not_contains "${rendered}" '${WORKERPOOL_PULL_SECRETS_IN_TEMPLATE}'
    assert_contains "${rendered}" 'imagePullSecrets:'
    assert_contains "${rendered}" '    - name: test-pull-secret'
  done
}

test_demo_deploy_renders_pull_secret() {
  local rendered=""

  ensure_crds() { :; }
  maybe_install_docker_pull_secret_in_namespace() { :; }
  log_step() { :; }
  run_kubectl() { :; }
  run_ko() { cat; }

  rendered="$(BUCKET_NAME=test-bucket INSTALL_DOCKER_PULL_SECRET=true ATE_DOCKER_PULL_SECRET_NAME=test-pull-secret demo-counter_deploy false)"
  assert_contains "${rendered}" 'imagePullSecrets:'
  assert_contains "${rendered}" '    - name: test-pull-secret'
  assert_not_contains "${rendered}" '${WORKERPOOL_PULL_SECRETS}'
}

test_install_docker_pull_secret_commands() {
  local temp_dir=""
  local log_file=""
  temp_dir="$(mktemp -d)"
  log_file="${temp_dir}/kubectl.log"
  mkdir -p "${temp_dir}/docker"
  printf '{"auths":{}}\n' > "${temp_dir}/docker/config.json"

  run_kubectl() {
    printf '%s\n' "$*" >> "${log_file}"
    case "$*" in
      create\ namespace*|create\ secret*)
        printf 'apiVersion: v1\nkind: Stub\n'
        ;;
      get\ serviceaccount*)
        return 1
        ;;
      apply\ -f\ -)
        cat >/dev/null
        ;;
    esac
  }

  DOCKER_CONFIG="${temp_dir}/docker" ATE_DOCKER_PULL_SECRET_NAME=test-pull-secret \
    install_docker_pull_secret_in_namespace test-ns default worker-sa

  local commands=""
  commands="$(cat "${log_file}")"
  assert_contains "${commands}" 'create namespace test-ns --dry-run=client -o yaml'
  assert_contains "${commands}" 'create secret generic test-pull-secret --from-file=.dockerconfigjson='
  assert_contains "${commands}" '--type=kubernetes.io/dockerconfigjson -n test-ns --dry-run=client -o yaml'
  assert_contains "${commands}" 'create serviceaccount default -n test-ns'
  assert_contains "${commands}" 'patch serviceaccount default -n test-ns -p {"imagePullSecrets":[{"name":"test-pull-secret"}]}'
  assert_contains "${commands}" 'create serviceaccount worker-sa -n test-ns'
  assert_contains "${commands}" 'patch serviceaccount worker-sa -n test-ns -p {"imagePullSecrets":[{"name":"test-pull-secret"}]}'

  rm -rf "${temp_dir}"
}

test_workerpool_pull_secret_template_injection
test_all_workerpool_templates_accept_pull_secret_injection
test_demo_deploy_renders_pull_secret
test_install_docker_pull_secret_commands

echo "install-ate pull secret tests passed"
