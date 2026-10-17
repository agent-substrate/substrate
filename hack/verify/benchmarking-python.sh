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

# Runs the benchmarking Python test suites under pytest. Each suite gets a venv
# from its own requirements.txt plus pytest, and runs with the suite directory
# as the working directory because the tests import their modules by bare name.
# pytest is the runner because the nighthawk-ingress tests are plain functions,
# which unittest does not collect.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"
source hack/util/venv.sh

# <suite directory> <tests path relative to it>. A suite without a
# requirements.txt gets pytest alone.
SUITES=(
  "benchmarking/analysis ."
  "benchmarking/automation ."
  "benchmarking/locust unit_tests"
  "benchmarking/nighthawk-ingress tests"
  "benchmarking/workloads/manifests/sweperf ."
)

# A test that escapes its mocks must not reach a cluster or a registry with
# the developer's credentials: the commands the suites shell out to are
# stubbed to fail, and the kubernetes client finds no kubeconfig.
stubs="$(mktemp -d)"
trap 'rm -rf "${stubs}"' EXIT
for cmd in docker gcloud kubectl; do
  printf '#!/bin/sh\necho "%s: refusing to run %s from a unit test" >&2\nexit 1\n' \
    "$(basename "$0")" "${cmd}" >"${stubs}/${cmd}"
  chmod +x "${stubs}/${cmd}"
done
export PATH="${stubs}:${PATH}" KUBECONFIG=/nonexistent

for entry in "${SUITES[@]}"; do
  read -r dir tests <<<"${entry}"
  venv="${dir}/venv"
  ensure_venv "${venv}"
  if [[ -f "${dir}/requirements.txt" ]]; then
    venv_sync_requirements "${venv}" "${dir}/requirements.txt"
  fi
  "${venv}/bin/pip" install --quiet pytest
  echo "Running ${dir}/${tests}"
  (cd "${dir}" && "${ROOT}/${venv}/bin/python" -m pytest -q "${tests}")
done
