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

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

if [[ -r .ate-dev-env.sh ]] && [[ -z "${NO_DEV_ENV:-}" ]]; then
	source .ate-dev-env.sh
fi

show_help() {
    cat <<EOF
Usage: $0 [target-path] [go-test-flags] [-args [e2e-flags]]

Runs End-to-End tests.

The optional "target-path" must be the first argument and must start with
"./internal/e2e" or "internal/e2e". It defaults to "./internal/e2e/suites/...".

Arguments before "-args" (excluding the target-path) are passed directly to "go test".
Arguments after "-args" are passed to the test binary.

Example:
  $0 -run TestExample                 # Run only TestExample suite
  $0 -args -kube-context my-context   # Pass kube-context to E2E framework
  $0 -run TestExample -args -no-color # Combine both

Common E2E Flags (passed after -args):
  -e2e            Enable E2E tests (implied by this script)
  -no-color       Disable colored output
  -kube-config    Path to kubeconfig file
  -kube-context   Kubernetes context to use

Common Go Test Flags (passed before -args):
  -run <regexp>   Run only tests matching regexp
  -v              Verbose output
  -count n        Run tests n times

See "go help testflag" for more Go test flags.
EOF
}

target_path="./internal/e2e/suites/..."

if [[ "$#" -gt 0 ]]; then
    if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        show_help
        exit 0
    fi

    if [[ "$1" == "./internal/e2e"* || "$1" == "internal/e2e"* ]]; then
        target_path="$1"
        shift
    elif [[ "$1" == -* ]]; then
        # It's a flag, keep default target_path, don't shift
        :
    else
        echo "Error: Invalid target path '$1'." >&2
        echo "The first argument must be a valid E2E path starting with './internal/e2e' or a flag starting with '-'." >&2
        echo "Use '$0 -h' for help." >&2
        exit 1
    fi
fi

go_test_args=()
e2e_args=()
found_args_sep=false

for arg in "$@"; do
    if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
        show_help
        exit 0
    fi
    if [[ "$arg" == "-args" ]]; then
        found_args_sep=true
        continue
    fi

    if [[ "$found_args_sep" == "true" ]]; then
        e2e_args+=("$arg")
    else
        go_test_args+=("$arg")
    fi
done

extra_e2e_args=()
if [[ -n "${KUBECTL_CONTEXT:-}" ]]; then
    extra_e2e_args+=("-kube-context" "${KUBECTL_CONTEXT}")
fi

exec go test -v "$target_path" "${go_test_args[@]}" -args -e2e "${extra_e2e_args[@]}" "${e2e_args[@]}"

