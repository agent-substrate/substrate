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
  $0 -args --kube-context my-context   # Pass kube-context to E2E framework
  $0 -run TestExample -args --no-color # Combine both

Common E2E Flags (passed after -args):
  --e2e            Enable E2E tests (implied by this script)
  --no-color       Disable colored output
  --kube-config    Path to kubeconfig file
  --kube-context   Kubernetes context to use
  --storage-class  StorageClass to use for external volume tests (default: csi-nfs-sc)

Common Go Test Flags (passed before -args):
  -run <regexp>    Run only tests matching regexp
  -v               Verbose output
  -count n         Run tests n times
  -p n             Test package concurrency (default: \$E2E_PARALLELISM, or 4)
  -timeout d       Per-binary timeout (default: \$E2E_TIMEOUT, or 30m)

Passing -p or -timeout explicitly overrides the default for that flag.

Environment Variables:
  E2E_PARALLELISM  Default for -p (default: 4)
  E2E_TIMEOUT      Default for -timeout (default: 30m)

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
has_p_flag=false
has_timeout_flag=false

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
        # Both spellings: go's flag package accepts -flag, --flag, and either with =value.
        case "$arg" in
            -p|-p=*|--p|--p=*) has_p_flag=true ;;
            -timeout|-timeout=*|--timeout|--timeout=*) has_timeout_flag=true ;;
        esac
        go_test_args+=("$arg")
    fi
done

extra_e2e_args=()
if [[ -n "${KUBECTL_CONTEXT:-}" ]]; then
    extra_e2e_args+=("--kube-context" "${KUBECTL_CONTEXT}")
fi

# Pin the two bounds go test would otherwise infer from the machine.
#
# -p: the system default is GOMAXPROCS causing the suite concurrency to track the
# runner's CPU count rather than what the cluster can absorb, in this case a single
# node Kind cluster. Explicitly setting this to E2E_PARALLELISM (default 4) avoids
# overshooting the cluster's capacity.
#
# -timeout: Go's default, when no value is provided, is 10m. TemplateReadyTimeout
# for the micro-VM class (internal/e2e/sandbox.go) is also 10 minutes. Any E2E
# test that times out waiting for the template will actually be killed by the global
# suite timeout instead, which skips t.Cleanup, potentially leaking resources.
# Explicitly setting the E2E_TIMEOUT (default 30m) here avoids this suite-level and
# test-level timeout conflict.
default_go_test_args=()
if [[ "${has_p_flag}" == "false" ]]; then
    default_go_test_args+=("-p" "${E2E_PARALLELISM:-4}")
fi
if [[ "${has_timeout_flag}" == "false" ]]; then
    default_go_test_args+=("-timeout" "${E2E_TIMEOUT:-30m}")
fi

# Assembled once so the two execution paths below cannot drift apart.
test_argv=(-v "$target_path")
test_argv+=(${default_go_test_args[@]+"${default_go_test_args[@]}"})
test_argv+=(${go_test_args[@]+"${go_test_args[@]}"})
test_argv+=(-args --e2e)
test_argv+=(${extra_e2e_args[@]+"${extra_e2e_args[@]}"})
test_argv+=(${e2e_args[@]+"${e2e_args[@]}"})

# E2E_JUNIT_FILE opts into a machine-readable record of the run: the XML for
# report consumers, the JSON event stream for failure analysis. Unset, this is a
# plain go test and gotestsum is never built, so a local run needs no toolchain
# beyond go itself.
if [[ -n "${E2E_JUNIT_FILE:-}" ]]; then
    mkdir -p "$(dirname "${E2E_JUNIT_FILE}")"
    exec "${ROOT}/hack/run-tool.sh" gotestsum \
        --junitfile "${E2E_JUNIT_FILE}" \
        --jsonfile "${E2E_JUNIT_FILE%.xml}.json" \
        --format standard-verbose \
        -- "${test_argv[@]}"
fi

exec go test "${test_argv[@]}"

