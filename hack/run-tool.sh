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

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 [--print-bin-path] <tool-name> [args...]" >&2
  exit 1
fi

PRINT_PATH=false
if [ "$1" = "--print-bin-path" ]; then
  PRINT_PATH=true
  shift
fi

TOOL_NAME="$1"
shift

ROOT="$(git rev-parse --show-toplevel)"
case "${TOOL_NAME}" in
  "client-gen"|"informer-gen"|"lister-gen")
    TOOL_DIR="${ROOT}/hack/tools/code-generator"
    ;;
  *)
    TOOL_DIR="${ROOT}/hack/tools/${TOOL_NAME}"
    ;;
esac

TOOL_BIN="$(cd "${TOOL_DIR}" && go tool -n "${TOOL_NAME}")"

if [ "${PRINT_PATH}" = true ]; then
  echo "${TOOL_BIN}"
  exit 0
fi

# Run the tool binary from original CWD
exec "${TOOL_BIN}" "$@"
