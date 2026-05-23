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
  echo "Usage: $0 <tool-name> [version]" >&2
  exit 1
fi

TOOL_NAME="$1"
VERSION="${2:-}"

ROOT="$(git rev-parse --show-toplevel)"
TOOL_DIR="${ROOT}/hack/tools/${TOOL_NAME}"

if [ ! -d "${TOOL_DIR}" ]; then
  # Try mapping common tools if the name matches a binary instead of directory
  case "${TOOL_NAME}" in
    "client-gen"|"informer-gen"|"lister-gen")
      TOOL_DIR="${ROOT}/hack/tools/code-generator"
      ;;
  esac
fi

if [ ! -d "${TOOL_DIR}" ]; then
  echo "Error: tool directory not found for '${TOOL_NAME}' at ${TOOL_DIR}" >&2
  exit 1
fi

if [ ! -f "${TOOL_DIR}/go.mod" ]; then
  echo "Error: ${TOOL_DIR}/go.mod does not exist" >&2
  exit 1
fi

cd "${TOOL_DIR}"

# Extract tool import paths declared in go.mod
tool_paths=()
while IFS= read -r line; do
  if [ -n "$line" ]; then
    tool_paths+=("$line")
  fi
done < <(go tool | grep '/' || true)

if [ ${#tool_paths[@]} -eq 0 ]; then
  echo "Error: no tools declared in ${TOOL_DIR}/go.mod" >&2
  exit 1
fi

for tool_path in "${tool_paths[@]}"; do
  if [ -n "${VERSION}" ]; then
    echo "Updating ${tool_path} to ${VERSION} in ${TOOL_DIR}..."
    go get "${tool_path}@${VERSION}"
  else
    echo "Updating ${tool_path} to latest in ${TOOL_DIR}..."
    go get "${tool_path}"
  fi
done

echo "Tidying module in ${TOOL_DIR}..."
go mod tidy
