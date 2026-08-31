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

# Regenerates the Python proto clients used by the locust load tests.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment variables if configured
if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

OUT_DIR="benchmarking/locust/common"

# Codegen has its own venv and requirements, separate from the load test's
# runtime ones: the only thing needed to compile a .proto is grpcio-tools, and
# installing the runtime list here would drag in locust, the opentelemetry
# exporters and google-cloud-storage for nothing.
CODEGEN_DIR="benchmarking/locust/codegen"
VENV_DIR="${CODEGEN_DIR}/venv"
source hack/util/venv.sh
ensure_venv "${VENV_DIR}"
venv_sync_requirements "${VENV_DIR}" "${CODEGEN_DIR}/requirements.txt"

# generate_proto compiles a single .proto file into ${OUT_DIR}, prepends the
# project's license header, and rewrites the generated grpc file's intra-package
# import to a relative form so it resolves under the `common` package.
#
# Args:
#   $1  Directory containing the .proto file (passed to protoc -I)
#   $2  Base name of the .proto (e.g. "ateapi" for ateapi.proto)
generate_proto() {
  local proto_path="$1"
  local proto_base="$2"
  local proto_file="${proto_path}/${proto_base}.proto"

  echo "Generating Python proto clients from ${proto_file}..."
  python3 -m grpc_tools.protoc \
    -I"${proto_path}" \
    --python_out="${OUT_DIR}/" \
    --grpc_python_out="${OUT_DIR}/" \
    "${proto_file}"

  local pb_file="${OUT_DIR}/${proto_base}_pb2.py"
  local grpc_file="${OUT_DIR}/${proto_base}_pb2_grpc.py"

  for file in "${pb_file}" "${grpc_file}"; do
    if [ -f "${file}" ]; then
      cat hack/boilerplate/sh.txt "${file}" > "${file}.tmp"
      mv "${file}.tmp" "${file}"
    fi
  done

  # protoc emits `import foo_pb2 as foo__pb2`, which doesn't resolve under our
  # `common` package; rewrite to a relative import.
  #
  # Written through a temp file rather than `sed -i`, whose spelling differs
  # between GNU sed (`-i`) and the BSD sed on macOS (`-i ''`).
  if [ -f "${grpc_file}" ]; then
    sed "s/^import ${proto_base}_pb2 as ${proto_base}__pb2/from . import ${proto_base}_pb2 as ${proto_base}__pb2/" \
      "${grpc_file}" > "${grpc_file}.tmp"
    mv "${grpc_file}.tmp" "${grpc_file}"
  fi
}

# Run inside a subshell so `activate` doesn't leak VIRTUAL_ENV/PATH back to
# the caller; the subshell inherits errexit/nounset/pipefail.
(
  echo "Activating virtual environment..."
  source "$VENV_DIR/bin/activate"

  generate_proto "pkg/proto/ateapipb" "ateapi"
  generate_proto "internal/proto/glutton" "glutton"
)

echo "Done!"
