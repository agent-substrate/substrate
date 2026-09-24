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

# Generates the Python gRPC clients the locust tests import into
# benchmarking/locust/common. They are not checked in: protoc's Python output
# puts the whole serialized descriptor on one line, so any two concurrent proto
# changes conflict on it. The locust and nighthawk-ingress images run this
# script at build time, and hack/verify/python-protos.sh runs it in CI.
#
# Usage: generate.sh [PROTO...]
#
# PROTO is a repo-relative .proto path; without arguments, every proto the
# locust tests import is compiled. Set OUT_DIR to write somewhere other than
# benchmarking/locust/common. Set PYTHON to an interpreter that already has
# requirements.txt installed to skip the virtual environment, as the
# Dockerfiles do.

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${ROOT}"

CODEGEN_DIR="benchmarking/locust/codegen"
OUT_DIR="${OUT_DIR:-benchmarking/locust/common}"

if [[ $# -eq 0 ]]; then
    set -- pkg/proto/ateapipb/ateapi.proto internal/proto/glutton/glutton.proto
fi

if [[ -z "${PYTHON:-}" ]]; then
    source hack/util/venv.sh
    ensure_venv "${CODEGEN_DIR}/venv"
    venv_sync_requirements "${CODEGEN_DIR}/venv" "${CODEGEN_DIR}/requirements.txt"
    PYTHON="${CODEGEN_DIR}/venv/bin/python3"
fi

# python_proto compiles ${1}/${2}.proto into ${OUT_DIR} and rewrites the grpc
# file's intra-package import to a relative one so it resolves under the
# `common` package.
function python_proto() {
    local proto_path="$1" proto_base="$2"
    "${PYTHON}" -m grpc_tools.protoc \
        -I"${proto_path}" \
        --python_out="${OUT_DIR}/" \
        --grpc_python_out="${OUT_DIR}/" \
        "${proto_path}/${proto_base}.proto"

    local grpc_file="${OUT_DIR}/${proto_base}_pb2_grpc.py"
    # protoc emits `import foo_pb2 as foo__pb2`, which does not resolve under
    # the `common` package. Written through a temp file: `sed -i` is spelled
    # differently by GNU and BSD sed.
    sed "s/^import ${proto_base}_pb2 as ${proto_base}__pb2/from . import ${proto_base}_pb2 as ${proto_base}__pb2/" \
        "${grpc_file}" > "${grpc_file}.tmp"
    mv "${grpc_file}.tmp" "${grpc_file}"
}

mkdir -p "${OUT_DIR}"
echo "Generating Python proto clients into ${OUT_DIR}"
for proto in "$@"; do
    python_proto "$(dirname "${proto}")" "$(basename "${proto}" .proto)"
done
