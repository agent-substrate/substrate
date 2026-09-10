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

# The boilerpolate header we use for generated Go code.
GO_BOILERPLATE=hack/boilerplate/go.txt

#
# The order of these matters - some of the codegen tools depend on the output
# of others.
#

# TODO: We should move the rest of these into this file.
function codegen::go_generate() {
    echo "Running go generate"
    go generate ./...
}
codegen::go_generate

function codegen::protobuf() {
    local proto_dirs=()
    # shellcheck disable=SC2207 # reading array
    proto_dirs=(
    $(git ls-files \
        -cm \
        --exclude-standard \
        -- \
        ':(glob)**/*.proto' \
        ':!:vendor/*' \
        ':!:**/vendor/*' \
        ':!:third_party/*' \
        ':!:**/third_party/*' \
        ':!:_LICENSES/*' \
        | while read -r FILE; do dirname "${FILE}"; done \
        | sort \
        | uniq)
    )

    echo "Generating protobuf code"
    for dir in "${proto_dirs[@]}"; do
        local protoc_gen_go
        protoc_gen_go="$(./hack/run-tool.sh --print-bin-path protoc-gen-go)"
        local protoc_gen_go_rpc
        protoc_gen_go_rpc="$(./hack/run-tool.sh --print-bin-path protoc-gen-go-grpc)"
        (
            cd "${dir}" || exit 1
            "${ROOT}"/hack/protoc.sh \
                -I "${ROOT}" -I . \
                --plugin=protoc-gen-go="${protoc_gen_go}" \
                --plugin=protoc-gen-go-grpc="${protoc_gen_go_rpc}" \
                --go_out=paths=source_relative:. \
                --go-grpc_out=paths=source_relative:. \
                ./*.proto
        )
    done
}
codegen::protobuf

# python_proto compiles ${2}/${3}.proto into ${1}, prepends the
# license header, and rewrites the grpc file's intra-package import to a
# relative one so it resolves under the `common` package.
function python_proto() {
    local out_dir="$1" proto_path="$2" proto_base="$3"
    python3 -m grpc_tools.protoc \
        -I"${proto_path}" \
        --python_out="${out_dir}/" \
        --grpc_python_out="${out_dir}/" \
        "${proto_path}/${proto_base}.proto"

    local pb_file="${out_dir}/${proto_base}_pb2.py"
    local grpc_file="${out_dir}/${proto_base}_pb2_grpc.py"
    for file in "${pb_file}" "${grpc_file}"; do
        cat hack/boilerplate/sh.txt "${file}" > "${file}.tmp"
        mv "${file}.tmp" "${file}"
    done
    # protoc emits `import foo_pb2 as foo__pb2`, which does not resolve under
    # the `common` package. Written through a temp file: `sed -i` is spelled
    # differently by GNU and BSD sed.
    sed "s/^import ${proto_base}_pb2 as ${proto_base}__pb2/from . import ${proto_base}_pb2 as ${proto_base}__pb2/" \
        "${grpc_file}" > "${grpc_file}.tmp"
    mv "${grpc_file}.tmp" "${grpc_file}"
}

# Python proto clients for the locust load tests. Codegen has its own venv and
# requirements, separate from the load test's runtime ones: compiling a .proto
# needs only grpcio-tools, and the runtime list would drag in locust, the
# opentelemetry exporters and google-cloud-storage for nothing.
function codegen::python() {
    local out_dir="benchmarking/locust/common"
    local codegen_dir="benchmarking/locust/codegen"
    local venv_dir="${codegen_dir}/venv"

    source hack/util/venv.sh
    ensure_venv "${venv_dir}"
    venv_sync_requirements "${venv_dir}" "${codegen_dir}/requirements.txt"

    echo "Generating Python proto clients"
    # A subshell so `activate` does not leak VIRTUAL_ENV/PATH into the steps
    # that follow; it inherits errexit/nounset/pipefail.
    (
        source "${venv_dir}/bin/activate"
        python_proto "${out_dir}" pkg/proto/ateapipb ateapi
        python_proto "${out_dir}" internal/proto/glutton glutton
    )
}
codegen::python

function codegen::validation() {
    local validation_dirs=()
    # shellcheck disable=SC2207 # reading array
    validation_dirs=(
    $(git grep -l \
        '+k8s:validation-gen' \
        -- \
        ':(glob)**/doc.go' \
        ':!:vendor/*' \
        ':!:**/vendor/*' \
        ':!:third_party/*' \
        ':!:**/third_party/*' \
        ':!:_LICENSES/*' \
        | while read -r FILE; do dirname "${FILE}"; done \
        | sort \
        | uniq)
    )

    echo "Generating validation code"
    for dir in "${validation_dirs[@]}"; do
        ./hack/run-tool.sh validation-gen \
            --go-header-file="${GO_BOILERPLATE}" \
            --output-file=zz_generated.validation.go \
            --readonly-pkg=google.golang.org/protobuf/types/known/timestamppb \
            --readonly-pkg=google.golang.org/protobuf/types/known/fieldmaskpb \
            --readonly-pkg=google.golang.org/protobuf/types/known/emptypb \
            "./${dir}"
    done
}
codegen::validation
