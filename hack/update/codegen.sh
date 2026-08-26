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
                --plugin=protoc-gen-go="${protoc_gen_go}" \
                --plugin=protoc-gen-go-grpc="${protoc_gen_go_rpc}" \
                --go_out=paths=source_relative:. \
                --go-grpc_out=paths=source_relative:. \
                ./*.proto
        )
    done
}
codegen::protobuf

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
            "./${dir}"
    done
}
codegen::validation
