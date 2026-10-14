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

# Regenerates internal/hardware/zz_generated.cpufeatures.go, the gVisor CPU
# feature tables behind the cpu_features compatibility attribute, from the
# gVisor sources committed under hack/third_party/gvisor. That directory is the
# only pin: VERSION names the gvisor.dev/gvisor module version and SHA256SUMS
# guards the copies, so a plain run needs no network and is what
# hack/verify/cpufeatures.sh checks.
#
# To move to a newer gVisor, which is needed once a shipped runsc prints a
# feature name the tables lack:
#
#   GVISOR_VERSION=v0.0.0-<yyyymmddhhmmss>-<commit> hack/update/cpufeatures.sh
#
# This refetches hack/third_party/gvisor from the Go module proxy and
# regenerates. Review the diff of the generated file: within SchemaVersionV1
# only appended names are expected, because a name's index is a bit in every
# stored snapshot's cpu_features. The generator refuses a name that vanished
# upstream; if gVisor renamed one, mark the old entry "// retired" (see
# tools/cpufeatures). Pin a version at least as new as the newest runsc in
# manifests/ate-install/sandboxconfig-gvisor.yaml.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

args=(
  -gvisor-dir "${ROOT}/hack/third_party/gvisor"
  -out "${ROOT}/internal/hardware/zz_generated.cpufeatures.go"
)
if [[ -n "${GVISOR_VERSION:-}" ]]; then
  args+=(-fetch="${GVISOR_VERSION}")
fi

go -C tools/cpufeatures run . "${args[@]}" "$@"
