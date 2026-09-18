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

# CI-only opt-in. Run after baseline tests and before creating fresh MITM actors.
[[ "${E2E_EXPERIMENT_KVM_CLOCK:-0}" == 1 ]] || exit 0
[[ "$(go env GOARCH)" == amd64 ]] || exit 0

context="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME:-kind}}"
bucket="${BUCKET_NAME:-ate-snapshots}"
experiment_dir="$(mktemp -d)"
trap 'rm -rf "${experiment_dir}"' EXIT
config=configuration-clh-kvm-clock.toml
python3 - "bin/microvm-assets/amd64/configuration-clh.toml" "${experiment_dir}/${config}" <<'PYTHON'
import pathlib
import re
import sys
import tomllib

source = pathlib.Path(sys.argv[1]).read_text()
updated, count = re.subn(r'(?m)^(kernel_params\s*=\s*")', r'\1clocksource=kvm-clock ', source)
assert count == 1, "expected one kernel_params setting"
params = tomllib.loads(updated)["hypervisor"]["clh"]["kernel_params"].split()
assert [p for p in params if p.startswith("clocksource=")] == ["clocksource=kvm-clock"]
pathlib.Path(sys.argv[2]).write_text(updated)
PYTHON
OUT="${experiment_dir}" BUCKET="${bucket}" KUBECTL_CONTEXT="${context}" \
  hack/microvm-assets/stage-to-rustfs.sh "${config}"
config_sha="$(sha256sum "${experiment_dir}/${config}" | awk '{print $1}')"
patch="$(jq -n --arg url "gs://${bucket}/kata-assets/${config}" --arg sha "${config_sha}" \
  '{spec: {assets: {amd64: {"kata-config": {url: $url, sha256: $sha}}}}}')"
kubectl --context "${context}" patch sandboxconfig microvm --type=merge --patch "${patch}"
