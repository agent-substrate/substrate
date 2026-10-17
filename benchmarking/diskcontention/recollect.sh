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

# Re-runs collect.py for every run under a results dir (e.g. after adding a
# metric), using each run's meta.json window. Needs Prometheus to still hold
# the data. Env: PROM_URL, DEVICE, PROJECT_ID, GCE_INSTANCE_ID as run_arm.sh.
set -o errexit -o nounset -o pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
while IFS= read -r -d '' meta; do
  d="$(dirname "${meta}")"
  read -r start end warmup < <(python3 -c "import json,sys; m=json.load(open(sys.argv[1])); print(m['start'], m['end'], m['warmup'])" "${meta}")
  args=(--prom "${PROM_URL:-http://localhost:19090}" --start "${start}" --end "${end}"
    --warmup "${warmup}" --device "${DEVICE:-auto}" --out "${d}/collect.json")
  if [[ -n "${GCE_INSTANCE_ID:-}" && -n "${PROJECT_ID:-}" ]]; then
    args+=(--project "${PROJECT_ID}" --instance-id "${GCE_INSTANCE_ID}")
  fi
  python3 "${SCRIPT_DIR}/collect.py" "${args[@]}" >/dev/null && echo "recollected ${d}"
done < <(find "${1:?results dir}" -name meta.json -print0)
