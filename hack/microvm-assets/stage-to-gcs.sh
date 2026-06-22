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

# Stage the assembled micro-VM asset set into the GCS snapshot bucket under
# kata-assets/, where atelet fetches it (per demos/counter/counter-microvm.yaml.tmpl).
# The GKE counterpart of stage-to-rustfs.sh. Run after assemble.sh has produced $OUT.
#
# Requires the `gcloud` CLI authenticated for the bucket's project. Env: OUT (asset
# dir, default ./bin/microvm-assets/amd64), BUCKET (default ate-snapshots),
# PROJECT_ID (optional; passed to gcloud as --project when set).

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"

OUT="${OUT:-${ROOT}/bin/microvm-assets/amd64}"
BUCKET="${BUCKET:-ate-snapshots}"

# Pass --project only when PROJECT_ID is set (mirrors hack/teardown.sh); otherwise
# gcloud uses its active config project. ${PROJECT_ID:+...} elides the flag entirely
# when unset (same idiom as KUBECTL_CONTEXT in hack/run-microvm-demo.sh).
echo ">> Uploading assets to gs://${BUCKET}/kata-assets/ ..."
for f in cloud-hypervisor vmlinux rootfs.img configuration-clh.toml; do
  echo "   $f"
  gcloud storage cp ${PROJECT_ID:+--project="${PROJECT_ID}"} "${OUT}/${f}" "gs://${BUCKET}/kata-assets/${f}"
done

echo ">> Done. Verify:"
gcloud storage ls ${PROJECT_ID:+--project="${PROJECT_ID}"} "gs://${BUCKET}/kata-assets/"
