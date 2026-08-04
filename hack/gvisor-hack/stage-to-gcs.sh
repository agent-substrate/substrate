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

# Stage a locally-built runsc into the GCS snapshot bucket, where atelet fetches
# it (per hack/gvisor-hack/sandboxconfig.yaml.tmpl). The GKE counterpart of
# stage-to-rustfs.sh; usually invoked by deploy.sh rather than directly.
#
# The bucket must be readable by atelet's service account: atelet tries an
# anonymous client first and falls back to its main (authenticated) client, so a
# private bucket in the cluster's own project works.
#
# Requires the `gcloud` CLI authenticated for the bucket's project. Env: RUNSC
# (binary to stage, default ./bin/runsc), OBJECT (destination object name,
# default gvisor-hack/runsc), BUCKET (default ate-snapshots), PROJECT_ID (optional;
# passed to gcloud as --project when set).

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"

RUNSC="${RUNSC:-${ROOT}/bin/runsc}"
OBJECT="${OBJECT:-gvisor-hack/runsc}"
BUCKET="${BUCKET:-ate-snapshots}"

if [[ ! -f "${RUNSC}" ]]; then
  echo "error: runsc binary not found at ${RUNSC} (set RUNSC=/path/to/runsc)" >&2
  exit 1
fi

# Pass --project only when PROJECT_ID is set (mirrors hack/microvm-assets/stage-to-gcs.sh);
# otherwise gcloud uses its active config project.
echo ">> Uploading ${RUNSC} to gs://${BUCKET}/${OBJECT} ..."
gcloud storage cp ${PROJECT_ID:+--project="${PROJECT_ID}"} "${RUNSC}" "gs://${BUCKET}/${OBJECT}"

echo ">> Done. Verify:"
gcloud storage ls ${PROJECT_ID:+--project="${PROJECT_ID}"} "gs://${BUCKET}/$(dirname "${OBJECT}")/"
