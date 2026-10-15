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

# Verifies that the checked-in sweperf ActorTemplates under generated/ match
# what sweperf-images.json and sweperf-template.yaml.j2 render, and runs the
# generator's unit tests.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

source hack/util/venv.sh

DIR="benchmarking/workloads/manifests/sweperf"
ensure_venv "${DIR}/venv"
venv_sync_requirements "${DIR}/venv" "${DIR}/requirements.txt"

"${DIR}/venv/bin/python3" "${DIR}/generate_sweperf_templates.py" --check
(cd "${DIR}" && venv/bin/python3 -m unittest -q test_generate_sweperf_templates)
