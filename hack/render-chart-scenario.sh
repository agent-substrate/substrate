#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
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

# Renders the charts with the values of one scenario in charts/scenarios, and
# prints the objects. Each chart gets <chart>-values.yaml from the scenario
# directory when it exists, and its defaults otherwise. The release names are
# the chart names. The substrate-crds chart is left out:
# hack/verify/chart-generated.sh checks the CRDs.
#
#   hack/render-chart-scenario.sh charts/scenarios/kind-envoy

set -o errexit -o nounset -o pipefail

if [[ $# -ne 1 || ! -d "$1" ]]; then
  echo "Usage: $0 SCENARIO_DIR" >&2
  exit 1
fi
DIR="$(cd "$1" && pwd)"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

# render <chart> <namespace>
render() {
  local values=()
  if [[ -f "${DIR}/${1}-values.yaml" ]]; then
    values=(--values "${DIR}/${1}-values.yaml")
  fi
  ./hack/run-tool.sh helm template "${1}" "charts/${1}" --namespace "${2}" ${values[@]+"${values[@]}"}
}

render substrate-podcert podcertificate-controller-system
render substrate-atelet ate-system
render substrate ate-system
