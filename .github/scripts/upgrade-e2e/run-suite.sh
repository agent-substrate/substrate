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

# Runs the upgrade suite (internal/e2e/upgrade) from a copy of the old
# release's install record, so this checkout's ate-setup upgrades the cluster
# in the runbook's order.
#
#   ATE_RECORD_DIR=<dir> E2E_ATE_SETUP_CONFIG=<file> E2E_JUNIT_FILE=<file> \
#     .github/scripts/upgrade-e2e/run-suite.sh
#
# Runs in the checkout of the release under test. The old release is installed
# with no preview gates, and CI has to say so. Used by
# .github/workflows/upgrade-e2e.yaml.

set -o errexit -o nounset -o pipefail

cp "${ATE_RECORD_DIR}"/installs/*.yaml "${E2E_ATE_SETUP_CONFIG}"
hack/run-e2e-kind.sh ./internal/e2e/upgrade -timeout 90m -args --no-color --preview=""
