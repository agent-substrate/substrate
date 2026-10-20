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

# Logs docker in to ghcr.io as GITHUB_ACTOR with the token in TOKEN. ko and
# ate-setup's registry checks read the credentials docker login stores.
#
#   GITHUB_ACTOR=<user> TOKEN=<token> .github/scripts/release/ghcr-login.sh
#
# Used by the images job in .github/workflows/release.yaml.

set -o errexit -o nounset -o pipefail

docker login ghcr.io --username "${GITHUB_ACTOR}" --password-stdin <<<"${TOKEN}"
