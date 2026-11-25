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

# Install the helm-unittest plugin from its release tarball, pinned by SHA-256.
# The tarball is a complete plugin directory, so unpacking it is the install.
set -o errexit -o nounset -o pipefail

version=1.0.3
sha256=9761f23d9509c98770c026e019e743b524b57010f4bc29175f78d2582ace0633

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
curl -fsSL -o "${tmp}/helm-unittest.tgz" \
  "https://github.com/helm-unittest/helm-unittest/releases/download/v${version}/helm-unittest-linux-amd64-${version}.tgz"
echo "${sha256}  ${tmp}/helm-unittest.tgz" | sha256sum --check --quiet

plugin_dir="$(helm env HELM_PLUGINS)/helm-unittest"
mkdir -p "${plugin_dir}"
tar -xzf "${tmp}/helm-unittest.tgz" -C "${plugin_dir}"
