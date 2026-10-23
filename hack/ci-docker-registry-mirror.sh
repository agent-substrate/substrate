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


# Points the Docker daemon at mirror.gcr.io. It rewrites /etc/docker/daemon.json
# and restarts dockerd, so it is for CI runners only.
#
# Anonymous Docker Hub pulls are rate limited per runner IP. mirror.gcr.io is
# meant to be used as a daemon mirror; images it lacks still come from Docker Hub.

set -o errexit -o nounset -o pipefail

cfg=/etc/docker/daemon.json
[[ -s "${cfg}" ]] || echo '{}' | sudo tee "${cfg}" >/dev/null
sudo jq '. + {"registry-mirrors": ["https://mirror.gcr.io"]}' "${cfg}" | sudo tee "${cfg}.new" >/dev/null
sudo mv "${cfg}.new" "${cfg}"
sudo systemctl restart docker
