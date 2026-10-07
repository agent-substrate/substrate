#!/bin/bash
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

# Source from other lab scripts; never changes the installed container runtime.
ROOT=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
LAB="$ROOT/.amp/in/mac-lab"
export CONTAINER_APP_ROOT="$LAB/runtime"
export CONTAINER_INSTALL_ROOT="$LAB/dist"
export XDG_CONFIG_HOME="$LAB/config"
CONTAINER="$CONTAINER_INSTALL_ROOT/bin/container"
machine() {
  # machine run passes its arguments through the guest login shell.
  local command
  printf -v command '%q ' "$@"
  "$CONTAINER" machine run --root -i -n substrate-lab "$command"
}
