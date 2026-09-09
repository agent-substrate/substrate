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

# Shared helpers for the scripts that need a Python virtual environment.

# venv_is_usable reports whether the venv at $1 can be activated and run. A
# directory alone does not mean usable: an interrupted create leaves no
# bin/activate, and an interpreter upgrade leaves bin/python3 dangling.
venv_is_usable() {
  local venv="$1"
  [[ -f "${venv}/bin/activate" ]] && [[ -x "${venv}/bin/python3" ]] &&
    "${venv}/bin/python3" -c '' >/dev/null 2>&1
}

# ensure_venv creates the venv at $1 unless a usable one is already there.
ensure_venv() {
  local venv="$1"

  if venv_is_usable "${venv}"; then
    return 0
  fi
  if [[ -e "${venv}" ]]; then
    echo "Recreating unusable virtual environment in ${venv}..."
  else
    echo "Creating virtual environment in ${venv}..."
  fi
  # --clear is required: plain `python3 -m venv` and --upgrade both leave a
  # dangling bin/python3 in place.
  python3 -m venv --clear "${venv}"
  # Only on create: this one asks the index every run.
  "${venv}/bin/pip" install --quiet --upgrade pip
}

# venv_sync_requirements installs $2 (a requirements.txt) into the venv at $1.
# Cheap to repeat: pip satisfies an unchanged file from installed metadata
# without asking the index.
venv_sync_requirements() {
  local venv="$1"
  local requirements="$2"
  "${venv}/bin/pip" install --quiet -r "${requirements}"
}
