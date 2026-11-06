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

with_retries() {
  local retries=5
  local backoff=2
  local count=0
  while true; do
    local exit_code=0
    "$@" || exit_code=$?
    if [[ ${exit_code} -eq 0 ]]; then
      return 0
    fi
    # Exit code 150 is a special signal to abort retries immediately
    if [[ ${exit_code} -eq 150 ]]; then
      return 1
    fi
    count=$((count + 1))
    if [[ ${count} -ge ${retries} ]]; then
      return ${exit_code}
    fi
    echo "warning: command '$1' failed. Retrying in ${backoff}s (${count}/${retries})..." >&2
    sleep "${backoff}"
    backoff=$((backoff * 2))
  done
}
