#!/bin/sh

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

set -eu

state_dir="$2"
case "$3" in
  create)
    touch "${CLEANUP_TEST_CGROUP}.created"
    mkdir -p "${CLEANUP_TEST_CGROUP}"
    printf retained | tee "${state_dir}/state" "${state_dir}/../pidfiles/_sandbox.pid"
    if [ "${CLEANUP_TEST_BLOCKED}" = true ]; then
      touch "${CLEANUP_TEST_CGROUP}/remaining"
    fi
    if [ "${CLEANUP_TEST_CANCEL}" = true ]; then
      mkfifo "${state_dir}/cancel"
      printf 'cancel-startup\n'
      read -r _ < "${state_dir}/cancel"
    fi
    exit 1
    ;;
  state | kill | wait) exit 0 ;;
  delete) exit 1 ;;
  *) exit 2 ;;
esac
