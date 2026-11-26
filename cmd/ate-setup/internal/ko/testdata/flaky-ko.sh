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

# A ko stand-in for `ko build <package> ...`. It fails the first
# FLAKY_KO_FAILURES builds of each package, printing FLAKY_KO_STDERR, then
# prints a ref for it. It counts builds per package in FLAKY_KO_DIR.

set -o errexit -o nounset -o pipefail

pkg="$(basename "$2")"
runs="${FLAKY_KO_DIR}/runs-${pkg}"
echo run >> "${runs}"
builds="$(wc -l < "${runs}")"
if (( builds <= FLAKY_KO_FAILURES )); then
  echo "${FLAKY_KO_STDERR}" >&2
  exit 1
fi
echo "repo/${pkg}:v1@sha256:a"
