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

set -o errexit -o nounset -o pipefail

CLANG_FORMAT_VERSION="22.1.8"

ROOT="$(git rev-parse --show-toplevel)"
INSTALL_DIR="${ROOT}/bin/clang-format-install/${CLANG_FORMAT_VERSION}"
CLANG_FORMAT_BIN="${INSTALL_DIR}/clang-format"

if [[ ! -x "${CLANG_FORMAT_BIN}" ]]; then
  os_name="$(uname -s)"
  arch_name="$(uname -m)"

  case "${os_name}/${arch_name}" in
    Darwin/arm64)
      wheel_url="https://files.pythonhosted.org/packages/2e/55/539cc1036dae16659f50500ca34838cc5b16cd3e98e3faaf164186b98093/clang_format-${CLANG_FORMAT_VERSION}-py2.py3-none-macosx_11_0_arm64.whl"
      expected_sha="d1147107222c0dda3e4869e9e8c4a79f9ed1de83819e5274de42b82adf3d2129"
      ;;
    Darwin/x86_64)
      wheel_url="https://files.pythonhosted.org/packages/5d/d8/29b9db6098da1a011ca3f7560c3942fa81404dbbb4367c3bd1d5c435da3b/clang_format-${CLANG_FORMAT_VERSION}-py2.py3-none-macosx_10_9_x86_64.whl"
      expected_sha="fc2ac5bd0ea41af49968fb69426207806d5f7016cb8f4bfbd44f4f1ffe8d53f2"
      ;;
    Linux/aarch64|Linux/arm64)
      wheel_url="https://files.pythonhosted.org/packages/50/25/a9734da014eecc1f54c051ad643a28f2f6643dcc812ac59320e80e2b1a3b/clang_format-${CLANG_FORMAT_VERSION}-py2.py3-none-manylinux_2_26_aarch64.manylinux_2_28_aarch64.whl"
      expected_sha="48c3b8dcfe9d4e964ced0e744e0f1f8ddc711bce92e50f6cab21e10f54857d08"
      ;;
    Linux/x86_64)
      wheel_url="https://files.pythonhosted.org/packages/e5/88/b82c066fa807da4ca2518fecf79071361f6324b77375e5e92c059c0697fd/clang_format-${CLANG_FORMAT_VERSION}-py2.py3-none-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl"
      expected_sha="b00cff6bfd1f1686f073a4fdf1cb937dbd58bf7510c659477805c03afdea0816"
      ;;
    *)
      echo "Unsupported platform ${os_name}/${arch_name}" >&2
      exit 1
      ;;
  esac

  echo "Downloading clang-format v${CLANG_FORMAT_VERSION} for ${os_name}/${arch_name}..." >&2
  mkdir -p "${INSTALL_DIR}"
  wheel="$(mktemp "${INSTALL_DIR}/clang-format.whl.XXXXXX")"
  binary="$(mktemp "${INSTALL_DIR}/clang-format.XXXXXX")"
  trap 'rm -f "${wheel}" "${binary}"' EXIT

  # The PyPI wheels contain a standalone clang-format binary; Python is not
  # needed to install or run it.
  curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
    "${wheel_url}" --output "${wheel}"
  actual_sha="$(shasum -a 256 "${wheel}" | awk '{print $1}')"
  if [[ "${actual_sha}" != "${expected_sha}" ]]; then
    echo "Checksum verification failed for clang-format download" >&2
    echo "Expected: ${expected_sha}" >&2
    echo "Got:      ${actual_sha}" >&2
    exit 1
  fi

  unzip -p "${wheel}" clang_format/data/bin/clang-format >"${binary}"
  chmod 0755 "${binary}"
  mv "${binary}" "${CLANG_FORMAT_BIN}"
  rm -f "${wheel}"
  trap - EXIT
fi

exec "${CLANG_FORMAT_BIN}" "$@"
