#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Boot-test a micro-VM guest image via TestGuestRootfsImage in
# cmd/ateom-microvm/rootfs_test.go. Linux only: the test takes the kernel command
# line from ateom-microvm itself, which builds only for Linux.
#
# Usage: test-rootfs.sh <rootfs.img> <vmlinux>
#
# Env: ARCH (the guest's arch, amd64|arm64, default host arch; another arch boots
#      under emulation),
#      CONTAINER_CLI (default docker),
#      QEMU_IMAGE (the image QEMU runs in, default the Dockerfile's DEBIAN_IMAGE),
#      BOOT_TIMEOUT (seconds for kata-agent to start, default 300).

set -o errexit -o nounset -o pipefail

USAGE="usage: test-rootfs.sh <rootfs.img> <vmlinux>"
IMG="${1:?${USAGE}}"
KERNEL="${2:?${USAGE}}"
BOOT_TIMEOUT="${BOOT_TIMEOUT:-300}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "test-rootfs.sh: needs a Linux host (TestGuestRootfsImage builds only for Linux); CI's guest-image job runs it" >&2
  exit 1
fi
if [[ ! "${BOOT_TIMEOUT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "test-rootfs.sh: BOOT_TIMEOUT must be a positive number of seconds, got '${BOOT_TIMEOUT}'" >&2
  exit 1
fi
for f in "${IMG}" "${KERNEL}"; do
  if [[ ! -f "${f}" ]]; then
    echo "test-rootfs.sh: ${f}: no such file" >&2
    exit 1
  fi
done

ROOT="$(git rev-parse --show-toplevel)"
ATEOM_TEST_ROOTFS_IMG="$(realpath "${IMG}")"
ATEOM_TEST_KERNEL="$(realpath "${KERNEL}")"
export ATEOM_TEST_ROOTFS_IMG ATEOM_TEST_KERNEL BOOT_TIMEOUT

LOG="$(mktemp)"
trap 'rm -f "${LOG}"' EXIT

cd "${ROOT}"
# The boot wait, the test's 120 s for the guest checks, and slack for compiling
# the test and pulling the QEMU image.
go test -v -count=1 -timeout "$(( BOOT_TIMEOUT + 300 ))s" \
  ./cmd/ateom-microvm -run '^TestGuestRootfsImage$' 2>&1 | tee "${LOG}"
# go test also succeeds when nothing matched -run, so require the test's own PASS.
if ! grep -q '^--- PASS: TestGuestRootfsImage ' "${LOG}"; then
  echo "test-rootfs.sh: TestGuestRootfsImage did not run" >&2
  exit 1
fi
