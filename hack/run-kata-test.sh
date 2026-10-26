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


# Runs the Kata worker tests that start real VMs (cmd/ateworker-kata, build tag
# kata_integration). They need root, /dev/kvm, cgroup v2 and a Kata static
# install (default /opt/kata). They are not part of any CI lane; this script is
# how to run them. See docs/dev/kata.md.
#
# The tests run as root in a cgroup and mount namespace of their own, see
# hack/run-in-private-cgroup.sh.
#
# Environment:
#   KATA_ROOT               Kata install root. Default /opt/kata.
#   KATA_TEST_ROOTFS        Directory holding an unpacked image with sh, uname, nc
#                           and wget. Default: the pinned alpine image, unpacked
#                           with docker or podman.
#   KATA_TEST_HYPERVISORS   Some of qemu, clh, dragonball and fc, separated by
#                           commas. Default: all.
#   KATA_TEST_CONFIG_<HYPERVISOR>
#                           The Kata configuration to test a hypervisor with, with
#                           the hypervisor in upper case, for example
#                           KATA_TEST_CONFIG_CLH. Default: the install's own. For fc
#                           that is a copy with dial_timeout_ms lowered, since the
#                           shipped value fails the shim's validation.
#   REQUIRE_KATA            Set to true to fail, rather than skip, when a
#                           prerequisite is missing. CI=true does the same.
#
# Extra arguments go to the test binary, for example -test.run TestKata/clh.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# The alpine image the actors run, pinned by digest as in .ko.yaml.
ALPINE_IMAGE="docker.io/library/alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a"

# kata_required is true when a missing prerequisite must fail the run.
kata_required() {
  [[ "${CI:-}" == "true" || "${REQUIRE_KATA:-}" == "true" ]]
}

# missing reports an unmet prerequisite: a failure where kata_required, else a skip.
missing() {
  if kata_required; then
    echo "run-kata-test.sh: Kata is required (CI or REQUIRE_KATA is set) but: $1" >&2
    exit 1
  fi
  echo "run-kata-test.sh: skipping, $1" >&2
  exit 0
}

required=false
if kata_required; then
  required=true
fi

SUDO=()
if [[ "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null || missing "not root, and there is no sudo"
  SUDO=(sudo)
fi

KATA_ROOT="${KATA_ROOT:-/opt/kata}"
[[ -x "${KATA_ROOT}/runtime-rs/bin/containerd-shim-kata-v2" ]] || missing "no shim at ${KATA_ROOT}/runtime-rs/bin/containerd-shim-kata-v2"
[[ -e /dev/kvm ]] || missing "no /dev/kvm"
[[ -e /sys/fs/cgroup/cgroup.controllers ]] || missing "cgroup v2 is not mounted at /sys/fs/cgroup"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/kata-test.XXXXXX")"
container=""
cli=""
cleanup() {
  if [[ -n "${container}" ]]; then
    "${cli}" rm "${container}" >/dev/null 2>&1 || true
  fi
  "${SUDO[@]}" rm -rf "${WORK}"
}
trap cleanup EXIT

# Build the test binary as the invoking user, so the build cache stays theirs.
go test -c -tags kata_integration -o "${WORK}/ateworker-kata.test" ./cmd/ateworker-kata/

if [[ -z "${KATA_TEST_ROOTFS:-}" ]]; then
  for candidate in docker podman; do
    if command -v "${candidate}" >/dev/null; then
      cli="${candidate}"
      break
    fi
  done
  [[ -n "${cli}" ]] || missing "KATA_TEST_ROOTFS is unset and there is neither docker nor podman"
  KATA_TEST_ROOTFS="${WORK}/rootfs"
  mkdir "${KATA_TEST_ROOTFS}"
  container="$("${cli}" create "${ALPINE_IMAGE}")"
  # Unpacked as root, to keep the image's ownership.
  "${cli}" export "${container}" | "${SUDO[@]}" tar -x -C "${KATA_TEST_ROOTFS}" -f -
fi

# Firecracker's shipped configuration does not pass the shim's validation.
hypervisors="${KATA_TEST_HYPERVISORS:-qemu,clh,dragonball,fc}"
if [[ ",${hypervisors}," == *,fc,* && -z "${KATA_TEST_CONFIG_FC:-}" ]]; then
  shipped="${KATA_ROOT}/share/defaults/kata-containers/runtime-rs/configuration-rs-fc.toml"
  [[ -f "${shipped}" ]] || missing "no firecracker configuration at ${shipped}"
  KATA_TEST_CONFIG_FC="${WORK}/configuration-rs-fc.toml"
  sed 's/^dial_timeout_ms = .*/dial_timeout_ms = 100/' "${shipped}" >"${KATA_TEST_CONFIG_FC}"
  export KATA_TEST_CONFIG_FC
fi

env_args=("KATA_ROOT=${KATA_ROOT}" "KATA_TEST_ROOTFS=${KATA_TEST_ROOTFS}" "KATA_TEST_HYPERVISORS=${hypervisors}" "REQUIRE_KATA=${required}")
for hypervisor in QEMU CLH DRAGONBALL FC; do
  var="KATA_TEST_CONFIG_${hypervisor}"
  if [[ -n "${!var:-}" ]]; then
    env_args+=("${var}=${!var}")
  fi
done

"${SUDO[@]}" env "${env_args[@]}" \
  "${ROOT}/hack/run-in-private-cgroup.sh" ate-kata-test \
  "${WORK}/ateworker-kata.test" -test.v -test.count=1 -test.timeout 30m "$@"
