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

# Boot-test a micro-VM guest image. QEMU boots rootfs.img with the kernel command line
# ateom boots it with, and once kata-agent has started its ttRPC server, a script runs
# in the agent's debug console and checks what the guest has to provide: the commands
# the agent and ateom run (iptables-save/-restore for the agent's iptables RPCs; ip,
# ps, ls, head and grep for DebugConsoleDump), a read-only root, an intact dpkg
# database, and no hostname left behind by the build.
#
# Usage: test-rootfs.sh <rootfs.img> <vmlinux>
#
# QEMU runs in a container, with KVM when /dev/kvm is usable and emulated otherwise,
# which is slower but needs nothing from the host. The debug console is on the serial
# port rather than on vsock as in ateom, so the host needs no vhost-vsock device
# either. amd64 only.
#
# Env: CONTAINER_CLI (default docker),
#      BOOT_TIMEOUT (seconds for kata-agent to start, default 300).

set -o errexit -o nounset -o pipefail

# The DEBIAN_IMAGE of rootfs/Dockerfile.
QEMU_IMAGE="debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a"
BOOT_TIMEOUT="${BOOT_TIMEOUT:-300}"
CHECK_TIMEOUT=120

if [[ "${1:-}" != "--in-container" ]]; then
  USAGE="usage: test-rootfs.sh <rootfs.img> <vmlinux>"
  IMG="${1:?${USAGE}}"
  KERNEL="${2:?${USAGE}}"
  CONTAINER_CLI="${CONTAINER_CLI:-docker}"
  if [[ "$(uname -m)" != x86_64 ]]; then
    echo "test-rootfs.sh: only amd64 is supported" >&2
    exit 1
  fi
  for f in "${IMG}" "${KERNEL}"; do
    if [[ ! -f "${f}" ]]; then
      echo "test-rootfs.sh: ${f}: no such file" >&2
      exit 1
    fi
  done
  DEVICES=()
  if [[ -r /dev/kvm && -w /dev/kvm ]]; then
    DEVICES+=(--device /dev/kvm)
  fi
  exec "${CONTAINER_CLI}" run --rm "${DEVICES[@]}" -e BOOT_TIMEOUT="${BOOT_TIMEOUT}" \
    -v "$(realpath "${IMG}"):/rootfs.img:ro" \
    -v "$(realpath "${KERNEL}"):/vmlinux:ro" \
    -v "$(realpath "${BASH_SOURCE[0]}"):/test-rootfs.sh:ro" \
    "${QEMU_IMAGE}" bash /test-rootfs.sh --in-container
fi

# Everything below runs in the container.

# What runs in the guest, in a non-interactive bash. Each check line is one check: it
# prints the command's output, then OK:<name> or FAIL:<name>.
GUEST_CHECKS="$(cat <<'EOF'
check() {
  echo
  echo "== $1: $2"
  if eval "$2"; then echo "OK:$1"; else echo "FAIL:$1"; fi
}
echo "== guest checks"
check agent 'kata-agent --version'
check root-ro 'grep " / " /proc/mounts && grep -Eq "^[^ ]+ / ext4 ro[ ,]" /proc/mounts'
check mount 'mount | grep -E " on / type ext4 \(ro[,)]"'
check hostname 'cat /proc/sys/kernel/hostname && test "$(cat /proc/sys/kernel/hostname)" = localhost'
check ip 'ip addr && ip route && ip neigh'
check ps 'ps -N --ppid 2 -p 2 -o pid,comm && test "$(ps -o comm= -p 1)" = kata-agent'
check coreutils 'ls -la / | head -n 4 && head -n 2 /etc/os-release && grep -c . /proc/mounts'
check iptables "printf '*filter\n-A INPUT -i lo -j ACCEPT\nCOMMIT\n' | iptables-restore && iptables-save | grep -- '-A INPUT -i lo -j ACCEPT'"
check ip6tables "printf '*filter\n-A INPUT -i lo -j ACCEPT\nCOMMIT\n' | ip6tables-restore && ip6tables-save | grep -- '-A INPUT -i lo -j ACCEPT'"
check dpkg 'n="$(dpkg-query -W | wc -l)" && echo "${n} packages" && test "${n}" -gt 0 && test -z "$(dpkg --audit)"'
echo "== guest checks done"
EOF
)"
mapfile -t CHECK_NAMES < <(sed -n 's/^check \([a-z0-9-]*\) .*/\1/p' <<<"${GUEST_CHECKS}")

# ateom's command line (buildVMConfig in cmd/ateom-microvm/run.go plus baseKernelParams
# in cmd/ateom-microvm/internal/kata/config.go), with the debug console on
# /dev/console, which the last console= makes the serial port.
CMDLINE="root=/dev/vda1 rootflags=data=ordered,errors=remount-ro ro rootfstype=ext4"
CMDLINE+=" panic=1 no_timer_check noreplace-smp console=hvc0 init=/usr/bin/kata-agent"
CMDLINE+=" cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1"
CMDLINE+=" agent.debug_console console=ttyS0"

WORK="$(mktemp -d)"
LOG="${WORK}/serial.log"
SERIAL_IN="${WORK}/serial.in"
QEMU_PID=""
trap '[[ -z "${QEMU_PID}" ]] || kill "${QEMU_PID}" 2>/dev/null || true' EXIT

fail() {
  echo "test-rootfs.sh: $*" >&2
  if [[ -s "${LOG}" ]]; then
    echo "--- serial console, last 80 lines ---" >&2
    tail -n 80 "${LOG}" | tr -d '\r' >&2
  fi
  if [[ -s "${WORK}/qemu.err" ]]; then
    echo "--- qemu ---" >&2
    cat "${WORK}/qemu.err" >&2
  fi
  exit 1
}

# Waits until the serial console shows $1, failing if QEMU exits or $2 seconds pass.
wait_for() {
  local deadline=$(( SECONDS + $2 ))
  until grep -q "$1" "${LOG}"; do
    kill -0 "${QEMU_PID}" 2>/dev/null || fail "the guest stopped before printing '$1'"
    (( SECONDS < deadline )) || fail "the guest did not print '$1' within $2s"
    sleep 1
  done
}

echo ">> Installing QEMU..."
if ! out="$(apt-get update -qq 2>&1 && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y -qq --no-install-recommends qemu-system-x86 2>&1)"; then
  echo "${out}" >&2
  fail "installing QEMU failed"
fi

ACCEL=tcg
if [[ -c /dev/kvm && -w /dev/kvm ]]; then
  ACCEL=kvm
fi

echo ">> Booting rootfs.img (${ACCEL})..."
: >"${LOG}"
mkfifo "${SERIAL_IN}"
BOOT_START="${SECONDS}"
# The drive is read-only, as the guest mounts it. panic=1 with -no-reboot turns a
# failed boot into QEMU exiting.
qemu-system-x86_64 -nodefaults -no-user-config -machine "q35,accel=${ACCEL}" -cpu max \
  -smp 2 -m 512 -display none -monitor none -no-reboot \
  -kernel /vmlinux -append "${CMDLINE}" \
  -drive file=/rootfs.img,if=none,id=root,format=raw,readonly=on \
  -device virtio-blk-pci,drive=root \
  -object rng-random,id=rng,filename=/dev/urandom -device virtio-rng-pci,rng=rng \
  -serial stdio <"${SERIAL_IN}" >"${LOG}" 2>"${WORK}/qemu.err" &
QEMU_PID=$!
# Held open for the whole run: QEMU reads the console's input from it.
exec 3>"${SERIAL_IN}"

wait_for "ttRPC server started" "${BOOT_TIMEOUT}"
echo ">> kata-agent started its ttRPC server $(( SECONDS - BOOT_START ))s after QEMU did"

# Sent base64-encoded, so the console's echo of the input cannot be mistaken for the
# results. The guest decodes it with coreutils' base64.
{
  echo "base64 -d <<'CHECKS' | bash 2>&1"
  base64 <<<"${GUEST_CHECKS}"
  echo "CHECKS"
} >&3
wait_for "== guest checks done" "${CHECK_TIMEOUT}"

RESULTS="$(tr -d '\r' <"${LOG}" | sed -n '/== guest checks$/,/== guest checks done/p')"
echo "${RESULTS}"
echo

FAILED=()
for name in "${CHECK_NAMES[@]}"; do
  if ! grep -q "^OK:${name}\$" <<<"${RESULTS}"; then
    FAILED+=("${name}")
  fi
done
if [[ ${#FAILED[@]} -gt 0 ]]; then
  fail "failed checks: ${FAILED[*]}"
fi
echo ">> All ${#CHECK_NAMES[@]} checks passed: ${CHECK_NAMES[*]}"
