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

# Shrink the upstream Kata Containers guest rootfs image (kata-ubuntu-noble.image,
# 256 MiB) in place, to ~32 MiB for kata 4.1.0 on amd64, inside a privileged Docker
# container (matching Kata osbuilder's USE_DOCKER=true pattern and stage-to-rustfs.sh).
#
# Why this is safe for Substrate micro-VMs:
#   1. Outer VM OS vs. Inner Workload OS:
#      ateom-microvm boots the guest with init=/usr/bin/kata-agent (cmd/ateom-microvm/run.go,
#      buildVMConfig). Requires Cloud Hypervisor >= v53 (where the VMM advances the guest
#      clock on snapshot restore without falling back to systemd/chronyd).
#      The outer VM rootfs (/dev/vda1) ONLY runs PID 1 (kata-agent). All actor
#      workloads and shell commands (bash, python, git, node, etc.) execute
#      inside the OCI container rootfs shared from the host over virtio-fs
#      (kataShared) after kata-agent pivot_roots into the container.
#   2. Preserves ateom DebugConsoleDump diagnostics:
#      Retains /usr/bin/kata-agent, the diagnostic binaries invoked by
#      DebugConsoleDump on vsock port 1026 (sh, dash, bash, ip, ls, head, grep,
#      cat, mount, umount, ps), iptables/xtables, and every shared library they
#      load, as listed by the image's own dynamic loader (ld.so --list).
#   3. Privileged Docker execution (uid=0):
#      Runs inside `docker run --rm --privileged ubuntu:24.04` as real root
#      (uid=0, gid=0) so loop mounting, chown -R 0:0, and mkfs.ext4 -d work
#      identically across developer workstations and Ubuntu 24.04 GitHub Actions
#      runners (where AppArmor restricts unprivileged user namespaces).
#
# Usage:
#   hack/microvm-assets/slim-rootfs.sh <path-to-rootfs.img>

set -o errexit -o nounset -o pipefail

IMAGE_PATH="${1:?usage: slim-rootfs.sh <path-to-rootfs.img>}"
ARCH="${ARCH:-amd64}"
EXT4_NO_JOURNAL="${EXT4_NO_JOURNAL:-true}"
# Pinned to the multi-arch index digest, so --platform still selects the arch. The
# repack's own tools, e2fsprogs included, all come from this image, so the digest also
# fixes how the new filesystem is laid out.
DOCKER_IMAGE="${DOCKER_IMAGE:-ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3}"

if [[ ! -f "${IMAGE_PATH}" ]]; then
  echo "error: rootfs image not found at ${IMAGE_PATH}" >&2
  exit 1
fi

# Re-exec inside a privileged Docker container as root (uid=0, gid=0), matching
# Kata osbuilder (USE_DOCKER=true). Inside the container, loop mounts, chroot,
# chown 0:0, and mkfs.ext4 -d run with full root capabilities and zero host
# package dependencies beyond docker.
if [[ "${IN_SHRINK_CONTAINER:-}" != "true" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "error: 'docker' is required to run slim-rootfs.sh" >&2
    exit 1
  fi
  ABS_IMAGE="$(cd "$(dirname "${IMAGE_PATH}")" && pwd)/$(basename "${IMAGE_PATH}")"
  IMG_DIR="$(dirname "${ABS_IMAGE}")"
  IMG_BASE="$(basename "${ABS_IMAGE}")"
  SCRIPT_ABS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

  exec docker run --rm --privileged \
    --platform "linux/${ARCH}" \
    -e IN_SHRINK_CONTAINER="true" \
    -e ARCH="${ARCH}" \
    -e EXT4_NO_JOURNAL="${EXT4_NO_JOURNAL}" \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    -v "${SCRIPT_ABS}:/slim-rootfs.sh:ro" \
    -v "${IMG_DIR}:/work" \
    "${DOCKER_IMAGE}" \
    /bin/bash /slim-rootfs.sh "/work/${IMG_BASE}"
fi

ORIG_BYTES="$(stat -c %s "${IMAGE_PATH}")"
WORK_DIR="$(mktemp -d)"
MNT_DIR="${WORK_DIR}/mnt"
mkdir -p "${MNT_DIR}"

cleanup() {
  # The chroot self-test mounts /proc under the staging tree; if a check fails,
  # errexit fires before its umount, so unmount it here before deleting.
  umount "${WORK_DIR}/minimal/proc" 2>/dev/null || true
  umount -l "${MNT_DIR}" 2>/dev/null || true
  rm -rf --one-file-system "${WORK_DIR}"
}
trap cleanup EXIT

# --- 1. Read Partition 1 start/size from DOS MBR or GPT ----------------------
PART_TYPE="$(od -An -tx1 -j 450 -N 1 "${IMAGE_PATH}" | tr -d ' ')"
if [[ "${PART_TYPE}" == "ee" ]]; then
  # GPT Partition Table: Partition Entry #1 starts at LBA 2 (offset 1024).
  # FirstLBA is uint64 at offset 1056 (1024+32); LastLBA is uint64 at offset 1064 (1024+40).
  START_SECTOR="$(od -An -tu8 -j 1056 -N 8 "${IMAGE_PATH}" | tr -d ' ')"
  LAST_SECTOR="$(od -An -tu8 -j 1064 -N 8 "${IMAGE_PATH}" | tr -d ' ')"
  SIZE_SECTORS=$(( LAST_SECTOR - START_SECTOR + 1 ))
else
  # DOS/MBR Partition Table: Partition #1 LBA start is uint32 at 454, size at 458.
  START_SECTOR="$(od -An -tu4 -j 454 -N 4 "${IMAGE_PATH}" | tr -d ' ')"
  SIZE_SECTORS="$(od -An -tu4 -j 458 -N 4 "${IMAGE_PATH}" | tr -d ' ')"
fi

if [[ -z "${START_SECTOR}" || "${START_SECTOR}" -eq 0 || -z "${SIZE_SECTORS}" || "${SIZE_SECTORS}" -le 0 ]]; then
  echo "error: failed to read Partition 1 LBA start/size from ${IMAGE_PATH}" >&2
  exit 1
fi

echo ">> Mounting Partition 1 (start=${START_SECTOR}, sectors=${SIZE_SECTORS}) from ${IMAGE_PATH}..."
if mount -o "loop,ro,offset=$(( START_SECTOR * 512 )),sizelimit=$(( SIZE_SECTORS * 512 ))" "${IMAGE_PATH}" "${MNT_DIR}" 2>/dev/null; then
  SRC_DIR="${MNT_DIR}"
else
  # Fallback to debugfs rdump if loop devices are unavailable in the container environment.
  dd if="${IMAGE_PATH}" of="${WORK_DIR}/orig_part1.ext4" \
    bs=512 skip="${START_SECTOR}" count="${SIZE_SECTORS}" status=none
  SRC_DIR="${WORK_DIR}/extract"
  mkdir -p "${SRC_DIR}"
  /usr/sbin/debugfs -R "rdump / ${SRC_DIR}" "${WORK_DIR}/orig_part1.ext4" 2>/dev/null
fi

# --- 2. Assemble minimal Outer VM OS rootfs tree -----------------------------
DST="${WORK_DIR}/minimal"
mkdir -p "${DST}"/{usr/bin,usr/sbin,dev,proc,sys,run,var,etc,mnt,root}

# Because /dev/vda1 is mounted read-only (ro) and kata-agent (PID 1) mounts a
# writable tmpfs at /run (src/agent/src/mount.rs, init_agent_as_init) before
# initializing agent policy or containers, symlink /tmp, /var/run, /var/tmp, and
# /var/lock to /run so runtime writes (/var/run/sandbox-ns, /var/run/cdi,
# /tmp/policy.jsonl) always target the writable /run tmpfs instead of EROFS.
ln -sf /run "${DST}/tmp"
ln -sf /run "${DST}/var/run"
ln -sf /run "${DST}/var/tmp"
ln -sf /run/lock "${DST}/var/lock"

# Preserve Ubuntu merged-/usr top-level symlinks (bin -> usr/bin, sbin -> usr/sbin, lib -> usr/lib).
ln -sf usr/bin "${DST}/bin"
ln -sf usr/sbin "${DST}/sbin"
ln -sf usr/lib "${DST}/lib"
if [[ -L "${SRC_DIR}/lib64" || -d "${SRC_DIR}/lib64" ]]; then
  mkdir -p "${DST}/lib64"
fi

# Detect multiarch library directory (usr/lib/x86_64-linux-gnu or usr/lib/aarch64-linux-gnu).
MULTIARCH_DIR="$(find "${SRC_DIR}/usr/lib" -maxdepth 1 -type d -name "*-linux-gnu" | head -n1)"
if [[ -z "${MULTIARCH_DIR}" ]]; then
  echo "error: could not locate usr/lib/*-linux-gnu in guest rootfs" >&2
  exit 1
fi
MULTIARCH_REL="usr/lib/$(basename "${MULTIARCH_DIR}")"
mkdir -p "${DST}/${MULTIARCH_REL}"

# Binaries required by PID 1 (kata-agent), GetIPTables/SetIPTables RPCs
# (src/agent/src/rpc.rs, get_iptables/set_iptables), and ateom's DebugConsoleDump
# (cmd/ateom-microvm/run.go, DebugConsoleDump).
ESSENTIAL_BINS=(
  "/usr/bin/kata-agent"
  "/usr/bin/sh"
  "/usr/bin/dash"
  "/usr/bin/bash"
  "/usr/bin/ip"
  "/usr/bin/ls"
  "/usr/bin/head"
  "/usr/bin/grep"
  "/usr/bin/cat"
  "/usr/bin/mount"
  "/usr/bin/umount"
  "/usr/bin/ps"
  "/usr/sbin/xtables-nft-multi"
  "/usr/sbin/xtables-legacy-multi"
  "/usr/sbin/iptables"
  "/usr/sbin/iptables-save"
  "/usr/sbin/iptables-restore"
  "/usr/sbin/ip6tables"
  "/usr/sbin/ip6tables-save"
  "/usr/sbin/ip6tables-restore"
  # /usr/sbin/iptables* -> /etc/alternatives/iptables* -> /usr/sbin/iptables-nft*
  # -> xtables-nft-multi. The loop below only follows one hop, so name the
  # alternatives targets explicitly or iptables-restore dangles in the image.
  "/usr/sbin/iptables-nft"
  "/usr/sbin/iptables-nft-save"
  "/usr/sbin/iptables-nft-restore"
  "/usr/sbin/iptables-legacy"
  "/usr/sbin/iptables-legacy-save"
  "/usr/sbin/iptables-legacy-restore"
  "/usr/sbin/ip6tables-nft"
  "/usr/sbin/ip6tables-nft-save"
  "/usr/sbin/ip6tables-nft-restore"
  "/usr/sbin/ip6tables-legacy"
  "/usr/sbin/ip6tables-legacy-save"
  "/usr/sbin/ip6tables-legacy-restore"
)

for b in "${ESSENTIAL_BINS[@]}"; do
  if [[ -e "${SRC_DIR}${b}" || -L "${SRC_DIR}${b}" ]]; then
    cp -a "${SRC_DIR}${b}" "${DST}${b}"
    if [[ -L "${SRC_DIR}${b}" ]]; then
      target="$(readlink "${SRC_DIR}${b}")"
      target_name="$(basename "${target}")"
      for dir in /usr/bin /usr/sbin; do
        if [[ -f "${SRC_DIR}${dir}/${target_name}" ]]; then
          cp -a "${SRC_DIR}${dir}/${target_name}" "${DST}${dir}/${target_name}"
        fi
      done
    fi
  fi
done

# Ensure both /usr/bin/kata-agent and /sbin/init resolve to the agent binary.
ln -sf /usr/bin/kata-agent "${DST}/usr/sbin/init"

# Copy the ELF dynamic linker (ld-linux-x86-64.so.2 on amd64, ld-linux-aarch64.so.1 on arm64)
# along with its resolved target file if it is a symlink.
LD_SO_CHROOT=""
for ld_so in "${MULTIARCH_DIR}"/ld-linux*.so*; do
  if [[ -e "${ld_so}" ]]; then
    cp -a "${ld_so}" "${DST}/${MULTIARCH_REL}/"
    ld_real="$(readlink -f "${ld_so}")"
    if [[ -f "${ld_real}" ]]; then
      cp -a "${ld_real}" "${DST}/${MULTIARCH_REL}/"
    fi
    ld_base="$(basename "${ld_so}")"
    LD_SO_CHROOT="/${MULTIARCH_REL}/${ld_base}"
    if [[ -d "${DST}/lib64" ]]; then
      ln -sf "../${MULTIARCH_REL}/${ld_base}" "${DST}/lib64/${ld_base}"
    fi
    # Relative to /usr/lib itself: on arm64 this link *is* the ELF interpreter
    # (/lib/ld-linux-aarch64.so.1 via lib -> usr/lib), so it must resolve.
    ln -sf "$(basename "${MULTIARCH_REL}")/${ld_base}" "${DST}/usr/lib/${ld_base}"
  fi
done

# Copy dynamically dlopen(3)-loaded libraries that ld.so --list cannot see:
#   1. glibc Name Service Switch modules (libnss_files, libnss_dns, libnss_compat, ~104 KiB)
#   2. iptables/xtables match & target plugins (usr/lib/*-linux-gnu/xtables/*.so, ~2.0 MiB)
for nss_so in "${MULTIARCH_DIR}"/libnss_*.so*; do
  [[ -e "${nss_so}" ]] || continue
  case "$(basename "${nss_so}")" in
    libnss_systemd*|libnss_mymachines*|libnss_resolve*) continue ;;
  esac
  cp -a "${nss_so}" "${DST}/${MULTIARCH_REL}/"
done

if [[ -d "${MULTIARCH_DIR}/xtables" ]]; then
  cp -a "${MULTIARCH_DIR}/xtables" "${DST}/${MULTIARCH_REL}/"
fi

# Resolve all shared libraries for each essential binary using the ELF dynamic
# linker (--list) inside SRC_DIR (which works on both stock and already-slimmed
# images), copying both the SONAME symlink (e.g. libelf.so.1) AND its resolved
# target file (e.g. libelf-0.190.so).
for b in "${ESSENTIAL_BINS[@]}"; do
  if [[ -e "${SRC_DIR}${b}" ]]; then
    while IFS= read -r lib_path; do
      [[ -n "${lib_path}" ]] || continue
      lib_base="$(basename "${lib_path}")"
      for cand in "${MULTIARCH_DIR}/${lib_base}"*; do
        if [[ -e "${cand}" ]]; then
          cp -a "${cand}" "${DST}/${MULTIARCH_REL}/"
          real_cand="$(readlink -f "${cand}")"
          if [[ -f "${real_cand}" ]]; then
            cp -a "${real_cand}" "${DST}/${MULTIARCH_REL}/"
          fi
        fi
      done
    done < <(chroot "${SRC_DIR}" "${LD_SO_CHROOT}" --list "${b}" 2>/dev/null | awk '{for(i=1;i<=NF;i++) if ($i ~ /^\//) print $i}')
  fi
done

# Copy /etc (~776 KiB) to preserve /etc/ld.so.cache, /etc/passwd, /etc/group,
# /etc/nsswitch.conf, /etc/protocols, and /etc/alternatives (required by the
# /usr/sbin/iptables-restore -> /etc/alternatives/iptables-restore -> xtables-nft-multi
# symlink chain). Also retains /etc/kata-opa (~4 KiB) so slim-rootfs.sh remains
# compatible if ever run standalone against a stock kata-agent (AGENT_POLICY=yes).
cp -a "${SRC_DIR}/etc/." "${DST}/etc/"
if [[ -f "${DST}/etc/nsswitch.conf" ]]; then
  sed -i 's/[[:space:]]*systemd//g' "${DST}/etc/nsswitch.conf"
fi

# Ensure all files and directories in DST are owned by root:root (0:0).
chown -R 0:0 "${DST}"

# Verify via chroot (with /proc briefly mounted for libproc2/ps) that PID 1
# (/sbin/init & /usr/bin/kata-agent), /var/run, iptables-restore, and every
# DebugConsoleDump utility execute cleanly with zero missing libraries.
mount -t proc proc "${DST}/proc"
chroot "${DST}" /sbin/init --version >/dev/null
chroot "${DST}" /usr/bin/kata-agent --version >/dev/null
chroot "${DST}" /bin/sh -c "test -L /var/run && test -L /tmp && iptables-restore --version && ip6tables-restore --version && ip -V && ls / && head -n 1 /etc/passwd && grep root /etc/passwd && cat /etc/passwd && mount --version && umount --version && ps aux" >/dev/null
umount "${DST}/proc"

# Normalize staging tree timestamps before mkfs.ext4.
find "${DST}" -exec touch -h -d "@1700000000" {} +

# --- 3. Build slim Partition 1 ext4 filesystem -------------------------------
PART_IMG="${WORK_DIR}/part1.ext4"
DIR_MIB="$(du -sm "${DST}" | awk '{print $1}')"
if [[ "${EXT4_NO_JOURNAL}" == "true" ]]; then
  PART_MIB=$(( DIR_MIB + 12 ))
  truncate -s "${PART_MIB}M" "${PART_IMG}"
  E2FSPROGS_FAKE_TIME=1700000000 /usr/sbin/mkfs.ext4 -q -F \
    -O ^has_journal \
    -E root_owner=0:0,hash_seed=00000000-0000-4000-8000-000000000002 \
    -N 2048 \
    -U 00000000-0000-4000-8000-000000000001 \
    -b 4096 -m 0 -d "${DST}" "${PART_IMG}"
else
  PART_MIB=$(( DIR_MIB + 16 ))
  truncate -s "${PART_MIB}M" "${PART_IMG}"
  E2FSPROGS_FAKE_TIME=1700000000 /usr/sbin/mkfs.ext4 -q -F \
    -J size=4 \
    -E root_owner=0:0,hash_seed=00000000-0000-4000-8000-000000000002 \
    -N 2048 \
    -U 00000000-0000-4000-8000-000000000001 \
    -b 4096 -m 0 -d "${DST}" "${PART_IMG}"
fi

# e2fsck: 0 = clean, 1 = errors corrected; anything higher is a real failure.
fsck_rc=0
E2FSPROGS_FAKE_TIME=1700000000 /usr/sbin/e2fsck -fy "${PART_IMG}" >/dev/null 2>&1 || fsck_rc=$?
if (( fsck_rc > 1 )); then
  echo "error: e2fsck failed on ${PART_IMG} (exit ${fsck_rc})" >&2
  exit 1
fi
E2FSPROGS_FAKE_TIME=1700000000 /usr/sbin/resize2fs -M "${PART_IMG}" >/dev/null 2>&1

# ateom mounts /dev/vda1 with rootflags=data=ordered (cmd/ateom-microvm/run.go,
# buildVMConfig). On a journal-less ext4, the kernel rejects any data= mode that
# differs from the superblock default and the guest panics. Recording ordered as
# the default makes the image mount under both the current and a data=-less cmdline.
E2FSPROGS_FAKE_TIME=1700000000 /usr/sbin/tune2fs -o journal_data_ordered "${PART_IMG}" >/dev/null

# --- 4. Wrap Partition 1 in DOS/MBR disk image so /dev/vda1 mounts -----------
NEW_IMG="${WORK_DIR}/rootfs.img"
PART_SECTORS=$(( $(stat -c %s "${PART_IMG}") / 512 ))
TOTAL_SECTORS=$(( 2048 + PART_SECTORS ))

truncate -s $(( TOTAL_SECTORS * 512 )) "${NEW_IMG}"

# Write standard 512-byte DOS/MBR Partition 1 entry at offset 446 (0x1BE):
# bootable=0x80, CHS=feffff, type=0x83 (Linux), CHS=feffff, LBA start=2048 (0x00080000),
# LBA size=PART_SECTORS (uint32 little-endian), and MBR signature 0x55AA at offset 510.
printf "\x76\x98\xa3\x65" | dd of="${NEW_IMG}" bs=1 seek=440 conv=notrunc status=none
printf "\x80\xfe\xff\xff\x83\xfe\xff\xff\x00\x08\x00\x00" | dd of="${NEW_IMG}" bs=1 seek=446 conv=notrunc status=none
HEX_SECTORS="$(printf '\\x%02x\\x%02x\\x%02x\\x%02x' \
  $(( PART_SECTORS & 0xff )) \
  $(( (PART_SECTORS >> 8) & 0xff )) \
  $(( (PART_SECTORS >> 16) & 0xff )) \
  $(( (PART_SECTORS >> 24) & 0xff )))"
printf '%b' "${HEX_SECTORS}" | dd of="${NEW_IMG}" bs=1 seek=458 conv=notrunc status=none
printf "\x55\xaa" | dd of="${NEW_IMG}" bs=1 seek=510 conv=notrunc status=none

dd if="${PART_IMG}" of="${NEW_IMG}" bs=512 seek=2048 conv=notrunc status=none

mv "${NEW_IMG}" "${IMAGE_PATH}"
if [[ -n "${HOST_UID:-}" && -n "${HOST_GID:-}" ]]; then
  chown "${HOST_UID}:${HOST_GID}" "${IMAGE_PATH}"
fi

NEW_BYTES="$(stat -c %s "${IMAGE_PATH}")"
echo ">> Shrunk ${IMAGE_PATH}: $(( ORIG_BYTES / 1048576 )) MiB -> $(( NEW_BYTES / 1048576 )) MiB"
