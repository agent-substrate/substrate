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

# The micro-VM guest image (rootfs.img): debian:trixie-slim with the few packages the
# guest runs, plus a kata-agent rebuilt without the features ateom never uses, packed
# into the partitioned ext4 disk ateom boots. hack/microvm-assets/build-rootfs.sh
# drives it.
#
#   agent   builds kata-agent at KATA_VER
#   scripts prune.sh alone, for rootfs to mount
#   rootfs  the guest's root tree
#   image   packs that tree into rootfs.img
#   export  the files build-rootfs.sh takes: rootfs.img, kata-agent, rootfs-packages.txt
#
# No step needs privileges: no --privileged, loop device or mount, and every file is
# written as root inside the build's own containers. So a rootless builder, such as
# rootless BuildKit, builds it too, into the same bytes (see pack-image.sh). RUN
# --mount needs BuildKit or Buildah.

# Debian 13 (trixie), the current stable release: it gets regular security updates
# until 2028 and LTS until 2030. Both pinned to their multi-arch index digests, so
# --platform still selects the arch. Keep DEBIAN_IMAGE and RUST_IMAGE on the same
# Debian release: the agent links against the build image's glibc and libseccomp, and
# the guest supplies them at runtime.
ARG DEBIAN_IMAGE=debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a
ARG RUST_IMAGE=rust:trixie@sha256:a8a5f0a1e5fe7dfe1d352591e4a1c7dd2c08fd70475cae872cf3458ba0df0546

# kata's release pipeline builds the agent with the OPA/regorus policy engine and
# initdata support (kata-deploy-binaries.sh defaults AGENT_POLICY=yes). ateom never
# sends a policy and never uses initdata, so both are dead weight in every boot and
# every snapshot. Without them, and with fat LTO at opt-level=s, kata 4.1.0's stripped
# amd64 agent goes from 30.8 MB to 15.5 MB. Measured with kata 4.0.0 on GKE (amd64,
# counter demo, medians of 7-9 cold bakes), that took ~30 ms off a ~500 ms cold boot
# and ~3.5 MiB off the compressed golden snapshot.
FROM ${RUST_IMAGE} AS agent
RUN apt-get update -qq \
 && apt-get install -y -qq build-essential clang pkg-config protobuf-compiler \
      libseccomp-dev git >/dev/null \
 && rm -rf /var/lib/apt/lists/*
# Required: build-rootfs.sh passes the KATA_VER assemble.sh pins, so the agent always
# matches the kata release the kernel comes from.
ARG KATA_VER
RUN test -n "${KATA_VER}" \
 && git clone --depth 1 --branch "${KATA_VER}" \
      https://github.com/kata-containers/kata-containers /kata
WORKDIR /kata/src/agent
# s by default; z measured the same within noise and is ~0.5 MB smaller, 3 is
# upstream's default and ~2.8 MB larger. rustc is whatever kata's rust-toolchain.toml
# pins; rustup installs it on the first cargo call.
ARG OPT_LEVEL=s
# SECCOMP stays on: it is what applies a container's seccomp profile inside the guest,
# and it measured free (identical stripped size with and without).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    LIBC=gnu SECCOMP=yes AGENT_POLICY=no INIT_DATA=no \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
    CARGO_PROFILE_RELEASE_LTO=true \
    CARGO_PROFILE_RELEASE_OPT_LEVEL="${OPT_LEVEL}" \
    make \
 && install -D -m 0755 /kata/target/*-unknown-linux-gnu/release/kata-agent /out/kata-agent \
 && strip --strip-all /out/kata-agent

# The rootfs stage mounts prune.sh from here rather than the whole build context, so
# editing pack-image.sh or another stage leaves the guest tree cached.
FROM scratch AS scripts
COPY prune.sh /

FROM ${DEBIAN_IMAGE} AS rootfs
# kata-agent is PID 1 (ateom boots init=/usr/bin/kata-agent) and the only thing that
# runs unasked. The actor's processes run in its container rootfs, which the agent
# pivots into from the virtio-fs share, never in this tree. So the guest needs only
# what the agent and ateom call on:
#   iptables     the agent's GetIPTables/SetIPTables RPCs (iptables-save/-restore)
#   libseccomp2  kata-agent links it (SECCOMP=yes)
#   bash, coreutils, grep, iproute2, mount, procps
#                the agent's debug console and the commands ateom runs in it
#                (DebugConsoleDump in cmd/ateom-microvm/run.go: ip, ls, head, grep...)
ARG GUEST_PACKAGES="bash coreutils grep iproute2 iptables libseccomp2 mount procps"
# prune.sh then purges everything those packages do not depend on, which is most of
# trixie-slim: apt, perl and the rest of the Essential set. PRUNE=false keeps it all,
# for comparison; the image roughly doubles.
ARG PRUNE=true
# prune.sh is mounted at /mnt, which the image already has, so the mount leaves
# nothing behind in the tree. apt-get upgrade brings the base image's own packages up
# to date as well, so the guest carries trixie's current security fixes however old
# the DEBIAN_IMAGE pin is, and prune.sh resolves against the installed versions.
# kata-agent names the guest after /etc/hostname at boot, and the base image ships the
# name of the tool that built it there (debuerreotype); without the file the agent
# uses localhost until CreateSandbox sets the sandbox's hostname.
RUN --mount=type=bind,from=scripts,target=/mnt \
    apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ${GUEST_PACKAGES} \
 && if [ "${PRUNE}" = true ]; then bash /mnt/prune.sh ${GUEST_PACKAGES}; fi \
 && rm -rf /var/lib/apt/lists/* /var/cache/* /var/log/* /var/lib/dpkg/status-old \
      /etc/hostname
COPY --from=agent /out/kata-agent /usr/bin/kata-agent
# /sbin/init is the agent too, so a kernel command line without init= boots it as well.
RUN ln -sf ../bin/kata-agent /usr/sbin/init
# Everything the guest runs must start: a missing library or a dangling alternative
# fails the build here rather than a boot or a debug dump later.
RUN kata-agent --version \
 && /sbin/init --version \
 && bash -c 'ip -V && ip addr show lo && iptables-save --version \
      && iptables-restore --version && ip6tables-save --version \
      && ip6tables-restore --version && ps -e && ls -la / && head -n1 /etc/passwd \
      && grep -q "^root:" /etc/passwd && cat /etc/os-release && mount --version \
      && umount --version && sh -c true' >/dev/null

# Needs e2fsprogs 1.47.1 or later, which trixie has: from 1.47.1, mkfs.ext4 -d clamps
# the timestamps it copies to SOURCE_DATE_EPOCH, which pack-image.sh relies on to
# produce the same bytes on every build.
FROM ${DEBIAN_IMAGE} AS image
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends e2fsprogs fdisk \
 && rm -rf /var/lib/apt/lists/*
ARG SOURCE_DATE_EPOCH=1700000000
RUN --mount=type=bind,from=rootfs,target=/rootfs \
    --mount=type=bind,source=pack-image.sh,target=/usr/local/bin/pack-image.sh \
    mkdir -p /out \
 && SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" bash /usr/local/bin/pack-image.sh /rootfs /out/rootfs.img \
 && dpkg-query --admindir=/rootfs/var/lib/dpkg -W -f '${Package}\t${Version}\n' \
      > /out/rootfs-packages.txt

FROM scratch AS export
COPY --from=image /out/ /
COPY --from=agent /out/kata-agent /kata-agent
