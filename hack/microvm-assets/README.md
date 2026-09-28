# Micro-VM runtime assets + counter demo (kind, fetch-not-bake)

The `microvm` runtime (`cmd/ateom-microvm`, kata + cloud-hypervisor) fetches its
toolchain at runtime — nothing kata-specific is baked into the worker image. ateom drives
the kata-agent directly (no kata shim, no containerd). Each actor container's rootfs is an
overlay of a read-only lower (the OCI image, served into the guest over virtio-fs by
`virtiofsd`) and a writable upper on a guest tmpfs, so `virtiofsd` is part of the asset
set. The asset set is four files:

- `cloud-hypervisor` — the VMM binary (fetched from its release)
- `virtiofsd` — the virtio-fs daemon serving the RO lower (from kata-static)
- `vmlinux` — the guest kernel (from kata-static)
- `rootfs.img` — the guest rootfs image (from kata-static)

These helpers assemble the asset set for your node arch, stage it into the cluster's rustfs
S3 bucket, and the demo manifest's `SandboxConfig` points at it. When `/dev/kvm` is
available, `hack/create-kind-cluster.sh` mounts it into the node; atelet then advertises
it as a device, which is what places micro-VM workers there.

> [!TIP]
> `hack/run-microvm-demo.sh` automates the full bring-up below (assets, control plane,
> demo apply) for kind OR GKE without editing committed files. The steps here are the
> manual equivalent.

## Steps (run on a KVM-capable Linux host matching the node arch)

1. **Assemble assets for your arch:**
   ```sh
   ARCH=arm64 hack/microvm-assets/assemble.sh
   ```

2. **Bring up the cluster + control plane:**
   ```sh
   hack/create-kind-cluster.sh        # mounts /dev/kvm into the nodes
   hack/install-ate-kind.sh           # control plane + rustfs (bucket: ate-snapshots)
   ```

3. **Stage assets into rustfs:**
   ```sh
   OUT="$PWD/microvm-assets-arm64" hack/microvm-assets/stage-to-rustfs.sh
   ```

4. **Apply the demo + drive it:**
   ```sh
   BUCKET_NAME=ate-snapshots SUBSTRATE_VERSION="$(git describe --tags --always --dirty)" envsubst < demos/counter/counter-microvm.yaml.tmpl | kubectl apply -f -   # the pool pins workers to nodes labeled with this version
   ```
   Create an actor from `counter-microvm`, hit the in-RAM counter to increment it, suspend
   (checkpoint), resume on a different worker pod, and confirm the count continues — proving the
   guest-memory snapshot round-tripped across pods.

## Optional: slim guest image (`SLIM_ROOTFS=true`)

Off by default. With `SLIM_ROOTFS=true`, `assemble.sh` builds `rootfs.img` with
`build-rootfs.sh` instead of taking kata's. The build is `rootfs/Dockerfile`:

- `debian:trixie-slim` (Debian 13, the current stable release) plus the packages the
  guest runs: `iptables` for the agent's iptables RPCs, and the tools `DebugConsoleDump`
  runs. `prune.sh` then purges every package those do not depend on, which is most of
  the base image: apt, perl and the rest of Debian's Essential set.
- `kata-agent`, recompiled at the same `KATA_VER` without the policy engine and initdata
  support, which ateom never uses. It is the guest's PID 1.
- `pack-image.sh` packs the tree into a journal-less ext4 partition in userspace, with
  `mkfs.ext4 -d` and `sfdisk`.

On amd64 with kata 4.1.0 this takes `kata-agent` from 30.8 MB to 15.5 MB and `rootfs.img`
from 256 MiB to 70 MiB, which holds 59 Debian packages.

```sh
SLIM_ROOTFS=true hack/install-microvm-deps.sh --install   # assemble + stage + apply
SLIM_ROOTFS=true ARCH=amd64 hack/microvm-assets/assemble.sh   # assemble only
ARCH=amd64 KATA_VER=4.1.0 OUT=/tmp/guest hack/microvm-assets/build-rootfs.sh   # image only
hack/microvm-assets/test-rootfs.sh /tmp/guest/rootfs.img bin/microvm-assets/amd64/vmlinux   # boot test
```

- It is an ordinary container build, with no `--privileged`, loop device or mount, so it
  also runs on a rootless BuildKit, and produces the same bytes there. `CONTAINER_CLI`
  selects the CLI (default `docker`), and extra `build-rootfs.sh` arguments go to the
  build, e.g. `--builder`. `assemble.sh` passes none, so there set `BUILDX_BUILDER` to
  pick a buildx builder. Build on a host of the target arch: under emulation the agent
  build takes hours.
- apt installs trixie's current packages whenever the `rootfs` stage runs, which picks
  up Debian's security fixes. CI runs it on every build, from an empty cache, but a
  local builder keeps the stage cached until its inputs change. To refresh a local
  image, rerun `build-rootfs.sh` into the asset dir with `--no-cache-filter rootfs`;
  `install-microvm-deps.sh` then stages the new image. `$OUT/rootfs-packages.txt` lists
  what went in, and the image keeps its dpkg database, so scanners that read
  `/var/lib/dpkg/status` see the same list. The same packages always pack into the same
  bytes, whichever builder runs the build.
- `test-rootfs.sh` boots an image under QEMU (in a container; KVM if available) with
  ateom's kernel command line, and checks in the agent's debug console that the
  commands the agent and ateom run work, the root is read-only and the hostname is not
  left over from the build. amd64 only. The `guest-image` job of
  `.github/workflows/microvm-slim-smoke.yaml` builds the image on a rootless BuildKit,
  rebuilds it to compare the bytes, and runs this test.
- The slim guest has no systemd or chrony, so it relies on Cloud Hypervisor >= v53 (the
  default `CH_VER`) to advance the guest clock on restore.
- The built `rootfs.img` is the one asset without a committed pin. `assemble.sh` checks
  that kata's image for `KATA_VER` is the `kata-image` pin and saves its sha256 to
  `$OUT/.upstream-rootfs.sha256`, and `install-microvm-deps.sh` checks it again and swaps
  in the built image's sha256 when it applies the `SandboxConfig`. Use
  `install-microvm-deps.sh` for this path.
- The asset stamp covers the flag, `build-rootfs.sh` and everything in `rootfs/`, so
  toggling `SLIM_ROOTFS` or editing any of them re-assembles a cached `$OUT`.

## Notes
- `assets` is single-arch (unlike runsc's amd64/arm64): stage assets matching the node arch.
