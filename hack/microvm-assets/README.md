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

Off by default. With `SLIM_ROOTFS=true`, `assemble.sh` builds `rootfs.img` from
`rootfs/Dockerfile` instead of downloading kata's image. The other three assets do not
change.

### Usage

Build on a host of the target arch. The build needs Docker (or another BuildKit CLI),
but no `--privileged`, loop device or mount, so a rootless BuildKit works too.

```sh
# Full demo on kind (after hack/create-kind-cluster.sh): assemble, stage, apply, run
SLIM_ROOTFS=true hack/run-microvm-demo-kind.sh

# Assets + SandboxConfig only (kind or GKE)
SLIM_ROOTFS=true hack/install-microvm-deps.sh --install

# Build and boot-test just the image
ARCH=amd64 KATA_VER=4.1.0 OUT=/tmp/guest hack/microvm-assets/build-rootfs.sh
hack/microvm-assets/test-rootfs.sh /tmp/guest/rootfs.img bin/microvm-assets/amd64/vmlinux
```

- Use `install-microvm-deps.sh` (or a script that calls it) to apply the
  `SandboxConfig`. The built image has no committed sha256 pin, so this script puts the
  built image's sha256 in place of kata's pin. Applying the demo manifest by hand will
  not.
- Settings:
  - `OPT_LEVEL`: the agent's Rust opt-level, `3` by default.
  - `CONTAINER_CLI`: the build CLI, `docker` by default.
  - `BUILDX_BUILDER`: picks a buildx builder.
  - `ALLOW_CROSS_ARCH_BUILD=true`: allows a build under emulation, which takes hours.
- Changing `SLIM_ROOTFS`, `OPT_LEVEL`, `build-rootfs.sh` or any file in `rootfs/`
  re-assembles a cached `$OUT`.
- A local build caches the apt step. To pick up new Debian security fixes, rerun
  `build-rootfs.sh` into the asset dir with `--no-cache-filter rootfs`, then
  `install-microvm-deps.sh --install`. CI always builds from an empty cache.
- The slim guest has no chrony, so it needs Cloud Hypervisor v53 or later (the default
  `CH_VER`) to advance the guest clock after a restore.

### Compared with kata's image

| | kata's image | Slim image |
|---|---|---|
| Base | Ubuntu, built by kata's osbuilder | `debian:trixie-slim` (Debian 13) |
| Init | systemd starts the agent | `kata-agent` is PID 1 |
| Contents | systemd, chrony, iptables, dbus and more | 52 packages: what the agent links plus the tools ateom runs in the debug console |
| `kata-agent` | 30.8 MB, with policy engine and initdata support | 20.5 MB, without them (ateom uses neither) |
| `rootfs.img` | 256 MiB | 74 MiB |

Both use the agent from the same `KATA_VER`. The slim image is aligned to 2 MiB, so it
works on both `virtio-blk` and Cloud Hypervisor's `virtio-pmem` with DAX.

Performance: one GKE test (kata 4.1.0, counter demo, cold boot) measured about 605 ms
to boot and a 26.8 MiB golden snapshot, against about 630 ms and 29.4 MiB for kata's
image. That covers boot only. There is no benchmark yet for the agent's speed after
boot, so the agent keeps upstream's `opt-level=3` and glibc.

### How it is built and tested

- `rootfs/Dockerfile` builds `kata-agent` from source at `KATA_VER` with
  `AGENT_POLICY=no` and `INIT_DATA=no`. It installs the guest packages on
  `debian:trixie-slim`, and `prune.sh` removes every package they do not need.
- `pack-image.sh` packs the tree into a journal-less ext4 partition with
  `mkfs.ext4 -d` and `sfdisk`. The guest mounts it read-only.
- `$OUT/rootfs-packages.txt` lists the guest's packages, and the image keeps its dpkg
  database, so image scanners see the same list.
- `test-rootfs.sh` (Linux only) boots the image under QEMU with the same kernel
  command line as `ateom-microvm`. It then checks through the agent's debug console that
  the guest commands work and the root is read-only.
- The `microvm-slim-smoke` CI workflow builds the image twice to check the bytes match,
  runs the boot test, and runs the counter demo and the actor lifecycle E2E on it.

## Notes
- `assets` is single-arch (unlike runsc's amd64/arm64): stage assets matching the node arch.
