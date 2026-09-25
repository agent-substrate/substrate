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

Off by default. With `SLIM_ROOTFS=true`, `assemble.sh` rebuilds the downloaded `rootfs.img`
into a much smaller guest image:

- `slim-agent.sh` recompiles `kata-agent` (same `KATA_VER`) without the policy engine and
  initdata support, which ateom never uses, and patches it into the image.
- `slim-rootfs.sh` repacks `rootfs.img` as a journal-less ext4 image holding only that agent
  (PID 1), the tools `DebugConsoleDump` runs, and their shared libraries.

On amd64 with kata 4.1.0 this takes `kata-agent` from 30.8 MB to 15.5 MB and `rootfs.img`
from 256 MiB to 32 MiB.

```sh
SLIM_ROOTFS=true hack/install-microvm-deps.sh --install   # assemble + stage + apply
SLIM_ROOTFS=true ARCH=amd64 hack/microvm-assets/assemble.sh   # assemble only
```

- Needs Docker (`slim-rootfs.sh` runs `--privileged`) on a host of the target arch, and a
  few extra minutes for the agent build.
- The slim guest has no systemd or chrony, so it relies on Cloud Hypervisor >= v53 (the
  default `CH_VER`) to advance the guest clock on restore.
- `assemble.sh` only slims an upstream image whose sha256 is a `kata-image` pin in the
  manifest, because `slim-rootfs.sh` runs that image's binaries as root. The slim
  `rootfs.img` is then the one asset without a committed pin: `assemble.sh` saves the
  upstream sha256 to `$OUT/.upstream-rootfs.sha256`, and `install-microvm-deps.sh` checks
  it again and swaps in the slim image's sha256 when it applies the `SandboxConfig`. Use
  `install-microvm-deps.sh` for this path.
- The asset stamp covers the flag and both scripts, so toggling `SLIM_ROOTFS` or editing
  either script re-assembles a cached `$OUT`.

## Notes
- `assets` is single-arch (unlike runsc's amd64/arm64): stage assets matching the node arch.
