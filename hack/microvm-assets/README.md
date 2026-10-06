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

`go run ./cmd/ate-setup deploy microvm-deps` (or the `hack/install-microvm-deps.sh --install`
shim) assembles the asset set for your node arch into `bin/microvm-assets/$ARCH`, stages it
into the cluster's object store (`rustfs` on kind, GCS on GKE), and applies the cluster-wide
`microvm` `SandboxConfig`. When `/dev/kvm` is available, `hack/create-kind-cluster.sh` mounts
it into the node; atelet then advertises it as a device, which is what places micro-VM
workers there.

> [!TIP]
> `hack/run-microvm-demo.sh` (and `hack/run-microvm-demo-kind.sh` for kind) automates the
> full bring-up below (control plane, micro-VM assets + `SandboxConfig`, and demo apply)
> without editing committed files.

## Steps (run on a KVM-capable Linux host matching the node arch)

1. **Bring up the cluster + control plane:**
   ```sh
   hack/create-kind-cluster.sh                          # mounts /dev/kvm into the nodes
   hack/install-ate-kind.sh --deploy-ate-system         # control plane + rustfs (bucket: ate-snapshots)
   ```

2. **Assemble and stage the micro-VM assets + `SandboxConfig`:**
   ```sh
   ATE_INSTALL_KIND=true hack/install-microvm-deps.sh --install
   ```

3. **Apply the demo + drive it:**
   ```sh
   hack/install-ate-kind.sh --deploy-demo-counter-microvm
   ```
   Create an actor from `counter-microvm`, hit the in-RAM counter to increment it, suspend
   (checkpoint), resume on a different worker pod, and confirm the count continues — proving the
   guest-memory snapshot round-tripped across pods.

## Notes
- `assets` is single-arch (unlike runsc's amd64/arm64): stage assets matching the node arch.
