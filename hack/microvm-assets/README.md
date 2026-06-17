# Micro-VM runtime assets + counter demo (kind, fetch-not-bake)

The `microvm` runtime (`cmd/ateom-microvm`, kata + cloud-hypervisor) fetches its
toolchain at runtime — nothing kata-specific is baked into the worker image. These helpers
assemble the asset set for your node arch, stage it into the cluster's rustfs S3 bucket, and the
demo manifest points the ActorTemplate at it.

The worker image base is `debian:stable-slim` (provides `nsenter` + `tar` + glibc for the fetched
Rust binaries); see `.ko.yaml`. When `/dev/kvm` is available, `hack/create-kind-cluster.sh`
mounts it into the node and labels the node `ate.dev/sandboxClass=microvm`.

## Steps (run on a KVM-capable Linux host matching the node arch)

1. **Assemble assets for your arch** (builds the patched virtiofsd — needs rust via rustup, NOT
   distro cargo which is too old for the v4 lockfile, plus build deps:
   `apt-get install -y git libcap-ng-dev libseccomp-dev pkg-config && curl -fsSL https://sh.rustup.rs | sh -s -- -y`):
   ```sh
   ARCH=arm64 hack/microvm-assets/assemble.sh
   ```
   Copy the printed sha256 sums into `demos/counter/counter-microvm.yaml.tmpl` `runtime.assets`
   (the committed values are arm64; other arches differ).

2. **Bring up the cluster + control plane:**
   ```sh
   hack/create-kind-cluster.sh        # mounts /dev/kvm, labels node ate.dev/sandboxClass=microvm
   hack/install-ate-kind.sh           # control plane + rustfs (bucket: ate-snapshots)
   ```

3. **Stage assets into rustfs:**
   ```sh
   OUT="$PWD/microvm-assets-arm64" hack/microvm-assets/stage-to-rustfs.sh
   ```

4. **Apply the demo + drive it:**
   ```sh
   BUCKET_NAME=ate-snapshots envsubst < demos/counter/counter-microvm.yaml.tmpl | kubectl apply -f -
   ```
   Create an actor from `counter-microvm`, hit the in-RAM counter to increment it, suspend
   (checkpoint), resume on a different worker pod, and confirm the count continues — proving the
   guest-memory snapshot round-tripped across pods.

## Notes
- `assets` is single-arch (unlike runsc's amd64/arm64): stage assets matching the node arch.
