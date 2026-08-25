# Running the microVM runtime locally

The microVM sandbox class (`ateom-microvm`: a Kata guest on Cloud Hypervisor)
needs `/dev/kvm`, which takes some extra setup compared to the default gVisor
path. This guide covers just that delta: getting a KVM-capable Docker
environment — on Linux, or on Apple Silicon macOS via
[Lima](https://lima-vm.io/) — then running the microVM counter demo and
verifying a guest-memory snapshot round-trip.

## Prerequisites

Complete the
[Quickstart (Development)](../../README.md#quickstart-development) in the
README first — it covers the base tooling and the default (gVisor) path this
guide builds on. For background on the runtime, see
[architecture.md](../architecture.md) and
[hack/microvm-assets/README.md](../../hack/microvm-assets/README.md).

## Option A: Linux host with KVM

Works on bare-metal Linux or any cloud VM with nested virtualization enabled
(e.g. GCE N2/N2D instances with nested virt, or equivalent on other clouds).

### 1. Verify KVM

```sh
ls -la /dev/kvm
# Expected: crw-rw---- 1 root kvm 10, 232 ... /dev/kvm

grep -cE '(vmx|svm)' /proc/cpuinfo   # >0 means CPU virt support (x86)
```

`hack/create-kind-cluster.sh` probes for KVM by running a root container with
`--device /dev/kvm`, which works out of the box with a standard (rootful)
Docker install. With **rootless Docker** the container's root is remapped to
your user, so the probe fails with `permission denied` — use rootful Docker
instead, or open up the device with `sudo chmod 666 /dev/kvm`.

### 2. Create the cluster

```sh
./hack/create-kind-cluster.sh
# Look for: "/dev/kvm found: micro-VM (kata + cloud-hypervisor) support will be enabled."

kubectl get nodes --show-labels | grep 'ate.dev/sandboxClass=microvm'
```

### 3. Run the microVM demo

```sh
./hack/run-microvm-demo-kind.sh
```

This is a one-shot bring-up: it deploys the control plane, installs the
cluster-wide microVM deps via `hack/install-microvm-deps.sh` — assembling the
guest runtime assets for your architecture (skipped if already present under
`bin/microvm-assets/`), staging them into the in-cluster rustfs bucket, and
applying the `microvm` `SandboxConfig` — then deploys the demo worker pool +
template.

### 4. Verify

```sh
kubectl get pods -n ate-demo-counter-microvm
kubectl get workerpools,actortemplates -A
```

Expected:

```
NAMESPACE                  NAME                                 DESIRED  READY  AVAILABLE
ate-demo-counter-microvm   workerpool.ate.dev/counter-microvm   1        1      1

NAMESPACE                  NAME                                      READY  AGE
ate-demo-counter-microvm   actortemplate.ate.dev/counter-microvm     True   1m
```

## Option B: Apple Silicon macOS via Lima

Lima can run a Linux VM with **nested virtualization**, exposing `/dev/kvm` to
Docker (and therefore to the kind node) inside the VM. This is a well-trodden
path — much of Substrate's development happens on macOS via limactl.

> [!IMPORTANT]
> Apple's Virtualization framework
> [supports nested virtualization only on M3 and later](https://developer.apple.com/documentation/virtualization/vzgenericplatformconfiguration/isnestedvirtualizationsupported)
> — on earlier Apple Silicon (M1/M2), Lima fails with
> `[hostagent] Starting VZ ... FATA exiting`. Fall back to Option A on a
> Linux host.

### 1. Install Lima and the Docker CLI

```sh
brew install lima docker
```

### 2. Launch Lima with nested virtualization

The arm64 nested-virtualization kernel regression affects kernels 6.19 and
newer, so use a guest image with an older kernel — pinned here to Ubuntu
24.04 LTS:

```sh
limactl start --name=docker-nested template://docker-rootful --nested-virt --set '.images = [
{"location":"https://cloud-images.ubuntu.com/releases/noble/release/ubuntu-24.04-server-cloudimg-arm64.img","arch":"aarch64"},
{"location":"https://cloud-images.ubuntu.com/releases/noble/release/ubuntu-24.04-server-cloudimg-amd64.img","arch":"x86_64"}
]'
```

When prompted to edit the configuration, set at least:

```yaml
cpus: 8
memory: "16GiB"
nestedVirtualization: true
networks:
  - vzNAT: true
mounts:
  - location: "~"
    writable: true
```

The writable home mount lets the kind/ko workflows write into your checkout,
8 CPUs / 16 GiB is a comfortable floor for the control plane plus a microVM
worker, and `vzNAT` gives the VM outbound networking under the vz VM type.

### 3. Assemble the arm64 assets inside the Lima VM

`hack/microvm-assets/assemble.sh` must run on a Linux host of the target
architecture (on arm64 it builds `virtiofsd` from source with cargo against
Linux-only libraries), so run it in the Lima guest, not on macOS:

```sh
limactl shell docker-nested

# Inside the VM — install build deps once:
sudo apt-get update && sudo apt-get install -y git pkg-config libcap-ng-dev libseccomp-dev zstd
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh   # rust via rustup
source "$HOME/.cargo/env"

cd <your substrate checkout>    # visible via the writable home mount
./hack/microvm-assets/assemble.sh
exit
```

The assets land in `bin/microvm-assets/arm64/` in your checkout, which is
shared with the host through the home mount — the demo script will find them
there and skip re-assembling.

### 4. Point the Docker CLI at Lima and bring everything up (on macOS)

```sh
export DOCKER_HOST="unix://${HOME}/.lima/docker-nested/sock/docker.sock"
echo 'export DOCKER_HOST="unix://${HOME}/.lima/docker-nested/sock/docker.sock"' >> ~/.zprofile

cd <your substrate checkout>

./hack/create-kind-cluster.sh
./hack/run-microvm-demo-kind.sh
```

Verify as in Option A, step 4.

## Trying it out

On completion, `run-microvm-demo-kind.sh` prints next steps: create an actor
from the `counter-microvm` template, hit the in-RAM counter, then suspend and
resume it and confirm the count continues — proving the guest-memory snapshot
round-tripped. The flow is the same as the
[README Quickstart](../../README.md#quickstart-development), just with the
microVM template; see the
[counter demo's micro-VM variant](../../demos/counter/README.md#micro-vm-variant)
for background. Note that an `actortemplate` reporting `READY=True` in the
verify step already exercises the runtime end-to-end — the golden snapshot
requires a full guest boot and checkpoint.

## Troubleshooting

| Symptom | Root cause | Fix |
|---|---|---|
| `/dev/kvm: permission denied` during the kind KVM probe | Rootless Docker: the probe container's root is remapped to your user, which can't open the device (`660 root:kvm`) | Use rootful Docker, or `sudo chmod 666 /dev/kvm` before `./hack/create-kind-cluster.sh` |
| `cargo not found` from `assemble.sh` | On arm64, `virtiofsd` is built from source | Install the build deps listed in Option B, Step 3 |
| Lima: `[hostagent] Starting VZ ... FATA exiting` on M1/M2 | Apple's Virtualization framework supports nested virtualization only on M3 and later | Use an M3+ Mac, or a Linux/KVM host (Option A) |
