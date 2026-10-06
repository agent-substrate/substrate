# Running the microVM runtime locally

The microVM sandbox class (`ateom-microvm`: a Kata guest on Cloud Hypervisor)
needs `/dev/kvm` or `/dev/mshv`, which takes some extra setup compared to the default gVisor
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

## Microsoft Hypervisor (MSHV)

On an AMD64 Microsoft Hypervisor root-partition node, such as an AKS
`KataVmIsolation` node, atelet discovers `/dev/mshv` and advertises
`ate.dev/mshv`. Select that resource in the MicroVM WorkerPool:

```yaml
spec:
  sandboxClass: microvm
  template:
    resources:
      limits:
        ate.dev/mshv: "1"
```

The controller defaults to KVM if neither hypervisor resource is specified.
Explicit hypervisor requests or limits must equal one; specifying both backends
is rejected. MSHV workers select AMD64 nodes. Atelet's device plugin grants the
device to the nonprivileged worker, so no hypervisor hostPath mount or manual
hypervisor node label is required.

Workers use the normal container runtime. Do not put the Substrate worker inside
`kata-vm-isolation` or `kata-v2`: `ateom-microvm` launches its own Cloud Hypervisor
VM and communicates directly with the Kata guest agent. The node's Kata handler
and the guest assets named by Substrate's SandboxConfig are independent.

The AMD64 Cloud Hypervisor v53 release includes both KVM and MSHV. It prefers KVM
when both devices are visible; expose only the selected device to a worker.
MSHV guests use the kernel's default clocksource and retain the guest time-sync
service. Substrate also sets guest wall time through the Kata agent on boot and
after restore, before readiness. Guest instructions resume before the correction
RPC, so this does not guarantee correct time in the first instructions after
resume or advance guest monotonic timers by the suspended interval.

Use a homogeneous hypervisor deployment initially. Actor scheduling does not
automatically distinguish KVM workers from MSHV workers. FULL snapshots record
the backend and reject incompatible restores; that check does not route an actor
to another compatible worker. Recreate existing FULL/golden snapshots when
installing this snapshot format: missing backend metadata is rejected. DATA
restores cold-boot rather than restore VM memory.

The cluster must also provide Substrate's certificate APIs and installation
dependencies; exposing MSHV alone is not a complete AKS deployment integration.

Validate the exact guest kernel/image and host driver combination with cold boot,
FULL checkpoint/restore, cross-worker restore, memory and filesystem continuity,
guest wall time after a long suspension, and repeated restores. A working Kata-v2
pod alone does not validate the Substrate lifecycle.

## Option A: Linux host with KVM

Works on bare-metal Linux or any cloud VM with nested virtualization enabled
(e.g. GCE N4/N4D instances with nested virt, or equivalent on other clouds).

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
```

Once the control plane is up, atelet advertises the device on each KVM-capable
node, which is what places micro-VM workers:

```sh
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.capacity.ate\.dev/kvm}{"\n"}{end}'
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
kubectl get workerpools -A
kubectl ate get actor-templates -a ate-demo-counter-microvm
```

Expected:

```
NAMESPACE                  NAME                                 DESIRED  READY  AVAILABLE
ate-demo-counter-microvm   workerpool.ate.dev/counter-microvm   1        1      1

ATESPACE                   NAME              SANDBOX CLASS           GOLDEN SNAPSHOT                        ERROR   AGE
ate-demo-counter-microvm   counter-microvm   SANDBOX_CLASS_MICROVM   b9f6bd93-3c5a-4b64-9d5e-2f8a1c7d0e42           1m
```

The template is ready once the GOLDEN SNAPSHOT column is non-empty (the
value is a UUID); ERROR means the golden build failed, and `-o yaml` shows
the full error message. An empty GOLDEN SNAPSHOT with no ERROR means the
golden build — a full guest boot plus checkpoint — is still running.

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

Assemble the arm64 assets in the guest because macOS does not ship `zstd` by default:

```sh
limactl shell docker-nested

# Inside the VM — 24.04's git 2.43 can't read a reftable checkout:
sudo add-apt-repository -y ppa:git-core/ppa
sudo apt-get install -y git

cd <your substrate checkout>    # visible via the writable home mount
./hack/microvm-assets/assemble.sh
exit
```

The script downloads about 590 MB and takes a minute or two, leaving ~285 MB in
`bin/microvm-assets/arm64/`. That directory is shared with the host through the
home mount — the demo script will find the assets there and skip re-assembling.

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
for background. Note that an actor template showing a GOLDEN SNAPSHOT in the
verify step already exercises the runtime end-to-end — the golden snapshot
requires a full guest boot and checkpoint.

## Troubleshooting

| Symptom | Root cause | Fix |
|---|---|---|
| `/dev/kvm: permission denied` during the kind KVM probe | Rootless Docker: the probe container's root is remapped to your user, which can't open the device (`660 root:kvm`) | Use rootful Docker, or `sudo chmod 666 /dev/kvm` before `./hack/create-kind-cluster.sh` |
| Lima: `[hostagent] Starting VZ ... FATA exiting` on M1/M2 | Apple's Virtualization framework supports nested virtualization only on M3 and later | Use an M3+ Mac, or a Linux/KVM host (Option A) |
