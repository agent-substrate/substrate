# Kata Containers worker (preview)

`ateworker-kata` runs each actor in a [Kata Containers](https://katacontainers.io/)
VM. It starts the Kata shim (`containerd-shim-kata-v2`, runtime-rs) and drives
its ttrpc task API itself, as containerd would. There is no containerd, and no
Kubernetes object beyond the worker pod. The hypervisor is the one the Kata
configuration names: QEMU, Cloud Hypervisor, Firecracker or Dragonball. The
tests run all four on a host. Nothing has run in a cluster, and the worker pod
as the controller shapes it has `/dev/kvm` and the tun device only, which
suits Cloud Hypervisor and Dragonball (see "Setting up a pool").

It is a preview. The sandbox class is `kata`, behind the `KataSandbox` gate.
Enable the gate on atelet and atecontroller (`ate-setup --preview=KataSandbox`,
or `ALL`), which passes it to the workers. Without it atelet and the worker
refuse the class. Nothing in the default install, the gVisor worker or the
microVM worker uses it.

## How it works

- The pool's `SandboxConfig` has `sandboxClass: kata`. Its one asset,
  `kata-config`, is the Kata configuration file, which selects the hypervisor.
  Each version of the config can select another one. The worker hands the path
  to the shim as the containerd runtime option `config_path`, since the shim
  accepts `KATA_CONF_FILE` only for its shipped files. It removes
  `KATA_CONF_FILE` from the shim's environment, which the shim reads first.
- The Kata install itself (shim, hypervisors, guest kernel and image) is in the
  worker image, where the configuration file names it. The shipped files name
  `/opt/kata`. `--kata-shim` is the path of the shim. A request cannot name a
  binary: the shim is a flag, and the configuration is an asset atelet staged and
  checked, which only picks among the image's binaries.
- The first container of the actor boots the VM, and the others join it. The
  worker makes each bundle with `internal/ocispec`, shaped for Kata, and the
  shim shares the rootfs and the volumes with the guest.
- The shim, the hypervisor and the helpers run in a cgroup leaf named by the
  actor UID. Its usage is what `GetWorkloadStats` reports, and killing it ends
  everything the VM left (Linux 5.14 or later, for `cgroup.kill`).
- The actor's limits size the VM through the CRI sandbox annotations, which the
  shim reads at boot when its configuration sets `static_sandbox_resource_mgmt`,
  as the shipped ones do. The shim adds its own overhead. Without a limit, or
  without that setting, the VM has the size of `default_vcpus` and
  `default_memory`.
- The VM joins the actor's network namespace and veth, as set up for gVisor.
  The shim plugs the veth into the VM, which gets the actor's address, its
  route to the gateway and its `resolv.conf`.

## Setting up a pool

```yaml
apiVersion: ate.dev/v1
kind: SandboxConfig
metadata:
  name: kata-clh
  annotations:
    sandboxconfig.ate.dev/is-class-default: "true"
spec:
  sandboxClass: kata
  defaultVersion: v1
  versions:
  - name: v1
    assets:
      amd64:
        kata-config:
          url: gs://my-bucket/kata/configuration-clh-runtime-rs.toml
          sha256: <sha256 of the file>
---
apiVersion: ate.dev/v1
kind: WorkerPool
metadata:
  name: kata
  namespace: my-agents
spec:
  replicas: 1
  workerImage: my-registry/ateworker-kata:dev
  sandboxClasses:
  - name: kata
```

Build `workerImage` from the `ateworker-kata` binary and a Kata static install
at `/opt/kata`, on any base image that has the libraries the shim needs. It is
not part of `ate-setup`'s images. Firecracker also needs `mke2fs` in the image.

The controller gives the pod the microVM pod shape: `/dev/kvm` from atelet's
device plugin, `/dev/net/tun`, and the microVM capability set. A Kata
configuration that needs more devices needs them in the pod too. QEMU opens
`/dev/vhost-vsock`, which the pod does not get. Cloud Hypervisor and Dragonball
open only `/dev/kvm`.

An `ActorTemplate` for a kata pool sets
`snapshotConfig.goldenSnapshotConfig.mode: GOLDEN_SNAPSHOT_MODE_DISABLED`,
because there are no snapshots (see below).

The worker pod needs a Linux 5.14 kernel or later on its node (`cgroup.kill`),
and 5.8 or later for the loop devices of Firecracker.

## Limits

- **No snapshots.** The Kata shim has no checkpoint API, so `CheckpointWorkload`
  and `RestoreWorkload` fail with `Unimplemented` for every fidelity. Saving a
  rootfs or volumes without memory is not implemented either. An actor can be
  resumed from nothing (a cold boot), terminated and deleted, but not suspended:
  a suspend fails like any failed checkpoint, which makes the control plane mark
  the actor `CRASHED` and release its worker, so the worker stops the actor's VM
  before it answers.
- **Firecracker has no shared filesystem.** The worker builds an ext4 image of
  each container's rootfs with `mke2fs` and attaches it to a loop device, which
  needs `mke2fs` in the image and access to `/dev/loop*`, which the pod does not
  get. A Run with volumes fails with `FailedPrecondition`. The container's
  writes go to the image and are lost with it.
- **Kata logs go to the systemd journal** of the host (`journalctl -t kata`),
  not to a file. In a pod without a journal they are lost.
- **No usage records.** The worker serves the stats RPCs from the actor's
  cgroup leaf, but does not write `ate.actor.usage_sampled` records.
- In a delegated cgroup, such as the pod's, the shim cannot remove its own
  cgroup and does not exit. After a shutdown the worker waits a second for it,
  then kills the leaf.
- Not tried in a cluster: the pod shape, the capability set, the asset staging
  through atelet, and atunnel's egress and DNS for the VM. The tests cover the
  ingress dial and the guest's route to its gateway.

## Testing

`go test ./cmd/ateworker-kata/...` runs the unit tests, with a fake shim, and
needs nothing else.

`hack/run-kata-test.sh` starts real VMs through the real shim, with the worker's
gRPC interface in front, once for each hypervisor of the Kata install: boot, the
guest kernel, a second container, a volume, ingress, a second process in the
guest, stats, termination, a crash, a stop, and a restart over a leftover VM.
Each ends by looking for processes, mounts, netns, cgroups and loop devices it
left behind. It needs root, `/dev/kvm`, a Kata static install (`KATA_ROOT`,
default `/opt/kata`) and docker or podman, and it is not part of any CI lane.
Where a prerequisite is missing it skips, unless `REQUIRE_KATA=true` or `CI=true`
is set, which makes it fail. See the script for the other variables.
