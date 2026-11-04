---
title: "Sandbox Classes"
linkTitle: "Sandbox Classes"
weight: 6
description: >
  How the gvisor and microvm sandbox classes differ in sizing, capabilities, and snapshots.
---

# Sandbox Classes

Each [`WorkerPool`](api-guide.md#1-workerpool-the-physical-capacity) runs one sandbox class, set in `spec.sandboxClasses[].name`: `gvisor` (`ateworker-gvisor`) or `microvm` (`ateworker-microvm`). The pool resolves a cluster-scoped [`SandboxConfig`](api-guide.md#3-sandboxconfig-the-sandbox-itself) for that class.

An [`ActorTemplate`](api-guide.md#2-actortemplate-the-workload-blueprint) selects eligible pools through `workerSelector`, combined with the actor's own `worker_selector`. An actor without a snapshot may start on either class if its selectors match pools of both. Its first snapshot records the class in `sandboxRuntime`. Every later restore, at any fidelity, runs on a worker of that class. A new actor created from the template's golden snapshot or from a `sourceTag` starts with that snapshot, so it runs on the class the snapshot was taken on.

## Summary of Differences

| Area | `gvisor` (`ateworker-gvisor`) | `microvm` (`ateworker-microvm`) |
| :--- | :--- | :--- |
| Isolation | User-space kernel (`runsc`) | KVM guest ([Kata Containers](https://katacontainers.io/) on [Cloud Hypervisor](https://www.cloudhypervisor.org/)) |
| Node requirements | Standard Linux node | Linux node with `/dev/kvm`, requested as the `ate.dev/kvm` device resource |
| Reported CPU count | `max(2, ceil(cpuMillicores / 1000))`, at most the CPUs the sandbox can use | `max(1, ceil(cpuMillicores / 1000))` |
| CPU limit enforcement | Host cgroup v2 `cpu.max` on the sandbox | Guest vCPU count, plus host cgroup v2 `cpu.max` on the VMM and `virtiofsd` |
| Minimum `limits.memoryBytes` | None enforced by worker | `128 MiB` (`134217728` bytes) |
| Per-container limits (`containers[].resources.limits`) | Accepted in the API, ignored at runtime | Enforced via guest cgroups (`cpuMillicores` floored at 10) |
| `VOLUMES` and `ROOTFS` snapshot format | `runsc` filesystem checkpoint bundles (`fs/`, `rootfs/`) | Tar archives of the host directories |

## CPU Count and Throttling

An actor's CPU size comes from `resources.limits.cpuMillicores` on its `ActorTemplate`:

- **gVisor (`gvisor`):** `ateworker-gvisor` sets the sandbox's cgroup v2 `cpu.max` quota from `cpuMillicores` and passes `--cpu-num-from-quota` to `runsc`. `runsc` rounds the quota up to whole CPUs, with a minimum of 2, and never reports more CPUs than the sandbox can use. An actor with `cpuMillicores: 500` or `1000` sees 2 CPUs, and the host kernel throttles it to its quota.
- **MicroVM (`microvm`):** `ateworker-microvm` sets Cloud Hypervisor `BootVcpus` and `MaxVcpus` to `ceil(cpuMillicores / 1000)` with a minimum of 1. It also caps the VMM and `virtiofsd` at `cpuMillicores` with a host cgroup v2 `cpu.max`. An actor with `cpuMillicores: 500` boots with 1 vCPU and gets half a CPU of host time.

## Memory Sizing

An actor's memory size comes from `resources.limits.memoryBytes` on its `ActorTemplate`:

- **gVisor (`gvisor`):** `memoryBytes` sets the sandbox cgroup memory limit and the total memory reported by the Sentry.
- **MicroVM (`microvm`):** `memoryBytes` sets the guest RAM size and must be at least `128 MiB`. The VMM and `virtiofsd` overhead counts against the worker's capacity on top of `memoryBytes`. A `MEMORY` restore reuses the guest RAM and vCPU count recorded in the snapshot.

## Per-Container Resource Limits

Containers may declare optional `containers[].resources.limits` (`cpuMillicores` and `memoryBytes`):

- **gVisor (`gvisor`):** One Sentry process runs every container in the actor ([google/gvisor#190](https://github.com/google/gvisor/issues/190)). The API accepts per-container limits, and gVisor enforces only the actor's `resources.limits`.
- **MicroVM (`microvm`):** The guest enforces each container's limits in its own cgroup. Substrate raises a `cpuMillicores` limit below 10 to 10. A container that exceeds its memory limit is OOM-killed on its own while sibling containers in the actor keep running.

See [Per-container limits](api-guide.md#per-container-limits) in the API Guide.

## System Mounts (`/dev/shm` and `/tmp`)

Each container gets its own `tmpfs` at `/dev/shm` (`64 MiB`, mode `1777`, `noexec`) and `/tmp` (mode `1777`). `/tmp` holds at most half of `limits.memoryBytes` on `gvisor`, and half of usable guest RAM (`MemTotal`) on `microvm`. The `/tmp` mount hides any files the image has in `/tmp`. A volume at either path replaces the `tmpfs`. Files in these `tmpfs` mounts count against the actor's memory limit. They live outside the container's rootfs overlay, so `MEMORY` snapshots keep them and `ROOTFS` snapshots start with them empty.

Neither sandbox class mounts anything at `/run`. As with containerd, `/run` is part of the container's rootfs overlay, so the image's files there stay visible and `ROOTFS` snapshots keep them.

Volumes cannot mount at `/dev`, or at or under `/dev/pts`, `/proc`, or `/sys`.

## Snapshots (`VOLUMES`, `ROOTFS`, and `MEMORY`)

On both classes, a restore prefers a worker that restores all of the snapshot by its `sandboxRuntime`. If none of those workers has room, a durable snapshot restores less of it on another eligible worker of the same class, and cold-boots under that worker's default runtime when it loses memory state. A worker that cannot restore the snapshot is never used. See `sandboxRuntime` in [Snapshot Storage Layout](api-guide.md#snapshot-storage-layout).

How `VOLUMES` and `ROOTFS` state is stored differs by class:

- **gVisor (`gvisor`):** `durableDir` volumes and container rootfs overlays live inside the Sentry's internal filesystems. `runsc` saves them as filesystem checkpoint bundles in the snapshot's `fs/` and `rootfs/` directories. A `VOLUMES` or `ROOTFS` restore uses the worker's default `runsc`, which may be a different version from the one that wrote the bundles. See the [Version Skew Policy](version-skew-policy.md#sandboxconfig).
- **MicroVM (`microvm`):** Container rootfs upper layers and `durableDir` volumes live on the host filesystem and are shared into the guest over `virtio-fs`. `ateworker-microvm` archives them as tar files, one per volume and one per container. The format does not depend on the guest kernel or Cloud Hypervisor version.
