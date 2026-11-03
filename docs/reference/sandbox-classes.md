---
title: "Sandbox Classes"
linkTitle: "Sandbox Classes"
weight: 6
description: >
  How the gvisor and microvm sandbox classes differ in sizing, capabilities, mounts, and snapshots.
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
| `NET_RAW` capability | Stripped (`runsc` default `--net-raw=false`) | Granted when listed in `securityContext.capabilities.add` |
| `/dev/shm` | `0755` directory on the `runsc` `dev` filesystem | `64 MiB` `tmpfs` (`mode=1777`) |
| `/run` | Part of the container rootfs overlay | `64 MiB` `tmpfs` (`mode=0755`) |
| `/tmp` | `tmpfs` when the image's `/tmp` is missing or empty, otherwise rootfs overlay | Part of the container rootfs overlay |
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

## Capabilities (`NET_RAW`)

Both sandbox classes start from the same default capability set (`AUDIT_WRITE`, `KILL`, `NET_BIND_SERVICE`), allow binding ports below 1024 without `NET_BIND_SERVICE`, and allow `setuid` binaries.

They differ on `NET_RAW`:

- **gVisor (`gvisor`):** `ateworker-gvisor` does not enable `runsc --net-raw`, which defaults to `false`. `runsc` then removes `CAP_NET_RAW` from the container and disables raw sockets (`AF_PACKET` and `SOCK_RAW`), even when `securityContext.capabilities.add` includes `NET_RAW`.
- **MicroVM (`microvm`):** Adding `NET_RAW` in `securityContext.capabilities.add` grants `CAP_NET_RAW` inside the guest container.

See [Container Capabilities](api-guide.md#container-capabilities-securitycontextcapabilities) in the API Guide.

## System Mounts (`/dev/shm`, `/run`, `/tmp`)

- **gVisor (`gvisor`):** `runsc` mounts its internal `dev` filesystem at `/dev`, where `/dev/shm` is a `0755` directory, so only root can create files there. `/run` lives on the container's rootfs overlay. `runsc` mounts a `tmpfs` (`mode=01777`) at `/tmp` when the image's `/tmp` is missing or empty and no volume is mounted there. Files in `/dev/shm` and in that `/tmp` `tmpfs` live outside the rootfs overlay, so only `MEMORY` snapshots keep them.
- **MicroVM (`microvm`):** `ateworker-microvm` mounts a `64 MiB` `tmpfs` at `/dev/shm` (`mode=1777`, `nosuid`, `nodev`, `noexec`) and at `/run` (`mode=0755`, `nosuid`, `strictatime`). `/tmp` lives on the container's host-backed rootfs overlay, so `ROOTFS` snapshots keep its files. Files in `/dev/shm` and `/run` live in guest memory, so only `MEMORY` snapshots keep them.

## Snapshots (`VOLUMES`, `ROOTFS`, and `MEMORY`)

On both classes, a `MEMORY` restore prefers a worker that offers a runtime compatible with the snapshot's `sandboxRuntime`. If none of those workers has room, a durable snapshot restores its `ROOTFS` state on any eligible worker of the same class and cold-boots under that worker's default runtime. See `sandboxRuntime` in [Snapshot Storage Layout](api-guide.md#snapshot-storage-layout).

How `VOLUMES` and `ROOTFS` state is stored differs by class:

- **gVisor (`gvisor`):** `durableDir` volumes and container rootfs overlays live inside the Sentry's internal filesystems. `runsc` saves them as filesystem checkpoint bundles in the snapshot's `fs/` and `rootfs/` directories. A `VOLUMES` or `ROOTFS` restore uses the worker's default `runsc`, which may be a different version from the one that wrote the bundles. See the [Version Skew Policy](version-skew-policy.md#sandboxconfig).
- **MicroVM (`microvm`):** Container rootfs upper layers and `durableDir` volumes live on the host filesystem and are shared into the guest over `virtio-fs`. `ateworker-microvm` archives them as tar files, one per volume and one per container. The format does not depend on the guest kernel or Cloud Hypervisor version.
