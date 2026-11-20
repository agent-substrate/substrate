---
title: "Node Storage"
linkTitle: "Node Storage"
weight: 11
docsite: true
description: >
  Where atelet keeps node-local state, how to size the disk for snapshots, how
  to give snapshot I/O its own disk, and what happens when the disk fills.
---

# Node Storage

atelet and the workers keep node-local state under `/var/lib/ate`. Apart from
the snapshot directory below, its layout is an implementation detail, please
don't depend on it.

## Dedicated Snapshot Storage

Snapshot staging lives under `/var/lib/ate/snapshots`. By default this shares
the node's boot disk, so heavy guest disk writes can slow checkpoint and
restore. To avoid that, you can mount a dedicated disk there:

- Mount it before any actors run on the node. A mount over a populated
  directory hides the snapshots already there.
- Use a mount instead of a symlink, since worker pods only see `/var/lib/ate`.
- Keep `/var/lib/ate/actors` on the same filesystem as `/var/lib/ate`.
  atelet checks this at startup.
- The shared snapshot cache stays on the boot disk, so gVisor restores from a
  shared snapshot copy the cached files instead of hard-linking them.

## Disk Space for Suspended Actors

Each suspend writes the actor's snapshot under `/var/lib/ate/snapshots`, and
deletes the actor's previous local snapshot once the new one is written. atelet
keeps the latest one for `--snapshot-detach-delay` (30 minutes by default) after
the suspend, even after the upload makes it durable. An actor that resumes on
the same node keeps it while it runs, until its next suspend replaces it. A node
holds about this much snapshot data:

```
actors with a local snapshot on the node × snapshot size
```

For example, if 60 different actors suspend on a node within 30 minutes and
their snapshots are 1 GiB each, they keep about 60 GiB. A memory snapshot is
about the size of the memory the actor uses, plus its filesystem changes. Local
snapshots are uncompressed, so they take more space than the zstd-compressed
copies in object storage. The `atelet.snapshot.size` metric reports the size of
each snapshot image on disk.

An actor briefly holds up to 3 local snapshots. These are the latest one, one a
suspend is writing, and an older one atelet is still uploading. A detach uploads
the latest snapshot first if it isn't durable yet, and keeps it until the upload
succeeds, so a failing object store fills the disk faster.

Size the disk that holds `/var/lib/ate/snapshots` for this. A dedicated
snapshot disk needs room for the snapshots only. On the boot disk, they share
space with the OS, container images, actor files, and atelet's caches. All of
it has to stay above the kubelet's `evictionHard` thresholds, which
[default](https://github.com/kubernetes/kubernetes/blob/a972b672c7612c4892557f3d6d2a952c741c5d1f/pkg/kubelet/eviction/defaults_linux.go#L22-L28)
to 10% free for `nodefs.available` and 15% free for `imagefs.available`. On
most nodes both are the boot disk.

By default, atelet's snapshot cache and sandbox asset cache are on the boot
disk. They grow without limit unless you set `--snapshot-cache-max-bytes` and
`--sandbox-asset-cache-max-bytes`.

## When the Snapshot Disk Runs Out of Space

The kubelet doesn't watch a dedicated snapshot disk, so when it fills, the node
gets no condition. Its workers stay Ready and keep taking actors. Checkpoints
and restores from object storage both write under `/var/lib/ate/snapshots`, so
they fail, and the actors they were suspending or resuming move to
`ACTOR_STATE_CRASHED`. This goes on until atelet detaches enough snapshots to
free space.

## When the Boot Disk Runs Out of Space

When free space on the boot disk falls below the kubelet's threshold, the node
gets the `DiskPressure` condition. The kubelet evicts pods, including the
workers, and admits only critical pods until the condition clears. No worker
runs on the node in the meantime. atelet runs at `system-cluster-critical`, so
the kubelet neither evicts nor rejects it, and its uploads and detaches go on.

This is a hard eviction, which differs from the graceful shutdown that
[Eviction](../reference/api-guide.md#eviction) describes. The kubelet still
sends `SIGTERM`, but kills the worker 2 seconds later, so its actors don't get
30 minutes to suspend. They move to `ACTOR_STATE_CRASHED` and lose everything
since their last snapshot.

Snapshot files belong to no pod, so evicting pods frees almost none of the
space. The node recovers when atelet detaches the snapshots,
`--snapshot-detach-delay` after each actor's latest suspend. Until then the
condition can clear and return, and the kubelet evicts the replacement workers
each time.

A platform that repairs unhealthy nodes can recreate the node first. For
example, [GKE node auto-repair](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/node-auto-repair)
recreates a node whose boot disk stays out of space for about 30 minutes, the
same as the default `--snapshot-detach-delay`. The new boot disk starts empty,
so any snapshot not yet uploaded is lost.

To recover sooner, lower atelet's `--snapshot-detach-delay`. atelet doesn't
start if it's shorter than `--snapshot-upload-delay`, which defaults to 5
minutes, so lower both to go below that. An actor that resumes after its detach
reads the snapshot from object storage.

To check a node, look at its conditions and eviction events, and at the pods
running on it.

```
kubectl describe node NODE
kubectl get pods -A -o wide --field-selector spec.nodeName=NODE
```

The `atelet.snapshot.undurable.age.max` metric is the age of the oldest local
snapshot on the node that object storage doesn't hold yet. It's 0 when every
local snapshot is uploaded.

Don't delete files under `/var/lib/ate` to free space. Until its upload
finishes, a suspended actor's latest snapshot exists only on that node. atelet
also keeps its record of each actor's snapshots there.
