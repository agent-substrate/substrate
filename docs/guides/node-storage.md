---
title: "Node Storage"
linkTitle: "Node Storage"
weight: 11
docsite: false
description: >
  Where atelet keeps node-local state, how to size the disk for snapshots, and
  how to give snapshot I/O its own disk.
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
about the size of the memory the actor uses, plus its filesystem changes.

An actor briefly holds up to 3 local snapshots. These are the latest one, one a
suspend is writing, and an older one atelet is still uploading. A detach uploads
the latest snapshot first if it isn't durable yet, and keeps it until the upload
succeeds, so a failing object store fills the disk faster.

Size the disk that holds `/var/lib/ate/snapshots` for this. On the boot disk, it
has to fit above the kubelet's `nodefs.available` eviction threshold, which is
10% of the disk on GKE. A dedicated snapshot disk keeps this data off the boot
disk, but it needs the same size. The kubelet doesn't watch that disk, so when
it fills, the node gets no condition and atelet can't write new snapshots there.

## When the Boot Disk Runs Out of Space

When free space on the boot disk falls below the kubelet's threshold, the node
gets the `DiskPressure` condition. The kubelet evicts pods, including the
workers, and admits only critical pods until the condition clears. No worker
runs on the node in the meantime. atelet runs at `system-node-critical`, so the
kubelet neither evicts nor rejects it, and its uploads and detaches go on.

Snapshot files belong to no pod, so evicting pods frees almost none of the
space. The node recovers when atelet detaches the snapshots,
`--snapshot-detach-delay` after each actor's latest suspend. Until then the
condition can clear and return, and the kubelet evicts the replacement workers
each time.

To check a node, look at its conditions and eviction events, and at the pods
running on it:

```
kubectl describe node NODE
kubectl get pods -A -o wide --field-selector spec.nodeName=NODE
```

Don't delete files under `/var/lib/ate` to free space. Until its upload
finishes, a suspended actor's latest snapshot exists only on that node. atelet
also keeps its record of each actor's snapshots there.
