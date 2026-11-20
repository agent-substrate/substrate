---
title: "Node Storage"
linkTitle: "Node Storage"
weight: 11
docsite: false
description: >
  Where atelet keeps node-local state, and how to give snapshot I/O its own disk.
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

## Nodes That Enforce SELinux

Worker pods mount `/var/lib/ate` from the node. On nodes that enforce SELinux,
that directory is labeled `container_var_lib_t`, which the container runtime's
default type, `container_t`, can neither read nor write. Worker containers
therefore run as the SELinux type `spc_t`. Nodes without SELinux ignore the
setting.
