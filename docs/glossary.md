# Agent Substrate Glossary

This document defines the core terms used across Agent Substrate.

For how the pieces fit together, see the [Architecture](architecture.md) and
[API Guide](api-guide.md).

## Resources (declarative, Kubernetes CRDs)

- **ActorTemplate**: the definition of an actor "class": the container image(s)
  and snapshot configuration. Creating an `ActorTemplate` triggers creation of
  a [Golden Snapshot](#snapshots). It is treated as immutable: you create a new
  template for a new version rather than editing an existing one. It is
  analogous to a Pod template, but for a checkpointable workload.

- **WorkerPool**: declares warm compute capacity, a fleet of pre-started worker
  pods. It is reconciled into a Kubernetes `Deployment` by the
  [atecontroller](#components).

- **SandboxConfig**: a cluster-scoped resource holding the sandbox binaries for
  one runtime family (the gVisor `runsc` binary, or a micro-VM
  kernel/firmware/config). A `WorkerPool` resolves its binaries from the config
  it names, or from the cluster default for its class, so one config pins the
  runtime version for many templates.

## Records (dynamic state, in the control-plane store)

These are not Kubernetes objects; they live in the control-plane database
because they change too frequently for etcd.

- **Atespace**: the isolation boundary an Actor belongs to, and the first half
  of its identity: an Actor is addressed by `(atespace, name)`, so the same
  name can exist in two atespaces. Atespaces are global-scoped, not Kubernetes
  namespaces, and are distinct from the namespace an `ActorTemplate` lives in.
  One must exist before any Actor can be created in it, and it can only be
  deleted once empty.

- **Actor**: a single instance derived from an `ActorTemplate`, identified by a
  DNS-1123 name. It is the unit that is suspended and resumed, and it moves
  between workers over its lifetime. An Actor record tracks its lifecycle
  status and snapshot references.

- **Worker**: a record representing one worker pod in a `WorkerPool`. A Worker
  hosts at most one Actor at a time; many Actors are multiplexed across a pool
  over time.

## Components

- **ate-api-server** (binary `ateapi`): the control plane. It owns the Actor
  lifecycle, schedules Actors onto Workers, and coordinates their snapshots,
  all backed by the state store. The `kubectl-ate` CLI talks to it.

- **atecontroller**: the Kubernetes controller that reconciles the CRDs (for
  example, it turns a `WorkerPool` into a `Deployment`).

- **atelet**: the node-level supervisor, run as a DaemonSet. It pulls images,
  assembles OCI bundles, drives the sandbox lifecycle on the node via ateom,
  and streams snapshots to and from snapshot storage.

- **ateom**: the coordinator that runs inside each worker pod and drives the
  sandbox runtime on behalf of atelet. This decouples the physical pod
  lifecycle from the sandboxed agent process.

- **atenet**: the networking stack. It provides a DNS server for actor
  resolution and a router that resumes suspended Actors on demand and routes
  traffic to the right worker pod.

- **podcertcontroller**: issues short-lived pod certificates that components
  use as their TLS identity to authenticate connections to one another
  (mutual TLS).

- **kubectl-ate**: a `kubectl` plugin CLI for managing the Actor lifecycle and
  listing Workers.

## Lifecycle

- **Suspend**: hibernate a running or paused Actor into a durable snapshot in
  external storage. A running Actor is checkpointed on its Worker (which is
  then freed); a paused Actor's node-local snapshot is uploaded — narrowed to
  the commit scope when the pause captured more — ending its node pinning.

- **Pause**: a short-term checkpoint of a running Actor. Snapshot files remain
  on the node VM, and the following Resume is prioritized onto the node VM
  where the snapshots are persisted.

- **Resume**: activate a suspended/paused Actor by restoring it onto a Worker. The
  common path restores from a snapshot rather than cold-booting.

## Volumes

- **DurableDir volume**: a directory mounted into one or more containers
  whose contents are preserved by the [`Data` snapshot scope](#snapshots)
  and therefore survive across Suspend/Resume independently of process
  memory or other rootfs writes. A volume may be mounted into multiple
  containers, potentially at different paths. This is the per-Actor
  application-data surface.

  How many an `ActorTemplate` may declare depends on its `sandboxClass`:
  a `microvm` template may declare several (they are subdirectories of one
  virtio-fs share, so they cost nothing extra per volume), while a `gvisor`
  template is limited to one until gVisor can accept more than a single
  durable mount.

## Snapshots

- **Snapshot scope**: what an `ActorTemplate`'s `SnapshotsConfig` includes
  in a given snapshot. Two scopes exist today:
  - **`Full`**: process memory plus the rootfs delta on top of the OCI
    image, and any attached `DurableDir` volumes. Used to capture
    everything needed to resume hot.
  - **`Data`**: only the contents of attached volumes that support
    snapshots — currently `DurableDir` volumes. Process memory and the
    rest of rootfs are discarded. Used to persist application data
    cheaply without the cost of a full memory image. How the Actor comes
    back on Resume is governed by the
    [Resume sources](#snapshots) (`onResume.fromData`): cold-boot by
    default, or combined with the [Golden Snapshot](#snapshots).

  Scopes describe only what a snapshot *captures*. They are configured
  per-trigger via `onPause` and `onCommit`: `onPause` selects what is
  captured during a [Pause](#lifecycle) (kept on the node), and
  `onCommit` selects what is captured during a [Suspend](#lifecycle)
  (uploaded to snapshot storage). `onCommit` must be a subset of
  `onPause`.

- **Resume sources**: an `ActorTemplate`'s `onResume` block selects, per
  snapshot situation, what supplies the guest state on Resume. Each field
  names what is being resumed *from*; the value names the boot source.
  `onResume.fromData` applies when the Resume uses a `Data`-scope snapshot
  (from either trigger):
  - **`ColdBoot`** (default): start the containers afresh from the OCI image
    with the `DurableDir` contents restored.
  - **`Golden`**: restore the template's [Golden Snapshot](#snapshots)
    (process memory + rootfs delta) and serve the Actor's `DurableDir` data
    to it, so the Actor resumes with the golden's warm state over its own
    data. Currently `microvm`-only.

  A still-valid `Full` snapshot always restores from its own content and is
  not configurable here.

- **Golden Snapshot**: the initial checkpoint captured once, when an
  `ActorTemplate` is created, from a temporary "golden" boot of the workload.
  By default an Actor of that template is first restored from this shared
  snapshot. It is always a `Full` capture, and under `onResume.fromData:
  Golden` (see [Resume sources](#snapshots)) it also supplies the guest state
  that an Actor's `Data` snapshot is combined with on Resume.

- **Last Snapshot**: the most recent per-Actor snapshot, written on Suspend and
  used to restore that specific Actor on the next Resume.

- **Snapshot storage**: the object store (GCS or S3) where snapshots are
  persisted so Actor state is durable and portable across the cluster.

## Networking

- **Uniform DNS Mesh**: every Actor is reachable at a uniform address,
  `<actor-name>.<atespace>.actors.resources.substrate.ate.dev`, resolved by atenet. Traffic to
  that name is routed (and the Actor resumed if needed) automatically.
