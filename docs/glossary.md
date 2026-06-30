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

## Records (dynamic state, in the control-plane store)

These are not Kubernetes objects; they live in the control-plane database
because they change too frequently for etcd.

- **Actor**: a single instance derived from an `ActorTemplate`, identified by a
  DNS-1123 actor ID. It is the unit that is suspended and resumed, and it moves
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

- **Suspend**: hibernate a running Actor by checkpointing it to a snapshot and
  freeing its Worker. The requested snapshots are uploaded to external storage.

- **Pause**: a short-term checkpoint of a running Actor. Snapshot files remain
  on the node VM, and the following Resume is prioritized onto the node VM
  where the snapshots are persisted.

- **Resume**: activate a suspended/paused Actor by restoring it onto a Worker. The
  common path restores from a snapshot rather than cold-booting.

## Volumes

- **DurableDir volume**: a directory mounted into one or more containers
  whose contents are preserved by the [`Data` snapshot scope](#snapshots)
  and therefore survive across Suspend/Resume independently of process
  memory or other rootfs writes. A single `ActorTemplate` may declare
  multiple `DurableDir` volumes, and the same volume may be mounted into
  multiple containers (potentially at different paths). This is the
  per-Actor application-data surface.

## Snapshots

- **Snapshot scope**: what an `ActorTemplate`'s `SnapshotsConfig` includes
  in a given snapshot. Two scopes exist today:
  - **`Full`**: process memory plus the rootfs delta on top of the OCI
    image (which also includes any attached `DurableDir` volumes,
    since they live inside rootfs). Used to capture everything needed
    to resume hot.
  - **`Data`**: only the contents of attached volumes that support
    snapshots — currently `DurableDir` volumes. Process memory and the
    rest of rootfs are discarded; on Resume the Actor cold-boots from
    the OCI image with `DurableDir` contents restored. Used to persist
    application data cheaply without the cost of a full memory image.

  Configured per-trigger via `onPause` and `onCommit`: `onPause` selects
  what is captured during a [Pause](#lifecycle) (kept on the node), and
  `onCommit` selects what is captured during a [Suspend](#lifecycle)
  (uploaded to snapshot storage). `onCommit` must be a subset of `onPause`.

- **Golden Snapshot**: the initial checkpoint captured once, when an
  `ActorTemplate` is created, from a temporary "golden" boot of the workload.
  By default an Actor of that template is first restored from this shared
  snapshot.

- **Last Snapshot**: the most recent per-Actor snapshot, written on Suspend and
  used to restore that specific Actor on the next Resume.

- **Snapshot storage**: the object store (GCS or S3) where snapshots are
  persisted so Actor state is durable and portable across the cluster.

## Networking

- **Uniform DNS Mesh**: every Actor is reachable at a uniform address,
  `<actor-id>.<atespace>.actors.resources.substrate.ate.dev`, resolved by atenet. Traffic to
  that name is routed (and the Actor resumed if needed) automatically.
