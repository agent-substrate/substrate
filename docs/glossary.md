# Agent Substrate Glossary

This document defines the core terms used across Agent Substrate.

For how the pieces fit together, see the [Architecture](architecture.md) and
[API Guide](api-guide.md).

## Resources (declarative)

- **ActorTemplate** (ate API resource): the definition of an actor "class":
  the container image(s) and snapshot configuration. Creating an
  `ActorTemplate` triggers creation of a [Golden Snapshot](#snapshots) by
  default. Set `snapshotConfig.goldenSnapshotConfig.mode` to
  `GOLDEN_SNAPSHOT_MODE_DISABLED` to skip it. The template is
  treated as immutable: you create a new template for a new version rather
  than editing an existing one. It is analogous to a Pod template, but for a
  checkpointable workload. ActorTemplates are created and managed through the
  substrate gRPC API (e.g. `kubectl ate create actor-template`) and stored in
  the control-plane database; they are not Kubernetes objects.

- **WorkerPool** (Kubernetes CRD): declares warm compute capacity, a fleet of
  pre-started worker pods. It is reconciled into a Kubernetes `Deployment` by
  the [atecontroller](#components).

- **SandboxConfig** (Kubernetes CRD): a cluster-scoped resource holding the
  sandbox binaries for one runtime family (the gVisor `runsc` binary, or a
  micro-VM kernel/firmware/config). An actor resolves its sandbox at first cold
  boot from the config its `ActorTemplate` names (naming one is currently
  required; per-class cluster defaults are planned), so one config pins the
  runtime version for many templates.

## Records (dynamic state, in the control-plane store)

These live in the control-plane database because they change too frequently
for etcd.

- **Atespace**: the isolation boundary an Actor belongs to, and the first half
  of its identity: an Actor is addressed by `(atespace, name)`, so the same
  name can exist in two atespaces. Atespaces are global-scoped, not Kubernetes
  namespaces — an `ActorTemplate` also lives in an atespace, while
  `WorkerPool`s live in Kubernetes namespaces. One must exist before any
  Actor can be created in it, and it can only be deleted once empty.

- **Actor**: a single instance derived from an `ActorTemplate`, identified by a
  DNS-1123 name. It is the unit that is suspended and resumed, and it moves
  between workers over its lifetime. An Actor record tracks its lifecycle
  status and snapshot references.

- **Worker**: a record representing one worker pod in a `WorkerPool`. A Worker
  hosts several Actors at once, each in its own sandbox, up to its actor limit
  and its compute capacity; many more are multiplexed across a pool over time.

## Components

- **ate-api-server** (binary `ateapi`): the control plane. It owns the Actor
  lifecycle, schedules Actors onto Workers, and coordinates their snapshots,
  all backed by the state store. The `kubectl-ate` CLI talks to it.

- **atecontroller**: the Kubernetes controller that reconciles the CRDs (for
  example, it turns a `WorkerPool` into a `Deployment`).

- **atelet**: the node-level supervisor, run as a DaemonSet. It pulls images,
  assembles OCI bundles, drives the sandbox lifecycle on the node via ateworker,
  and streams snapshots to and from snapshot storage.

- **ateworker**: the coordinator that runs inside each worker pod and drives the
  sandbox runtime on behalf of atelet. This decouples the physical pod
  lifecycle from the sandboxed agent process. It embeds a networking service
  called `atunnel` that handles network traffic for the sandboxed Actor.
  ateworker is Substrate's worker implementation, one binary per sandbox class
  (`ateworker-gvisor`, `ateworker-microvm`).

- **atenet**: the networking stack. Its router resumes suspended Actors on
  demand and routes traffic to the right worker pod.

- **podcertcontroller**: issues short-lived pod certificates that components
  use as their TLS identity to authenticate connections to one another
  (mutual TLS).

- **kubectl-ate**: a `kubectl` plugin CLI for managing the Actor lifecycle and
  listing Workers.

## Lifecycle

- **Suspend**: hibernate a running Actor into a snapshot. The Actor is
  checkpointed on its Worker, which is then freed. A synchronous suspend
  uploads the snapshot to external storage before it returns; an asynchronous
  one is a Pause. Suspending an Actor that is already suspended or paused
  returns it as it is.

- **Pause**: a short-term checkpoint of a running Actor, taken by an
  asynchronous suspend. Snapshot files remain on the node VM, and the following
  Resume is prioritized onto the node VM where the snapshots are persisted.
  atelet uploads the snapshot to external storage in the background, and ends
  the node pinning once it deletes the local files.

- **Resume**: activate a suspended/paused Actor by restoring it onto a Worker. The
  common path restores from a snapshot rather than cold-booting.

## Volumes

- **DurableDir volume**: a directory mounted into one or more containers
  whose contents are preserved by the [`VOLUMES` snapshot fidelity](#snapshots)
  and therefore survive across Suspend/Resume independently of process
  memory or other rootfs writes. A volume may be mounted into multiple
  containers, potentially at different paths. This is the per-Actor
  application-data surface.

  An `ActorTemplate` may declare several, on either sandbox class.

## Snapshots

- **Snapshot fidelity**: the state layers a snapshot holds, ordered lowest
  to highest; each level includes the ones below it.
  - **`VOLUMES`**: only the contents of attached volumes that support
    snapshots — currently `DurableDir` volumes. Process memory and rootfs
    changes are discarded. Used to persist application data cheaply
    without the cost of a memory image. On Resume the containers start
    afresh from the OCI image with the `DurableDir` contents restored.
  - **`ROOTFS`**: volumes plus the root filesystem changes made since boot.
    Process memory is discarded; on Resume the containers cold-boot from the
    OCI image with their rootfs writes back in place.
  - **`MEMORY`**: volumes, rootfs changes, and process memory. Used to
    capture everything needed to resume hot.

  Fidelity describes only what a snapshot *holds*. A template sets one
  level, `preferredFidelity`, the ceiling for every snapshot of its actors:
  the node-local checkpoint a [Pause](#lifecycle) keeps on the node and the
  snapshot a [Suspend](#lifecycle) uploads to snapshot storage. It is best
  effort: the system never packages more than that level, and may package
  less when a layer cannot be captured or is too expensive to capture at
  the time. A process resumed in place keeps its memory regardless.

- **Golden Snapshot**: the initial checkpoint normally captured once, when an
  `ActorTemplate` is created, from a temporary "golden" boot of the workload.
  Templates with golden snapshot creation disabled do not create one. By
  default an Actor of that template is first restored from this shared
  snapshot when it is available; otherwise it cold-boots. It is always a
  `MEMORY` capture.

- **Last Snapshot**: the most recent per-Actor snapshot, written on Suspend and
  used to restore that specific Actor on the next Resume.

- **Snapshot storage**: the object store (GCS or S3) where snapshots are
  persisted so Actor state is durable and portable across the cluster.

## Networking

- **Actor routing header**: a higher-order system sends traffic to the
  Substrate router with `ate-target-actor: <atespace>/<actor>`. The router
  uses this header to locate and resume the Actor.
