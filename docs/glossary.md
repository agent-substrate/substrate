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
  freeing its Worker.

- **Resume**: activate a suspended Actor by restoring it onto a Worker. The
  common path restores from a snapshot rather than cold-booting.

## Snapshots

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
  `<actor-id>.actors.resources.substrate.ate.dev`, resolved by atenet. Traffic to
  that name is routed (and the Actor resumed if needed) automatically.
