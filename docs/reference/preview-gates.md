---
title: "Preview Gates"
linkTitle: "Preview Gates"
weight: 7
description: >
  Explore, understand, and enable preview features on Agent Substrate servers and clients.
---

# Preview Gates

Preview gates allow including new features in a release while keeping them disabled by default. Every feature in Agent Substrate is either in **preview** (gated and off by default) or **GA** (always on). Preview APIs and behavior are outside the compatibility policy and may change or be removed between releases. Preview gates prevent accidental adoption of unstable features. Preview gates do not enforce access control.

## Enabling and Using Preview Features

Using a gated feature in the Control API requires enabling the gate on the server and (sometimes) opting in on the client:

1. **Server**: Pass `--preview=<gates>` to each Substrate binary as a comma-separated list of gate names, or `ALL` to enable every known gate. When deploying with `ate-setup`, set `preview` in the configuration file, `ATE_PREVIEW` in the environment, or `--preview=<gates>` on the command line (see the [Operator Install Guide](../guides/operator-install.md)). `ate-setup` passes the flag to every Substrate container, and `atecontroller` propagates its gates to the `ateworker` containers of each `WorkerPool`. Unknown gate names cause startup to fail.
2. **Client**: Send the `Ate-Preview-Allowed: true` gRPC metadata header on requests to `ate-api-server`. Without this header, `ate-api-server` rejects requests that set gated fields or create gated resources. Pass `--preview` to `kubectl ate` to send the header from the CLI (see the [CLI Reference](kubectl-ate.md#global-flags)).

Only requests that configure a gated feature require the client header, such as creating an `ActorTemplate` with a gated field or calling `CreateActor` from a template with external volumes. Subsequent RPCs on an existing `Actor` or `ActorTemplate`, including `ResumeActor`, `SuspendActor`, `RevertActor`, `DeleteActor`, and `DeleteActorTemplate`, do not require the header. If `--preview` is later turned off on the server, unchanged fields on existing objects are not re-validated on update, and `SuspendActor`, `DeleteActor`, `DeleteActorTemplate`, and `DeleteTag` continue to release workers, detach external volumes, and delete snapshots so existing objects are not stranded.

Component-level gates that expose no Control API fields take effect as soon as `--preview` is enabled on the server. When a feature reaches GA or is removed, `--preview` continues to accept its gate name for compatibility while keeping the feature fixed on (if GA) or off (if removed).

## Available Preview Gates

### `ExternalVolumes`

`ExternalVolumes` integrates Agent Substrate with Container Storage Interface (CSI) drivers to dynamically provision per-actor external volumes and attach or detach them as actors resume and suspend. Enable it when actors need larger or CSI-backed persistent filesystems whose state stays on external storage across suspends and resumes. Enable `--preview=ExternalVolumes` on the server, opt in on the client, register a `CSIDriverConfig` for each CSI driver, and configure volume capacity and `StorageClass` under `volumes[].external` on `ActorTemplate`. See full documentation at [CSI Volumes](../guides/csi-volumes.md) and [Deploying CSI Drivers](../guides/csi-deployment.md).

### `GoldenEgressPolicy`

`GoldenEgressPolicy` lets an `ActorTemplate` define outbound network rules that the template controller applies to the temporary golden actor while building its golden snapshot. Actors later created from the template keep default-deny outbound networking until given their own `EgressPolicy`. Enable it when golden snapshot warmup must reach external endpoints, such as package registries or model stores, without granting that network access to actors at runtime. Enable `--preview=GoldenEgressPolicy` on the server, opt in on the client, and configure rules in `golden_egress_policy` on `ActorTemplate`. See full documentation at [Golden Egress Policy](api-guide.md#golden-egress-policy-goldenegresspolicy).

### `IngressCache`

`IngressCache` caches `<atespace>/<actor>` to worker IP bindings in `atenet-router` (`--atenet-dataplane=envoy`) so bodiless HTTP requests to warm actors bypass `ext_proc`, evicting stale bindings and replaying cached requests that never reached the actor. Enable it to lower ingress latency (on cache hit) and reduce `ResumeActor` load on `ate-api-server`. Enable `--preview=IngressCache` on the server (no client header is required) and optionally tune `--ingress-cache-ttl` and `--ingress-cache-max-cached-actors` on `atenet-router`. See full documentation at [Envoy Substrate Ingress Cache](../../cmd/dataplane/envoy/dynamic-modules/ingress-cache/README.md).

### `PerTemplateSnapshotLocation`

`PerTemplateSnapshotLocation` lets each `ActorTemplate` store actor and tag snapshots under its own object-storage URI prefix instead of `ate-api-server`'s cluster-wide `--snapshot-location`. Enable it to isolate snapshot storage across buckets or prefixes per workload or tenant, or to deploy `ate-api-server` without a cluster-wide default snapshot location. Enable `--preview=PerTemplateSnapshotLocation` on the server, opt in on the client, and set `snapshot_config.storage_location` on `ActorTemplate`. See full documentation at [Snapshot Storage Layout](api-guide.md#snapshot-storage-layout) and [Snapshot storage](../guides/operator-install.md#snapshot-storage).
