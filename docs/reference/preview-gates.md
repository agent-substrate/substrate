---
title: "Preview Gates"
linkTitle: "Preview Gates"
weight: 7
description: >
  Enable and opt in to preview features on Agent Substrate servers and clients, and review all available preview gates.
---

# Preview Gates

Preview gates allow new features in a release while keeping them disabled by default. Every feature in Agent Substrate is either in **preview** (gated and off by default) or **GA** (always on). Preview APIs and behavior are outside the compatibility policy and may change or be removed between releases. Gates prevent accidental adoption of unstable features. They do not enforce access control.

## Enabling and Using Preview Features

Using a gated feature in the Control API requires enabling the gate on the server and opting in on the client:

1. **Server**: Pass `--preview=<gates>` to each Substrate binary as a comma-separated list of gate names, or `ALL` to enable every known gate. When deploying with `ate-setup`, set `preview` in the configuration file, `ATE_PREVIEW` in the environment, or `--preview=<gates>` on the command line (see the [Operator Install Guide](../guides/operator-install.md)). `ate-setup` passes the flag to every Substrate container, and `atecontroller` propagates its gates to the `ateworker` containers of each `WorkerPool`. Unknown gate names cause startup to fail.
2. **Client**: Send the `Ate-Preview-Allowed: true` gRPC metadata header on requests to `ate-api-server`. Without this header, `ate-api-server` rejects requests that set gated fields or create gated resources. Pass `--preview` to `kubectl ate` to send the header from the CLI (see the [CLI Reference](kubectl-ate.md#global-flags)).

Only requests that configure a gated feature require the client header, such as creating an `ActorTemplate` with a gated field or calling `CreateActor` from a template with external volumes. Subsequent RPCs on an existing `Actor` or `ActorTemplate`, including `ResumeActor`, `SuspendActor`, `RevertActor`, `DeleteActor`, and `DeleteActorTemplate`, do not require the header. If `--preview` is later turned off on the server, unchanged fields on existing objects are not re-validated on update, and `SuspendActor`, `DeleteActor`, `DeleteActorTemplate`, and `DeleteTag` continue to release workers, detach external volumes, and delete snapshots so existing objects are not stranded.

Component-level gates that expose no Control API fields take effect as soon as `--preview` is enabled on the server. When a feature reaches GA or is removed, `--preview` continues to accept its gate name for compatibility while keeping the feature fixed on or off.

## Available Preview Gates

Gated fields in the [API Reference](api.md) carry a `preview: <Gate>` label and can be shown or hidden with the **Show preview-gated features** toggle.

### `ExternalVolumes`

`ExternalVolumes` enables dynamically provisioned, per-actor CSI volumes declared through `volumes[].external` on `ActorTemplate` and reported in `Actor.status.external_volumes`. Creating an `ActorTemplate` with external volumes or calling `CreateActor` from that template requires `--preview=ExternalVolumes` on the server and preview opt-in on the client. Subsequent operations on the actor do not need the client header, though `ResumeActor` still requires `ExternalVolumes` on the server when the actor has external volumes. See full documentation at [CSI Volumes](../guides/csi-volumes.md) and [Deploying CSI Drivers](../guides/csi-deployment.md).

### `GoldenEgressPolicy`

`GoldenEgressPolicy` enables `golden_egress_policy` on `ActorTemplate`. When set, the template controller creates an `EgressPolicy` for the temporary golden actor before resuming it to capture a golden snapshot. Setting `golden_egress_policy` requires `--preview=GoldenEgressPolicy` on the server and preview opt-in on the client. Actors created from the template or restored from its golden snapshot do not inherit this policy and retain default-deny outbound networking until given their own `EgressPolicy`. Disabling the gate blocks creating new templates with `golden_egress_policy` while preserving existing templates and policies. See full documentation at [Golden Egress Policy](api-guide.md#golden-egress-policy-goldenegresspolicy).

### `IngressCache`

`IngressCache` enables the Envoy dynamic-module filters on `atenet-router` (`--atenet-dataplane=envoy`, the default) that cache `<atespace>/<actor>` to worker IP bindings per Envoy worker thread and route bodiless cache hits around `ext_proc`. Requests with a body, including gRPC calls, continue through `ext_proc` and refresh the cache on response. The filter evicts stale bindings on upstream connection failures or `421 Misdirected Request` responses, and replays cached requests that failed before reaching the actor. `atenet-router` tunes the cache with `--ingress-cache-ttl` and `--ingress-cache-max-cached-actors`. Enabling `--preview=IngressCache` on the server activates the cache with no client header required. See full documentation at [Envoy Substrate Ingress Cache](../../cmd/dataplane/envoy/dynamic-modules/ingress-cache/README.md).

### `PerTemplateSnapshotLocation`

`PerTemplateSnapshotLocation` enables `snapshot_config.storage_location` on `ActorTemplate`, allowing individual templates to store actor and tag snapshots under a dedicated object-storage URI prefix instead of `ate-api-server`'s cluster-wide `--snapshot-location`. Setting `storage_location` requires `--preview=PerTemplateSnapshotLocation` on the server and preview opt-in on the client, and `ate-api-server` ignores `storage_location` on existing templates while the gate is off. Enabling the gate on the server also makes `--snapshot-location` optional at `ate-api-server` startup and during `ate-setup` deployment, though `CreateActorTemplate` fails with `FAILED_PRECONDITION` when neither location is set. See full documentation at [Snapshot Storage Layout](api-guide.md#snapshot-storage-layout) and [Snapshot storage](../guides/operator-install.md#snapshot-storage).
