# Migrating substrate-plane resources from CRDs to ateapi (issue #368)

Companion to [ateapi-resource-api.md](ateapi-resource-api.md), which defines
the target API. This doc covers how to move the current Kubernetes-backed
implementation to the Worker-centric model.

## Target shape

Substrate core should manage scheduling intent and observed worker capacity:

- `ActorTemplate` is a global control-plane resource in ateapi.
- `Actor` is an atespace-scoped tenant/control-plane resource in ateapi.
- `Worker` is the substrate scheduling primitive and a managed Admin resource.
- Kubernetes may still have a WorkerPool CRD, Helm value, Deployment wrapper,
  or autoscaler input, but that belongs to the Kubernetes worker provider.

The split mirrors Kubernetes Nodes and cloud-provider NodePools: core
substrate schedules against Workers; providers decide how to create and manage
many Workers.

## Current CRD responsibilities

The existing Kubernetes CRDs combine several responsibilities that need to be
split apart:

| Current concept | Current responsibility | Target owner |
|---|---|---|
| `ActorTemplate` CRD | workload template and scheduling constraints | ateapi `ActorTemplate` |
| `WorkerPool` CRD | desired worker capacity, Kubernetes placement, common labels | Kubernetes worker provider |
| Worker pod | actual schedulable capacity | ateapi `Worker` projection/resource |

The key change is that substrate scheduling no longer follows
`ActorTemplate -> WorkerPool -> Pod`. It follows
`ActorTemplate/Actor -> Worker`.

## Kubernetes provider flow

The Kubernetes implementation can start with a simple pod watcher:

1. Some Kubernetes mechanism creates worker Pods. This can be a Deployment,
   Helm-managed workload, an existing WorkerPool CRD, or a future autoscaler.
2. Worker Pods opt into substrate by setting
   `pod.ate.dev/is-worker: "true"` from
   [label-registry.md](label-registry.md).
3. A worker sync goroutine watches those Pods.
4. For each matching Pod, the syncer creates or updates an ateapi `Worker`.
5. When a matching Pod disappears, the syncer deletes or marks the Worker not
   ready.
6. The scheduler uses the existing worker cache, but the cache is populated
   from `WatchWorkers` / `ListWorkers` rather than Kubernetes-specific pod
   state.

The syncer can initially run inside `ateapi` as a standalone goroutine. It
should be factored so it can later move into a separate worker-provider
process without changing the core API.

## Worker data copied from Pods

The first Kubernetes syncer should populate provider-neutral Worker fields:

- `meta.name`: stable substrate Worker name.
- `meta.labels`: scheduling labels copied under a documented policy.
- `spec.provider`: `"kubernetes"`.
- `spec.provider_id`: Kubernetes Pod UID or namespace/name for correlation.
- `spec.address`: routable ateom endpoint.
- `spec.sandbox_class`: runtime class advertised by this Worker.
- `status.state`: ready/not-ready/draining.
- `status.slots_total` and `status.slots_available`: reported capacity.
- `status.last_heartbeat_time`: last observed provider update.

Open label policy: copy all Pod labels, only substrate-owned labels, or a
configured allowlist. Because selectors depend on Worker labels, copied labels
become API contract and should be documented before broad use.

## Kubernetes WorkerPool

WorkerPool remains useful as a Kubernetes/provider abstraction, just not as a
portable substrate API resource.

Provider-level WorkerPool can own Kubernetes-specific concerns:

- desired replica count or autoscaling policy
- pod template
- node selectors, tolerations, affinity, resources, priority class
- common labels copied onto produced Workers
- future desired distributions of worker sizes, e.g. `4x2Gi`, `8x4Gi`,
  `12x8Gi`

Substrate core should not depend on that shape. If the Kubernetes provider
uses WorkerPool internally, it reconciles WorkerPool to Pods; the pod syncer
then reconciles Pods to ateapi Workers.

## Behavior deltas

1. **Scheduling uses Workers directly.** Selectors match
   `Worker.meta.labels`, and sandbox compatibility matches
   `ActorTemplate.spec.sandbox_class` to `Worker.spec.sandbox_class`.
2. **Worker is no longer debug-only.** It becomes the managed Admin resource
   that worker providers create/update/delete.
3. **Kubernetes fields leave core ateapi.** Pod namespace/name/UID, node name,
   tolerations, affinity, and resource requirements are provider details.
   Correlation can live in `Worker.spec.provider_id`; placement stays in the
   provider.
4. **Process snapshots, including goldens, are deferred.** P0 should work
   without golden snapshots. Re-enabling them requires a compatibility model
   across runtime version, architecture, kernel/hardware shape, and possibly N
   goldens per template.
5. **Delete returns the resource.** Standard methods return the deleted
   resource instead of `Empty`.
6. **Required update_mask.** Update without a mask returns
   `INVALID_ARGUMENT`; accepted paths are defined by the API.
7. **Watch SYNCED event.** Watch streams deliver initial state, `SYNCED`, then
   incremental events.

## Validation migration

Validation currently expressed as CEL/OpenAPI on CRDs moves into ateapi
handlers or provider controllers, depending on ownership:

| Validation | Target owner |
|---|---|
| DNS-1123 names for ateapi resources | ateapi handlers |
| ActorTemplate spec immutability | no UpdateActorTemplate method |
| container images pinned by digest | ateapi handlers |
| volume mount-path rules | ateapi handlers |
| `snapshots_config.on_commit` subset of `on_pause` | ateapi handlers |
| Worker labels and selector syntax | ateapi handlers |
| Worker status/capacity invariants | ateapi handlers |
| Kubernetes namespace, pod template, tolerations, affinity, resources | Kubernetes provider |

## Implementation migration path

1. Update the draft proto to expose Worker CRUD/watch.
2. Update the store to key Workers by provider-neutral resource identity
   instead of Kubernetes namespace/pool/pod.
3. Convert `cmd/ateapi/internal/workercache` to consume `ListWorkers` and
   `WatchWorkers` over managed Worker records.
4. Add a Kubernetes worker sync goroutine that watches Pods labeled
   `pod.ate.dev/is-worker=true` and writes Worker records through the same
   store/API path as any future provider.
5. Change scheduling to select Workers directly using Worker labels,
   `sandbox_class`, readiness, and capacity.
6. Remove Actor references to WorkerPool; record the assigned Worker instead.
7. Move Kubernetes WorkerPool reconciliation out of substrate core. It may
   stay as a Kubernetes provider controller or be replaced by plain
   Deployments for the first cut.
8. Cut clients and demos to create ActorTemplates/Actors through ateapi and to
   inspect Workers through Admin APIs.
9. Delete the old CRD-watching/projector path from core once the Kubernetes
   provider path owns pod-to-Worker reconciliation.
