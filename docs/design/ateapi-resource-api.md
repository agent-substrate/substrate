# ateapi resource API (issue #368)

Proto: [`pkg/proto/ateapipb/ateapi.proto`](../../pkg/proto/ateapipb/ateapi.proto)

This is the target API direction for moving substrate scheduling resources out
of Kubernetes CRDs into the substrate API
([#368](https://github.com/agent-substrate/substrate/issues/368)). Substrate
core understands Workers, while WorkerPool is a provider-level capacity
abstraction, similar to Kubernetes understanding Nodes while cloud providers
manage NodePools. The transition from the Kubernetes-backed implementation is
covered separately in
[ateapi-crd-migration.md](ateapi-crd-migration.md).

The API follows the draft
[API style guide](https://github.com/agent-substrate/substrate/pull/351);
deliberate divergences are listed at the [end](#style-guide-conformance).

## Resource model

| Resource | Scope | Service | Mutable? | Notes |
|---|---|---|---|---|
| Atespace | global | Control | no | isolation boundary |
| ActorTemplate | atespace | Control | no (immutable spec) | + Watch |
| Actor | atespace | Control | `worker_selector` only | + Suspend/Pause/Resume |
| Worker | global | Admin | yes | provider-owned capacity; + Watch |

All managed resources carry `ResourceMetadata meta = 1` (atespace, name,
uid, version, create_time, update_time, labels) and use the standard method
shapes from the style guide: Get/Create/Update return the bare resource,
Delete returns the deleted resource, List paginates with
`page_size`/`page_token`, identity travels as `ObjectRef{atespace, name}`,
and optimistic concurrency uses `meta.version` (int64, `ABORTED` on
mismatch, 0 skips).

WorkerPool is not part of the core ateapi resource model in this direction. A
Kubernetes provider may still have a WorkerPool CRD or config object, but that
object belongs to the provider implementation. Its job is to create
Pods/processes that become Workers in substrate.

## Revised direction

1. **Worker is the substrate scheduling primitive.** The scheduler matches
   Actor/ActorTemplate requirements against Worker labels, runtime class, and
   readiness/capacity. It does not schedule against WorkerPool.
2. **WorkerPool is provider-level.** Kubernetes can keep a WorkerPool CRD,
   Deployment wrapper, Helm value, or autoscaler input, but substrate core
   only sees the Workers produced by that provider. WorkerPool may later grow
   useful provider semantics such as desired distributions of worker sizes.
3. **Worker registration is one-directional.** Worker owners create, update,
   heartbeat, and delete Workers. The scheduler already talks to ateom for
   placement, so the worker-owner API does not need assignment watches.
4. **Kubernetes worker discovery starts with Pods.** A standalone sync
   goroutine can watch Pods labeled `pod.ate.dev/is-worker=true`, copy the
   approved metadata onto Worker records, and later move into a distinct
   process.
5. **Owned label keys must be documented.** Scheduling labels become API
   contract, so substrate-owned labels live in a registry:
   [label-registry.md](label-registry.md).
6. **Process snapshots and golden snapshots are deferred for P0.** They are
   valuable, but they imply compatibility constraints across gVisor/runtime
   version, kernel/hardware shape, architecture, and possibly one golden per
   template/runtime/hardware version. The first API should work without
   depending on process snapshots.

## Design decisions

**D1. Versioned package `ateapi.v1alpha1`.** gRPC method paths embed the
package, so the package name is the API-versioning mechanism.

**D2. Two services with distinct audiences.** `Control` is the tenant
surface (atespaces, templates, actors); `Admin` is the platform surface
(workers and debug). Watch RPCs live next to their resource. The split gives
a future authz story a natural boundary.

**D3. ActorTemplate spec is immutable — no UpdateActorTemplate.** Template
changes = create a new template and migrate actors. Delete returns
`FAILED_PRECONDITION` while any actor references the template.

**D4. Worker is a managed Admin resource.** Worker owners create/update/delete
Workers through ateapi; substrate stores them, watches them, and schedules
Actors onto them. The existing internal worker cache remains useful, but its
input should become Worker CRUD/watch rather than Kubernetes-specific pod
state.

**D5. WorkerPool policy is provider-owned.** Once WorkerPool is not a core
resource, tenant access to provider capacity is not modeled as a core ateapi
object. If a provider-level WorkerPool needs multi-tenant policy, that policy
belongs to the provider until substrate has a concrete cross-provider access
model.

**D6. Referential integrity on delete.** Deletes that would orphan
dependents return `FAILED_PRECONDITION`: atespace↔actors/templates,
template↔actors, worker↔assigned actors.

**D7. Status is a closed enum, not phase strings or conditions.** Actors,
templates, and workers use closed state enums plus a human-readable error or
message field where needed.

**D8. Equality-only `Selector{match_labels}`** for template and per-actor
worker selectors, matched against Worker labels. The scheduler
implements equality matching only; set-based expressions can be added as a
new field if a consumer appears.

**D9. Env vars are a `oneof {value | secret}`** with a plane-neutral
`SecretRef{name, key, optional}` — value-vs-secret is exclusive by
construction, and no k8s types leak into the container spec.

**D10. `sandbox_class` is an open string** on templates and workers. It is a
scheduling compatibility label. Runtime setup remains provider/runtime
configuration. Workers may technically support multiple sandbox classes; P0
keeps this scalar and can extend it to a repeated field once scheduling
semantics for multi-runtime workers are needed.

**D11. Update masks accept top-level paths only.** `update_mask` is
required (per the style guide); paths one level below the resource root
(`spec.address`, `status`, `meta.labels`) are accepted, and
naming a message or map field replaces it wholesale. Unknown, immutable, or
deeper paths → `INVALID_ARGUMENT`. Servers may widen to deeper paths later
without breaking clients.

**D12. Watch = snapshot, SYNCED, then incremental events.** Streams
(templates, workers) deliver an initial snapshot, a `SYNCED` marker, then
at-least-once CREATED/UPDATED/DELETED events carrying full resource state.
No resume tokens: consumers are in-cluster caches and re-syncing is cheap;
`meta.version` lets them discard stale events.

**D13. Worker identity is provider-neutral.** Core identity is
`ResourceMetadata.name`. Provider details such as Kubernetes namespace, Pod
name, Pod UID, VM ID, or runtime endpoint are fields on Worker spec/status for
debugging and connection, not part of the resource key.

**D14. Actor is a managed resource like the rest.** `meta` carries its
identity; `actor_template` and assigned `worker` are `ObjectRef`s; lifecycle
verbs (Suspend/Pause/Resume) are custom methods returning the Actor.

**D15. Atespace and SessionIdentity are unchanged** beyond conforming to
`meta` and standard method shapes. Redesigning them is a non-goal of #368.

## Style-guide conformance

The surface follows PR #351 (identity model, `ResourceMetadata`,
`ObjectRef`, standard method shapes, required `update_mask`, `version`
concurrency). Four deliberate divergences remain in this draft:

1. **`labels` in `ResourceMetadata`** — the guide's meta has no labels
   field, but scheduling requires Worker labels as a selector match target.
2. **Immutable resources may omit Update** (D3) — the guide implies all
   five standard methods everywhere.
3. **Top-level-only mask paths** (D11) — the guide requires the mask but
   leaves path granularity undefined.
4. **Empty `atespace` = list across all atespaces** — the guide reads as if
   `atespace` were required on List; admin/debug flows need cross-atespace
   listing.

## Open questions

- **Label copy policy**: for Kubernetes-discovered Workers, copy all labels,
  only `*.ate.dev/*`, or a configured allowlist? This is part of the API
  contract once selectors depend on it.
- **Worker heartbeat semantics**: define whether Worker owners use Update,
  an explicit Heartbeat RPC, or both. CRUD is enough on paper; Heartbeat may
  be operationally clearer.
- **Golden snapshots**: process snapshots, including goldens, are deferred.
  If re-enabled, define the compatibility key (runtime version, architecture,
  kernel/hardware shape) and whether templates maintain N goldens.
- **Secrets plane**: `SecretRef` resolves against the worker's environment
  (a k8s Secret today). Freeze that rule, or design secret distribution
  before v1beta1?
- **Pagination defaults**: pick a server default page size (proposal: 500,
  max 1000) and document it once, centrally.
- **Watch scope**: watches are global today (atelet caches everything).
  Add an optional `atespace` filter now, or when a consumer needs it?
