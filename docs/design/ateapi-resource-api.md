# ateapi resource API (issue #368)

Proto: [`pkg/proto/ateapipb/ateapi.proto`](../../pkg/proto/ateapipb/ateapi.proto)

This is the proposed API surface once ActorTemplate, WorkerPool,
SandboxConfig and WorkerPoolGrant move out of Kubernetes CRDs into the
substrate API ([#368](https://github.com/agent-substrate/substrate/issues/368)).
It builds on [decoupling-planes.md](https://github.com/agent-substrate/substrate/blob/poc-decouple-upstream/docs/design/decoupling-planes.md)
(on the POC branch), which settled the
resource model (global pools + per-atespace grants); this doc covers the API
shape. The transition from the CRDs/POC is covered separately in
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
| WorkerPool | global | Admin | yes | + Watch; labels are the selector match target |
| SandboxConfig | global | Admin | yes | de-facto registry of sandbox classes |
| WorkerPoolGrant | atespace | Admin | no | at most one grant per (atespace, pool) |
| Worker | — | Admin | — | debug projection, ListWorkers only |

All managed resources carry `ResourceMetadata meta = 1` (atespace, name,
uid, version, create_time, update_time, labels) and use the standard method
shapes from the style guide: Get/Create/Update return the bare resource,
Delete returns the deleted resource, List paginates with
`page_size`/`page_token`, identity travels as `ObjectRef{atespace, name}`,
and optimistic concurrency uses `meta.version` (int64, `ABORTED` on
mismatch, 0 skips).

## Design decisions

**D1. Versioned package `ateapi.v1alpha1`.** gRPC method paths embed the
package, so the package name is the API-versioning mechanism.

**D2. Two services with distinct audiences.** `Control` is the tenant
surface (atespaces, templates, actors); `Admin` is the platform surface
(pools, sandbox configs, grants, debug). Watch RPCs live next to their
resource. The split gives a future authz story a natural boundary.

**D3. ActorTemplate spec is immutable — no UpdateActorTemplate.** Golden
snapshots are only valid for the exact spec they were taken from, so
in-place mutation is meaningless. Template changes = create a new template
and migrate actors. Delete returns `FAILED_PRECONDITION` while any actor
references the template.

**D4. WorkerPoolGrant is a separate resource because pool capacity and
atespace access have different owners and lifecycles.** A WorkerPool is
global platform capacity; an ActorTemplate is tenant/atespace-scoped demand.
The scheduler needs an explicit admission edge between them: selectors answer
"does this pool match?", while grants answer "may this atespace use it?".
Putting grants on WorkerPool would make every pool update rewrite a shared
allow-list and would couple capacity changes to tenant access control.
Putting grants on ActorTemplate would duplicate the same permission across
templates and make revocation ambiguous. A separate resource gives admins a
small auditable object to create/delete, supports future per-grant policy
(quotas, priority, expiry), and keeps scheduling as a simple
`(atespace, worker_pool)` check.

At most one WorkerPoolGrant may exist per `(atespace, worker_pool)`. Grants
are fully conventional resources (caller-named, standard methods), but Create
enforces uniqueness on the pair — a duplicate grant for the same pool returns
`ALREADY_EXISTS` regardless of its name (same pattern as the single-default
SandboxConfig rule). This keeps revocation unambiguous and lets the
scheduler's `(atespace, pool)` check stay a point lookup via a server-side
index. Grants are immutable; future policy fields would add Update.

**D5. One Kubernetes pocket: `WorkerPoolSpec.kubernetes`.** Everything
k8s-specific about materializing a pool — namespace, node_selector,
tolerations, node_affinity, resources, priority_class_name — lives in a
single `KubernetesPlacement` message. Everything outside it is
plane-neutral; a future non-k8s substrate adds a sibling placement message.

**D6. Referential integrity on delete.** Deletes that would orphan
dependents return `FAILED_PRECONDITION`: atespace↔actors/templates,
template↔actors, pool↔grants and pool↔assigned actors, config↔pools.

**D7. Status is a closed enum, not phase strings or conditions.**
`ActorTemplateStatus.state` ∈ {PENDING, SNAPSHOTTING, READY, FAILED} plus
`golden_actor_id`, `golden_snapshot`, `error_message`. Likewise
`SnapshotsConfig` scopes are a `Scope` enum (FULL/DATA).

**D8. Equality-only `Selector{match_labels}`** for template and per-actor
worker selectors, matched against `WorkerPool.meta.labels`. The scheduler
implements equality matching only; set-based expressions can be added as a
new field if a consumer appears.

**D9. Env vars are a `oneof {value | secret}`** with a plane-neutral
`SecretRef{name, key, optional}` — value-vs-secret is exclusive by
construction, and no k8s types leak into the container spec.

**D10. `sandbox_class` is an open string** on templates and pools.
SandboxConfigs are the class registry; discoverability comes from
ListSandboxConfigs, not a proto enum.

**D11. Update masks accept top-level paths only.** `update_mask` is
required (per the style guide); paths one level below the resource root
(`spec.replicas`, `spec.kubernetes`, `meta.labels`) are accepted, and
naming a message or map field replaces it wholesale. Unknown, immutable, or
deeper paths → `INVALID_ARGUMENT`. Servers may widen to deeper paths later
without breaking clients.

**D12. Watch = snapshot, SYNCED, then incremental events.** Streams
(templates, pools) deliver an initial snapshot, a `SYNCED` marker, then
at-least-once CREATED/UPDATED/DELETED events carrying full resource state.
No resume tokens: consumers are in-cluster caches and re-syncing is cheap;
`meta.version` lets them discard stale events.

**D13. Worker stays a debug projection.** No `ResourceMetadata`, no CRUD.
Making Worker a managed resource would promise lifecycle semantics the
system doesn't offer — workers are pool-managed.

**D14. Actor is a managed resource like the rest.** `meta` carries its
identity; `actor_template` and `worker_pool` are `ObjectRef`s; lifecycle
verbs (Suspend/Pause/Resume) are custom methods returning the Actor. The
`ateom_pod_*` fields remain, marked output-only infrastructure debugging.

**D15. Atespace and SessionIdentity are unchanged** beyond conforming to
`meta` and standard method shapes. Redesigning them is a non-goal of #368.

## Style-guide conformance

The surface follows PR #351 (identity model, `ResourceMetadata`,
`ObjectRef`, standard method shapes, required `update_mask`, `version`
concurrency). Four deliberate divergences, feedback filed on PR #351:

1. **`labels` in `ResourceMetadata`** — the guide's meta has no labels
   field, but scheduling requires pool labels as a selector match target.
2. **Immutable resources may omit Update** (D3, D4) — the guide implies all
   five standard methods everywhere.
3. **Top-level-only mask paths** (D11) — the guide requires the mask but
   leaves path granularity undefined.
4. **Empty `atespace` = list across all atespaces** — the guide reads as if
   `atespace` were required on List; admin/debug flows need cross-atespace
   listing.

## Open questions

- **Secrets plane**: `SecretRef` resolves against the worker's environment
  (a k8s Secret today). Freeze that rule, or design secret distribution
  before v1beta1?
- **Pagination defaults**: pick a server default page size (proposal: 500,
  max 1000) and document it once, centrally.
- **Watch scope**: watches are global today (atelet caches everything).
  Add an optional `atespace` filter now, or when a consumer needs it?
