# Migrating substrate-plane resources from CRDs to ateapi (issue #368)

Companion to [ateapi-resource-api.md](ateapi-resource-api.md), which defines
the target API. This doc covers the transition: what changes relative to the
Kubernetes CRDs and the POC proto (`poc-decouple-upstream`), and the
migration path.

## Why the POC shape changes

The POC copied the CRD Go types field-for-field to move fast. That imported
Kubernetes idioms that don't pull their weight in a native API:

| # | Leak | Where | Problem |
|---|------|-------|---------|
| 1 | `deployment_atespace` | WorkerPoolSpec | It's a **Kubernetes namespace** (the projector uses it verbatim as the Deployment namespace), mislabeled with a substrate term. Renamed `kubernetes.namespace`. |
| 2 | `WorkerPoolPodTemplate` + Toleration/NodeAffinity/ResourceRequirements at top level | WorkerPool | corev1 mirrored into the public API without a boundary marking it k8s-specific. Quarantined into `KubernetesPlacement`. |
| 3 | `EnvVarSource`/`SecretKeySelector` | Container env | corev1's three-field env shape; invalid states representable (value + value_from). Now a `oneof`. |
| 4 | `LabelSelector.match_expressions` | template worker_selector | Set-based operators copied but only equality is implemented or meaningful today. Dropped. |
| 5 | `Condition` list + string `phase` | ActorTemplateStatus | KRM condition machinery; `observed_generation` has no generation to observe. Replaced by a state enum. |
| 6 | Stringly-typed enums (`on_pause: "Full"`) | SnapshotsConfig | CEL-validated strings instead of proto enums. Now `Scope` enum. |
| 7 | Update RPCs with unspecified FieldMask semantics | templates, pools, configs | The POC marked mask semantics TODO. Now defined (top-level paths), and templates lose Update entirely. |
| 8 | Grant identity `(atespace, name)` with free-form name | WorkerPoolGrant | The scheduler looks up grants by `(atespace, pool)`; the POC allows multiple grants for the same pair, making revocation ambiguous. The POC's own `GetWorkerPoolGrant` is documented "by atespace and worker pool" but keyed by name. Now: Create enforces at most one grant per (atespace, pool). |

## Behavior deltas vs the POC

1. **No UpdateActorTemplate** — POC has it; the target API drops it
   (immutable spec).
2. **Grant uniqueness** — Create rejects a second grant for the same
   (atespace, worker_pool) pair with `ALREADY_EXISTS`; the store gains a
   `(atespace, pool)` index so the scheduler check stays a point lookup.
3. **Delete returns the resource** — POC returned `Empty` for
   templates/pools/configs/grants and an empty message for actors.
4. **Required update_mask** — Update without a mask becomes
   `INVALID_ARGUMENT`; POC treated masks as TODO/full-replace.
5. **Actor RPC shapes** — bare-resource returns, `ObjectRef` identity, and
   the legacy `actor_template_namespace/name` create path is gone. The
   package rename (`ateapi` → `ateapi.v1alpha1`) makes every message a new
   proto type, so this is a clean break: field numbers are assigned fresh,
   no `reserved` scar tissue, and migration is a coordinated client cutover
   rather than in-place field evolution. (The POC has no external users.)
6. **Referential integrity on delete** — template↔actor, pool↔grant,
   pool↔assigned-actor, config↔pool checks return `FAILED_PRECONDITION`;
   the POC checks only atespace emptiness.
7. **Watch SYNCED event** — new; consumers can await cache completeness.

## CRD validations that move into handlers

Validation currently expressed as CEL/OpenAPI on the CRDs moves into
Create (and Update, where applicable) handlers:

- DNS-1123 label names (atespace, name — per style guide §2.3)
- template spec immutability (`self == oldSelf` → no Update method)
- container images pinned by digest
- volume mount-path rules
- `snapshots_config.on_commit` ⊆ `on_pause`
- at most one `default: true` SandboxConfig

## Migration path

1. Land the target proto as `ateapi.v1alpha1` alongside the POC `ateapi`
   package; generate both.
2. Port store keys/values: add the grant `(atespace, pool)` uniqueness
   index; template records gain immutability enforcement.
3. Move CRD validation into handlers (table above); delete the CRDs and the
   projector's CRD-watching path.
4. Cut clients (atectl, atelet caches, demos) over to `v1alpha1`; delete
   the POC package.
