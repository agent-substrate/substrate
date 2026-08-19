# Worker CRUD API

**Status:** design agreed. Step 1 of the implementation plan has landed — the
RPC surface exists as `UNIMPLEMENTED` stubs. Everything below still describes
the target state; the plan at the end tracks what is done.

## Motivation

Workers are currently created, updated, and deleted by writing directly to the
store. `WorkerPoolSyncer` reconciles Kubernetes pod state into the store via
`store.Interface` — `CreateWorker` at `syncer.go:231`, `UpdateWorker` at `:269`
and `:294`, `DeleteWorker` at `:307` — and five other call sites mutate worker
rows the same way:

- `crash.go:111` (read), `:134` (write)
- `volumes.go:198` (read)
- `workflow_resume.go:353` (read), `:395`, `:470`, `:506` (writes)
- `workflow_suspend.go:349` (read), `:360` (write)
- `workflow_pause.go:218` (read), `:232` (write)

Only `ListWorkers` is exposed as an RPC. Validation lives outside the write
path — `resources.ValidateWorker` is called by the syncer, with a standing
`TODO(thockin)` noting it should move into the API if Workers ever become a
regular resource (`syncer.go:220`).

This document defines that API. Workers become a first-class resource in the
`Control` service, following `docs/api-style-guide.md`, and the store's worker
methods become a private storage primitive with the API as their only caller.

## Resource model

`Worker` is **global-scoped**: `metadata.atespace` is always empty. Workers are
Kubernetes infrastructure and belong to no atespace.

`metadata.name` is the **Kubernetes pod UID**. Treat it as opaque — read pod
identity from the named fields on the resource, not by parsing the name.

Rationale for pod-UID naming:

- A UUID4 is 36 characters of lowercase alphanumerics and hyphens, so it
  satisfies §2.2's RFC-1123 label rule by construction. Pod names are DNS-1123
  *subdomains* (dots allowed, up to 253 chars) and are validated as such today
  (`internal/resources/validate.go:233-238`), so they are not guaranteed to be
  valid resource names.
- Pod name alone is not unique across Kubernetes namespaces, and a composite
  like `{namespace}-{pool}-{pod}` is both collidable and length-unbounded. Both
  store backends key Workers on exactly that composite today — the Redis key and
  the Postgres primary key are `(worker_namespace, worker_pool, worker_pod)`.
- It removes a class of bug. The syncer currently compares
  `w.WorkerPodUid != string(pod.UID)` to detect a pod deleted and recreated
  under the same name with coalesced events (`syncer.go:234-244`). Under UID
  naming the replacement is simply a different resource.

Workers are **derived state**. Every field is reconstructed from the Pod and its
WorkerPool by the syncer; nothing is user-authored. A full rebuild is always
possible from the informer, which is why no data migration is planned.

## Proto

```proto
// Worker is a schedulable worker pod.
//
// Global-scoped: metadata.atespace is always empty. metadata.name is the
// Kubernetes pod UID — treat it as opaque; read pod identity from the named
// fields below.
//
// Workers are derived state. Every field is reconstructed from the Pod and
// its WorkerPool by the syncer; nothing here is user-authored.
message Worker {
  ResourceMetadata metadata = 1;

  // Kubernetes coordinates. Immutable, set at creation.
  string worker_namespace = 2;
  string worker_pool      = 3;
  string worker_pod       = 4;
  string worker_pod_uid   = 5;
  string node_name        = 6;
  string ip               = 7;

  // Observed pool state. Mutable via UpdateWorker.
  string sandbox_class       = 8;
  map<string, string> labels = 9;

  enum State {
    STATE_UNSPECIFIED = 0;
    STATE_ACTIVE      = 1;  // Ready; schedulable.
    STATE_NOT_READY   = 2;  // Pod Ready=false. Not schedulable; row retained
                            // so a bound Actor survives a readiness blip.
    STATE_DRAINING    = 3;  // Pod terminating. Not schedulable; one-way.
  }
  State state = 10;

  // Never valid in an UpdateWorker mask; see "Assignment stays internal".
  ActorAssignment assignment = 11;
}

// ActorAssignment names the Actor currently bound to a Worker — the inverse
// of WorkerAssignment.
message ActorAssignment {
  ObjectRef actor  = 1;
  string actor_uid = 2;

  // ActorTemplates are Kubernetes CRDs rather than Substrate API resources,
  // so this stays a kube reference. Revisit if they move into the API
  // (api-style-guide.md §2.3).
  KubeNamespacedObjectRef actor_template = 3;
}

// WorkerAssignment points at the Worker currently hosting an Actor.
//
// A denormalized snapshot, not merely a reference: atenet reads
// worker_pod_ip on the request-routing path and must not need a second
// lookup, and kubectl-ate displays the pod name. The copies are valid for
// the life of the assignment — a Worker's identity fields are immutable and
// the assignment is cleared when the Worker goes away.
message WorkerAssignment {
  // The assigned Worker. atespace is always empty.
  ObjectRef worker = 1;

  string worker_namespace = 2;
  string worker_pool      = 3;
  string worker_pod       = 4;
  string worker_pod_uid   = 5;
  string worker_pod_ip    = 6;
}
```

`STATE_NOT_READY` takes 2, moving `STATE_DRAINING` from 2 to 3 so the values
read in lifecycle order. This renumbering means an already-stored Worker with
`state = 2` is read back as `NOT_READY` instead of `DRAINING`. That is accepted:
pre-1.0 breaking changes are expected, Workers are derived state that the syncer
rebuilds in full from the informer, and both values are non-schedulable — so the
worst case during a rolling deploy is a draining Worker briefly mislabeled, not
one wrongly handed new Actors.

Added to the existing `service Control`, alongside the current `ListWorkers`:

```proto
  // Get a Worker.
  rpc GetWorker(GetWorkerRequest) returns (Worker) {}

  // Register a Worker. Called once its Pod is Ready and has an IP.
  rpc CreateWorker(CreateWorkerRequest) returns (Worker) {}

  // Update observed pool state on a Worker.
  rpc UpdateWorker(UpdateWorkerRequest) returns (Worker) {}

  // Deregister a Worker. Does not cascade: the caller is responsible for
  // cleaning up related resources.
  rpc DeleteWorker(DeleteWorkerRequest) returns (Worker) {}

  // Mark a Worker as terminating. Idempotent: a Worker already DRAINING is
  // returned unchanged, without a version bump. One-way.
  rpc DrainWorker(DrainWorkerRequest) returns (Worker) {}
```

```proto
message GetWorkerRequest    { ObjectRef worker = 1; }
message CreateWorkerRequest { Worker worker = 1; }

message UpdateWorkerRequest {
  // metadata.version and metadata.uid are honored as optional guards
  // (api-style-guide.md §7). They must not appear in update_mask.
  Worker worker = 1;

  // Required. Permitted paths: sandbox_class, labels, state.
  google.protobuf.FieldMask update_mask = 2;
}

message DeleteWorkerRequest {
  ObjectRef     worker  = 1;
  DeleteOptions options = 2;
}

message DrainWorkerRequest { ObjectRef worker = 1; }
```

`ListWorkers` is unchanged. Global scope means no `atespace` field, and it
already carries only `page_size` and `page_token`. No pool filter, per §3.2.

## Assignment stays internal

There is **no `AssignWorker` / `ReleaseWorker` RPC**. Binding an Actor to a
Worker and releasing it stay inside `ate-api-server`, which writes the
`assignment` field to the store directly.

Every caller that binds or releases is already in-process — the actor workflows
(`workflow_resume.go`, `workflow_suspend.go`, `workflow_pause.go`, `crash.go`)
and the syncer's `releaseActorOnDeadWorker`. None of them is a remote client, so
an RPC would buy nothing and would cost something real: with `ateapiauth`
authenticating but not authorizing (see "Deferred"), an exposed `AssignWorker`
lets any authenticated client steal a Worker out from under a running Actor.
Keeping the binding in-process keeps that surface closed and keeps the
compare-and-set local to the store call.

`assignment` is therefore output-only over the API. It appears on the `Worker`
resource so readers — `ListWorkers`, `GetWorker`, `kubectl-ate` — can see
occupancy, and it is never accepted on an `UpdateWorker` mask.

The occupancy compare-and-set described under "Concurrency" is still needed;
it just lives on `store.Interface` rather than behind an RPC.

## Field mutability

| Field | Class |
|---|---|
| `metadata.*` | Output-only (server-assigned) |
| `worker_namespace`, `worker_pool`, `worker_pod`, `worker_pod_uid`, `node_name`, `ip` | Immutable after create |
| `sandbox_class`, `labels`, `state` | Mutable via `UpdateWorker` mask |
| `assignment` | Output-only over the API; written in-process (see above) |

`ip` is immutable, but note this is **currently enforced in only one backend**.
`ateredis.UpdateWorker` rejects a changed IP with `"ip is immutable"`
(`ateredis.go:1058-1060`, alongside the same check for `worker_pod`);
`atepg.UpdateWorker` (`atepg.go:1350-1385`) has no equivalent check — it is a
plain `UPDATE ... WHERE version = $6`, so under Postgres the IP is silently
mutable. No `storecontract` case covers immutability for Workers (there is one
for Actors, `contract.go:281`), which is why the divergence went unnoticed.

The syncer's "IP changed" branch (`syncer.go:249`) therefore fails under Redis
and succeeds under Postgres; its own comment says the case is believed
unreachable either way. Lifting the rule into the API makes the behavior
backend-independent, and the backends should converge regardless — see the
implementation plan.

## Lifecycle

Workers are registered only once their pod is Ready and has an IP, so the
initial state is always `ACTIVE`. There is no `PENDING` state.

| Transition | Trigger | Mechanism |
|---|---|---|
| ∅ → `ACTIVE` | Pod Ready and has an IP | `CreateWorker` |
| `ACTIVE` → `NOT_READY` | Pod Ready=false | `UpdateWorker` mask on `state` |
| `NOT_READY` → `ACTIVE` | Readiness recovers | `UpdateWorker` mask on `state` |
| any → `DRAINING` | `DeletionTimestamp` set | `DrainWorker` (one-way) |
| `DRAINING` → ∅ | Pod gone | `DeleteWorker` |

The lifecycle is deliberately asymmetric: a Worker is not created until its pod
is ready, but it is **not** deleted when the pod goes un-ready. The row must
survive a readiness blip so a bound Actor is not torn down.

Pod phases `Failed` and `Succeeded` need no separate state — a terminated pod
reports Ready=false, so `NOT_READY` already covers it.

Three axes are kept independent rather than folded into one enum, matching what
the scheduler already does (`scheduling.go:106-108` tests occupancy separately
from `Applies()`):

- **Lifecycle/health** — the `state` enum.
- **Occupancy** — the `assignment` field. `DRAINING`-and-assigned is a real,
  load-bearing state: drain deliberately leaves the bound Actor alone
  (`syncer.go:182-189`).
- **Administrative** — a future `cordoned` bool. Deferred.

## Method semantics

**Idempotency**, chosen to preserve current behavior:

- `DrainWorker` on a Worker already `DRAINING` returns it unchanged, no version
  bump.

The in-process bind/release path keeps the behavior it has today: releasing an
unassigned Worker is a no-op, and releasing one whose `actor_uid` no longer
matches is also a no-op rather than an error — the superseded-assignment case
that returns `nil` at `syncer.go:359-361`.

**`DeleteWorker` does not cascade** and has no precondition on `assignment`. An
Actor pointing at a Worker that no longer exists is an expected steady state,
not corruption, and is handled deliberately at every call site:

- Resume: `ErrNotFound` → `crashActor(..., ReasonWorkerPodGone)` → `ABORTED`
  (`workflow_resume.go:353-357`).
- Suspend: `ErrNotFound` → "Worker already gone during finalize suspend,
  skipping release", then continues (`workflow_suspend.go:349-354`).
- Pause: same shape, "Worker already gone during finalize pause"
  (`workflow_pause.go:218-224`).
- Where the Worker does exist, release is guarded by
  `wass.GetActorUid() == latestActor.GetMetadata().GetUid()`.

`reconcileDeadWorker` (`syncer.go:303`) therefore keeps its current shape —
release the Actor, then delete the Worker — and treats `NOT_FOUND` from the
delete as success. The ordering matters: releasing first means a failure leaves
the row in place for `enqueueStoredWorkers` (`syncer.go:313`) to rediscover
after a restart, which is what the comment at `syncer.go:299` is protecting.

`NOT_FOUND` on an absent Worker is required by §3.5 ("If the resource does not
exist: return `NOT_FOUND`"), which offers no idempotent-delete alternative — the
guide does not discuss delete idempotency at all.

`DeleteWorker` is the only delete on `store.Interface` that does not already
comply: it is documented as idempotent ("does nothing if worker is not found",
`store.go:190-191`) and both backends implement it that way, whereas
`DeleteActor:77`, `DeleteAtespace:134`, `DeleteActorTemplate:158`, and
`DeleteActorTemplateVersion:179` all return the resource with a documented
`ErrNotFound`. Bringing Worker into line is a correction, not a new convention.

Either the store method starts reporting absence — `atepg` already uses
`DELETE ... RETURNING proto` and discards the `pgx.ErrNoRows` case, so it is one
line — or the API layer reads before deleting. Prefer the former; the latter
races a concurrent delete between the read and the delete.

Idempotency then lives at the caller, which is where the Actor path already puts
it: `workflow_delete.go:29` calls the workflow "Idempotent" while mapping
`store.ErrNotFound` to `codes.NotFound` at `:58` and `:118` — meaning the
multi-step workflow is safe to re-drive, not that absence is success.
`reconcileDeadWorker` treating `NOT_FOUND` as success is the same pattern.

## Error codes

| Condition | Code |
|---|---|
| Worker absent | `NOT_FOUND` |
| Duplicate `name` on create | `ALREADY_EXISTS` |
| `version` / `uid` guard mismatch | `ABORTED` |
| Bad mask path, immutable field, illegal transition | `INVALID_ARGUMENT` |

The bind path has no RPC and so no status codes of its own. It keeps reporting
an unavailable Worker the way it does today: `Schedule()` finding nothing free
surfaces as `ResourceExhausted: "no free workers available"`, and losing the
compare-and-set surfaces as `store.ErrVersionConflict`, which the caller
retries against a freshly chosen Worker.

## Concurrency

Assignment mutual exclusion comes from the **storage layer**. That is true
whether or not a bind ever gets an RPC in front of it, which is why the
requirement survives dropping `AssignWorker`: it has to be stated as a
`store.Interface` contract rather than a Redis recipe, because there are now two
backends (`ateredis` and `atepg`, added in #640) and `storecontract` runs the
same suite against both.

**The contract.** The store must expose a compare-and-set on the assignment
field: *read the Worker, evaluate `state == ACTIVE && assignment == nil`, and
write the assignment — with no interleaved write to that Worker visible between
the read and the write.* Both backends can satisfy it:

- `ateredis` — `s.rdb.Watch(ctx, fn, dbKey)` (`ateredis.go:1031`, inside
  `UpdateWorker` at `:1024-1084`). The closure re-reads the key inside the
  watch; `EXEC` fails with `redis.TxFailedErr` if anything wrote that key in
  between. Bounded retry of the closure on `TxFailedErr`.
- `atepg` — the write already runs inside a `pgx.Tx` supplied by
  `writeAndNotify`, and `getWorkerRow` accepts any `querier`, so it can read
  inside the same transaction. The occupancy predicate cannot be plain SQL: the
  `workers` table stores an opaque marshaled `proto` blob with only
  `worker_namespace`/`worker_pool`/`worker_pod`/`version` as columns, so
  `assignment` is not addressable in a `WHERE` clause. Read-check-then
  `UPDATE ... WHERE version = $n` inside the transaction, and treat a zero-row
  update as a lost race.

`atepg` has no occupancy check at all today, because nothing above it needed
one — the syncer and workflows do read-modify-write on the whole object. Adding
it is a piece of work in its own right, not a free consequence of tidying the
layer above.

A bind is therefore: begin → read → check `state == ACTIVE && assignment == nil`
→ write with the assignment set → commit. A concurrent bind loses at commit; on
re-read the Worker is either now occupied, and the caller picks another, or
still free, and the retry succeeds.

Note what this replaces. `workflow_resume.go:493-506` binds today by cloning a
possibly-stale cached Worker and writing it back wholesale under a client
`version` guard. That guard is protecting a client-side read-modify-write, and
it is a poor fit for occupancy: the cache lags, so an unrelated field update
bumps the version on a perfectly assignable Worker and the bind fails
spuriously. A store-level compare-and-set on `state` and `assignment` checks the
condition that actually matters.

## Transport

The syncer calls the API over **gRPC loopback**, not in-process, because the
Worker registry is expected to move out of `ate-api-server` shortly. There is an
established client pattern — `ateapiauth.DialOptions(ClientConfig{...})`, used
by `atecontroller/main.go:124`, `atenet/internal/router/router.go:195`, and
`atelet/main.go:253`.

Open items for implementation:

- **Credentials.** `ClientConfig` offers mTLS via a client cred bundle or a
  ServiceAccount-token bearer. `ate-api-server` needs its own client identity to
  call itself.
- **Address.** A flag defaulting to the Service address rather than localhost —
  localhost bakes in the co-location assumption this move is about to break.
- **Startup ordering.** The syncer needs the gRPC server listening before its
  first reconcile burst, or the initial keys back off and retry noisily.

**Error handling changes shape.** The syncer branches on sentinel errors today
(`errors.Is(err, store.ErrNotFound)` at `syncer.go:205`, `:284`, `:343`,
`:354`); those become `status.Code(err)` checks. With validation moving
server-side, one case needs care: an invalid Worker is currently **terminal** —
the syncer logs and returns `nil` so the key is not requeued forever, because
"the inputs are deterministic, retrying cannot help" (`syncer.go:224`). Behind an RPC, the
syncer must map `INVALID_ARGUMENT` to *do not requeue* while every other error,
including transport failures, requeues. Inverting that gives either a hot loop
on a permanently-invalid pod or silently dropped reconciles.

## Deferred

- **Authorization.** `ateapiauth` authenticates but does not authorize, so until
  that lands any authenticated client can call `DeleteWorker` or `DrainWorker`.
  Accepted knowingly.
- **A bind RPC.** Needed only if the Worker registry moves out of
  `ate-api-server` (see "Transport"), at which point the bind path stops being
  in-process. Until then, see "Assignment stays internal".
- **`WatchWorkers` stays on `store.Interface`**, outside the CRUD API. This has
  an expiry date: `workercache` (`workercache.go:107`, `:122`) reads the store
  directly, so if the Worker registry moves out of `ate-api-server` and the data
  goes with it, watch must become a streaming RPC — and the style guide has no
  convention for streaming yet.
- **Scheduler retry loop.** `workflow_resume.go:477-506` is linear: `Schedule()`
  once, assign, `return err`. Making a lost compare-and-set cheap to recover
  from needs a bounded schedule-and-assign loop with an in-request exclusion set —
  `workercache` will not know the Worker is taken until the watch event lands, so
  a naive retry hands back the same Worker. Also wants a new `ateattr` scheduler
  outcome alongside `SchedulerOutcomeNoFreeWorker`. Does not affect the API.
- **`CordonWorker` / `UncordonWorker`.** No current need. Note that
  api-style-guide.md §5.2 already models cordon as a plain field.
- **Single-pointer binding.** The cleanest fix for Actor/Worker coordination is
  to drop `Worker.assignment` and derive occupancy from an index over Actors —
  one pointer instead of two eliminates the divergence class. The cost is that
  `scheduling.go:107` gets occupancy in O(1) from `worker.GetAssignment() == nil`
  today; deriving it means `workercache` must index Actors, which means watching
  Actors, which does not exist. Recorded as long-term direction. The current
  two-pointer design with `actor_uid` guards on both sides works, and the
  denormalized copy is what keeps atenet off a hot-path lookup —
  `cmd/atenet/internal/router/ingress/ingress.go:114` reads
  `actor.GetWorkerAssignment().GetWorkerPodIp()` straight off the Actor with no
  Worker fetch.
- **Soft delete / finalizers.** api-style-guide.md §3.5 already carries a TODO
  for this. `DRAINING` covers part of the role.
- **Non-atomic two-sided assignment writes.** Pre-existing; the `actor_uid`
  guards are the reconciliation mechanism.

### Style guide gaps found

- §7 defines version guards only for Update (in `metadata`) and Delete (in
  `DeleteOptions`). Guards on custom methods are unspecified — moot here now
  that `DrainWorker` is the only custom method and takes none, but it will
  resurface.
- No convention for watch/streaming methods.
- §5.1 contradicts itself for nested enums: values "must not be prefixed", but
  the zero value "must be `{ENUM_NAME}_UNSPECIFIED`". The codebase resolves this
  by prefixing (`Actor.Status` uses `STATUS_ACTIVE`), which this document
  follows.

## Implementation plan

Sliced so each change compiles and keeps the existing tests green on its own.

1. **Proto surface — additive only. _Landed._** Five RPCs on `Control`
   (`GetWorker`, `CreateWorker`, `UpdateWorker`, `DeleteWorker`,
   `DrainWorker`), their request messages, a shared `DeleteOptions`, and
   `STATE_NOT_READY`. Every method is a stub returning `UNIMPLEMENTED`
   (`controlapi/worker_api.go`, pinned by `worker_api_test.go`). Nothing
   existing changes shape, so the ripple is zero.

   `AssignWorker` and `ReleaseWorker` were drafted here and then dropped before
   landing: the bind path has no out-of-process caller, so it stays in-process
   against the store. See "Assignment stays internal".

   The request messages address Workers by `ObjectRef`, which the store cannot
   serve yet — that is why the stubs return `UNIMPLEMENTED` rather than being
   wired up.

2. **Proto reshape. _Landed._** `Worker` gains `ResourceMetadata` and gives up
   its top-level `version`; `Assignment` becomes `ActorAssignment`;
   `WorkerAssignment` gains `ObjectRef worker`. Worker fields are numbered
   cleanly with no reserved tags: this is pre-1.0 and stored Workers are
   derived state, fully rebuilt by the syncer. `WorkerAssignment` is durable
   Actor state, so its new field is appended rather than renumbered.

   `WorkerAssignment.worker` is now the field to look a Worker up by. The
   remaining `worker_*` fields stay as a denormalized copy — that is what keeps
   atenet off a hot-path lookup (`ingress.go` reads `worker_pod_ip` straight off
   the Actor) — but nothing resolves a Worker through them. In particular, no
   caller re-derives the Worker's name from `worker_pod_uid`. Worker names are
   opaque: the syncer chooses them (it names Workers after the pod UID, in
   `workerKey.workerName`), and every other component carries the name it was
   handed. `ValidateWorker` no longer asserts `metadata.name == worker_pod_uid`
   and validates the name as an ordinary resource name.

   `MintCertRequest` was reshaped in the same slice: its three
   `worker_namespace`/`worker_pod`/`worker_pod_uid` fields collapse to a single
   `ObjectRef worker`, per api-style-guide.md §2.3 and §3.1. The atelet fills
   that ref from the worker Pod's attested certificate, which makes it the one
   sanctioned exception to the opacity rule — everywhere else a Worker name is
   carried, never reconstructed. Since the ref alone identifies the Worker,
   `authorizeActor` drops the namespace/pod cross-check that used to follow the
   lookup; the node check and the reciprocal actor↔worker assignment checks
   still carry the authorization. That reciprocal check is now a single
   comparison of `WorkerAssignment.worker.name` against the Worker's
   `metadata.name`.
3. **Store. _Partially landed._** Landed:
   - **Re-key by `name` (pod UID).** Both backends used to key on the composite
     `(worker_namespace, worker_pool, worker_pod)` — a Redis key format in
     `ateredis`, and actual primary-key *columns* in `atepg`. The five worker
     methods on `store.Interface` all take a single name now.

     The `atepg` `workers` table changed primary key. There is no migration
     framework in-tree, so existing dev databases must be dropped and recreated.

   Deferred to the slice that implements the RPCs, since nothing calls the API
   until then:
   - **Return the resource.** `CreateWorker`/`UpdateWorker`/`DeleteWorker`
     return bare `error`; §3.5 requires the mutated resource. `DeleteActor` and
     `DeleteAtespace` already do this — follow them.
   - **`DeleteWorker` reports absence** instead of succeeding silently, per the
     Method-semantics note above.
   - **Converge immutability.** `atepg` gains the `ip` and `worker_pod` checks
     `ateredis` already has.

   Independent of the RPCs, since the bind path is in-process:
   - **Occupancy compare-and-set** in both backends, per "Concurrency".
4. **Server.** Implement the five methods in
   `cmd/ateapi/internal/controlapi/` following the one-file-per-RPC convention.
   Move `resources.ValidateWorker` (`validate.go:214`) into the API layer, split
   into create-time and update-time (immutable-field, legal-transition) checks.
   Drop the `STATE_UNSPECIFIED` tolerance at `validate.go:265-267` — it is now
   unreachable.
5. **Syncer.** Migrate to the gRPC client. `isWorkerEligible` grows a
   Ready-condition check. Add the `NOT_READY` transitions. Convert
   sentinel-error branches to status codes.

   `workerKey` was already re-shaped in step 3: it swapped `pool` for `uid`, so
   a deleted pod and its same-named replacement no longer coalesce in the
   queue. It did *not* collapse to a single field — `namespace` and `name` stay
   because `reconcile` looks the pod up with `GetIndexer().GetByKey`, whose key
   is namespace/name.

   It **can** collapse to a bare UID string, and should here: add a `byUID`
   indexer in `WorkerPodInformer` (`informer.go` registers two already) and the
   key becomes a plain string. That also deletes the `pod.UID != key.uid` branch
   in `reconcile` — a UID index cannot return a pod with a different UID, so
   "pod deleted" and "pod replaced" merge into one not-found path. Nothing else
   needs namespace/name structurally: every other read has either the `pod` or
   the fetched store row in hand, and the remainder are log strings. Deferred
   out of the reshape slice only to keep it reviewable.
6. **Remaining in-process callers.** `crash.go`, `volumes.go`,
   `workflow_resume.go`, `workflow_suspend.go`, `workflow_pause.go`,
   `actoridentity.go`. All of them already do the one-argument lookup after
   step 3; what remains is moving their *reads* onto the gRPC client. Their
   bind and release writes stay on `store.Interface` — that is the whole of
   "Assignment stays internal" — so these call sites end up split across both,
   which is worth a comment where it happens. `MintCert` is the one that got
   materially simpler: it resolves by the attested worker ref with nothing left
   to cross-check (see step 2), and is read-only.
7. **`workercache`. _Landed._** Re-keyed from `namespace+":"+pod` to the
   Worker's resource name. No by-namespace/pod lookup remained, so no secondary
   index was needed.
8. **Downstream consumers of the reshaped `Worker`.** `kubectl-ate`
   (`workers.go:67`, `printer.go:83-88`, `:103-126`) and
   `demos/claude-code-multiplex/ui/server.go:374-386`.

### Test coverage

There **is** a store conformance suite —
`cmd/ateapi/internal/store/storecontract/contract.go`, run against both
`ateredis` and `atepg` — with eleven Worker cases: `GetWorker_NotFound:339`,
`CreateWorker_Success:349`, `CreateWorker_AlreadyExists:391`,
`UpdateWorker_Success:404`, `UpdateWorker_Conflict:450`, `DeleteWorker:480`,
`DeleteWorker_Idempotent:508`, `WatchWorkers_ClosedOnClose:517`,
`ListWorkers:602`, `ListWorkers_Empty:637`, `ListWorkers_Pagination:650`.
Extend it rather than starting fresh.

Cases the suite is missing, each corresponding to a step above:

- `UpdateWorker_ImmutableFields` — there is an `UpdateActor_ImmutableFields`
  (`contract.go:281`) but no Worker equivalent, which is how the `ateredis`/
  `atepg` divergence on `ip` survived.
- Occupancy compare-and-set under concurrent assign.
- `DeleteWorker` on an absent Worker — `DeleteWorker_Idempotent:508` asserts the
  *current* behavior and will need inverting.

`syncer_test.go` is the other existing safety net.

### Related work not scoped here

The `ACTIVE`-through-a-readiness-flap gap is a live bug independent of this
refactor: nothing reads `pod.Status.Conditions` or `pod.Status.Phase` today, and
`isWorkerEligible` only gates *creation*, so a Worker whose pod fails a probe
stays `ACTIVE` and keeps receiving Actors. Fixed here as a side effect of the
`NOT_READY` transitions; decide whether it also warrants a standalone fix first.

Adding `NOT_READY` changes what `recordEligibleWorkers` (`scheduling.go:113`)
measures. Worth confirming that is intended before it reaches dashboards.

The `ateredis`/`atepg` immutability divergence described under "Field
mutability" is a live inconsistency today, independent of this refactor. It is
fixed here as part of step 3, but it is a defect in its own right and could be
closed first with just the missing `atepg` checks and a contract case.
