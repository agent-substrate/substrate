# Admin & List Operations

This page covers the storage operations that are **not** on the actor
lifecycle critical path: bulk listing, debug utilities, and the
implicit worker-record bookkeeping driven by the Kubernetes informer.

They are labelled "non-critical" because they do not directly drive an
actor's state machine. However, one of them — `ListWorkers` — is
**called on the critical path** of every `ResumeActor`, and that
crossing is the most important thing this document exists to surface.

See [`actor-lifecycle.md`](./actor-lifecycle.md) for the operations that
*are* on the critical path.

## The cluster-mode SCAN constraint

Every list operation in Valkey Cluster has the same shape: a key range
cannot be scanned cluster-wide in one command. You have to ask each
primary separately and merge results client-side. The codebase uses
`s.rdb.ForEachMaster(ctx, fn)` to fan out, then runs `SCAN` against
each primary's connection. Three implications colour every list
operation:

- Per-shard `SCAN` is non-atomic and may return duplicates or miss
  recently-modified keys. The proto comments on `ListActorsRequest`
  acknowledge this explicitly: *"actors may be missed or duplicated if
  the system state changes during pagination."*
- Cross-shard ordering is undefined. Callers that need a stable order
  must impose it themselves.
- Adding a primary mid-list invalidates any in-flight cursor — the new
  primary will not be visited, its keys will not appear in this call.

These constraints fall out of Valkey Cluster's design and are not
solvable in the application layer. They drive the soft-semantics
contract on every list method.

## `ListActors` — paginated, well-formed

<details>
<summary><em>Expand</em></summary>

See `cmd/ateapi/internal/store/ateredis/ateredis.go`, function
`ListActors`.

The implementation does the right things:

- Sorts primaries by a stable address hash so pagination order is
  deterministic across calls.
- Walks one primary at a time, advancing per-shard `SCAN` cursor until
  exhausted, then moves to the next primary.
- Encodes `(shard_hash, cursor)` into an opaque page token so the next
  call resumes exactly where the previous one stopped.
- Honours `pageSize` as a target ceiling per call (Valkey's `SCAN COUNT`
  is a hint, not a hard cap, so the actual page can drift slightly over).
- Issues a `GET` per key after the `SCAN` returns the keys for a page.

**Cost per page**: 1 `SCAN` call + `pageSize` `GET` calls (serial) on a
single shard's connection. At a `pageSize` of 100, that's roughly 100 ms
of round-trip-bound latency per page on intra-cluster mTLS — fine for a
UI-style listing.

**Risks**:

- The per-key `GET` is serial. A pipelined or `MGET`-equivalent could
  cut the wall-clock by an order of magnitude. Worth it if list pages
  ever become user-facing critical-path.
- Soft semantics: a user paginating through 1 M actors will *miss*
  actors that are deleted during the pagination and may *see twice*
  actors whose keys are touched during pagination. This is documented
  on the request proto but worth surfacing in operator-facing tooling.
- A primary failover mid-pagination invalidates the cursor (cursors
  are per-connection in Valkey). The next page request will get an
  error from the affected shard's cursor and start that shard from
  the beginning, double-returning keys at the head.

</details>

## `ListWorkers` — unbounded, and on the critical path

<details>
<summary><em>Expand</em></summary>

See `cmd/ateapi/internal/store/ateredis/ateredis.go`, function
`ListWorkers`.

**This is the most operationally significant function in the storage
layer.** It is invoked by `AssignWorkerStep` inside every `ResumeActor`
workflow (see [`actor-lifecycle.md`](./actor-lifecycle.md) — Transition
2). Its cost directly determines the floor on actor-resume latency.

Implementation properties:

- **No pagination.** Reads every worker key in the cluster on every call.
- **Fans out to every primary** via `ForEachMaster`. The fan-out is
  sequential, not parallel (one primary at a time inside the callback).
- **Serial `GET` per key** with the default `SCAN COUNT` (10). At
  N workers, this is roughly N/10 `SCAN` calls + N `GET` calls per
  invocation.
- Returns the full list as one slice in memory. At 100 k workers and
  ~200 bytes per Worker proto serialized, that is ~20 MB transferred,
  parsed, and held per call.

**Cost model**:

| Workers | SCAN ops | GET ops | Round trips | Approx wall-clock (1ms/op) |
|---|---|---|---|---|
| 100   | ~10    | 100     | ~110    | ~110 ms |
| 1 000 | ~100   | 1 000   | ~1 100  | ~1.1 s |
| 10 000 | ~1 000 | 10 000 | ~11 000 | ~11 s |
| 100 000 | ~10 000 | 100 000 | ~110 000 | **~110 s** |

These numbers are storage-tier latency floors. They are **paid on every
`ResumeActor` call**, before atelet is even contacted. The <10 ms
whole-path budget for the state machine is unreachable at any
non-trivial worker count under the current implementation.

**Mitigation options**, in roughly increasing order of effort:

1. **Pipeline the per-key `GET`s.** Single most impactful per-line-of-code
   change. Cuts wall-clock by 5–10× by amortizing TLS round-trips
   without touching the API or the SCAN strategy.
2. **`MGET` per shard.** Even better — collapses N `GET`s into one
   `MGET` per shard chunk. Works because the keys returned by a single
   shard's `SCAN` are by definition co-located on that shard.
3. **Pool-scoped secondary index.** Each worker has a known `WorkerPool`.
   Maintain a Valkey set per pool (`pool:<ns>:<name>` → set of worker
   keys); `AssignWorkerStep` reads only the set for the relevant pool.
   Cuts read volume from "all workers" to "workers in the target pool"
   — typically 1–2 orders of magnitude smaller. Costs a write
   amplification (every CreateWorker / DeleteWorker also touches the
   set) and the set must hash to the same slot as the workers (hash
   tags required).
4. **In-process worker cache fed by the K8s informer.** The
   `WorkerPoolSyncer` already watches worker pods via the Kubernetes
   informer. The same data could be cached in the API server process,
   serving `AssignWorkerStep` from memory in microseconds, with the
   informer keeping the cache fresh. Removes `ListWorkers` from the
   critical path entirely. Cost: an extra source of truth to keep
   consistent with the Valkey worker records.
5. **Reverse the read direction.** Today, `AssignWorker` reads all
   workers and filters for free ones in the right pool. A
   `freeWorkers` set per pool (workers self-add when they become free,
   actors atomically `SPOP` to claim one) collapses worker assignment
   to two `O(1)` operations. Requires reworking the worker-claim
   protocol significantly.

Recommended near-term path: (1) + (2) for immediate relief, then (4)
for the structural fix when scale demands it. (3) and (5) are larger
designs that should be evaluated together.

</details>

## `GetActor`, `GetWorker` — point reads

<details>
<summary><em>Expand</em></summary>

Single key `GET` per call, single shard, no fan-out. These are the
cheapest operations in the storage tier: roughly 0.5–1 ms each on
intra-cluster mTLS. They are used liberally throughout the lifecycle
workflows (see the per-transition round-trip tables in
`actor-lifecycle.md`).

No risks beyond the standard cluster-mode failure modes (primary loss
→ shard unavailable for a few seconds; whole-shard loss →
cluster-wide stop with `cluster-require-full-coverage yes`). Covered
in `topology.md` / forthcoming `failure-modes.md`.

</details>

## `CreateWorker`, `UpdateWorker`, `DeleteWorker` — informer-driven

<details>
<summary><em>Expand</em></summary>

The `WorkerPoolSyncer` (see
`cmd/ateapi/internal/controlapi/syncer.go`) watches the pod informer
in Kubernetes and reconciles the Valkey worker records:

- `CreateWorker` on first sight of an eligible pod (`SET NX`-style).
- `UpdateWorker` on IP change (CAS-protected; the in-code comment
  notes this path is not believed to be reachable in practice).
- `DeleteWorker` on pod deletion or `DeletionTimestamp` set.

These are **not** triggered by client API calls; they fire whenever the
informer surfaces a pod event. At 100k workers, pod churn during
deploys, autoscaler activity, and node maintenance can produce
sustained CreateWorker/DeleteWorker traffic. The dominant cost is the
storage CAS for `UpdateWorker` (rare) and the `DEL` for `DeleteWorker`
(cheap).

**Concurrency note**: `DeleteWorker` is paired with
`releaseActorOnDeadWorker`, which mutates the actor record bound to
the dying worker. The race analysis for that pairing is in
[`actor-lifecycle.md`](./actor-lifecycle.md) under "Implicit
transition: syncer-driven RESET".

</details>

## `DebugClearAll` — test fixture, not production

<details>
<summary><em>Expand</em></summary>

`DebugClearAll` (in
`cmd/ateapi/internal/store/ateredis/ateredis.go`) calls
`FlushAllAsync` on every primary via `ForEachMaster`. It exists for
test setup and is referenced by the storetest harness. There is no
runtime guard against calling it in production; if exposed via an API
surface it would be catastrophic.

The function is package-public and reachable from any code that holds
a `*Persistence`. The handbook recommendation is to either rename it
to make the production-danger explicit (`DebugClearAll_TESTONLY`) or
to add a build-tag guard. Until then, treat the symbol's reachability
as a known sharp edge.

</details>

## `AcquireLock` / `ReleaseLock` — primitive, used by the lifecycle

<details>
<summary><em>Expand</em></summary>

Implementation summary:

- `AcquireLock` is `SET NX EX` with a caller-supplied token.
- `ReleaseLock` is a Lua CAS script (read token, compare, conditional
  delete). Safe under the "lock expired and was re-acquired by another
  caller" scenario — the script will not delete a lock held by a
  different token.
- Single key, single shard per lock. No cluster-wide coordination.

These are primitives consumed by the actor lifecycle (lock key
`lock:actor:<id>`). They are not directly exposed to API clients but
are recorded here because they are part of the storage-tier surface
area.

**Risk**: there is no fencing token. A caller that holds the lock,
stalls long enough for the TTL to expire, then attempts a state
mutation will have its mutation rejected by the version-CAS on the
actor record — but only if the actor's version has advanced. In the
narrow window where the lock has expired but no one else has claimed
it (and no one else has updated the actor), a stalled caller can
write through. The actor's version-CAS catches the common case; a
true fencing-token discipline would close the remaining gap. Worth
revisiting if the lock is ever extended to longer-running operations.

</details>

## Summary: which admin operations touch the critical path

| Operation | On critical path | Cost (current impl) | Recommended action |
|---|---|---|---|
| `GetActor`, `GetWorker` | yes (many places) | ~1 ms | nothing |
| `CreateActor` | yes (creation) | ~2 ms (one extra GET) | could drop the post-create GET |
| `UpdateActor`, `UpdateWorker` | yes (every transition) | ~2 ms uncontended | see contention notes in lifecycle |
| `DeleteActor` | yes (deletion) | ~2 ms | nothing |
| `ListActors` | no (user-facing UI) | ~100 ms / page of 100 | pipeline / MGET if it ever moves to critical path |
| `ListWorkers` | **yes** (every ResumeActor) | **O(N) round trips** | **fix this before scaling — pool index or in-process cache** |
| `CreateWorker`, `DeleteWorker`, `UpdateWorker` (syncer) | no (informer-driven) | ~1–2 ms | nothing |
| `DebugClearAll` | no (test only) | catastrophic if called | rename or guard |
| `AcquireLock`, `ReleaseLock` | yes (Resume / Suspend) | ~1 ms each | add fencing if locks ever extend |
