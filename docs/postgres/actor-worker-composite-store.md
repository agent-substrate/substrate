# Composite actor–worker store operations

## Status

Proposed. Builds on the PostgreSQL store in
[`postgres-store.md`](postgres-store.md). Redis remains a supported backend.

## Summary

Actor-to-worker assignment and release today issue separate
`UpdateWorker` / `UpdateActor` (and often preceding `Get*`) calls through
`store.Interface`. That shape is forced by Redis Cluster: actor and worker keys
live in different hash slots, so they cannot be updated in one atomic action.

PostgreSQL has no such restriction. This document proposes composite store
methods so control-plane call sites stay backend-agnostic:

- on PostgreSQL, one transaction updates both rows (and notifies) in a single
  round trip;
- on Redis, the same methods run the existing two-step sequence and preserve
  today’s recovery behavior.

## Motivation

Redis Cluster allows `WATCH` / `MULTI` / `EXEC` and Lua, but only for keys that
map to the same hash slot. Substrate stores:

- actors as `actor:<atespace>:<name>`;
- workers as `worker:<namespace>:<pool>:<pod>`.

Those keys hash independently. A single Redis transaction or Lua script cannot
claim a worker and mark an actor `RESUMING` together. The package comment in
`ateredis` documents this:

```text
it is not possible to atomically mark an actor as scheduled on a worker,
and the worker as busy.
```

PostgreSQL can update both tables in one transaction. Exposing that only inside
`atepg`, while leaving workflows on separate calls, leaves the round-trip and
half-state costs in place for the Postgres path as well. Composite methods move
the join into the store layer without teaching `controlapi` about backends.

## Example: assign on resume

`AssignWorkerStep` currently does:

```text
UpdateWorker(worker with Assignment = actor)
UpdateActor(actor with Status = RESUMING and ateom_* filled)
```

See `cmd/ateapi/internal/controlapi/workflow_resume.go` (`AssignWorkerStep.Execute`).

### Redis (today and under a composite wrapper)

Minimum path:

```text
RTT 1: WATCH/GET/MULTI SET  worker:<ns>:<pool>:<pod>
RTT 2: WATCH/GET/MULTI SET  actor:<atespace>:<name>
```

Timeline with a failure between the writes:

```text
t0  UpdateWorker commits   → worker busy, actor still SUSPENDED/PAUSED
t1  crash, timeout, or actor version conflict
t2  UpdateActor never ran  → half-state
```

Resume already compensates by scanning for a worker whose assignment points at
the actor from a previous failed attempt. A Redis implementation of
`AssignActorToWorker` would still perform those two single-key operations; the
API gets cleaner, but atomicity does not improve.

### PostgreSQL composite

One transaction:

```sql
BEGIN;

UPDATE workers
SET version = ..., proto = ...
WHERE worker_namespace = $1
  AND worker_pool = $2
  AND worker_pod = $3
  AND version = $expected_worker_version
  -- plus assignment / immutability checks as today
RETURNING proto;

UPDATE actors
SET version = ..., status = ..., update_time = ..., proto = ...
WHERE atespace = $4
  AND name = $5
  AND version = $expected_actor_version
  AND status IN (SUSPENDED, PAUSED)
RETURNING proto;

SELECT pg_notify('worker-changes', $payload);

COMMIT;
```

Either both rows commit or neither does. Approximate cost:

```text
RTT 1: BEGIN + both UPDATEs + NOTIFY + COMMIT
```

There is no durable window where the worker is claimed and the actor is not yet
`RESUMING`. Worker notification is tied to the same commit (already true for
single worker writes in `atepg`).

### Comparison

| Property | Redis composite | PostgreSQL composite |
|---|---|---|
| Client round trips | ≥ 2 | 1 |
| Atomic across actor and worker | No | Yes |
| Crash between the two writes | Possible | Impossible after commit |
| Worker notify vs write | Publish after write; may fail independently | `pg_notify` in the same commit |
| Root cause | Cluster hash-slot rule | Ordinary multi-row SQL transaction |

The advantage is not that Redis lacks transactions. Redis has them. The advantage
is that multi-row atomic updates are legal in PostgreSQL for these entities and
illegal across Redis Cluster slots for the current key layout.

## Proposed interface

Add composite methods to `store.Interface` (names indicative):

```go
// AssignActorToWorker claims worker for actor and marks the actor RESUMING.
// Both resources use optimistic concurrency on the provided expected versions.
AssignActorToWorker(
    ctx context.Context,
    actor *ateapipb.Actor, expectedActorVersion int64,
    worker *ateapipb.Worker, expectedWorkerVersion int64,
) (*ateapipb.Actor, error)

// ReleaseActorFromWorker frees the worker assignment (if still owned by the
// actor) and clears actor placement fields / sets the terminal status carried
// on actor (e.g. SUSPENDED, PAUSED, CRASHED).
ReleaseActorFromWorker(
    ctx context.Context,
    actor *ateapipb.Actor, expectedActorVersion int64,
) (*ateapipb.Actor, error)
```

Exact signatures should match what the call sites already pass after local
mutation (status, snapshot fields, placement clears). Prefer returning the
persisted actor clone, consistent with `UpdateActor`. Worker return values are
optional; workflows primarily need the updated actor and rely on worker watch
for cache freshness.

### Backend behavior

| Backend | `AssignActorToWorker` / `ReleaseActorFromWorker` |
|---|---|
| `atepg` | Single transaction; map unique / version misses to existing sentinel errors; emit worker `NOTIFY` only if the worker row changed |
| `ateredis` | Sequential `UpdateWorker` then `UpdateActor` (assign), or get/update worker then `UpdateActor` (release), matching current ordering and errors |

Workflows must not branch on `--store-backend`.

## Call sites to migrate

| Location | Today | Becomes |
|---|---|---|
| `workflow_resume.go` `AssignWorkerStep` | `UpdateWorker` then `UpdateActor` (+ `GetActor` on conflict) | `AssignActorToWorker` |
| `workflow_suspend.go` `FinalizeSuspendedStep` | `GetActor` → `GetWorker` → `UpdateWorker` → `GetActor` → `UpdateActor` | `ReleaseActorFromWorker` (status `SUSPENDED`) |
| `workflow_pause.go` `FinalizePausedStep` | Same pattern as suspend | `ReleaseActorFromWorker` (status `PAUSED` / `CRASHED` when node missing) |
| `crash.go` `crashActor` / `releaseWorker` | `GetActor` → release worker → `UpdateActor` | `ReleaseActorFromWorker` (status `CRASHED`) |

`WorkerPoolSyncer.releaseActorOnDeadWorker` mainly updates the actor after the
worker is gone; it is a weaker candidate and can stay on `UpdateActor` unless a
later pass finds a clean fit.

## Recovery and semantics

- **Redis:** keep the resume “stale assignment” scan. Half-state remains possible.
- **PostgreSQL:** that recovery path should become rare after assign is
  transactional. Leaving it in place is fine and keeps one workflow for both
  backends.
- Optimistic concurrency stays. A version conflict still surfaces as
  `store.ErrVersionConflict` (or the current sentinel) so workflow retry loops
  continue to work.
- Ordering for Redis assign remains worker-first, then actor, so a crash still
  leaves a reclaimable worker assignment rather than an actor pointing at a free
  worker.

## Non-goals

- Making Redis Cluster actor/worker updates truly atomic (would require key
  co-location / redesign).
- Backend-specific branches in `controlapi`.
- Redesigning scheduling, locks, or the worker cache.
- Broader workflow transactionalization beyond assign/release.

## Implementation sequence

1. Add composite methods to `store.Interface` with Redis sequential implementations
   and shared contract tests covering success, version conflict, and missing
   worker/actor.
2. Implement transactional versions in `atepg`, including `NOTIFY` on worker
   mutation.
3. Switch resume assign and suspend/pause/crash release call sites.
4. Benchmark assign/release-heavy lifecycle paths on both backends; document
   that Postgres gains atomicity and fewer store RTTs while Redis behavior is
   unchanged aside from going through the new methods.
5. Update [`postgres-store.md`](postgres-store.md) once implemented (remove the
   “assignment is not a single database transaction” limitation).

## Open questions

- Whether `ReleaseActorFromWorker` should accept a missing worker as success
  (today’s finalize/crash paths often treat that as skip-and-continue).
- Whether assign should require the worker assignment to be empty, or also
  accept an existing assignment already owned by the same actor (idempotent
  retry).
- How much of finalize’s second `GetActor` refresh can move inside the
  composite method versus remaining a workflow concern for snapshot field
  assembly.
