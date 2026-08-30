# Store Policy

Rules for `ateapi`'s persistence layer: the backend-neutral contract in
`cmd/ateapi/internal/store` and its PostgreSQL implementation in
`cmd/ateapi/internal/store/atepg`.

Nothing here is new. It is the contract in `store.go`, the schema header comment
in `atepg/schema.go`, and a run of store-PR review positions, collected so that a
store change is reviewed against a written standard rather than reviewer memory.
Read it before adding a table, a column, a `store.Interface` method, or a
transaction.

**Non-goals.** Not a design document, a tuning guide, or an operations runbook.
The proto surface is governed by the [API style guide](../api-style-guide.md) —
sections [6.3/6.4](../api-style-guide.md#63-uid) and
[7](../api-style-guide.md#7-resource-freshness-and-optimistic-concurrency) are
load-bearing here and are not repeated. Go conventions:
[code style guide](../code-style-guide.md). Layout: [code layout](code-layout.md).
There is no watch over actors and no global revision; sections 1 and 4 are what
keep the option of adding one open.

## 1. Data model

**Proto is the source of truth.** Each resource row stores one marshaled proto in
a `bytea proto` column. A field is projected into its own column only where
PostgreSQL itself needs the value — identity, foreign keys, ordering, paging, or
an atomic concurrency check. Everything else stays in the proto.

**Every proto-bearing table carries `uid` and `version`.** They are projections
of the proto's `ResourceMetadata` and must agree with it: a read that finds them
out of step fails loudly instead of repairing itself
(`validateProtoMetadataMatchesColumns`).

**`version` is a per-object counter.** The writer that read the row increments it
(`setUpdateMetadata`). It is not a global revision, a commit order, or a watch
cursor. Do not add a sequence, an `xmin` projection, or a "global version" column
to make it one: that funnels every write through one hot row, and it is the
hardest decision here to walk back.

**`atespace` leads every actor-family primary key.** `actors`,
`actor_egress_policies`, `actor_templates`, `actor_snapshots`, and
`actor_snapshot_tags` all key on `(atespace, …)`. Atespace is the only viable
shard key this data model has, and a new actor-family table that does not lead
with it forecloses partitioning before we get to choose. Workers are the
deliberate exception: global-scoped, keyed by Kubernetes pod UID.

**Page tokens stay topology-free.** A token carries a format version, the list
method it was issued for, its scope, and the last row's ordering values — enough
to resume a keyset scan, nothing more (`atepg/pagetoken.go`). No offsets,
snapshot identifiers, replica identity, xids, or LSNs. A token must stay valid
against any replica and across a failover, and it is a public API string with a
size budget.

## 2. Write path

**Optimistic uid+version compare-and-set only.** Every update takes a
`store.Precondition` carrying both guards; both are required, and a missing one
is `ErrPreconditionRequired` rather than a blind write. Two shapes give that
guarantee. A lock-free path checks the guards against the row it read and
re-asserts both in the `WHERE` clause of the `UPDATE`, so zero rows affected is
`ErrVersionConflict` (`UpdateActor` in `atepg/atepg.go`). A path that reads
`SELECT … FOR UPDATE` checks the precondition against the locked row and gets the
same guarantee from the lock (`UpdateWorker`). Either is acceptable; an update
that does neither is a blind write. `version` guards the lost update; `uid`
guards name reuse across lifecycles; neither substitutes for the other. No update
path may be last-writer-wins.

**Deletes guard differently, on purpose.** `store.DeletePreconditions` guards are
independently waivable — the zero value pins nothing, which is what an unguarded
delete wants. A delete that must act on one incarnation passes `UID`; one that
must act on one revision passes `Version`. A delete still reads `FOR UPDATE` so
the incarnation the guard was checked against is the one removed
(`DeleteWorker`), and a delete that touches a second row is still one
transaction.

**Auto-retry only on an explicit precondition failure.** The store runs `mutate`
once per attempt and surfaces the conflict. A caller that retries must re-read
and rebuild *all* of its intent from the state it lost to, not just the row that
conflicted: the assignment loop in `controlapi/workflow_resume.go` refreshes the
actor but rescans a stale worker cache, so it can claim a second worker and
strand the first. That is a known defect, not the pattern to copy. Never retry
`ErrUIDConflict` — the name now addresses a different incarnation, so no retry
can resolve it. Never retry a `mutate`'s own error; it is returned verbatim so
the caller can tell the cases apart.

`store.Interface` still reserves internal retry for backends: its update comments
say `mutate` may run more than once and that `ErrVersionConflict` can mean an
exhausted retry budget. No `atepg` path implements such a retry, and no caller
may depend on one. The interface comment should be narrowed to match the
backend, not the backend widened to match the comment.

**Cross-row invariants live in one store transaction.** An operation that must
leave two rows consistent — binding a worker to an actor is the canonical case —
belongs in a single `store.Interface` method with one transaction. Two calls from
the workflow layer leave a window in which a crash, a deadline, or a lost CAS
strands the first row; the claim-then-transition sequence in `workflow_resume.go`
is the known instance, tracked rather than endorsed. **Reviewers reject new
two-call claim/release patterns on sight.** A store primitive is also testable by
the contract suite, where the atomicity can actually be asserted.

**The actor lease guards atelet side effects, not store consistency.**
`AcquireLease` is a TTL row in `leases` that stops two replicas driving the same
sandbox through a resume or suspend. It is not what makes a multi-row write
correct and must not become that: TTL leases lapse under load, clock skew, and a
slow database; a CAS does not. An invariant that holds only while a lease is held
is not an invariant.

**No Go work under a row lock.** `UpdateWorker` holds `SELECT … FOR UPDATE`
across unmarshal, `mutate`, marshal, and the outbox insert, because the
assignment predicate lives inside the opaque proto and cannot be pushed into a
`WHERE` clause. That is what makes an occupancy test inside `mutate` a
compare-and-set, and it is the ceiling on worker write throughput. The lock scope
must not grow: no RPC, no cache lookup, no second table's read-modify-write, no
extra caller-supplied closure. New code should prefer the lock-free actor shape —
read, mutate, CAS `UPDATE`.

## 3. Projected-column checklist

A new column beside `proto` is the change most often regretted. Each one must:

1. **Name the query it serves** — a `WHERE`, `ORDER BY`, `JOIN`, or constraint
   the store issues today. "We will probably need it" is not a query.
2. **Be written in the same statement as `proto`, under the same CAS guard.** No
   second `UPDATE`, trigger, or backfill job that can lag. The projection and the
   proto may only ever be wrong together.
3. **Carry no index without a HOT/WAL benchmark.** The workload is whole-row
   replace; an index on a projected column can turn a HOT update into a non-HOT
   one and adds WAL on every write to that row.
4. **Not exist for observability.** Reporting, debugging, and metrics are served
   from the proto, the outbox, or an offline query — never a write-path tax for
   something no writer reads.
5. **Survive an N-1 binary** (section 5).

The same test applies to a column duplicating a value already in the proto: the
store treats disagreement as a hard failure, so each one is a new way to fail.

## 4. Outbox

**The outbox is a cache, never a source of truth.** `worker_outbox` is UNLOGGED
and range-partitioned by `created_at`; maintenance drops expired partitions and
records a trim high-water mark. It exists so watchers can avoid polling
`workers`, which stays authoritative: a watcher that falls behind the trim mark,
or sees the postmaster restart, closes its channel to force a resync. No data may
live only in the outbox, and no consumer may treat it as a durable log.

The poll is fenced behind `pg_snapshot_xmin(pg_current_snapshot())` so no
in-flight transaction's row can appear after the cursor has passed its xid. That
fence is also why any long-running transaction — an operator's `psql` session, a
migration — stalls delivery on every replica. Run DDL outside the serving window.

The payload is one event-type byte plus the marshaled proto, and that byte is
read by other replicas mid-rolling-deploy, so the values are append-only stable
like any persisted enum (section 6).

**Any future watch defines its own `xid8` cursor** over its own outbox. Do not
reuse per-object `version` as a cursor, and do not add a global revision to serve
one.

## 5. Schema evolution

- **Forward-only in production.** No automatic down migration; a bad migration is
  fixed by rolling forward.
- **Additive and expand-only.** Add a nullable or defaulted column, a table, an
  index. Do not rename, retype, or drop in place. Contract only once every binary
  that reads the old shape is gone.
- **The old binary must read the new schema.** N-1 and N serve simultaneously
  during a rolling upgrade, so N-1 has to keep working against the schema N
  installed. This is the rule that makes `SELECT proto` plus a few pinned columns
  preferable to a wide projected row.
- **The N-1 round-trip must be lossless.** A read-modify-write by an N-1 binary
  must not destroy state written by N. Proto unknown-field preservation gives
  this for free inside `proto` — the strongest reason to keep state there rather
  than in columns. Never round-trip a stored proto through JSON: `protojson`
  cannot represent unknown fields and drops them silently.
- **Migrations own the schema once the migration baseline lands.** Until then
  `atepg/schema.go` is idempotent DDL applied at startup. Either way, the rules
  above are what a schema change is reviewed against.

## 6. Proto compatibility is a storage rule

`proto` is stored verbatim, so every proto change is a storage migration.

- **Never reuse a field tag.** Removed tags are `reserved`, permanently; a reused
  tag decodes stored bytes into the wrong field.
- **Retyping or removing a field requires rewriting every stored row**, not just
  a proto edit. Budget it as a migration.
- **Enums are add-only.** A value that has ever been persisted can never be
  renumbered or removed, and readers must tolerate values they do not know.
- **Unknown fields must survive a read-modify-write** on every path that reads
  and rewrites a stored proto.

There is no `docs/api_changes.md` yet. Until there is, this section plus
[section 7 of the API style guide](../api-style-guide.md#7-resource-freshness-and-optimistic-concurrency)
is the rule.

## 7. Testing bar

- **`storecontract` is the contract.**
  `cmd/ateapi/internal/store/storecontract/contract.go` holds backend-neutral
  assertions, run against `atepg` from `atepg/contract_test.go`. Behavior callers
  rely on but the suite does not assert is not part of the contract: add the
  assertion in the same PR as the behavior.
- **Backend tests test the backend.** `atepg/atepg_test.go` covers what is
  specific to PostgreSQL: foreign-key error mapping, page-token shape, lease
  expiry and takeover.
- **Concurrency is tested by mutate injection, not sleeps.** The
  read-modify-write closure is the injection point: the test issues the competing
  write from inside `mutate`, making the interleaving deterministic. See
  `TestUpdateActor_ConcurrentWriteReturnsConflict`,
  `TestUpdateActorTemplate_ConcurrentWriteReturnsConflict`, and
  `TestUpdateActorSnapshotTag_CASPreventsDeleteRecreateABA` in
  `atepg/atepg_test.go`. Every new CAS path gets one.
- **The N-1 round-trip is asserted, not assumed.** A `storecontract` case must
  show that a stored proto carrying fields the running binary does not know
  survives a read-modify-write intact. No such case exists today; the first
  change that leans on the guarantee writes it.
- **Database tests must run in CI, not skip.** The fixtures skip when the
  PostgreSQL testcontainer is unavailable — right on a laptop without Docker,
  wrong in CI, where a green run with zero store tests has already happened. A
  skip in CI is a failure; do not add a fixture that can skip silently.

## 8. Operational limits

The honest v1 envelope. A description of what has been built, not a target:

- One tuned primary (8-16 vCPU), 2-4 `ateapi` replicas.
- Order 10^4-10^5 workers, 10^6 actor rows, 10^3 lifecycle operations per second.
- A cold resume is about five writes over four or five transactions. Isolated
  `UpdateWorker` throughput is a microbenchmark, not a system number.
- **The first ceiling is not PostgreSQL.** It is the O(fleet) scan in ateapi
  scheduling: each resume walks the whole worker cache. Scheduling cost should be
  O(candidates); a change that adds another full-fleet pass is a regression.
- **In-cluster PostgreSQL is for development and evaluation.** Production means
  managed PostgreSQL with backups and point-in-time recovery. There is no backup
  path for the StatefulSet today, and an empty database comes back reporting
  healthy.

## PR checklist for store changes

- [ ] New table stores its proto in `bytea` and carries `uid` + `version`; an
      actor-family primary key leads with `atespace`.
- [ ] Each new projected column names the query it serves, is written in the same
      statement under the same CAS guard, and any index has a benchmark.
- [ ] Every update requires a uid+version precondition and enforces it, either by
      re-asserting both guards in the `WHERE` clause or by checking them against
      a row read `FOR UPDATE`. No blind writes.
- [ ] A new delete states which guards it pins and which it waives, and reads the
      row it deletes `FOR UPDATE`.
- [ ] Retries happen only on an explicit precondition failure, after re-reading
      everything the attempt was built from. No retry on `ErrUIDConflict`.
- [ ] Any invariant spanning two rows is one store method and one transaction —
      not two calls from `controlapi`, and not a lease.
- [ ] No RPC, cache read, or extra closure runs inside a `FOR UPDATE`
      transaction.
- [ ] Schema change is additive, forward-only, and readable by the N-1 binary.
- [ ] A change relying on N-1 round-trip losslessness ships the `storecontract`
      case that asserts it.
- [ ] No field tag reused, no enum value removed or renumbered; a retype or
      removal ships with a row rewrite.
- [ ] New contract behavior asserted in `storecontract`; new CAS paths have a
      mutate-injection test; no new fixture that can skip silently.
- [ ] Page tokens carry no topology.
