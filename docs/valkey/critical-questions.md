# Critical Questions

This page tracks **open architectural decisions** specific to the storage
tier — questions where the team knows a choice will need to be made, but
the decision is not yet final. It is deliberately distinct from two
nearby concepts:

- The **risk register** (in [`topology.md`](./topology.md) and
  [`actor-lifecycle.md`](./actor-lifecycle.md)) catalogs operational
  hazards in the *current* design — things that are already true and
  may bite us. Critical questions are about decisions we have not
  made yet.
- **Upstream RFCs** (GitHub issues tagged RFC, e.g. issue #12, #135)
  are heavyweight design proposals with broad scope. A critical
  question entry is lighter weight: it names a choice, sketches the
  design space, and points at the constraints. Once a question grows
  large enough to warrant a real proposal, the entry's resolution
  links to the RFC.

## Entry lifecycle

Each entry below carries a **Status**:

- **OPEN** — under consideration. Design space is described; no
  binding choice has been made.
- **DECIDED** — a choice has been made. The entry retains its
  history but is annotated with the date, the deciding artifact
  (PR / issue / handbook page), and which design-space option was
  selected. Kept on the page so the reasoning is discoverable from
  the storage handbook, not buried in commit history.
- **DEFERRED** — explicitly punted, with a brief note on the trigger
  that would re-open it (e.g. "revisit if per-resume p99 exceeds
  20 ms").

## When to add an entry

Add a new entry when both are true:

1. The decision has more than one defensible answer.
2. The choice will affect more than one handbook page (topology,
   lifecycle, admin-operations, or a future failure-modes /
   latency-budget page).

If neither is true, the right home is probably an inline TODO in the
relevant handbook page or an upstream issue. If only condition 2 is
true (one obvious answer, multi-page consequence), capture the
guidance directly in the affected pages.

---

## Q-1: Can Valkey cluster mode support locality-aware scheduling?

<details>
<summary><em>Expand entry</em></summary>

- **Status:** OPEN
- **Tags:** scheduling, locality, sharding, cluster-mode, fallback
- **Related:** upstream issues
  [#119](https://github.com/agent-substrate/substrate/issues/119),
  [#52](https://github.com/agent-substrate/substrate/issues/52),
  [#44](https://github.com/agent-substrate/substrate/issues/44),
  [#47](https://github.com/agent-substrate/substrate/issues/47),
  [#198](https://github.com/agent-substrate/substrate/issues/198),
  [#212](https://github.com/agent-substrate/substrate/issues/212);
  [`topology.md`](./topology.md) — cluster-mode constraints and sizing;
  `docs/architecture.md` (data-locality section);
  `docs/roadmap.md` (data locality in scheduling);
  [Q-2](#q-2-can-valkey-cluster-mode-tolerate-hot-partitions-and-serve-flat-keyspace-queries)
  — secondary-indexing substrate (any locality index inherits Q-2's
  hot-shard and cross-key consistency concerns);
  [Q-3](#q-3-can-valkey-cluster-mode-support-label-based-worker-matching)
  — locality is a special case of label-based matching, so a Q-3
  answer may subsume this one.

> *Assume the current O(N) worker-listing pattern is fixed by the time
> this question matters; that fix is a tactical concern, not an
> architectural one. The question this entry exists to answer is whether
> distributed-Valkey-cluster-mode is a viable substrate for the
> scheduling model we ultimately want.*

### Why this matters

The proposed state model (upstream issue #119) introduces a `PAUSED`
state whose entire purpose is **locality**: keep snapshots on the node
where the actor was running so that resume is fast and the cluster's
NIC budget is not saturated re-uploading hot snapshots. Cache-aware
routing (originally discussed in upstream issue #52, since merged into
#119) is the read-side companion: route an actor's resume to a worker
on a node that already holds its snapshot.

Cluster-mode Valkey imposes structural constraints — hash-slot
partitioning, no cross-slot atomicity, shard-local availability
semantics — that may or may not support a locality-aware scheduler at
1B-actor / 100k-worker scale (see [`topology.md`](./topology.md)). The
question is **architectural, not implementation-level**: does cluster
mode have the data-model and consistency primitives we need, and what
does the fallback path look like when the primary mechanism misses or
is unavailable?

Answering this is a precondition for committing to either the PAUSED
state or cache-aware routing. If cluster-mode Valkey cannot reasonably
hold the snapshot-locality index, the locality story belongs on a
different substrate (in-process cache, separate service, or a
different persistence backend entirely). If it can, but with caveats,
the caveats become hard design constraints on every other piece of
the scheduler.

<details>
<summary><strong>What we know</strong></summary>

**About cluster mode** (from [`topology.md`](./topology.md)):

- 16,384 hash slots are partitioned across primaries. A key's home
  slot is `CRC16(key) mod 16384`.
- **Cross-slot atomicity is forbidden.** A single Valkey operation
  (multi-key command, transaction, Lua script) cannot touch keys in
  different slots. Hash tags (`{tag}` substring of a key) override
  the slot calculation and force co-location, at the cost of skewing
  distribution toward whichever shard owns the tag's slot.
- **`cluster-require-full-coverage yes`** (currently set) means
  losing any single shard takes down the entire cluster's writes.
  Flipping to `no` lets surviving shards keep serving their slot
  ranges at the cost of per-slot availability instead of all-or-nothing.
- Replica failover takes seconds (`cluster-node-timeout` 5 s plus
  election plus client slot-map refresh), during which the affected
  slot range is unavailable.
- Cluster-bus gossip cost grows roughly O(N²) in primary count; the
  practical comfort ceiling is widely cited at ~500 primaries.

**About the locality model:**

- A snapshot lives in object storage indefinitely (no Substrate-side
  cleanup today). When materialized for use, it is also cached on
  the node where the actor is running.
- The mapping `snapshot_id → {nodes currently caching it}` is the
  data the scheduler needs. Nothing in the current data model
  stores this; it would be net new.
- That mapping changes whenever a snapshot is created on a node,
  evicted from a node cache, or moved between nodes. Eviction is
  driven by node-local pressure (storage, memory, restart) and is
  not centrally coordinated today.
- Worker pods can be rescheduled by Kubernetes; the node a worker
  pod runs on is not stable over the pod's lifetime.

**About scale:**

- At target scale (1 B actors, see [`topology.md`](./topology.md)),
  even a modest 10–20 % of actors carrying a recently-cached
  on-node snapshot puts the index at **100 M – 200 M entries**. The
  Valkey keyspace handles that easily across 50–200 primaries; the
  question is what *kinds* of access patterns those keys see.
- A "popular" snapshot — e.g. the golden snapshot for an
  ActorTemplate used by many actors — could be cached on hundreds
  or thousands of nodes simultaneously. A single set-of-nodes
  member can be effectively unbounded.

</details>

<details>
<summary><strong>Design space</strong></summary>

Two intertwined sub-questions: **(L)** where does the locality
mapping live, and **(R)** what's the fallback when locality misses or
is unavailable. The answer is almost certainly an L + R pair.

**L: where does the locality data live**

- **L-A. Cluster-stored, sharded by snapshot ID.** `loc:<snapshot_id>`
  is a set of node IDs currently caching that snapshot. Scheduler
  does an O(1) `SRANDMEMBER loc:<snapshot_id>` to find a candidate
  node, then picks a free worker on that node. Property bought:
  cluster-wide consistent view of locality, accessible from any API
  server. Cost: every snapshot cache event (create / evict / move)
  is a write to a *different* shard than the actor record — so the
  actor-state and index-state writes cannot be made atomic, and we
  accept some best-effort window of disagreement.
- **L-B. Cluster-stored, sharded by node.** `nodecache:<node_id>`
  is a set of snapshot IDs cached on that node. To find candidate
  nodes for a snapshot, the scheduler would have to scan every
  `nodecache:*` set — O(nodes) reads per scheduling decision. Worse
  on the read side, simpler on the write side. Probably not viable
  at target scale; included for completeness.
- **L-C. Cluster-stored, co-located via hash tags.** Force the
  index and the relevant actor record onto the same shard by hash
  tag (`actor:{<snapshot_id>}:<actor_id>` and `loc:{<snapshot_id>}`).
  Allows atomic actor + index updates and removes cross-key
  consistency from the table. Cost: distribution skew — popular
  snapshots hot-spot the shard their tag hashes to, and the snapshot
  ID becomes a load-balancing dimension whether we want it to or
  not.
- **L-D. No separate index — hint in the actor record itself.**
  The actor's existing `AteomPod*` fields are retained across
  SUSPEND/RESUME as a *preference* rather than cleared. Resume
  reads the actor, attempts the hinted pod, falls back on miss.
  Property bought: zero new index, zero cross-key writes, fits in
  the existing actor GET. Cost: the hint is only as fresh as the
  last assignment — no awareness of snapshot eviction or node
  rescheduling between then and now.
- **L-E. Index lives outside Valkey.** API-server in-process cache
  fed by atelet heartbeats, or a separate locality service.
  Decouples the locality data path from Valkey entirely. Cost: a
  second source of truth to keep consistent; in-process variants
  lose state on API-server restart unless persisted somewhere.

**R: random-fallback path when L misses or is unavailable**

- **R-A. Random shard, random worker.** Pick a random shard from
  the slot map, then pick a random free worker on that shard.
  Requires that workers expose themselves shard-locally (e.g. a
  per-shard `workers` set written by the syncer, or an external
  registry). O(1), zero locality. Reasonable as a fallback; not
  reasonable as a primary mechanism unless locality is genuinely
  unwanted.
- **R-B. Pool-aware random.** Random *within the target pool* only.
  Same mechanics as R-A but restricted to the relevant worker pool.
  Required as soon as pools have different shapes (CPU class,
  image, runtime), which they will.
- **R-C. Park-and-retry** (per upstream issue #52, step 3). When
  locality misses, *hold* the request rather than falling back to
  random, betting that a locality-matching worker will free up soon.
  Trades availability for locality preservation. Probably useful as
  a top-up policy layered on R-B, not as a primary fallback.

</details>

<details>
<summary><strong>Open subquestions</strong></summary>

1. **Cross-key consistency.** Under L-A, snapshot cache updates and
   actor-state updates touch different shards and cannot be made
   atomic. Can the system tolerate brief windows where the actor is
   RUNNING but the index does not yet reflect the assignment, or
   vice versa? If yes, the simpler design works. If no, the answer
   forces L-C (hash-tag co-location, accepting skew) or a
   reconciliation protocol.
2. **Availability under shard loss.** If the shard holding the
   locality index (under L-A or L-C) or the shard holding a
   per-shard worker registry (under R-A) has a failover or total
   loss, what happens? Under `cluster-require-full-coverage yes`
   (current), all scheduling stops cluster-wide. Under `no`,
   scheduling continues with degraded behavior for the affected
   slot range. Which mode are we committing to, and what's the
   user-visible contract during a shard outage?
3. **Popular-snapshot hot keys.** A golden snapshot cached on
   hundreds of nodes is a single set with hundreds of members.
   `SRANDMEMBER` is O(1) per pick, but every node-cache update is a
   write to that same set. Does the hot-key write rate survive
   cluster mode's single-threaded primary execution? At what fan-in
   does it stop?
4. **Eviction staleness.** When a node evicts a snapshot from its
   cache, what is the acceptable lag before the index reflects that
   eviction? Stale entries cause sniper-shot misses (acceptable —
   fallback covers them), but at some rate they degrade
   scheduling quality enough to be measurable. What's the budget?
5. **K8s pod rescheduling.** Worker pods can be moved between nodes
   by Kubernetes (eviction, node failure, deploys). When a pod
   moves nodes, the on-node snapshot cache does *not* follow.
   Does the locality index key on node ID (cache survives pod
   reschedule, pod-to-node mapping must be looked up) or on pod ID
   (cache invalidated on pod reschedule, no extra lookup)? Each
   choice has materially different invalidation semantics.
6. **Index size at scale.** 100–200 M entries spread across 50–200
   primaries works for the keyspace. But does it survive
   `BGSAVE` / `BGREWRITEAOF` fork latency and full-resync time
   bounds on individual primaries as per-shard memory grows? This
   feeds back into [`topology.md`](./topology.md)'s primary-count
   sizing.
7. **Fallback observability.** Without a metric for "fraction of
   scheduling decisions that hit the fallback path," locality
   regressions are invisible. What metric primitives need to be in
   place before any L-A / L-C design ships?
8. **Whether Valkey is the right substrate at all.** If the answers
   to (1)–(7) push us toward L-E (index outside Valkey), the
   locality story decouples from the persistence-backend decision.
   Valkey may still be fine for actor records but not for the
   locality index. That changes the broader storage evaluation.

</details>

<details>
<summary><strong>What would change in the handbook</strong></summary>

- [`topology.md`](./topology.md) — sizing math grows by the index
  size and access pattern if the answer is L-A / L-B / L-C. If the
  answer is L-D or L-E, sizing is unaffected.
- [`actor-lifecycle.md`](./actor-lifecycle.md) — Transition 2
  (Resume) gains an explicit locality-lookup step and an explicit
  random-fallback step, each with its own latency budget and
  failure mode.
- [`admin-operations.md`](./admin-operations.md) — locality-index
  inspection, repair, and reconciliation become admin operations in
  their own right under any L-A / L-B / L-C answer.
- Future `failure-modes.md` — index inconsistency (entries pointing
  at nodes that have evicted the snapshot) and index unavailability
  (the locality shard is failing over) need their own failure-mode
  entries with detection signals and recovery procedures.

</details>

</details>

## Q-2: Can Valkey cluster mode tolerate hot partitions and serve flat-keyspace queries?

<details>
<summary><em>Expand entry</em></summary>

- **Status:** OPEN
- **Tags:** hot-keys, sharding, indexing, cluster-mode, queryability
- **Related:** [`topology.md`](./topology.md) — cluster-mode
  constraints and sharding model;
  [`admin-operations.md`](./admin-operations.md) — `ListWorkers` /
  `ListActors` cluster-wide scan patterns;
  [`failure-modes.md`](./failure-modes.md) — primary-loss / shard-loss
  blast radius; [Q-1](#q-1-can-valkey-cluster-mode-support-locality-aware-scheduling)
  — same write-amplification and consistency concerns apply to any
  secondary index;
  [Q-3](#q-3-can-valkey-cluster-mode-support-label-based-worker-matching)
  — label-based scheduling is the most likely first need for real
  secondary indexes.

> *Two distinct concerns travel together here: **hot partitions**
> (one shard receives disproportionate load — usually because of a
> hot key or a slot range that happens to hold popular data) and
> **flat keyspace** (no native way to ask "all actors matching X"
> without a cluster-wide scan). They share a substrate — cluster
> mode's hash-slot model — and the available mitigations overlap.*

### Why this matters

Cluster mode assumes keys are uniformly distributed across the
16,384 slots. Real access patterns are not uniform. Two concrete
failure shapes:

- **A single hot actor.** Some actors get hammered far harder than
  others — a debugging target, a misbehaving client in a retry
  loop, a popular template's instance. That actor's key lives on
  one slot, on one primary. That primary's single-threaded command
  execution becomes the cluster-wide bottleneck for any traffic
  involving that actor — and, under
  `cluster-require-full-coverage yes`, any spillover impacts the
  whole cluster.
- **Implicit hot prefix.** If business logic ever creates keys that
  cluster on certain slots (e.g. tenant-scoped hash tags that
  concentrate a big tenant onto one shard), the same single-primary
  bottleneck applies but is harder to detect because the keys look
  different.

The flat-keyspace problem is the other half of the same model. The
current key scheme (`actor:<id>`, `worker:<ns>:<pool>:<pod>`) gives
us point lookup by full key and SCAN-based enumeration — nothing
else. Any "find actors matching X" query either reduces to "iterate
everything" (the [`admin-operations.md`](./admin-operations.md)
`ListWorkers` problem, generalized) or requires explicit secondary
indexes that the application maintains by hand.

Both failure shapes worsen with scale. A 6-pod MVP cluster rarely
notices a hot key — there's enough headroom on each primary's core.
A 200-primary target-scale cluster, with finer slot partitioning
and tighter per-shard QPS ceilings, is much more sensitive. The
flat-keyspace problem similarly: it's an annoyance at MVP, a
correctness/SLO concern at target scale.

<details>
<summary><strong>What we know</strong></summary>

**About hot keys under cluster mode:**

- Valkey's primary is single-threaded for command execution. One
  hot key saturates one CPU core. Adding cluster nodes does not
  help — the hot key still lives on one primary.
- Replicas can serve reads if the client opts in
  (`ReadOnly` / `RouteByLatency` / `RouteRandomly` in go-redis).
  Currently the API server does **not** opt in
  (see [`topology.md`](./topology.md)) — all reads route to primaries.
- Resharding (moving slots between primaries) is supported but
  manual and disruptive. There is no automatic hot-slot rebalancing.
- Valkey's `--hotkeys` mode for `valkey-cli` can sample hot keys,
  but it's expensive to run and not suitable for continuous
  monitoring.

**About flat keyspace:**

- Current key scheme is intentionally flat. No hash tags.
- The only enumeration primitives are `SCAN` (per-shard, fan-out
  via `ForEachMaster`) — both are O(N) over the relevant key set.
- `ateredis`'s denormalization (worker status inline in actor
  records) was specifically designed to avoid the cross-key
  problem this question asks about: it sidesteps the need for
  secondary indexes by duplicating data. That works at small scale
  but creates write amplification and consistency risk as data
  grows.
- The Worker proto has no labels / metadata beyond pool / pod
  identity. Any future label-based scheduling (Q-3) needs either a
  proto extension or an external label index.

**About scale interaction:**

- At MVP (3 primaries, low QPS), one core per shard is plenty;
  hot keys are unlikely to bind.
- At target (50–200 primaries, 100k workers' worth of traffic),
  per-shard QPS ceilings tighten and hot-key risk grows.
- Flat-keyspace `SCAN` operations that are tolerable at 10k actors
  become unusable at 1B. The
  [`admin-operations.md`](./admin-operations.md) numbers for
  `ListWorkers` are the worked example.

</details>

<details>
<summary><strong>Design space</strong></summary>

Two intertwined sub-questions: **(H)** how do we tolerate hot
partitions, and **(I)** how do we serve queries that the flat
keyspace doesn't natively support.

**H: hot-partition mitigations**

- **H-A. Read-side replica routing.** Configure the cluster client
  to send reads to replicas (`ReadOnly`, optionally with
  `RouteByLatency` / `RouteRandomly`). Buys read scale-out at the
  cost of read-after-write consistency (replicas lag the primary).
  Cheap to enable; well-suited to read-heavy hot keys.
- **H-B. API-server local cache with short TTL.** Cache hot reads
  in the API server process with a few-second TTL. Effectively
  another form of read-side scaling without touching Valkey
  config. Cost: cache invalidation discipline, slightly stale data.
- **H-C. Application-level key sharding.** Split a logically-single
  hot key into N sub-keys distributed by request-id hash. The
  application reassembles or routes per-request. Only useful for
  certain access patterns (counters, queues, aggregations).
  Inappropriate for entity records like `actor:<id>` where the key
  represents a single logical thing.
- **H-D. Manual slot rebalancing.** When a hot slot is identified
  via monitoring, manually move it to a less-loaded primary using
  `CLUSTER SETSLOT`. Disruptive and labor-intensive; treats the
  symptom, not the cause.
- **H-E. Redesign to eliminate the hot key.** Often the real fix —
  if a key is hot because the workload has a bottleneck on it, the
  bottleneck may be removable at the application level.
- **H-F. Move to a multi-threaded primary.** Garnet or KeyDB
  removes the single-core-per-shard ceiling. This is a backend
  swap, not a configuration knob — covered separately in the
  backend-selection conversation.

**I: secondary-index / non-flat-query strategies**

- **I-A. In-Valkey secondary index, hash-tagged.** Maintain
  explicit index keys (e.g. `idx:pool:{<pool>}` is a SET of worker
  IDs in that pool). Hash tags co-locate the index with its
  members, enabling atomic membership updates via single-key sets.
  Buys: O(1) lookup; structured queries inside cluster mode.
  Costs: write amplification on every entity create / delete /
  update (touch the entity record AND every index it belongs to);
  the hash-tag concentration means the index's host shard sees all
  traffic for that index — a new hot-shard risk.
- **I-B. In-Valkey secondary index, separately sharded.** Don't
  hash-tag; let the index spread by its own key's hash. Costs:
  index updates are cross-shard (no atomicity with the entity
  update); requires reconciliation discipline.
- **I-C. In-process index in the API server, fed by informer or by
  storage writes.** Build the index in memory in each API server,
  served from RAM in microseconds. Costs: an additional source of
  truth to keep consistent; rebuilds on API-server restart;
  per-server memory cost.
- **I-D. External index.** Drop indexing into a system designed
  for it (Elasticsearch, a custom service). Costs: a new
  operational dependency; new failure modes; no shared
  consistency story with Valkey.
- **I-E. Accept O(N) scans for admin queries; design the hot path
  never to need them.** The current state with discipline added.
  Works if every critical-path query has a known key.

The composition matters. Most realistic answers combine an `H`
choice for the hot-key direction and an `I` choice for the
queryability direction. A common shape: H-A + H-B + I-C — read
replicas + local cache + in-process index. That keeps the hot path
off the cluster entirely for both load-balancing and queryability.

</details>

<details>
<summary><strong>Open subquestions</strong></summary>

1. **What is the actual hot-key risk at our access pattern?** Most
   actors are touched by one or a few actors at a time — write
   patterns are write-heavy but not bursty per key. The real
   hot-key risk likely comes from accidental shared state (a hot
   counter, a popular template's reads, a registry key). Need
   monitoring to know.
2. **How do we monitor for hot keys continuously?** `--hotkeys`
   sampling is too expensive for production. Likely answer: track
   per-shard QPS imbalance as a leading indicator; sample only
   when imbalance is detected.
3. **Do we want a general secondary-indexing strategy, or do we
   solve each indexing need bespoke?** A general framework (in-
   process index pattern, informer-fed, generic) is reusable for
   pool indexing, label indexing (Q-3), locality indexing (Q-1).
   Without a framework, each new need invents its own pattern.
4. **How does an in-Valkey index (I-A / I-B) interact with
   `cluster-require-full-coverage`?** If the index's host shard
   has a failover or loss, all queries against that index fail.
   Same answer applies as Q-1 sub-question 2: pick a stance on
   coverage mode first.
5. **What's the cost ceiling on read replicas (H-A) before they
   become the new bottleneck?** Replica resync overhead, network
   amplification — each read-routed-to-replica is still a network
   request. At a high enough read rate the replicas saturate too.
6. **Should the in-process cache (H-B / I-C) be a project-wide
   primitive?** A general informer-fed cache layer in the API
   server (one source of truth, multiple indexed views) would
   solve hot-keys, flat-keyspace queries, and the worker-selection
   problem from Q-1 in one shape.

</details>

<details>
<summary><strong>What would change in the handbook</strong></summary>

- [`topology.md`](./topology.md) — if the answer is I-A / I-B (any
  Valkey-side secondary index), sizing grows by the index size and
  the index shard's QPS becomes a load-bearing capacity figure.
- [`actor-lifecycle.md`](./actor-lifecycle.md) — any write that
  must also touch an index becomes a cross-key operation, which
  needs new round-trip accounting on the relevant transitions.
- [`admin-operations.md`](./admin-operations.md) — index inspection
  and reconciliation become admin operations in their own right
  under any in-Valkey indexing answer.
- [`failure-modes.md`](./failure-modes.md) — new entries for index
  inconsistency (entity exists but not in its expected index, or
  index points to non-existent entity) and index unavailability
  (the index shard is down).
- [`risk-register.md`](./risk-register.md) — R-10
  (`ListWorkers` O(N)) and R-22 (popular-snapshot hot keys)
  collapse into or expand from this entry depending on which
  design is chosen.

</details>

</details>

## Q-3: Can Valkey cluster mode support label-based worker matching?

<details>
<summary><em>Expand entry</em></summary>

- **Status:** OPEN
- **Tags:** scheduling, labels, indexing, cluster-mode, k8s
- **Related:** [Q-1](#q-1-can-valkey-cluster-mode-support-locality-aware-scheduling)
  — locality scheduling is a special case of label-based matching
  ("has snapshot X cached" is one specific label);
  [Q-2](#q-2-can-valkey-cluster-mode-tolerate-hot-partitions-and-serve-flat-keyspace-queries)
  — label indexing is the most likely first concrete need for the
  secondary-indexing decision; upstream issues
  [#47](https://github.com/agent-substrate/substrate/issues/47),
  [#212](https://github.com/agent-substrate/substrate/issues/212);
  [`actor-lifecycle.md`](./actor-lifecycle.md) — `AssignWorkerStep`
  is where label filtering would land;
  [`admin-operations.md`](./admin-operations.md) — current scheduler
  filters by pool only.

> *Today, worker selection filters by pool name only. The natural
> next ask is multi-dimensional matching — "find a free worker with
> H100 AND in pool foo AND has snapshot xyz cached." That is a set
> intersection over arbitrary label dimensions, and the data model
> for it doesn't exist in the current Worker proto. This question
> exists to decide the shape of the answer before that ask becomes
> urgent.*

### Why this matters

Real scheduling requirements are not single-dimension. A few
plausible near-term examples:

- **Hardware affinity.** A workload needs an H100 GPU; the worker
  pod has the H100 label (it's running on a node with the GPU);
  the scheduler should only match those.
- **Snapshot locality** (Q-1, generalized). A worker that has the
  actor's snapshot cached is preferred. The "has snapshot X" is
  effectively a dynamic label that comes and goes as the cache
  changes.
- **Workload affinity / tenancy.** A worker tagged for a specific
  customer, or restricted to certain workload types, or running
  a specific runtime version.
- **Temporary draining state.** A worker marked "draining" should
  not receive new assignments. This is a single negative label,
  but it's the same primitive.

Each of these reduces to the same operation: **given a set of
required labels, find a free worker that has all of them.**

The current scheduler in `AssignWorkerStep` (see
[`actor-lifecycle.md`](./actor-lifecycle.md) Transition 2) does
this in the simplest possible way: read every worker, filter
in-process. That doesn't scale — the
[`admin-operations.md`](./admin-operations.md) cost table for
`ListWorkers` already shows it's O(N) round trips per scheduling
decision, and label filtering doesn't change that.

The deeper question is what shape worker labels take and where
they're stored. K8s pods already have labels — the informer already
sees them. So the answer might not involve Valkey at all (the
labels live in K8s, the scheduler reads from an informer-fed
in-process index). Or it might involve Valkey holding a label
index for cross-API-server consistency. The choice has cascading
consequences for the rest of the storage tier.

<details>
<summary><strong>What we know</strong></summary>

**About the current scheduler:**

- `AssignWorkerStep` filters by pool name and namespace only.
- The Worker proto carries `worker_namespace`, `worker_pool`,
  `worker_pod`, `actor_*` assignment fields, and `ip` / `uid` —
  **no label dimension**.
- The filter step happens after reading every worker in the cluster
  (the Q-1 / R-10 problem).
- `findFreeWorker` shuffles candidates randomly to spread load.

**About K8s labels:**

- Worker pods already carry K8s labels. `WorkerPoolSyncer` uses
  one of them (`workerPodLabel`) to identify the pool.
- The informer surfaces the full label set on every pod event.
- K8s label selectors support equality (`key=value`),
  set-membership (`key in (a, b)`), and exists/not-exists. They do
  not natively support range queries.
- At 100k worker pods, the informer cache holds a large amount of
  pod data in API-server memory — already a real cost.

**About cluster-mode constraints (re-stated):**

- Cross-slot atomicity is forbidden.
- Multi-key set intersection (`SINTER`) requires all keys to hash
  to the same slot. Forcing co-location via hash tags concentrates
  a whole index family on one shard — hot-shard risk per Q-2.
- Single-key operations (one set per label, queried independently)
  scale naturally but require the application to do the
  intersection client-side.

**About label cardinality (assumptions, not measurements):**

- Pool: low cardinality (tens, maybe hundreds).
- Hardware class (H100, A100, CPU-only): low cardinality (tens).
- Snapshot-cached labels (Q-1 locality): high cardinality and
  dynamic — one label per cached snapshot, set of nodes per label.
- Per-tenant / per-customer labels: potentially very high
  cardinality at multi-tenant scale.

The right data structure depends heavily on which of these regimes
the labels live in.

</details>

<details>
<summary><strong>Design space</strong></summary>

Six candidate shapes, in roughly increasing decoupling from
Valkey:

- **L-A. Brute-force scan with label filter.** Current pattern,
  extended to filter on label fields in the Worker proto. Requires
  proto extension; doesn't fix scaling. Useful only as the most
  minimal change to get *correct* label behavior; doesn't fix
  *fast* label behavior.
- **L-B. Per-label SET in Valkey, hash-tagged to one slot.**
  `label:{schedulingdomain}:H100` is a SET of worker IDs.
  Scheduling: `SINTER label:{schedulingdomain}:H100
  label:{schedulingdomain}:pool-foo` returns matching workers.
  Hash tag forces co-location so SINTER works in cluster mode.
  Costs: every label key shares one shard → hot-shard concern
  (Q-2); every worker update writes to many SETs.
- **L-C. Per-label SET in Valkey, sharded by label name.** No
  hash tag; each label's SET lives on whichever shard its key
  hashes to. Cross-shard intersection cannot be done with SINTER
  — the application must read each SET separately and intersect
  client-side. Costs: more round trips per scheduling decision;
  no atomicity between label updates.
- **L-D. In-process inverted index in the API server, fed by the
  K8s informer.** Each API server maintains
  `map[label] → set[worker_id]` in memory; intersections are
  microseconds. The informer already has the label data. Costs:
  per-server memory cost (significant at 100k workers with many
  labels); rebuild on API-server restart.
- **L-E. K8s label selectors via the informer.** Skip the
  inverted index entirely. The informer maintains its own indexed
  view; the scheduler queries it with a label selector and gets
  back the matching pods. Costs: tightly couples the scheduler to
  the K8s informer's query capabilities (no range queries, no
  fancy predicates); informer query rate at 100k workers needs
  measurement.
- **L-F. External label-search system.** Index workers in
  Elasticsearch / OpenSearch / similar. Schedule by querying the
  search system, then reading matching workers from Valkey
  (or the in-process cache). Costs: new operational dependency;
  new failure mode; consistency story between the index and the
  workers.

**Cross-cutting with Q-1.** Locality scheduling is L-D or L-E
applied to one specific label dimension ("has snapshot X cached").
A solution to Q-3 that handles dynamic high-cardinality labels
likely *subsumes* Q-1 — locality becomes one row in the label
matrix, not a separate system. Conversely, a Q-3 answer that only
handles low-cardinality static labels (L-B / L-C) leaves Q-1
unsolved.

**Cross-cutting with Q-2.** L-B and L-C are both instances of the
in-Valkey secondary-index pattern Q-2 addresses. The answer to
Q-2's I-A vs I-B vs I-C choice directly determines which Q-3
candidate is feasible.

</details>

<details>
<summary><strong>Open subquestions</strong></summary>

1. **Where do labels live — on the Worker proto, on K8s pod
   labels, or both?** If K8s-only, L-D / L-E / L-F are natural and
   the storage tier doesn't change. If Worker-proto-only, the
   index must be maintained by the API server's storage writes.
   If both, consistency between the two becomes an operational
   problem.
2. **What's the expected label cardinality?** Low-cardinality
   static labels (hardware class, pool) are easy under any scheme.
   Dynamic high-cardinality labels (snapshot-cached, per-tenant)
   force harder choices. Without numbers, the design picks the
   wrong tradeoffs.
3. **Are scheduling queries always conjunctions (AND)?** If yes,
   simple set intersection works. If we need OR / NOT / range,
   we likely need a real query layer (L-F).
4. **What is the consistency contract on label updates?** A
   worker re-tagged for a new GPU model needs to start receiving
   matching work; how stale can the index be? Q-1 has the same
   question for locality, with the same answer space.
5. **Does this interact with worker autoscaling (upstream issue
   #198)?** Yes — pods come and go, labels come and go with them.
   The index must tolerate the autoscaler's pace.
6. **Should this be in scope for MVP?** Probably not for the first
   alpha — the current pool-only filter covers the obvious
   workloads. Becomes urgent the first time a customer asks for
   hardware affinity or any cross-pool dimension. Worth deciding
   the shape now so the eventual implementation isn't
   pattern-incompatible with the existing scheduler.
7. **Should the answer collapse Q-1 and Q-3 into one decision?**
   If yes (label-based scheduling generalizes locality), the
   handbook benefits from one mechanism rather than two. If no
   (locality is dynamic enough to deserve special treatment),
   keep them separate.

</details>

<details>
<summary><strong>What would change in the handbook</strong></summary>

- [`actor-lifecycle.md`](./actor-lifecycle.md) — `AssignWorkerStep`
  in Transition 2 gains a label-filter step. Latency depends
  entirely on which design is chosen (L-D is microseconds; L-B is
  a SINTER round trip; L-F is an external HTTP hop).
- [`admin-operations.md`](./admin-operations.md) — label-index
  inspection becomes an admin operation under L-B / L-C / L-F.
  Under L-D / L-E, the inspection is in-process and surfaces via
  metrics rather than admin commands.
- [`topology.md`](./topology.md) — under any in-Valkey label-index
  answer, sizing math grows by the label-index data. Under L-D,
  per-API-server memory grows with label cardinality.
- [`failure-modes.md`](./failure-modes.md) — label-index
  inconsistency (worker matches the label predicate but isn't
  picked up, or is picked up but doesn't actually have the
  capability) becomes a failure-mode category.
- [Q-1](#q-1-can-valkey-cluster-mode-support-locality-aware-scheduling)
  — if Q-3 resolves first with L-D / L-E / L-F, the locality
  question may be answered as a special case rather than a
  separate mechanism.

</details>

</details>
