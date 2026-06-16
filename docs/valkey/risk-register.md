# Risk Register

This page is the consolidated catalogue of **known sharp edges** in
the current Valkey deployment and the configuration levers available
to mitigate each one. Every risk listed here is also discussed
contextually in another handbook page (topology, lifecycle, admin-ops,
failure-modes); the register's job is to give a single place to scan,
compare severities, and decide what to fix before MVP, before target
scale, or never.

This is **not** a list of every imaginable failure — that lives in
[`failure-modes.md`](./failure-modes.md). It is a list of things we
know we are deliberately leaving on, or have not yet decided about,
that an honest reader of the handbook should see surfaced and
prioritized in one place.

## How to read this register

Each risk carries five pieces of information:

- **Severity** — the worst-case impact category (see definitions
  below).
- **Status** — `ACCEPTED` (known and deliberately tolerated for
  now), `ACTIVE` (needs attention before some milestone), or
  `MITIGATED` (closed, with link to the resolution).
- **MVP / Target** — whether this is a risk at MVP scale (≤ 10 M
  actors, ≤ a few thousand workers) and / or at target scale (1 B
  actors, 100k+ workers). A risk can be safe at one and blocking at
  the other.
- **What it costs to mitigate** — the trade-off each lever forces.
  Most levers are not free; the handbook's job is to make the
  cost explicit.
- **Cross-references** — the handbook pages that cover this risk in
  context.

### Severity definitions

- **Data loss** — acked writes can be permanently lost. Highest
  severity; always merits explicit acceptance.
- **Availability** — operations can fail (refuse, error, time out)
  for some non-trivial window. Cluster-wide and per-shard
  availability are tracked separately.
- **Latency** — operations succeed but exceed the <10 ms whole-path
  budget.
- **Operational hazard** — sharp edge in code or process that can
  be triggered accidentally, with disproportionate consequences.
- **Scaling** — does not bite at MVP; will bite at target scale.

## Summary table

| ID | Risk | Severity | Status | MVP | Target |
|---|---|---|---|---|---|
| [R-1](#r-1-no-off-cluster-backups) | No off-cluster backups | Data loss | ACTIVE | Tolerable | **Blocking** |
| [R-2](#r-2-appendfsync-everysec-aof-default) | `appendfsync everysec` default | Data loss (≤1 s) | ACCEPTED | OK | Revisit |
| [R-3](#r-3-async-replication-tail) | Async-replication tail on failover | Data loss (variable) | ACCEPTED | OK | **Blocking** |
| [R-4](#r-4-no-anti-affinity-or-topology-spread) | No anti-affinity / topology spread | Data loss + Availability | ACTIVE | Tolerable | **Blocking** |
| [R-5](#r-5-cluster-require-full-coverage-yes) | `cluster-require-full-coverage yes` | Cluster-wide availability | ACCEPTED | OK | **Blocking** |
| [R-6](#r-6-ca-bundle-rotation-hazard) | CA bundle rotation hazard | Cluster-wide availability | ACTIVE | At risk | At risk |
| [R-7](#r-7-no-pod-certificate-monitoring) | No pod-cert expiry monitoring | Cluster-wide availability | ACTIVE | At risk | At risk |
| [R-8](#r-8-cluster-node-timeout-5-s) | `cluster-node-timeout 5000` | Availability (5–10 s) | ACCEPTED | OK | Revisit |
| [R-9](#r-9-no-pod-disruption-budget) | No Pod Disruption Budget | Availability | ACTIVE | Tolerable | **Blocking** |
| [R-10](#r-10-listworkers-on-critical-path) | `ListWorkers` O(N) on critical path | Latency | ACTIVE | At risk | **Blocking** |
| [R-11](#r-11-no-retry-on-terminal-lifecycle-steps) | No retry on `FinalizeRunning` / `FinalizeSuspended` | Operational | ACTIVE | At risk | At risk |
| [R-12](#r-12-1-gi-pvc-default) | 1 Gi PVC default | Latency / Operational | ACTIVE | At risk | **Blocking** |
| [R-13](#r-13-no-maxmemory-set) | No `maxmemory` set | Data loss + Availability | ACTIVE | At risk | **Blocking** |
| [R-14](#r-14-debugclearall-package-public) | `DebugClearAll` package-public | Operational hazard | ACTIVE | At risk | At risk |
| [R-15](#r-15-syncer-bypasses-actor-lock) | Syncer bypasses actor lock | Operational | ACCEPTED | Tolerable | Revisit |
| [R-16](#r-16-atelet-idempotency-undocumented) | Atelet-side idempotency assumed | Operational | ACTIVE | At risk | At risk |
| [R-17](#r-17-no-client-retry-budget) | No bounded retry budget on client errors | Operational | ACTIVE | Tolerable | At risk |
| [R-18](#r-18-acquirelock-no-fencing-token) | `AcquireLock` has no fencing token | Operational | ACCEPTED | OK | Revisit |
| [R-19](#r-19-snapshot-leak-on-delete) | Snapshot leak on `DeleteActor` | Operational | ACTIVE | At risk | At risk |
| [R-20](#r-20-cluster-bus-on2-gossip) | Cluster-bus O(N²) gossip at high primary count | Scaling | ACCEPTED | OK | At risk |
| [R-21](#r-21-bgsave-bgrewriteaof-fork-latency) | `BGSAVE` / `BGREWRITEAOF` fork latency at scale | Scaling | ACCEPTED | OK | At risk |
| [R-22](#r-22-popular-snapshot-hot-keys) | Popular-snapshot hot keys (locality index) | Scaling | OPEN (Q-1) | N/A | At risk |
| [R-23](#r-23-single-az-vs-multi-az-undecided) | Single-AZ vs multi-AZ deployment undecided | Scaling | ACTIVE | Tolerable | **Blocking** |

**Quick scan**: 8 risks are **blocking for target scale** (need to be
resolved before scaling beyond MVP); 6 are blocking for MVP or
already biting; the remainder are accepted with documented rationale
or scaling-only concerns. The biggest concentrations are durability
(R-1 through R-5) and scaling-readiness (R-20 through R-23).

## Durability & data-loss risks

### R-1: No off-cluster backups
<details>
<summary><em>Expand</em></summary>

**Severity.** Data loss. **Status.** ACTIVE.

**What it costs.** If both PVCs of a shard are lost (node loss with
node-local storage, accidental PVC deletion, storage-class outage),
all data on that shard is permanently gone. No external recovery
path exists today.

**Current state.** No backup mechanism is configured. Recovery
procedures ([`recovery-procedures.md`](./recovery-procedures.md)
R-3 Branch C) explicitly document this as escalation-only.

**Mitigation levers.**

- **Periodic `BGSAVE` + off-cluster copy** of the resulting RDB
  file. Buys point-in-time recovery to the last snapshot.
  Cost: snapshot creation forks the primary process (transient
  memory doubling on the affected shard); off-cluster transfer
  consumes network. Snapshot cadence vs RPO is the trade.
- **Continuous AOF shipping** to an off-cluster destination via
  log-tail. Stronger RPO (seconds rather than minutes / hours).
  Cost: continuous I/O / network overhead; more moving parts.
- **Resilient storage class with cross-AZ replication** (e.g. GCP
  regional PD-SSD). Reduces single-PVC-loss probability but does
  not protect against logical errors (accidental delete, app-side
  corruption).

**Recommended action.**

- **MVP**: tolerable if alpha customers accept the documented data
  loss risk. Add a one-line warning to operator-facing docs.
- **Target scale**: **blocking**. Pick a strategy (the periodic
  RDB + off-cluster copy is the lowest-effort start) and budget
  for it.

**Cross-references.** [`failure-modes.md`](./failure-modes.md)
Whole-shard loss; [`recovery-procedures.md`](./recovery-procedures.md)
R-3.

</details>

### R-2: `appendfsync everysec` AOF default
<details>
<summary><em>Expand</em></summary>

**Severity.** Data loss (bounded ≤ 1 s). **Status.** ACCEPTED.

**What it costs.** On a sudden shard outage (both pods lost
simultaneously, power-loss-style crash), up to ~1 s of acked writes
can be lost during AOF replay.

**Current state.** Default — no `appendfsync` override in
`valkey.yaml`. The AOF buffer flushes to disk once per second.

**Mitigation levers.**

- **`appendfsync always`** — fsync every write. Closes the window
  entirely. Cost: catastrophic for latency on cloud block storage
  — main-thread fsync stalls turn the <10 ms budget into a ~10–30 ms
  P99 floor on PD-SSD-class disks, 5–10× throughput collapse. Not
  viable as the primary durability lever.
- **`min-replicas-to-write 1` + `min-replicas-max-lag 10`** —
  approximates synchronous-ish durability at no latency cost (it's
  a precondition gate, not synchronous replication). Trades
  availability for durability. See R-3 — this lever covers both
  R-2 and R-3.

**Recommended action.**

- **MVP**: keep current. The 1 s window is bounded and acceptable
  for early users.
- **Target scale**: revisit jointly with R-3. The right lever is
  almost always `min-replicas-to-write` over `appendfsync always`.

**Cross-references.** [`topology.md`](./topology.md) latency
section; [`failure-modes.md`](./failure-modes.md) AOF / whole-shard
loss.

</details>

### R-3: Async-replication tail on failover
<details>
<summary><em>Expand</em></summary>

**Severity.** Data loss (variable window). **Status.** ACCEPTED.

**What it costs.** When a primary fails and a replica is promoted,
any write the old primary acked but had not yet shipped to the
replica is lost. Under light load this is sub-millisecond; under
burst load or network blips it can be hundreds of milliseconds to
seconds of writes.

**Current state.** No `min-replicas-to-write` config; primary
acks writes regardless of replica state.

**Mitigation levers.**

- **`min-replicas-to-write 1` + `min-replicas-max-lag 10`** —
  primary refuses writes if no replica is within 10 s of lag. This
  is a precondition gate (in-memory check), **not synchronous
  replication** — happy-path write latency is unchanged.
  - **Cost: availability.** During any window where the replica is
    restarting, lagging, or down, writes refuse on that shard
    with `NOREPLICAS`. A rolling deploy that takes the replica
    down briefly = a brief window of write refusals for that
    shard. The right setting requires
    `repl-ping-replica-period 1` so lag info is fresh (default 10
    is borderline).
- **`appendfsync always`** — see R-2; covers a different failure
  mode (whole-shard outage AOF replay) at huge latency cost.

**Recommended action.**

- **MVP**: tolerable. Async tail is small under expected light
  load; data-loss risk surfaces in postmortems as "we lost ~N
  writes during the X event."
- **Target scale**: **blocking**. At 100k workers' sustained
  write rate, the tail under burst load becomes large enough that
  any failover is a real data-loss event. Adopt
  `min-replicas-to-write 1` + `min-replicas-max-lag 10` with the
  ping-period change. Plan the availability trade explicitly.

**Cross-references.** [`failure-modes.md`](./failure-modes.md)
Primary loss; [`topology.md`](./topology.md) latency section.

</details>

### R-4: No anti-affinity or topology spread
<details>
<summary><em>Expand</em></summary>

**Severity.** Data loss + cluster-wide availability. **Status.** ACTIVE.

**What it costs.** Nothing in the StatefulSet manifest prevents
Kubernetes from scheduling both a primary and its replica onto the
same node. A single node failure can then take out both pods of a
shard simultaneously — a whole-shard loss event with all the
consequences thereof (cluster-wide pause under R-5, potential data
loss under R-1).

**Current state.** No `affinity` or `topologySpreadConstraints`
configured in `valkey.yaml`.

**Mitigation levers.**

- **Pod anti-affinity (required-during-scheduling)** — Kubernetes
  refuses to schedule pods of the same shard pair onto the same
  node. Cost: requires N nodes for N primaries' worth of pods;
  cannot be fully enforced in tiny clusters.
- **Topology spread constraints across zones/AZs** — distributes
  pods across failure domains. Cost: increased intra-cluster
  network latency (slight); requires multi-AZ deployment.
- **Pod anti-affinity (preferred-during-scheduling)** — softer
  variant. Kubernetes tries but doesn't fail scheduling. Cost:
  none, but provides no guarantee.

**Recommended action.**

- **MVP**: add required-during-scheduling anti-affinity. At 6
  pods on a typical kind / dev cluster the constraint is easily
  satisfied. Removes the most embarrassing single-node-loss
  scenario.
- **Target scale**: **blocking**. Required anti-affinity plus
  topology spread across AZs (joint decision with R-23).

**Cross-references.** [`topology.md`](./topology.md);
[`failure-modes.md`](./failure-modes.md) Multi-pod node loss /
Whole-shard loss; [`recovery-procedures.md`](./recovery-procedures.md)
R-14.

</details>

## Cluster-wide availability risks

### R-5: `cluster-require-full-coverage yes`
<details>
<summary><em>Expand</em></summary>

**Severity.** Cluster-wide availability. **Status.** ACCEPTED for MVP.

**What it costs.** With this setting (the Valkey default, in effect
today), *any* shard whose slot range is briefly uncovered — even
during a normal primary failover, not just on whole-shard loss —
flips `cluster_state` to `fail` and pauses **all** writes
cluster-wide. The blast radius of a single-primary failure is the
entire cluster for the ~5–10 s of the failover window.

**Current state.** Default; no override in `valkey.yaml`.

**Mitigation levers.**

- **`cluster-require-full-coverage no`** — surviving shards
  continue serving their slot ranges during a partial outage.
  Cost: callers receive errors for actors on the affected slot
  range specifically, not cluster-wide. A different operational
  contract — applications must be ready to handle per-actor
  unavailability rather than all-or-nothing.

**Recommended action.**

- **MVP**: keep current. 3-primary cluster, failover windows are
  brief, the failure semantics are simpler.
- **Target scale**: **blocking**. At 50+ primaries the
  probability of *some* shard being mid-failover is meaningful at
  any given second. With full-coverage on, the cluster has
  effectively zero steady-state availability above a certain
  shard count. Flip to `no`; document the per-slot-availability
  contract.

**Cross-references.** [`topology.md`](./topology.md);
[`failure-modes.md`](./failure-modes.md) Primary loss / Whole-shard
loss; [`recovery-procedures.md`](./recovery-procedures.md) R-1, R-3.

</details>

### R-6: CA bundle rotation hazard
<details>
<summary><em>Expand</em></summary>

**Severity.** Cluster-wide availability. **Status.** ACTIVE.

**What it costs.** A botched CA rotation (`valkey-ca-certs` secret
swapped without overlap) takes down every mTLS handshake in the
cluster simultaneously. Effectively a whole-cluster TLS outage
until rolled back.

**Current state.** No documented or enforced rotation procedure.
The rollback procedure exists in
[`recovery-procedures.md`](./recovery-procedures.md) R-11 but
relies on the operator knowing to apply it under time pressure.

**Mitigation levers.**

- **Documented bundle-overlap rotation procedure** in
  team-facing docs (referenced from R-11). Cost: process
  discipline.
- **Tooling guard** — a wrapper around the secret update that
  rejects a CA bundle change unless it contains both old and new
  CA. Cost: build the tool; small ongoing maintenance.
- **CA rotation as a planned-maintenance event with rehearsal** —
  rather than ad-hoc. Cost: planning overhead per rotation.

**Recommended action.**

- **MVP / Target scale**: build the documented procedure now
  (cheap). Decide on tooling guard at the same time as the
  team's broader secret-management discipline.

**Cross-references.** [`failure-modes.md`](./failure-modes.md) CA
bundle rotation; [`recovery-procedures.md`](./recovery-procedures.md)
R-11.

</details>

### R-7: No pod-certificate monitoring
<details>
<summary><em>Expand</em></summary>

**Severity.** Cluster-wide availability. **Status.** ACTIVE.

**What it costs.** Per-pod certs come from the `podCertificate`
projected volume. If the signing controller fails to rotate before
expiry, mTLS breaks for every affected pod simultaneously. If the
controller is broken cluster-wide and TTLs are short, the entire
Valkey cluster (and the API server's connection to it) goes down
within hours.

**Current state.** No monitoring on cert TTL or controller
health is configured (or, at least, none surfaced in the
handbook today).

**Mitigation levers.**

- **TTL-based alert at 50 % remaining lifetime.** Cost: minor
  monitoring setup; gives ample lead time for any rotation
  failure.
- **Controller-health alert** on the pod-certificate signer
  itself. Cost: same.
- **Shorter cert TTLs paired with reliable monitoring** —
  controlled by the signing controller's policy. Cost: more
  rotation events; only safe with reliable monitoring.

**Recommended action.**

- **MVP / Target scale**: build the alerts now. Cheapest, highest
  ROI item in this register.

**Cross-references.** [`failure-modes.md`](./failure-modes.md) Pod
certificate expiry / rotation failure;
[`recovery-procedures.md`](./recovery-procedures.md) R-10.

</details>

## Per-shard availability risks

### R-8: `cluster-node-timeout` 5 s
<details>
<summary><em>Expand</em></summary>

**Severity.** Availability (per failover event). **Status.** ACCEPTED.

**What it costs.** A primary loss event is unavailable for at least
`cluster-node-timeout + election + slot-map refresh` = roughly 5–10
seconds. Under R-5 (current default) this is cluster-wide.

**Current state.** `cluster-node-timeout 5000` in
`valkey.yaml`.

**Mitigation levers.**

- **Lower `cluster-node-timeout`** (e.g. 2000). Faster failover
  detection. Cost: spurious failovers under brief network blips,
  which are common on cloud networks. Practical floor for most
  cloud environments is 2–3 s.
- **Address R-5 first** — even with a 5 s timeout, per-slot
  availability (instead of cluster-wide) is the bigger win.

**Recommended action.**

- **MVP**: keep current.
- **Target scale**: revisit jointly with R-5. After flipping
  `cluster-require-full-coverage no`, consider lowering timeout
  to 3 s. Test under realistic network conditions before
  committing.

**Cross-references.** [`topology.md`](./topology.md);
[`failure-modes.md`](./failure-modes.md) Primary loss.

</details>

### R-9: No Pod Disruption Budget
<details>
<summary><em>Expand</em></summary>

**Severity.** Availability. **Status.** ACTIVE.

**What it costs.** Nothing prevents Kubernetes (rolling deploys,
node drains, autoscaler scale-downs) from disrupting multiple
Valkey pods simultaneously. The likely failure modes are:

- Two pods of the same shard down together → whole-shard loss
  during the disruption window.
- Multiple replicas restarted concurrently → resync storm
  ([`failure-modes.md`](./failure-modes.md), R-8 procedure).

**Current state.** No PDB on the `valkey-cluster` StatefulSet.

**Mitigation levers.**

- **PDB with `maxUnavailable: 1`** — enforces that K8s never
  voluntarily disrupts more than one Valkey pod at a time.
  Cost: rolling deploys are slower; node drains may block.
- **PDB with `minAvailable: N`** — alternative form, expresses
  the same constraint.

**Recommended action.**

- **MVP**: add the PDB. One line; closes the resync-storm path
  and the worst voluntary-disruption scenarios.
- **Target scale**: **blocking** — at high pod count, voluntary
  disruptions during maintenance are far more frequent.

**Cross-references.** [`failure-modes.md`](./failure-modes.md)
Resync storm / Whole-shard loss;
[`recovery-procedures.md`](./recovery-procedures.md) R-8.

</details>

## Latency & performance risks

### R-10: `ListWorkers` on critical path
<details>
<summary><em>Expand</em></summary>

**Severity.** Latency. **Status.** ACTIVE.

**What it costs.** Every `ResumeActor` call invokes `ListWorkers`,
which performs O(N) round trips across every primary. At 1 000
workers this is ~1 s; at 10 000 workers ~11 s; at 100 000 workers
~110 s. The <10 ms whole-path budget is unreachable.

**Current state.** Implementation as documented in
[`admin-operations.md`](./admin-operations.md).

**Mitigation levers.**

A full design-space discussion lives in
[`critical-questions.md`](./critical-questions.md) Q-1 (which
explicitly assumes this will be fixed and asks what the
architectural shape of worker selection should be). Short list of
near-term levers:

- **Pipeline / `MGET` per shard.** 5–10× latency relief; still
  O(N).
- **In-process worker cache from the K8s informer.** Removes
  storage tier from the worker-selection path entirely; reads in
  microseconds.
- **Pool-scoped Valkey set.** Filters at the storage tier;
  reduces read volume but still O(workers in pool).

**Recommended action.**

- **MVP**: pipeline / `MGET` fix is mechanically small;
  bake before any worker pool exceeds ~1k workers.
- **Target scale**: **blocking**. The architectural decision in
  [`critical-questions.md`](./critical-questions.md) Q-1 must be
  resolved.

**Cross-references.** [`admin-operations.md`](./admin-operations.md);
[`actor-lifecycle.md`](./actor-lifecycle.md);
[`critical-questions.md`](./critical-questions.md) Q-1.

</details>

### R-11: No retry on terminal lifecycle steps
<details>
<summary><em>Expand</em></summary>

**Severity.** Operational (with availability impact on stranded
actors). **Status.** ACTIVE.

**What it costs.** `FinalizeRunning` and `FinalizeSuspended` (the
terminal steps of the resume / suspend workflows) have no
`RetryBackoff()`. A single CAS conflict — easily caused by a racing
`releaseActorOnDeadWorker` from the syncer (R-15) — strands the
actor in RESUMING / SUSPENDING. Recovery requires external retry.

**Current state.** Code as documented in
[`actor-lifecycle.md`](./actor-lifecycle.md).

**Mitigation levers.**

- **Add `RetryBackoff` to both terminal steps** with the same
  pattern as `AssignWorker`. Cost: mechanically one-line per
  step; the contention model under the resulting retry pressure
  needs to be understood (related to R-15).
- **Background reconciler** that scans for actors stuck
  transitional longer than some threshold and re-runs the
  appropriate workflow. Cost: build the reconciler; another
  source of write pressure on actor records.

**Recommended action.**

- **MVP / Target scale**: add the retry backoff first (cheap).
  Reconciler is a larger investment and overlaps with the broader
  lifecycle design.

**Cross-references.** [`actor-lifecycle.md`](./actor-lifecycle.md).

</details>

### R-12: 1 Gi PVC default
<details>
<summary><em>Expand</em></summary>

**Severity.** Latency + Operational. **Status.** ACTIVE.

**What it costs.** Each Valkey pod is provisioned with a 1 Gi PVC.
This is enough for the AOF of a small / dev cluster but tight for
any realistic workload — AOF growth between rewrites can fill the
volume, at which point writes fail and pods crash.

**Current state.** `storage: 1Gi` in `valkey.yaml`.

**Mitigation levers.**

- **Increase default PVC size.** Cost: more provisioned storage;
  if the storage class is paid per-Gi, real money. Reasonable
  defaults: 10 Gi for dev / MVP, 50–100 Gi for production.
- **Tune AOF rewrite** (`auto-aof-rewrite-percentage`,
  `auto-aof-rewrite-min-size`). Cost: occasional fork/IO spikes
  when rewrites run; doesn't change the eventual ceiling.
- **Monitor PVC usage** at 50 / 70 / 90 % thresholds. Cost:
  monitoring setup; gives lead time before disk-full.
- **Storage class with online PVC expansion** so emergency growth
  doesn't require migration. Cost: depends on platform support.

**Recommended action.**

- **MVP**: bump to 10 Gi default and add monitoring.
- **Target scale**: **blocking**. Per-shard PVC sized to expected
  AOF + recovery headroom; expansion-capable storage class
  required.

**Cross-references.** [`topology.md`](./topology.md);
[`failure-modes.md`](./failure-modes.md) Disk full / IOPS
exhaustion; [`recovery-procedures.md`](./recovery-procedures.md)
R-5.

</details>

### R-13: No `maxmemory` set
<details>
<summary><em>Expand</em></summary>

**Severity.** Data loss + Availability. **Status.** ACTIVE.

**What it costs.** Without an explicit `maxmemory`, Valkey grows
until the K8s pod memory limit triggers OOMKill. Each OOMKill is
a pod restart with AOF replay (~1 s data loss per R-2) and, if
the killed pod is a primary, a cluster-wide pause per R-5. Memory
pressure tends to be correlated across primary and replica
(same workload, same data) — a single OOM can cascade.

**Current state.** No `maxmemory` configured; relies on K8s pod
limit as the only ceiling.

**Mitigation levers.**

- **`maxmemory` at ~75 % of K8s pod limit, with `maxmemory-policy
  noeviction`.** Cost: writes fail with a clear OOM error when
  the limit is reached, instead of triggering opaque OOMKill +
  AOF replay. **`noeviction` is the correct policy** for a
  persistence store — any other policy would silently delete
  actor records.
- **Sized K8s pod memory limit** that headroom-budgets for
  fork-on-BGSAVE (transient memory doubling), client buffers,
  replication backlog. Cost: more memory provisioned per pod.

**Recommended action.**

- **MVP**: set both. Cheap; pre-empts the OOMKill cycle as a
  whole class of incident.
- **Target scale**: **blocking**. Combined with shard-splitting
  discipline (see [`topology.md`](./topology.md) sizing) to keep
  per-shard memory in a safe operating range.

**Cross-references.** [`failure-modes.md`](./failure-modes.md)
Memory pressure & OOM; [`recovery-procedures.md`](./recovery-procedures.md)
R-7.

</details>

## Operational sharp edges

### R-14: `DebugClearAll` package-public
<details>
<summary><em>Expand</em></summary>

**Severity.** Operational hazard. **Status.** ACTIVE.

**What it costs.** `DebugClearAll` calls `FlushAllAsync` on every
primary. It exists for test fixtures. The symbol is package-public
and reachable from any code that holds a `*Persistence`. If
accidentally invoked (or exposed via an API surface), it
catastrophically wipes the cluster.

**Current state.** No runtime guard. No naming convention to
flag the danger.

**Mitigation levers.**

- **Rename with explicit suffix** (`DebugClearAll_TESTONLY`).
  Cost: tiny refactor; readers can no longer miss the danger.
- **Build-tag guard** restricting compilation to test binaries.
  Cost: slightly more invasive; requires moving the function or
  marking it.
- **Environment check at runtime** (refuse to run in production).
  Cost: needs a reliable "is this production" signal.

**Recommended action.**

- **MVP / Target scale**: rename now. Cheapest defense-in-depth
  available.

**Cross-references.** [`admin-operations.md`](./admin-operations.md).

</details>

### R-15: Syncer bypasses actor lock
<details>
<summary><em>Expand</em></summary>

**Severity.** Operational (with rare data-loss path). **Status.** ACCEPTED.

**What it costs.** `releaseActorOnDeadWorker` mutates the actor
record without acquiring `lock:actor:<id>`. Optimistic CAS catches
most races, but one specific interleaving silently drops the
user's snapshot intent: worker dies during suspend → syncer
resets actor first → SuspendActor fast-forwards and reports
success without checkpointing.

**Current state.** Code as documented in
[`actor-lifecycle.md`](./actor-lifecycle.md).

**Mitigation levers.**

- **Make the syncer acquire the lock** before mutating the actor.
  Cost: contention against in-flight workflows; needs a short
  timeout to keep the syncer responsive (otherwise the syncer
  itself stalls behind a long workflow).
- **Add explicit metrics** on `releaseActorOnDeadWorker` outcomes
  (won race / lost race / no actor bound). Cost: minor; surfaces
  the race rate so the operational decision is informed.

**Recommended action.**

- **MVP**: accept; add metrics so the race rate is visible.
- **Target scale**: revisit. If metrics show meaningful
  "won-race" rate (cases where the user-visible snapshot intent
  was silently dropped), close the gap with the lock.

**Cross-references.** [`actor-lifecycle.md`](./actor-lifecycle.md).

</details>

### R-16: Atelet-side idempotency assumed but undocumented
<details>
<summary><em>Expand</em></summary>

**Severity.** Operational. **Status.** ACTIVE.

**What it costs.** The lifecycle workflows assume that atelet's
`Restore` and `Checkpoint` RPCs can be safely re-invoked with the
same arguments — used during retry / recovery paths (e.g. after a
crash between `CallAteletRestore` and `FinalizeRunning`). If
atelet does not actually enforce idempotency, retries can
double-resume (two workloads booting against the same actor) or
double-checkpoint (races on the same snapshot URI).

**Current state.** The contract is implicit; not documented in
the atelet protobuf.

**Mitigation levers.**

- **Document and enforce idempotency in atelet's API surface.**
  Cost: spec work; atelet implementation work to add dedup if
  not present.
- **Wrap retries with a request-ID / dedup token** that atelet
  uses to short-circuit duplicates. Cost: small protocol change.
- **Avoid the retry path at this layer** by adding stronger
  state checks in the workflow. Cost: more workflow complexity;
  doesn't help against crash recovery.

**Recommended action.**

- **MVP**: document the assumed contract on the atelet proto
  and verify atelet honors it in tests.
- **Target scale**: enforce via the dedup-token mechanism.

**Cross-references.** [`actor-lifecycle.md`](./actor-lifecycle.md).

</details>

### R-17: No bounded retry budget on client errors
<details>
<summary><em>Expand</em></summary>

**Severity.** Operational. **Status.** ACTIVE.

**What it costs.** When the API returns `Aborted` ("concurrent
update conflict, please retry"), there's no server-side guidance
on retry cadence and no per-actor circuit breaker. A pathological
caller can hot-loop the same actor and starve other operations on
the same shard.

**Current state.** No retry-budget mechanism. Clients are
trusted to back off appropriately.

**Mitigation levers.**

- **Per-actor circuit breaker** on the API server side. After N
  Aborted errors for the same actor in a window, reject further
  attempts with `Unavailable` instead of `Aborted`. Cost: state
  per actor in the API server; cleanup logic.
- **Server-side retry hints** (e.g. `RetryAfter` metadata on the
  Aborted response). Cost: protocol addition; clients that
  ignore the hint are unaffected.
- **Rate limit at the gateway** rather than per actor. Cost:
  coarser; may rate-limit legitimate traffic.

**Recommended action.**

- **MVP**: tolerable. Real clients don't hot-loop today.
- **Target scale**: at risk. 100k workers create more
  contention; one bad client could starve a shard. Add the
  circuit breaker before scale-up.

**Cross-references.** [`actor-lifecycle.md`](./actor-lifecycle.md).

</details>

### R-18: `AcquireLock` has no fencing token
<details>
<summary><em>Expand</em></summary>

**Severity.** Operational (narrow window). **Status.** ACCEPTED.

**What it costs.** A caller that holds the actor lock, stalls past
the TTL, then attempts a state mutation can in principle write
through. The actor's version-CAS catches the common case (someone
else updated the version while we were stalled), but in the narrow
window where the lock has expired AND no one else has updated the
actor, a stalled caller can write through stale state.

**Current state.** Lock is `SET NX EX` with a UUID token; release
is Lua CAS. No fencing token tied to the mutation.

**Mitigation levers.**

- **Add a fencing token** — every lock acquire returns a
  monotonically-increasing token; every mutation includes the
  token; storage rejects mutations whose token is older than the
  last-seen one. Cost: storage-side state per actor or per
  resource; protocol addition.
- **Tighten the lock TTL** so stalled callers are caught sooner.
  Cost: more lock-loss-during-workflow false positives.
- **Defensively re-acquire the lock** in long workflows. Cost:
  more storage ops in the lifecycle.

**Recommended action.**

- **MVP / Target scale**: accept. The lock TTL (30 s) is well
  longer than the workflow timeout (28 s) padding; the
  stalled-window is realistically tiny. Revisit if locks are
  ever extended to longer-running operations (multi-minute
  snapshots, batch jobs).

**Cross-references.** [`admin-operations.md`](./admin-operations.md);
[`actor-lifecycle.md`](./actor-lifecycle.md).

</details>

### R-19: Snapshot leak on `DeleteActor`
<details>
<summary><em>Expand</em></summary>

**Severity.** Operational (cost / storage growth). **Status.** ACTIVE.

**What it costs.** `DeleteActor` removes the actor record from
Valkey but leaves the snapshot URIs in `Actor.LastSnapshot` (and
any in-progress snapshot) pointing at object storage that is
**not cleaned up**. Over time, deleted actors leave dangling
snapshot blobs that consume storage and cost.

**Current state.** No cleanup mechanism. Storage grows unbounded.

**Mitigation levers.**

- **Cascading delete** — `DeleteActor` enqueues a cleanup task
  that removes the snapshot blobs from object storage. Cost:
  cleanup must be reliable even across API server crashes
  (durable queue, retry logic).
- **Background GC** — periodic scan of object storage that
  reconciles against the live actor set and deletes orphans.
  Cost: O(snapshots) periodic work; risk of GC-ing live data if
  the actor set is inconsistent.
- **Lifecycle policy on the object-storage bucket** (e.g. TTL on
  snapshot prefixes). Cost: implicit data loss if TTL is shorter
  than an actor's suspended lifetime.

**Recommended action.**

- **MVP**: at risk; tolerable while actor delete volume is
  low. Document the leak so operators don't see surprise
  object-storage costs.
- **Target scale**: at risk; pick a strategy. Cascading delete
  is the cleanest for correctness; background GC is the
  cheapest to bolt onto an existing system.

**Cross-references.** [`actor-lifecycle.md`](./actor-lifecycle.md)
DeleteActor.

</details>

## Scaling-only concerns

### R-20: Cluster-bus O(N²) gossip
<details>
<summary><em>Expand</em></summary>

**Severity.** Scaling. **Status.** ACCEPTED for MVP.

**What it costs.** Every primary pings every other primary
periodically. At 200+ primaries, gossip traffic and per-primary
CPU consumed by gossip processing becomes a meaningful overhead.
Practical comfort ceiling: ~500 primaries.

**Current state.** 3 primaries; no concern.

**Mitigation levers.**

- **Cap primary count** at the comfort ceiling. Cost: at very
  large scale, requires a different shard-sizing strategy
  (bigger per-shard memory) to fit total working set.
- **Shard across multiple Valkey clusters** at higher scale.
  Cost: application-side cluster routing; double the operational
  footprint.

**Recommended action.**

- **MVP**: nothing to do.
- **Target scale**: model the primary-count requirement (see
  [`topology.md`](./topology.md) sizing). If above 200 primaries
  is on the curve, plan multi-cluster shape in advance.

**Cross-references.** [`topology.md`](./topology.md);
[`failure-modes.md`](./failure-modes.md) Cluster bus / gossip
storm.

</details>

### R-21: `BGSAVE` / `BGREWRITEAOF` fork latency at high per-shard memory
<details>
<summary><em>Expand</em></summary>

**Severity.** Scaling. **Status.** ACCEPTED for MVP.

**What it costs.** Valkey forks the main process for `BGSAVE` and
`BGREWRITEAOF`. On modern Linux this is copy-on-write, so the
initial fork is cheap, but as the working set grows the fork
duration grows and write activity during the fork drives memory
expansion (CoW pages get duplicated). At ~10 GB+ per shard, the
fork can transiently double memory usage and impact serving
latency.

**Current state.** Working sets are tiny; not a concern.

**Mitigation levers.**

- **Cap per-shard memory** by sharding more finely. Cost: more
  primaries (interacts with R-20).
- **Use storage class with adequate IOPS** so `BGSAVE` writes
  don't compete with serving I/O.
- **Schedule `BGREWRITEAOF` carefully** (during low-traffic
  windows). Cost: tuning per workload.

**Recommended action.**

- **MVP**: nothing to do.
- **Target scale**: enforce per-shard memory ceiling via
  `maxmemory` (R-13) and shard-count discipline.

**Cross-references.** [`topology.md`](./topology.md);
[`failure-modes.md`](./failure-modes.md) Memory & OOM.

</details>

### R-22: Popular-snapshot hot keys (locality index)
<details>
<summary><em>Expand</em></summary>

**Severity.** Scaling. **Status.** OPEN — see
[`critical-questions.md`](./critical-questions.md) Q-1.

**What it costs.** If the project adopts a snapshot-locality index
in Valkey, a "popular" snapshot (e.g. a golden snapshot for an
ActorTemplate used by many actors) becomes a single set with many
members. Every node-cache update is a write to that same hot key;
under cluster mode's single-threaded primary execution, this
becomes a write bottleneck.

**Current state.** No locality index exists; this is a future
concern dependent on the answer to Q-1.

**Mitigation levers.** See
[`critical-questions.md`](./critical-questions.md) Q-1 design
space. The lever choice depends on which locality scheme is
adopted (no Valkey-side index avoids this risk entirely).

**Recommended action.**

- **MVP / Target scale**: no action until Q-1 is decided. If a
  Valkey-side locality index is selected, this risk re-enters
  the register with a specific mitigation tied to the chosen
  scheme.

**Cross-references.** [`critical-questions.md`](./critical-questions.md)
Q-1.

</details>

### R-23: Single-AZ vs multi-AZ undecided
<details>
<summary><em>Expand</em></summary>

**Severity.** Scaling + availability. **Status.** ACTIVE.

**What it costs.** Current deployment is single-cluster, single
namespace, no AZ-spread enforcement. A single AZ outage takes
the entire cluster down (with all the consequences under R-5).
A correlated multi-pod loss (R-14) on a single-AZ deployment is
fatal.

**Current state.** No `topologySpreadConstraints` configured.
Single-AZ vs multi-AZ is implicitly decided by node pool layout,
not explicitly by the deployment.

**Mitigation levers.**

- **Multi-AZ deployment with topology spread** across at least
  two AZs (three for proper quorum-style availability). Cost:
  cross-AZ network latency (typically 1–3 ms intra-region),
  cross-AZ data transfer cost, more complex topology.
- **Single-AZ with disciplined backups** (R-1). Cost: an AZ
  outage is a full outage; recovery from backup is operationally
  heavy.

**Recommended action.**

- **MVP**: single-AZ tolerable with documented expectation. Set
  basic anti-affinity (R-4) so a single node loss doesn't kill
  a shard.
- **Target scale**: **blocking**. Multi-AZ with topology spread
  is effectively required for SLO. Decide jointly with R-1
  (backups) and R-4 (anti-affinity) — these three together
  define the durability / availability story.

**Cross-references.** [`topology.md`](./topology.md);
[`failure-modes.md`](./failure-modes.md) Network partition /
Multi-pod node loss.

</details>

## What to do with this register

The register is meant to be **acted on**, not just maintained. The
recommended cadence:

- **Before MVP launch**: triage every `ACTIVE` row tagged
  "MVP at risk." Decide explicitly per row whether to fix, accept
  with documented rationale, or defer with a trigger.
- **Before target-scale work begins**: triage every row tagged
  "Target blocking." Each one is a workstream of its own; budget
  for them in the scaling plan, not as afterthoughts.
- **Per incident**: update the matching row in this register.
  Severity assessments are inferences until reality validates
  them; every real incident is data that should sharpen the
  numbers.
- **Per quarter**: re-read the register end-to-end. Roll up
  `ACCEPTED` rows whose conditions have changed back to `ACTIVE`;
  close `MITIGATED` rows whose fixes have landed.

A risk register that doesn't change is either a perfect system or
an unused document. Neither describes this one.
