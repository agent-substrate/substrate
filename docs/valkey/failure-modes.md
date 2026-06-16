# Failure Modes

This page catalogs the ways the Valkey deployment can fail, what each
failure looks like from outside, how much of the system it takes down,
and what auto-recovery does (or doesn't do) for you. It is written for
the on-call engineer reading this at 3am with a paging alert open.

The framing scope is **the storage tier itself**. Failures that
originate in the actor lifecycle (stranded RESUMING, syncer races, etc.)
live in [`actor-lifecycle.md`](./actor-lifecycle.md). Failures that
originate in admin paths live in
[`admin-operations.md`](./admin-operations.md). Where a Valkey failure
*causes* a lifecycle failure, this page links across — but the
authoritative description of the lifecycle effect stays in the
lifecycle doc.

Two cross-cutting tables at the end summarize:
1. Which Valkey failures affect which actor operations.
2. Per-failure detection signal + first response (the cheatsheet).

## Detection model

Failure signals come from four places, in roughly increasing
specificity:

- **Client-side errors** in `ate-api-server` logs and metrics — usually
  the first thing on-call sees. Patterns: `MOVED` / `ASK` (slot-map
  staleness), `CLUSTERDOWN` (no full coverage), `MASTERDOWN`
  (replication issue), TLS handshake failures, context-deadline
  errors.
- **Kubernetes events and pod status** — pod restarts, OOMKilled,
  Pending state, PVC binding failures. Surfaced by `kubectl events`
  and the cluster's K8s event monitoring.
- **Valkey `INFO` and cluster commands** — `INFO replication`,
  `INFO persistence`, `CLUSTER INFO`, `CLUSTER NODES`. These give
  authoritative state but require shelling into a pod.
- **Prometheus / metric exporters** — replication lag, memory usage,
  ops/sec, fsync latency. These are the leading indicators that fire
  *before* the user-visible failure.

The handbook recommendation is to wire all four into a single
incident runbook so the on-call doesn't have to chase signals across
multiple consoles during an active page.

## Pod & process failures

> **Interaction with `cluster-require-full-coverage`.** All three
> pod-level failure modes below are amplified by the current
> `cluster-require-full-coverage yes` setting (see
> [`topology.md`](./topology.md)). Under that setting, **any** shard
> whose slot range is briefly uncovered — including during a normal
> primary failover, not just under whole-shard loss — flips
> `cluster_state` to `fail` and pauses **all** writes cluster-wide
> until coverage is restored. The blast-radius descriptions below
> assume the current config; flipping to `no` would localize each
> failure to its own slot range.

### Single replica loss
<details>
<summary><em>Expand</em></summary>

**What it is.** One replica pod dies — OOM kill, node failure,
manual delete, image-pull failure on restart. The StatefulSet will
recreate it with the same PVC, so on-disk state survives.

**Blast radius.** None for clients. The primary continues serving;
no data is lost; no slot range goes unavailable. The affected shard
runs *degraded* (no HA copy) until the replica is back and resynced.

**Detection.** K8s pod restart event for `valkey-cluster-<N>`. From
inside the cluster, `INFO replication` on the primary shows
`connected_slaves` decremented. Replica catch-up state visible via
`slave_repl_offset` lag.

**Recovery.** Automatic. StatefulSet recreates the pod; the new pod
starts with the existing PVC; Valkey performs a partial resync if
the replication backlog still covers the missed window, or a full
resync if not. Partial resync is near-instant; full resync of a
populated shard can take minutes (see "Replication lag / resync
storm" below).

**During the recovery window.** Writes succeed at the primary but
are not protected by a replica — a primary failure during this
window is a whole-shard loss with respect to recently-acked writes.
This is the single most important framing for incident handling: a
"benign" replica restart elevates risk on its shard until resync
completes.

**Mitigations to consider.** Pod Disruption Budget on the
StatefulSet to limit replica restart concurrency during voluntary
disruptions. Anti-affinity so a replica and its primary don't land
on the same node (so a node failure doesn't take both). Neither is
configured today — see [`topology.md`](./topology.md).

</details>

### Primary loss & automatic failover
<details>
<summary><em>Expand</em></summary>

**What it is.** A primary pod dies. The replica detects the loss via
cluster-bus gossip and, after a quorum-based election, gets promoted
to primary. The cluster slot map updates; clients eventually see
`MOVED` or `CLUSTERDOWN` errors and refresh their slot maps.

**Blast radius.** Under the current `cluster-require-full-coverage yes`
setting (see [`topology.md`](./topology.md)), a primary loss causes a
**cluster-wide pause** for the duration of the failover window. *Any*
shard whose slot range is briefly uncovered — between the primary
being declared dead and the replica being promoted — flips
`cluster_state` to `fail`, and all writes refuse across every shard,
not just the affected one. The window is roughly `cluster-node-timeout`
(5 s today) plus election delay (~500 ms) plus client slot-map refresh
(50–200 ms): call it **~5–10 s of cluster-wide unavailability**, then
automatic recovery once the replica is promoted. With
`cluster-require-full-coverage no`, the same event would limit
unavailability to ~1/N of the keyspace; that is not how the cluster is
configured today.

**Data loss window.** Async replication means any write the old
primary acked but had not yet shipped to the replica is **lost** on
promotion. Under light load this is sub-millisecond; under burst
write load or network blips, it can be hundreds of milliseconds to
seconds of writes.

**Lifecycle impact.** Calls to `GetActor` / `UpdateActor` for keys
on the affected slot range fail during the window. go-redis defaults
of 3 retries + 3 redirect hops will transparently cover a fast
failover for in-flight calls; slower failovers surface as gRPC
errors to the API client. See
[`actor-lifecycle.md`](./actor-lifecycle.md) for which lifecycle
steps have retry budgets and which strand on a single failure.

**Detection.** K8s pod restart on the primary. `MOVED` /
`CLUSTERDOWN` spike in `ate-api-server` logs. Cluster-state metrics
flip degraded. Once the new primary is up, `INFO replication` on it
reports `role:master`.

**Recovery.** Automatic. After the failover, the old primary
(recreated by the StatefulSet) joins back as a *replica* of the new
primary and begins resync. The shard runs without HA until that
resync completes — same elevated-risk window as a plain replica
loss.

**Mitigations to consider.** `min-replicas-to-write 1` +
`min-replicas-max-lag 10` to refuse writes when no replica is in
sync (closes the async-replication data-loss window at the cost of
write availability). Lower `cluster-node-timeout` (with spurious-
failover risk on cloud networks). Neither is configured today.

</details>

### Whole-shard loss (primary + replica both gone)
<details>
<summary><em>Expand</em></summary>

**What it is.** Both the primary and its replica for a single shard
are down simultaneously. Most common cause: a shared-infrastructure
failure (both pods on the same K8s node; both nodes in the same
rack; same AZ outage). Without anti-affinity — which isn't
configured today (see [`topology.md`](./topology.md)) — primary and
replica can absolutely land on the same node.

**Blast radius.** With **`cluster-require-full-coverage yes`** (the
current default), losing any single shard takes down **the entire
cluster's writes** — surviving shards refuse to serve until coverage
is restored. With `no`, surviving shards keep serving their slot
ranges; the lost shard's ~1/N of the keyspace is unavailable.

**Data loss window.** Recovery is from disk via AOF replay. With
the default `appendfsync everysec`, up to ~1 s of acked writes can
be lost. If the underlying PVCs are also lost (node disk failure,
PVC deleted), **all data on that shard is gone** — the only
recovery is from off-cluster backups (which are not configured
today).

**Lifecycle impact.** Severe. Under current config, *all* lifecycle
operations fail cluster-wide until the shard is back. Even with
`cluster-require-full-coverage no`, every actor and worker keyed
into the lost shard's slot range is inaccessible — those actors
look NOT-FOUND, workers similarly, and `releaseActorOnDeadWorker`
mutations targeting them fail silently. See
[`actor-lifecycle.md`](./actor-lifecycle.md) for the stranded-state
analysis.

**Detection.** Multiple K8s pod restarts in the same shard within
seconds. `CLUSTERDOWN` errors across all `ate-api-server` pods.
`CLUSTER INFO` reports `cluster_state:fail`.

**Recovery.** StatefulSet recreates both pods. Each loads its AOF
from PVC. Cluster reconverges when at least one of the shard's pods
finishes loading. Manual: if PVCs are lost, the shard cannot be
recovered without external backups; the operator must decide
between accepting the data loss (re-bootstrap the shard) and pulling
from a backup.

**Mitigations to consider.** Pod anti-affinity / topology spread
constraints to keep primary and replica on different nodes (and
ideally different AZs). `cluster-require-full-coverage no` so a
single-shard outage doesn't take down the cluster. Off-cluster
backups so PVC loss is recoverable. None of these are configured
today.

</details>

## Data & storage failures

### AOF corruption / partial write
<details>
<summary><em>Expand</em></summary>

**What it is.** The append-only file on disk gets corrupted — most
commonly a torn final write from a power-loss-style crash, less
commonly a real disk error. Valkey refuses to start a primary
whose AOF won't parse cleanly.

**Blast radius.** The affected pod fails to start, looking like a
pod crash loop. If the corrupted pod is a *replica*, no client
impact — the primary keeps serving. If it's the *primary*, the
event becomes a primary-loss / failover (see above), inheriting
the same **~5–10 s cluster-wide pause** under the current
`cluster-require-full-coverage yes` setting; the corrupted pod
then comes up as the new replica and stays in crash-loop until
manually repaired.

**Detection.** Pod in `CrashLoopBackOff`. Valkey logs show
`Bad file format reading the append only file` or similar AOF
parse error. K8s pod restart count climbing.

**Recovery.** Not automatic for a fully-corrupted AOF. Manual
recovery: shell into the pod (or attach a debug container with the
PVC mounted), run `valkey-check-aof --fix /data/appendonly.aof` to
truncate to the last valid command, restart. Modern Valkey defaults
include `aof-load-truncated yes` which auto-handles cleanly
truncated tails (e.g. a single torn final write), but anything
deeper still needs the manual fix.

**Lifecycle impact.** If the corrupted pod is a replica, none —
the primary keeps serving. If it's the primary, it's a primary loss
event; the replica is promoted, and the corrupted pod becomes the
new replica after the AOF fix. Writes that were in the corrupted
tail of the AOF are lost.

**Mitigations to consider.** Storage with stronger crash semantics
(e.g. PD-SSD with journaling; avoid local SSD on non-replicated
hosts). Monitoring for pod-restart loops as an early signal.
Periodic `BGREWRITEAOF` to keep AOF size bounded so a corruption
event loses less data on truncation.

</details>

### Disk full / IOPS exhaustion
<details>
<summary><em>Expand</em></summary>

**What it is.** AOF and `nodes.conf` grow on disk. If the PVC fills,
writes start failing. Separately, if the storage tier's IOPS quota
is exceeded (PD-SSD throttling, EBS burst credits exhausted), every
`fsync(2)` stalls and the single-threaded Valkey main thread blocks
on it — *all* operations on that primary back up behind the stall.

**Blast radius.** A full disk takes the affected pod down (writes
fail, AOF can't be rewritten). IOPS exhaustion is more insidious —
the primary appears up but its p99 latency spikes 10–100×, often
without an obvious error in logs.

**Detection.** Disk-usage metrics (the PVC is currently provisioned
at **1 Gi** — see [`topology.md`](./topology.md) — which is fine
for MVP but will need monitoring as data grows). fsync latency
metrics. Client-side latency p99 spikes without a corresponding
spike in QPS. Cloud-provider IOPS metrics if available.

**Recovery.** Disk full: expand the PVC (if the storage class
supports it) or migrate the shard. IOPS exhaustion: usually
transient — the storage class refills credits. Sustained
exhaustion requires provisioning a higher-IOPS tier.

**Lifecycle impact.** Latency spikes blow the <10 ms whole-path
budget on the affected shard, surfacing as user-visible slowness
for any operation touching keys in that slot range. See
[`topology.md`](./topology.md) for the budget analysis.

**Mitigations to consider.** Bigger PVCs (1 Gi is barely enough for
the AOF of a moderately busy shard). Alerts at 50% / 70% / 90%
disk usage. Higher-IOPS storage class for production. `auto-aof-
rewrite-percentage` and `auto-aof-rewrite-min-size` tuning to keep
AOF size from drifting unbounded.

</details>

### Memory pressure & OOM
<details>
<summary><em>Expand</em></summary>

**What it is.** Valkey is an in-memory store. If the working set
grows past available memory, behavior depends on `maxmemory` and
`maxmemory-policy`. The current `valkey.yaml` sets *neither* — there
is no explicit `maxmemory`, so Valkey grows until the K8s pod hits
its memory limit and gets OOMKilled.

**Blast radius.** OOMKill = pod restart = same effect as primary
loss (failover + replication tail loss) or replica loss, depending
on which pod died. If both primary and replica are under the same
memory pressure (likely — same workload, same data), an OOM on the
primary can quickly cascade into an OOM on the replica too.

**Detection.** K8s `OOMKilled` events. Memory-usage metrics
trending toward the pod limit. Replication lag growing if the
replica is also under pressure (fork-based BGSAVE doubles
memory transiently).

**Recovery.** Pod restarts, AOF replay restores state. Same
caveats as primary loss for the data-loss window.

**Lifecycle impact.** During the restart, the shard is unavailable
or degraded. If memory pressure recurs, the shard cycles —
catastrophic for the SLO.

**Mitigations to consider.** Set `maxmemory` explicitly, *below*
the K8s pod limit, with `maxmemory-policy noeviction` so writes
fail with a clear OOM error instead of triggering an opaque
OOMKill. Monitor memory headroom proactively. Plan shard splits
before memory hits 50–70 % of the pod limit (see scaling math in
[`topology.md`](./topology.md)).

</details>

### Replication lag / resync storm
<details>
<summary><em>Expand</em></summary>

**What it is.** Async replication is normally microseconds of lag.
Under sustained write load or network issues, lag can grow past
the replication backlog (`repl-backlog-size`, default ~1 MB). When
that happens, the replica must perform a **full resync**: the
primary forks for `BGSAVE`, transfers the entire RDB snapshot,
the replica loads it.

A **resync storm** happens when multiple replicas restart together
(rolling upgrade, node maintenance, autoscaler scale-down) and
each triggers a full resync against its primary in parallel.

**Blast radius.** During a full resync: primary memory usage spikes
(copy-on-write fork), network is saturated transferring the
snapshot, primary CPU is partially consumed by the BGSAVE. For
small shards (~100 MB) this is a sub-second event. For large
shards (~10 GB) it's tens of seconds during which the primary is
serving normally but resource-pressured.

**Detection.** `INFO replication` shows replica state transitions
(`send_bulk` is full-resync in progress). Network and disk-read
metrics on the primary spike during BGSAVE. K8s pod-restart events
correlated across multiple replicas in the same time window =
storm signal.

**Recovery.** Automatic when individual; storms self-resolve but
can take much longer than serial recovery.

**Lifecycle impact.** Generally none for correctness; latency on
the affected shard may briefly increase. If a primary failover
happens during a storm, the new primary may have a backlog of
catch-up work and serve degraded.

**Mitigations to consider.** Larger `repl-backlog-size` so brief
lag spikes don't escalate to full resync. Stagger replica
restarts during maintenance (don't restart all replicas of all
shards at once). Pod Disruption Budgets to enforce the staggering
automatically.

</details>

## Network & cluster topology failures

### Network partition / split-brain
<details>
<summary><em>Expand</em></summary>

**What it is.** A network partition cuts some pods off from others.
Could be a K8s network plugin failure, a node-level NIC issue, or
an AZ-to-AZ link issue (if Valkey is deployed multi-AZ).

**Blast radius.** Valkey Cluster uses majority-quorum elections, so
a *strict* split-brain (two primaries serving the same slot range
with both accepting writes) is avoided by design — a replica only
promotes if it can reach a majority of *other* primaries. The
practical failure modes are:

- **Symmetric partition** (~50/50): both sides lack majority,
  cluster enters a degraded state, neither side can promote
  replicas. Writes on both sides may continue against existing
  primaries until cluster timeouts fire.
- **Asymmetric partition** (minority cut off): the minority side
  loses access to majority-quorum services; its primaries
  eventually fail health checks; its writes stop succeeding.

**Detection.** `MASTERDOWN` / `CLUSTERDOWN` errors from clients
on the cut-off side. Cluster-state metrics divergence between
sides. K8s NetworkPolicy or CNI alerts if the partition is
configuration-level.

**Recovery.** Heals automatically when the network heals. Some
data loss is possible for writes that were acked on the minority
side but never replicated to majority replicas.

**Lifecycle impact.** Lifecycle operations on the affected side
fail or hang until the partition resolves. Lock TTLs (30 s for
the actor lock — see [`actor-lifecycle.md`](./actor-lifecycle.md))
will expire during a partition longer than that, releasing locks
on the still-healthy side; this can cause workflow interleavings
that wouldn't normally happen.

**Mitigations to consider.** Single-AZ deployment trades network
resilience for fewer partition modes; multi-AZ trades latency for
resilience. Today the cluster is single-cluster, single-namespace —
partition risk is dominated by K8s network plugin reliability.

</details>

### Cluster bus / gossip storm
<details>
<summary><em>Expand</em></summary>

**What it is.** Every pair of primaries exchanges cluster-bus
messages (port 16379) periodically. Message volume scales O(N²) in
primary count. At small N (3–10) this is invisible; at N=100+ it
starts to consume meaningful CPU and bandwidth on each primary.

**Blast radius.** Sustained high gossip overhead manifests as
elevated CPU on every primary in the cluster, with reduced headroom
for actual command processing. At extreme scales (~500+ primaries),
gossip can become the dominant cost.

**Detection.** CPU usage on primaries elevated and roughly equal
across all of them. Cluster-bus metrics if exported.

**Recovery.** Not really a "failure" — more of a scaling ceiling.
Mitigation is shard count discipline.

**Lifecycle impact.** Latency creep across all lifecycle operations
proportional to gossip overhead.

**Mitigations to consider.** Cap primary count. If target scale
requires more than ~500 primaries, consider sharding across
multiple Valkey clusters (each with its own slot map) rather than
growing a single cluster. See [`topology.md`](./topology.md) for
scaling-projection caveats.

</details>

### Client slot-map staleness
<details>
<summary><em>Expand</em></summary>

**What it is.** Each `ate-api-server` pod's `redis.ClusterClient`
caches the slot map. When a slot moves (failover, manual reshard),
the cached map is stale until the client refreshes it on the next
`MOVED` / `ASK` response.

**Blast radius.** Per-client transient: a small burst of redirected
requests after each topology change, then the client re-stabilizes.
go-redis handles this transparently with `MaxRedirects=3` (default).

**Detection.** Spike in redirect-retry metrics (if exported).
Brief latency bumps after failover events.

**Recovery.** Automatic. Each client converges within a few
operations after the topology change.

**Lifecycle impact.** Minor latency tax during the convergence
window. Can cumulatively widen the failover unavailability window
by a few hundred milliseconds.

**Mitigations to consider.** None typically needed — go-redis
defaults are fine. If the team observes pathological redirect
storms during failover, increasing `MaxRedirects` is the lever.

</details>

## Identity & security failures

### Pod certificate expiry / rotation failure
<details>
<summary><em>Expand</em></summary>

**What it is.** Per-pod server certs come from a `podCertificate`
projected volume signed by `servicedns.podcert.ate.dev/identity`
(see [`topology.md`](./topology.md)). If the signing controller
fails to rotate before the current cert expires, TLS handshakes
start failing.

**Blast radius.** Every client of the affected pod sees TLS
handshake errors. Cluster-internal traffic (replication, cluster
bus) is also mTLS, so a cert failure can also break replication
and gossip — potentially escalating a "TLS issue" into a primary-
loss event from the cluster's perspective.

**Detection.** TLS handshake error rate in `ate-api-server` logs
and Valkey logs. Cert-expiry metrics from the pod-certificate
controller if exported.

**Recovery.** Rotate the cert (forces a pod restart if the volume
projection doesn't pick up the new cert dynamically). If the
controller itself is unhealthy, fix the controller first.

**Lifecycle impact.** Cluster-wide if the certs across all Valkey
pods are expiring (same controller, same CA). Otherwise per-shard
depending on which pods have valid certs.

**Mitigations to consider.** Monitoring on cert TTL — alert at
50 % of TTL remaining. Monitoring on the pod-certificate
controller's own health. Rotate the CA bundle (`valkey-ca-certs`
secret) on a known schedule, before client-side `ca.crt`
expiration.

</details>

### CA bundle rotation
<details>
<summary><em>Expand</em></summary>

**What it is.** The `valkey-ca-certs` secret holds the CA cert
that all Valkey pods (and the API server) use to verify each
other's pod certs. Rotating it requires both sides to load the new
bundle before either side starts issuing certs signed only by the
new CA.

**Blast radius.** A botched rotation breaks every mTLS handshake
in the cluster — effectively a whole-cluster TLS outage.

**Detection.** TLS verification failures everywhere immediately
after a CA bundle change.

**Recovery.** Roll back the CA bundle (re-apply the previous
secret), then plan the rotation more carefully (typically: ship a
*bundle* containing both old and new CA certs, wait for all pods
to be re-rolled with the bundle, then narrow to new-CA-only).

**Lifecycle impact.** Cluster-wide unavailability until rotation
is fixed.

**Mitigations to consider.** Bundle-overlap rotation procedure
documented and rehearsed. CA rotation should be a planned
maintenance event, not an ad-hoc secret update.

</details>

## Kubernetes-level failures

### StatefulSet pod stuck pending
<details>
<summary><em>Expand</em></summary>

**What it is.** A pod can't schedule (node pressure, image-pull
failure, PVC binding failure, taints/tolerations mismatch). The
StatefulSet doesn't proceed; the affected ordinal stays Pending.

**Blast radius.** Depends on the ordinal. With
`podManagementPolicy: Parallel` (the current setting — see
[`topology.md`](./topology.md)), other ordinals come up in parallel,
so a single stuck pod is "just" one missing shard member. Under
the default `OrderedReady` policy, a stuck pod would block all
higher ordinals — but we don't use that policy.

**Detection.** Pod status `Pending` for extended time. K8s events
explaining the scheduling failure.

**Recovery.** Manual — fix the underlying scheduling issue.

**Lifecycle impact.** Same as replica loss (if stuck pod is a
replica) or primary loss (if primary) until resolved.

**Mitigations to consider.** Alerts on pods Pending > N minutes.
Pre-provisioned node capacity. Image-pull pre-warm via DaemonSet if
image-pull failures are common.

</details>

### PVC / PV failures
<details>
<summary><em>Expand</em></summary>

**What it is.** A PersistentVolume becomes unavailable — node
disk failure, PV provisioner outage, accidentally-deleted PVC. The
bound pod can't start (or can't write).

**Blast radius.** Loses the data on the affected PVC. If the
shard's other pod (primary or replica) is healthy, data is
recoverable via replication; if both PVCs are lost, the shard's
data is gone permanently (no off-cluster backups configured today).

**Detection.** Pod status Pending with PVC-related events. PV
state `Failed`.

**Recovery.** Depends on the cause. Recoverable: restore the PV
from the storage backend. Unrecoverable: provision a new PVC,
accept data loss for that shard, rely on replication or rebootstrap.

**Lifecycle impact.** If single-PVC loss with healthy peer:
replica loss equivalent. If both PVCs of a shard lost: whole-shard
loss + data loss (see "Whole-shard loss" above).

**Mitigations to consider.** Storage class with resilient PVs
(replicated block storage, e.g. PD-SSD with regional replication).
Periodic backups to off-cluster storage. Anti-affinity so primary
and replica PVCs don't sit on the same physical disk.

</details>

### Multi-pod node loss
<details>
<summary><em>Expand</em></summary>

**What it is.** A K8s node dies and takes every Valkey pod
scheduled on it with it. Without pod anti-affinity, primary and
replica for the same shard can be on the same node — a single
node failure becomes a whole-shard loss.

**Blast radius.** N pods (where N = pods scheduled to that node).
If those N include both members of any shard, that shard is
whole-shard-lost. With 6 pods spread across an undetermined number
of nodes today, the probability of this co-location depends on the
node pool size and scheduler choices.

**Detection.** Node `NotReady`. Multiple Valkey pod restarts
correlated to the node loss.

**Recovery.** K8s reschedules the pods (eventually) onto other
nodes. PVCs follow if the storage class supports it; otherwise
the data is lost.

**Lifecycle impact.** Same as whole-shard loss for any shards
that had both pods on the lost node. Cluster-wide outage under
current `cluster-require-full-coverage yes` if any shard is lost.

**Mitigations to consider.** **Pod anti-affinity is the
single most impactful mitigation** — it ensures primary and replica
of the same shard cannot co-locate. Topology-spread constraints
across AZs harden further. Neither is configured today — see
[`topology.md`](./topology.md).

</details>

## Cross-cutting: which Valkey failures affect which actor operations

| Valkey failure | `CreateActor` | `GetActor` | `ResumeActor` | `SuspendActor` | `DeleteActor` | `ListActors` | Syncer reconciliation |
|---|---|---|---|---|---|---|---|
| Replica loss (one shard) | unaffected | unaffected | unaffected | unaffected | unaffected | unaffected | unaffected |
| Primary loss (any shard, failover ~5–10 s; current config) | **fails cluster-wide briefly** | **fails cluster-wide briefly** | **fails cluster-wide briefly** | **fails cluster-wide briefly** | **fails cluster-wide briefly** | **fails cluster-wide briefly** | reconciliation paused |
| Whole-shard loss (current config) | **all writes fail cluster-wide** | **fails cluster-wide** | **fails** | **fails** | **fails** | **fails** | **all reconciliation halts** |
| Whole-shard loss (`require-full-coverage no`) | fails only for that slot | fails only for that slot | fails for actors on that slot | fails for actors on that slot | fails for actors on that slot | partial results | partial reconciliation |
| AOF corruption (one pod) | unaffected if other pod healthy | unaffected | unaffected | unaffected | unaffected | unaffected | unaffected |
| Network partition | per-side: like multiple primary losses | per-side | per-side | per-side | per-side | per-side | per-side |
| Cert expiry (one pod) | TLS errors for that pod | TLS errors | workflow may fail mid-step | workflow may fail mid-step | TLS errors | TLS errors | reconciler errors |
| Cert expiry (cluster-wide) | **all operations fail** | **all** | **all** | **all** | **all** | **all** | **halts** |

## Detection cheatsheet

| Failure | First signal | First action |
|---|---|---|
| Single replica loss | K8s pod restart event for `valkey-cluster-<N>` | Confirm primary still healthy; watch resync progress; do not panic. |
| Primary loss / failover | Spike in `MOVED` / `CLUSTERDOWN` errors in `ate-api-server` logs | Confirm failover completed (`CLUSTER INFO`); check for stranded RESUMING/SUSPENDING actors per [`actor-lifecycle.md`](./actor-lifecycle.md). |
| Whole-shard loss | Multiple shard pods restarting + `CLUSTERDOWN` | Determine if PVCs survived; if yes, wait for AOF replay; if no, escalate to data-recovery procedure. |
| AOF corruption | Pod in `CrashLoopBackOff` with AOF parse error in logs | Run `valkey-check-aof --fix` on the PVC; restart pod. |
| Disk full | PVC usage approaching cap; write errors | Expand PVC if storage class allows; otherwise migrate shard. |
| IOPS exhaustion | Client p99 spike without QPS spike | Check cloud-provider IOPS / burst-credit metrics; provision higher tier. |
| OOMKilled | K8s `OOMKilled` event | Check working-set growth trend; plan shard split or memory increase. |
| Resync storm | Multiple replica restarts correlated; primary CPU + network spike | Stagger remaining restarts; do not restart more replicas until storm subsides. |
| Network partition | `MASTERDOWN` from one subset of pods | Identify cut-off side; do not promote manually unless prepared for split-brain risk. |
| Cert expiry | TLS handshake errors | Check pod-certificate controller; force rotation; verify CA bundle freshness. |
| Pod stuck Pending | Pod in `Pending` > N minutes | Read K8s events to find scheduling cause (PVC, image-pull, resources). |
| PV failure | PVC events show `Failed` | Determine recoverability; restore PV or accept data loss + rebuild shard. |
| Multi-pod node loss | Node `NotReady` + cascading pod restarts | Treat as whole-shard loss for any co-located primary+replica pairs; same response. |
