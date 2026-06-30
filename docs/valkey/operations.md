# Operations

Operational reference: failure modes with inline recovery, common
admin commands, and the short list of risks worth tracking as the
deployment grows.

## How to use this page

During an incident: identify the failure mode by symptom (the
**Symptom** line at the top of each entry), follow the **Recovery**
steps, capture the **Postmortem** items before closing the ticket.

Outside an incident: read it end-to-end periodically. The risks
section is the rollup of "things we know are not yet addressed."

Each failure-mode entry is collapsed by default — click to expand.
The category headers and the at-a-glance risks table at the end stay
visible.

## Universal preflight

Run these before any non-trivial action. They preserve evidence and
confirm scope.

```bash
# Snapshot K8s state
kubectl -n ate-system get pods -l app=valkey-cluster -o wide > /tmp/incident-pods-$(date +%s).txt
kubectl -n ate-system get events --sort-by=.lastTimestamp | tail -100 > /tmp/incident-events-$(date +%s).txt

# vcli alias — substitute into every valkey-cli call below
alias vcli='valkey-cli --tls \
  --cacert /etc/valkey-ca/ca.crt \
  --cert /run/servicedns.podcert.ate.dev/credential-bundle.pem \
  --key /run/servicedns.podcert.ate.dev/credential-bundle.pem'

# Snapshot cluster's view of itself
kubectl -n ate-system exec valkey-cluster-0 -- vcli cluster info > /tmp/incident-cluster-info-$(date +%s).txt
kubectl -n ate-system exec valkey-cluster-0 -- vcli cluster nodes > /tmp/incident-cluster-nodes-$(date +%s).txt
```

## Pod & process failures

> **`cluster-require-full-coverage yes` amplifies these.** All three
> pod-level failures below interact with the current setting: *any*
> shard whose slot range is briefly uncovered — including a normal
> primary failover — flips `cluster_state` to `fail` and pauses
> **all** writes cluster-wide until coverage is restored. Flipping
> to `no` would localize each failure to its own slot range. See
> [`topology.md`](./topology.md).

### Single replica loss
<details>
<summary><em>Expand</em></summary>

**Symptom:** K8s pod restart for a Valkey pod that is currently a
replica. No client-visible errors.

**What's happening:** StatefulSet recreates the pod with the same
PVC. Valkey performs a partial resync if recent enough; full resync
if not. The shard runs without HA until resync completes (elevated
risk during this window).

**Recovery:** automatic — verify only.

```bash
kubectl -n ate-system exec <restarted-pod> -- vcli info replication
# Expect: role:slave, master_link_status:up
# slave_repl_offset should advance toward master_repl_offset
```

Do **not** restart any other Valkey pod until resync completes —
parallel restarts cascade into a resync storm.

**Postmortem:** what caused the restart (OOMKilled? manual? deploy?
node maintenance?). If full resync, note duration — informs
`repl-backlog-size` tuning.

</details>

### Primary loss & automatic failover
<details>
<summary><em>Expand</em></summary>

**Symptom:** K8s pod restart for a Valkey pod that was a primary.
Spike of `MOVED` / `CLUSTERDOWN` errors in `ate-api-server` logs.
Under current `cluster-require-full-coverage yes`, this is a
**cluster-wide write pause** for ~5–10 s (cluster-node-timeout 5 s +
election + slot-map refresh), not per-slot.

**Data loss window:** any writes the old primary acked but had not
yet shipped to its replica are lost. Sub-millisecond under light
load; can be hundreds of milliseconds to seconds under burst load.

**Recovery:** automatic — verify and sweep for stranded actors.

```bash
# Confirm failover completed
kubectl -n ate-system exec valkey-cluster-1 -- vcli cluster info
# Expect: cluster_state:ok

# Verify the recreated pod rejoined as replica
kubectl -n ate-system exec <recreated-pod> -- vcli info replication

# Sweep for stranded actors (RESUMING/SUSPENDING/PAUSING past lock TTL)
# This requires an admin command or direct query; if none exists yet,
# log into ateapi and check actor status counts via the API.
```

**Postmortem:** failover trigger; measured cluster-pause duration
(first CLUSTERDOWN to first successful write); any actors found
stranded and how they were unstuck.

</details>

### Whole-shard loss (both pods of one shard down simultaneously)
<details>
<summary><em>Expand</em></summary>

**Symptom:** Multiple shard pods restarting at once; cluster-wide
writes failing; `cluster_state:fail` persists past ~30 s.

**What's happening:** both primary and replica of a shard are down.
Most common cause is a shared infrastructure failure (both pods on
the same K8s node — anti-affinity not configured today, so this is
possible).

**Recovery:** depends on whether PVCs survived.

> **WARNING:** several branches below involve discarding data.
> Capture PVC contents and logs *before* any destructive action.

```bash
# 1. Identify the affected shard and its PVCs
kubectl -n ate-system get pods -l app=valkey-cluster -o wide
kubectl -n ate-system get pvc

# 2. PVCs survived → wait for AOF replay on pod restart
#    Up to ~1s of acked writes lost (appendfsync everysec default).

# 3. One PVC survived, one lost → surviving pod replays AOF and
#    serves; lost pod's new PVC will full-resync from the surviving
#    pod. No additional data loss beyond (2).

# 4. Both PVCs lost → permanent data loss for that shard's slot
#    range. No off-cluster backups today. Escalate; do not attempt
#    re-bootstrap without senior on-call involvement.
```

**Postmortem:** correlation cause (same node? same AZ?); PVC state
and recoverability; whether anti-affinity should be configured
before next deployment.

</details>

## Worker cache failures

### Cache not ready (during startup or resync)
<details>
<summary><em>Expand</em></summary>

**Symptom:** ResumeActor calls return errors mentioning "worker
cache not ready"; happens at API-server pod startup and during
post-pub/sub-disconnect resync windows.

**What's happening:** the cache is mid-`ListWorkers` initial sync.
At 10k workers this is seconds; at 100k workers it's tens of
seconds. During this window the pod accepts connections but
`Workers()` refuses with a clear error.

**Recovery:** wait. The cache will become ready when sync completes.
If the window is longer than expected, it means `ListWorkers` is
slow (Valkey cluster pressure, network issues) or the initial
worker count is unexpectedly large.

```bash
# Watch the API server logs for "worker cache synced" message
kubectl -n ate-system logs deployment/ate-api-server -f | grep -i "cache"
```

**Mitigation to consider:** stagger API-server pod restarts so the
fleet doesn't all sync at once. PDB with `maxSurge: 1` helps.

</details>

### Ghost workers (cache sees workers that don't exist)
<details>
<summary><em>Expand</em></summary>

**Symptom:** AssignWorker picks a worker, atelet dial fails (pod
unreachable), workflow retries, picks another, sometimes the same
ghost again. Background of intermittent `CallAteletRestore` errors.

**What's happening:** A K8s node failure has taken down worker pods,
but K8s has not yet evicted them from the API view. The default
node-failure-to-eviction delay is **several minutes**
(`node-monitor-grace-period` 40s + `pod-eviction-timeout` 5min).
During that window the cache contains records pointing at dead
pods. The syncer only sees the delete events after eviction, so the
cache doesn't update until then.

**Recovery:** the system self-heals once K8s evicts the dead pods
(syncer sees deletes → `DeleteWorker` → `WorkerEvent{Deleted}` →
caches update). The scheduler retry budget covers the meantime —
each failed dial moves to a different worker.

If the window is unacceptable for the workload:

```bash
# Identify the dead node and force pod eviction
kubectl get nodes
kubectl cordon <dead-node>
kubectl drain <dead-node> --ignore-daemonsets --delete-emptydir-data --force
# This causes immediate pod deletion; syncer will see the events and clean caches.
```

**Postmortem:** how long was the ghost window; how many ResumeActor
calls failed; whether `node-monitor-grace-period` should be tuned
for the workload sensitivity.

</details>

### Missed pub/sub events
<details>
<summary><em>Expand</em></summary>

**Symptom:** Worker state observed in `ate actor list` (or via
`vcli`) doesn't match what the cache returns. Typically: a worker
visible via direct Valkey query but not in the cache, or vice versa.

**What's happening:** Redis pub/sub is fire-and-forget. Events can be
dropped when the subscriber's connection blips, when the subscriber
buffer (128 events deep) fills under burst, or during a primary
failover. The periodic relist is the durability backstop but only
catches up at its scheduled interval.

**Recovery:** the periodic relist will fix it. If the discrepancy
needs to be resolved immediately, restart the affected API-server
pod — the new pod will run a fresh initial sync.

```bash
kubectl -n ate-system rollout restart deployment ate-api-server
# Wait for new pod to become ready (initial-sync cost applies)
```

**Postmortem:** what caused the drop; whether the periodic-relist
interval is appropriate (shorter = faster catch-up at cost of
periodic O(N) read).

</details>

### Pub/sub cluster-bus amplification
<details>
<summary><em>Expand</em></summary>

**Symptom:** at scale (50+ primaries), elevated CPU on every
Valkey primary, growing roughly with worker churn rate.

**What's happening:** classic Redis pub/sub (`PUBLISH` / `SUBSCRIBE`)
in cluster mode broadcasts every published message to every primary
via the cluster bus, regardless of where subscribers are. Each
worker event amplifies O(N) in primary count. The cost is invisible
at 3 primaries; meaningful at 50+; potentially binding at 200+.

**Recovery:** none in-incident — this is a scaling-curve concern,
not an outage.

**Mitigation:** migrate `Publish`/`Subscribe` to **sharded pub/sub**
(`SPublish`/`SSubscribe`, Redis 7+) — events propagate only within
the shard. Mechanical change in `ateredis.go` and `workercache.go`,
but it changes the routing semantics so all subscribers and
publishers must agree.

</details>

## Data & storage failures

### AOF corruption
<details>
<summary><em>Expand</em></summary>

**Symptom:** Pod in `CrashLoopBackOff` with logs showing `Bad file
format reading the append only file` or `Unexpected EOF`.

**What's happening:** AOF was torn — typically a power-loss-style
crash. Valkey refuses to start a primary with an unparseable AOF.
If the affected pod is a primary, this becomes a primary-loss event
(see above) inheriting the cluster-wide pause window; the corrupted
pod then comes up as the new replica in crash-loop until repaired.

**Recovery:**

```bash
# Copy the AOF off for postmortem before fixing
kubectl -n ate-system cp <affected-pod>:/data/appendonly.aof /tmp/incident-aof-$(date +%s).aof

# Attach a debug container that mounts the affected pod's PVC,
# then truncate the AOF to the last valid command:
kubectl -n ate-system exec -it <debug-pod> -- valkey-check-aof --fix /data/appendonly.aof
# Confirm acceptable loss; type 'y' to truncate.

# Delete the affected pod; StatefulSet recreates and AOF loads cleanly.
kubectl -n ate-system delete pod <affected-pod>
```

**Mitigation to consider:** `aof-load-truncated yes` is the modern
default and auto-handles cleanly-truncated tails; verify it's set
in your `valkey.conf`. Storage with stronger crash semantics
(PD-SSD with journaling) reduces corruption rate.

</details>

### Memory pressure / OOM
<details>
<summary><em>Expand</em></summary>

**Symptom:** K8s `OOMKilled` event on a Valkey pod. Pod restarts with
AOF replay. If primary, becomes a failover event.

**What's happening:** `maxmemory` is not set today — Valkey grows
until the pod's K8s memory limit triggers OOMKill.

**Recovery (immediate):** the pod restarts; verify per "Replica loss"
or "Primary loss" as appropriate.

**Mitigation (prevent recurrence):**

```bash
# Set maxmemory explicitly, below the K8s pod limit, with noeviction
kubectl -n ate-system exec <pod> -- vcli config set maxmemory 3gb
kubectl -n ate-system exec <pod> -- vcli config set maxmemory-policy noeviction
# Persist in valkey.yaml so it survives restarts.
```

`noeviction` makes writes fail with a clear OOM error rather than
silently evicting actor records.

</details>

### Disk full / PVC expansion
<details>
<summary><em>Expand</em></summary>

**Symptom:** writes failing with disk-related errors; PVC usage at
or near 100%.

**What's happening:** AOF grew past the PVC capacity (currently
1 Gi default).

**Recovery:**

```bash
# Confirm storage class supports online expansion
kubectl get storageclass <class> -o yaml | grep allowVolumeExpansion

# Patch the PVC to a larger size
kubectl -n ate-system patch pvc data-<affected-pod> \
  -p '{"spec":{"resources":{"requests":{"storage":"10Gi"}}}}'

# Wait for FileSystemResizeSuccessful; may require pod restart
kubectl -n ate-system describe pvc data-<affected-pod>
```

If the storage class doesn't support expansion, migrate the shard to
a new PVC of the right size.

</details>

### IOPS exhaustion
<details>
<summary><em>Expand</em></summary>

**Symptom:** client p99 latency spike with no corresponding QPS
spike; fsync latency elevated; cloud-provider burst-credit metrics
showing exhaustion.

**What's happening:** the storage tier's IOPS budget is exhausted
(PD-SSD throttling, EBS burst credits gone). Valkey's
single-threaded primary blocks on the next fsync, queueing every
other operation.

**Recovery:** usually transient — credits replenish. If sustained,
upgrade the storage class or provision explicit IOPS (PD-Extreme,
gp3 with provisioned IOPS). Not an in-incident fix.

</details>

## Network failures

### Network partition / split-brain
<details>
<summary><em>Expand</em></summary>

**Symptom:** `MASTERDOWN` / `CLUSTERDOWN` from a subset of pods;
cluster-state divergence between subsets.

**What's happening:** A network partition cuts some Valkey pods from
others. Cluster mode uses majority-quorum elections, so strict
split-brain (two primaries on same slots both accepting writes) is
avoided — replicas only promote with majority quorum.

**Recovery:** **do not** manually promote anything during a live
partition. Engage networking on-call. The cluster heals
automatically when network is restored; some writes from the
minority side may be lost on heal.

After heal, run the "Primary loss" recovery for each shard whose
primary was on the minority side.

</details>

### Client slot-map staleness
<details>
<summary><em>Expand</em></summary>

**Symptom:** transient `MOVED` / `ASK` errors in `ate-api-server`
logs after a failover.

**What's happening:** each `redis.ClusterClient` caches the slot
map; after a failover, the cached map is briefly stale until the
client refreshes it on the next `MOVED` response. go-redis handles
this transparently with 3 retries by default.

**Recovery:** none needed — auto-resolves within a few operations.
If redirect storms are frequent, increase `MaxRedirects` in the
client config.

</details>

## Security failures

### Pod certificate expiry
<details>
<summary><em>Expand</em></summary>

**Symptom:** TLS handshake errors in `ate-api-server` or Valkey logs.

**What's happening:** under normal operation Valkey re-reads its
cert from the projected volume every 12 hours
(`tls-auto-reload-interval 43200`), so the routine "signer rotated
the cert, Valkey picks it up" path is automatic and silent. TLS
errors mean one of:

1. **The signing controller failed to rotate** before the old cert
   expired. The projected volume still holds the expired cert.
2. **The signer rotated but Valkey hasn't reloaded yet.** The
   12-hour reload window means there's up to a 12-hour gap between
   a new cert appearing in the volume and Valkey actually serving
   it. Usually invisible (signer rotates well before expiry); only
   matters if the cert TTL is short or rotation happened during a
   tight window.
3. **CA bundle mismatch** — see next section.

**Recovery:**

```bash
# 1. Verify the controller is healthy.
kubectl -n <controller-ns> get pods -l app=pod-certificate-controller
# Fix it first if it's down — neither auto-reload nor a manual restart
# will help if the signer can't issue new certs.

# 2. Check whether the projected volume already has a fresh cert.
#    If it does, Valkey will pick it up at its next 12h reload; you
#    can wait, or force an immediate reload by restarting pods.
kubectl -n ate-system exec valkey-cluster-0 -- \
  openssl x509 -in /run/servicedns.podcert.ate.dev/credential-bundle.pem \
  -noout -dates

# 3. If immediate reload is needed (cert is fresh on disk, you don't
#    want to wait up to 12h), restart pods. Stagger to avoid
#    simultaneous failovers — replicas first, then primaries:
for pod in valkey-cluster-{3,4,5}; do
  kubectl -n ate-system delete pod $pod
  kubectl -n ate-system wait pod/$pod --for=condition=Ready --timeout=120s
done
for pod in valkey-cluster-{0,1,2}; do
  kubectl -n ate-system delete pod $pod
  kubectl -n ate-system wait pod/$pod --for=condition=Ready --timeout=120s
done
```

For most cert-rotation incidents under the auto-reload setup, the
pod restart in step 3 is **not** needed — fix the signer (step 1)
and the next 12h reload picks up the new cert. The restart loop is
the lever for cases where you can't wait 12h (emergency revocation,
CA bundle change requiring immediate effect).

**Mitigation:** alert on cert TTL at 50% remaining, not at 5% —
gives the signer + 12h reload window plenty of room. Monitor the
signing controller's own health independently; an unhealthy signer
will silently let certs age out even with auto-reload working
perfectly.

</details>

### CA bundle rotation
<details>
<summary><em>Expand</em></summary>

**Symptom:** TLS verification failures cluster-wide immediately
after a `valkey-ca-certs` secret change.

**What's happening:** the K8s projected volume updates the
on-disk CA bundle within ~60 s of the secret change. Valkey
re-reads its CA file from disk on the **same 12-hour auto-reload
cycle as the server cert** (`tls-auto-reload-interval 43200`). So
even after the secret is corrected, Valkey continues serving with
its previously-loaded CA until the next reload — up to 12 hours
unless you force a restart.

**Recovery (time-critical):** revert the CA bundle, then force an
immediate reload via pod restart.

```bash
# 1. Revert to the known-good CA bundle
kubectl -n ate-system apply -f /path/to/known-good-valkey-ca-certs.yaml

# 2. Wait briefly for the projected volume to pick up the secret
#    change (~60s)

# 3. Force Valkey to reload from disk immediately by restarting pods.
#    Stagger as in the cert-expiry recovery — replicas first, then
#    primaries:
for pod in valkey-cluster-{3,4,5}; do
  kubectl -n ate-system delete pod $pod
  kubectl -n ate-system wait pod/$pod --for=condition=Ready --timeout=120s
done
for pod in valkey-cluster-{0,1,2}; do
  kubectl -n ate-system delete pod $pod
  kubectl -n ate-system wait pod/$pod --for=condition=Ready --timeout=120s
done
```

Do **not** retry the CA rotation in-incident. Plan a proper
bundle-overlap rotation as a separate change: apply a CA bundle
containing *both* old and new CAs first; wait for at least the
auto-reload window (12h) or force a restart to ensure every pod has
loaded the bundle; only then narrow to new-CA-only and again wait
or restart.

</details>

## Kubernetes-level failures

### Pod stuck Pending
<details>
<summary><em>Expand</em></summary>

**Symptom:** a Valkey pod in `Pending` for more than a few minutes.

**Recovery:** read K8s events to find the cause and fix it.

```bash
kubectl -n ate-system describe pod <pending-pod>
# Common causes: insufficient resources, PVC binding failure,
# image pull failure, taint/toleration mismatch.
```

</details>

### PVC / PV failure
<details>
<summary><em>Expand</em></summary>

**Symptom:** PVC stuck `Pending` or PV in `Failed` state; pod can't
start because volume won't mount.

**Recovery:** depends on whether the underlying storage failure is
transient or permanent.

> **WARNING:** PVC deletion is permanent unless the PV reclaim
> policy is `Retain`. Confirm before any destructive action.

```bash
kubectl get pv | grep ate-system
kubectl describe pv <pv-name>

# If transient: wait or trigger retry
# If permanent + worker pod is a replica: delete PVC, let StatefulSet
#   create fresh; new pod full-resyncs from primary (no cluster-wide
#   data loss).
# If permanent + worker pod is a primary: replica gets promoted via
#   failover (see Primary loss); handle the new replica as above.
# If both PVCs of a shard lost: see Whole-shard loss.
```

</details>

### Multi-pod node loss
<details>
<summary><em>Expand</em></summary>

**Symptom:** K8s node `NotReady`; multiple Valkey pods restarting or
Pending; potentially a whole-shard loss if a shard's two pods
co-located on the lost node.

**Recovery:** identify the affected shards and apply the appropriate
single-failure recovery for each. If a shard had both pods on the
lost node, this is whole-shard loss.

This is the failure scenario anti-affinity prevents. Adding
required-during-scheduling pod anti-affinity to the Valkey
StatefulSet before the next production deployment closes this class
of incident.

</details>

## Common admin operations

### Inspect cluster state

```bash
kubectl -n ate-system exec valkey-cluster-0 -- vcli cluster info
kubectl -n ate-system exec valkey-cluster-0 -- vcli cluster nodes
# Expect cluster_state:ok, cluster_slots_ok:16384
```

### Inspect worker cache state from an API server pod

```bash
# Number of workers in the cache (via the API)
kubectl -n ate-system exec deployment/ate-api-server -- ate worker list | wc -l

# Compare against the source-of-truth count in Valkey
kubectl -n ate-system exec valkey-cluster-0 -- vcli --cluster call valkey-cluster-0:6379 --no-auth-warning eval "return #redis.call('keys', 'worker:*')" 0
```

If the two numbers differ persistently, the cache is missing events
— see "Missed pub/sub events" above.

### Trace which actor a worker is bound to

```bash
# Direct from Valkey
kubectl -n ate-system exec valkey-cluster-0 -- vcli get worker:<ns>:<pool>:<pod>
# Look for the actor_id field in the returned JSON.
```

### Force the workercache to re-sync

```bash
# Restart the API server pod — it will run a fresh initial sync
kubectl -n ate-system rollout restart deployment ate-api-server
```

## Open risks

The shortlist worth tracking as the deployment grows. Each one is
either accepted-with-rationale or carried-pending-action.

| Risk | Severity | Status |
|---|---|---|
| `cluster-require-full-coverage yes` makes any failover a cluster-wide pause | Cluster-wide availability | Accepted at MVP; flip to `no` before any scale-out |
| No pod anti-affinity → primary + replica can co-locate | Data loss + availability | **Active**: add before next production deployment |
| No off-cluster backups → PVC loss = permanent shard data loss | Data loss | Active: blocking for GA |
| Async-replication tail lost on failover (no `min-replicas-to-write`) | Data loss (variable) | Accepted at MVP; revisit at scale |
| No `maxmemory` set; relies on K8s pod limit as ceiling | Data loss + availability | **Active**: cheap fix, high ROI |
| 1 Gi PVC default; can fill under AOF growth | Latency / operational | **Active**: bump default + add monitoring |
| No Pod Disruption Budget on the Valkey StatefulSet | Availability | **Active**: cheap fix, prevents resync storms |
| No pod-certificate expiry alerting | Cluster-wide availability | **Active**: alert at 50% TTL remaining |
| Pub/sub broadcast amplification at high primary count | Scaling | Watch — migrate to `SPUBLISH`/`SSUBSCRIBE` if/when N > ~50 primaries |
| Worker cache initial-sync cost on every API-server restart | Operational | Bounded; stagger restarts via PDB |
| K8s node-failure → ghost workers in cache for minutes | Operational | Bounded by K8s eviction timeouts; consider tuning if user-visible |
| `DebugClearAll` is package-public with no production guard | Operational hazard | **Active**: rename to `_TESTONLY` or build-tag guard |

**Items marked Active are the short list of cheap, high-ROI
hardening steps that should land before any meaningful production
exposure.** Most are one-line config changes or single-line code
changes. The bigger architectural items (off-cluster backups,
anti-affinity, sharded pub/sub, `min-replicas-to-write`) deserve
their own design conversations and aren't single-PR work.
