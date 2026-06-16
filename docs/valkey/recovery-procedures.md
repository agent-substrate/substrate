# Recovery Procedures

This page is the runbook companion to
[`failure-modes.md`](./failure-modes.md). For each failure mode that
needs operator action (or operator-verified automatic recovery), it
gives a concrete procedure: what symptom brings you here, what state
you are trying to reach, the commands to run, and how to confirm
recovery.

These procedures are written for the engineer reading this with a
paging alert open. They assume working knowledge of `kubectl` and
shell, basic familiarity with Valkey / Redis cluster commands, and
the ability to `exec` into pods in the `ate-system` namespace. They
do not re-explain what each failure means — go read the matching
entry in [`failure-modes.md`](./failure-modes.md) first if you are
unfamiliar with the failure mode.

## How to use this page

1. **Identify the failure mode** from the detection signals in
   [`failure-modes.md`](./failure-modes.md). The detection cheatsheet
   at the end of that page is the fastest entry point.
2. **Find the matching procedure (R-N)** below. Each procedure names
   the failure modes it covers in its trigger section.
3. **Run universal preflight** before any destructive action — see
   the section immediately below.
4. **Work the procedure** top to bottom. Do not skip verification
   steps; "looks better" is not "recovered."
5. **Capture artifacts** for the postmortem as you go. Each procedure
   names what to capture before moving past each branch point.

> **Do not make it worse.** Several procedures below have steps that
> can permanently lose data or escalate the failure if performed at
> the wrong time. Where this is true, the procedure says so
> explicitly with a warning callout. If you are unsure, **stop and
> escalate** rather than proceeding.

## Universal preflight

Run these before any non-trivial recovery action. They serve two
purposes: confirming the scope of the incident, and preserving
evidence for the postmortem.

```bash
# 1. Snapshot the K8s state of the Valkey StatefulSet
kubectl -n ate-system get pods -l app=valkey-cluster -o wide > /tmp/incident-pods-$(date +%s).txt
kubectl -n ate-system describe statefulset valkey-cluster > /tmp/incident-sts-$(date +%s).txt
kubectl -n ate-system get events --sort-by=.lastTimestamp | tail -100 > /tmp/incident-events-$(date +%s).txt

# 2. Snapshot the cluster's own view of itself
kubectl -n ate-system exec valkey-cluster-0 -- \
  valkey-cli --tls \
    --cacert /etc/valkey-ca/ca.crt \
    --cert /run/servicedns.podcert.ate.dev/credential-bundle.pem \
    --key /run/servicedns.podcert.ate.dev/credential-bundle.pem \
    cluster info > /tmp/incident-cluster-info-$(date +%s).txt

kubectl -n ate-system exec valkey-cluster-0 -- \
  valkey-cli --tls \
    --cacert /etc/valkey-ca/ca.crt \
    --cert /run/servicedns.podcert.ate.dev/credential-bundle.pem \
    --key /run/servicedns.podcert.ate.dev/credential-bundle.pem \
    cluster nodes > /tmp/incident-cluster-nodes-$(date +%s).txt
```

The long TLS flag set is identical to the init Job; see
[`topology.md`](./topology.md). All `valkey-cli` invocations below
elide the TLS flags for readability — substitute the full set when
running.

A useful shorthand if you'll be running many commands:

```bash
alias vcli='valkey-cli --tls --cacert /etc/valkey-ca/ca.crt --cert /run/servicedns.podcert.ate.dev/credential-bundle.pem --key /run/servicedns.podcert.ate.dev/credential-bundle.pem'
```

## Pod & process recovery

### R-1: Verify automatic failover after primary loss
<details>
<summary><em>Expand</em></summary>

**Triggers.** Primary loss & automatic failover
([`failure-modes.md`](./failure-modes.md)). Symptoms: K8s pod restart
on a Valkey pod, spike of `MOVED` / `CLUSTERDOWN` errors in
`ate-api-server` logs, cluster-wide write pause for ~5–10 s.

**Goal.** Confirm the failover completed cleanly, the new topology
is stable, and no actor was left in a stranded transitional state.

**Procedure.**

1. Run universal preflight.

2. Identify which shard's primary died. From `cluster-nodes` output:
   ```
   <node-id> <ip>:6379 master fail - <ping-sent> <pong-recv> <epoch> disconnected <slots>
   <other-id> <ip>:6379 master - <ping-sent> <pong-recv> <epoch> connected <slots>
   ```
   The `master fail` line is the lost primary. The slot range on that
   line is what was unavailable during the failover window.

3. Confirm a new primary has been elected for that slot range:
   ```bash
   kubectl -n ate-system exec valkey-cluster-0 -- vcli cluster info
   ```
   Expect `cluster_state:ok`. If it still shows `cluster_state:fail`
   minutes after the original event, jump to **R-3** (whole-shard
   recovery) — failover did not complete.

4. Identify the recreated pod and verify it has come back as a
   replica:
   ```bash
   kubectl -n ate-system get pods -l app=valkey-cluster
   kubectl -n ate-system exec <recreated-pod> -- vcli info replication
   ```
   Expect `role:slave`, `master_link_status:up`, and
   `slave_repl_offset` advancing toward the new primary's
   `master_repl_offset`.

5. Watch resync complete. For a small data set (~100 MB) this is
   seconds; for ~10 GB it can be tens of seconds. The shard is
   single-pod (no HA) for the duration of the resync — flag this
   as elevated risk in the incident channel.

6. Check for stranded actors. Per
   [`actor-lifecycle.md`](./actor-lifecycle.md), an actor's
   workflow can strand in RESUMING or SUSPENDING if a CAS-write
   landed on the wrong side of the failover. Spot-check by running:
   ```bash
   # From a pod with API-server access
   ate actors list --status RESUMING,SUSPENDING --older-than 1m
   ```
   Any actor that has been transitional for more than ~1 minute is
   stranded and needs a manual `ate actor suspend` (then a
   subsequent resume) to reach a clean state.

**Verification.** All four conditions hold:
- `cluster_state:ok`
- All 6 pods Running and Ready
- All replicas show `master_link_status:up` with bounded
  `slave_repl_offset` lag
- No actors stranded transitional for > 1 minute

**Postmortem capture.** Note the failover trigger (was it
OOMKilled? node lost? voluntary disruption?), the failover
duration measured from first `CLUSTERDOWN` to first successful
write, and any data-loss tail (writes acked before the failure
that did not survive — usually only visible via application
log correlation).

</details>

### R-2: Verify replica resync after replica restart
<details>
<summary><em>Expand</em></summary>

**Triggers.** Single replica loss
([`failure-modes.md`](./failure-modes.md)). Symptoms: a single
non-primary Valkey pod restarted; no client-visible errors.

**Goal.** Confirm the replica is fully resynced and the shard is
back to HA.

**Procedure.**

1. Run universal preflight (lightweight version — pod state +
   cluster nodes is sufficient).

2. Identify the restarted pod:
   ```bash
   kubectl -n ate-system get pods -l app=valkey-cluster \
     --sort-by=.status.startTime
   ```
   Most-recently-started is at the bottom.

3. From the restarted pod, check replication state:
   ```bash
   kubectl -n ate-system exec <pod> -- vcli info replication
   ```
   Expect `role:slave`, `master_link_status:up`. If
   `master_link_status:down`, the replica cannot reach its primary
   (TLS issue, network, primary unhealthy) — escalate to investigate
   the primary side.

4. Confirm resync is bounded:
   ```bash
   # On the replica
   kubectl -n ate-system exec <pod> -- vcli info replication | \
     grep -E "(master_repl_offset|slave_repl_offset|master_sync_in_progress)"
   ```
   `master_sync_in_progress:0` means partial-resync (instant) or
   already-caught-up. `master_sync_in_progress:1` means full resync
   in flight — let it complete.

5. While resync runs, do **not** restart any other pod. Resync
   storms (see [`failure-modes.md`](./failure-modes.md)) are caused
   by parallel full resyncs against the same primary. If a deploy
   or rolling restart is in flight, pause it.

**Verification.** Replica reaches
`master_link_status:up`, `master_sync_in_progress:0`,
`slave_repl_offset` within a few hundred KB of
`master_repl_offset`. Shard is back to HA.

**Postmortem capture.** Note the restart cause (OOMKilled?
manual? deploy?). Note resync type (partial vs full). If full,
note the duration — this informs `repl-backlog-size` tuning.

</details>

### R-3: Recover from whole-shard loss
<details>
<summary><em>Expand</em></summary>

**Triggers.** Whole-shard loss
([`failure-modes.md`](./failure-modes.md)). Symptoms: both pods of
a shard restarting; cluster-wide writes failing with `CLUSTERDOWN`
(under current `cluster-require-full-coverage yes` setting);
`cluster_state:fail` persists past ~30 s.

**Goal.** Restore at least one pod of the affected shard to a
serving state. Accept the data-loss window if PVCs survived; if
not, decide between accepting full shard data loss and restoring
from off-cluster backup (note: no backups are configured today —
see [`failure-modes.md`](./failure-modes.md)).

**Procedure.**

> **WARNING.** Several branches below involve discarding data.
> Capture all PVC contents and pod logs *before* any destructive
> action. If you are unsure which branch applies, stop and
> escalate.

1. Run universal preflight in full.

2. Identify the affected shard's pods (e.g. `valkey-cluster-1` and
   `valkey-cluster-4` if shard B is down):
   ```bash
   kubectl -n ate-system get pods -l app=valkey-cluster -o wide
   ```

3. Check PVC status for both affected pods:
   ```bash
   kubectl -n ate-system get pvc -l app=valkey-cluster
   kubectl -n ate-system describe pvc data-valkey-cluster-1 data-valkey-cluster-4
   ```
   PVC `Bound` to a `Available` PV → data survives. PVC `Lost`
   or PV missing → data is gone for that pod.

4. Branch on PVC state:

   **Branch A — both PVCs survived.** Wait for AOF replay. Each
   pod will come up and load its AOF; this takes time proportional
   to data size. Up to ~1 s of acked writes may be lost (default
   `appendfsync everysec`). Skip to step 6.

   **Branch B — one PVC survived, one lost.** The surviving pod
   will load its AOF and serve. The other pod will start with an
   empty data dir; the cluster will treat it as a fresh replica
   and full-resync from the surviving pod. No additional data loss
   beyond Branch A. Skip to step 6.

   **Branch C — both PVCs lost.** Shard data is permanently lost
   unless an off-cluster backup exists. Confirm with leadership
   before proceeding. If no backup exists or accepting loss is
   approved:
   - The shard must be re-added to the cluster with an empty data
     dir.
   - This requires explicit `CLUSTER RESET` + re-bootstrap of just
     the affected pods. Procedure is **escalation-only** — do not
     attempt without engaging a senior on-call. Risk: a botched
     re-bootstrap can corrupt the whole cluster's slot map.

5. (Branch C continuation, escalation-only) Coordinate with senior
   on-call to:
   - Verify the slot range that needs reassignment.
   - Bring up the affected pods with empty PVCs.
   - Manually rebuild the cluster topology including the new
     pods.
   - Confirm `cluster_state:ok` and all 16,384 slots covered.

6. Once both pods are up: verify the cluster state:
   ```bash
   kubectl -n ate-system exec valkey-cluster-0 -- vcli cluster info
   ```
   Expect `cluster_state:ok`, `cluster_known_nodes:6`,
   `cluster_size:3`, `cluster_slots_assigned:16384`,
   `cluster_slots_ok:16384`.

7. Run **R-1** to verify the recovered shard is HA-healthy.

8. Sweep for stranded actors. Per
   [`actor-lifecycle.md`](./actor-lifecycle.md), every actor whose
   keys hashed into the lost shard's slot range was unavailable
   during the outage; some workflows likely stranded.

**Verification.** `cluster_state:ok`, both pods Running, replication
healthy, no stranded actors.

**Postmortem capture.** Critical:
- Cause of the simultaneous double pod loss (same node? same AZ?
  application-driven OOM cascade?).
- PVC state and any data lost.
- Whether `cluster-require-full-coverage yes` was the right call
  for this scale — see [`topology.md`](./topology.md) and
  [`failure-modes.md`](./failure-modes.md).
- Whether anti-affinity / topology spread should be configured
  *before* the next deployment to prevent recurrence.

</details>

## Data & storage recovery

### R-4: Recover from AOF corruption
<details>
<summary><em>Expand</em></summary>

**Triggers.** AOF corruption / partial write
([`failure-modes.md`](./failure-modes.md)). Symptoms: pod in
`CrashLoopBackOff`; pod logs contain
`Bad file format reading the append only file`,
`Unexpected EOF`, or similar AOF parse error.

**Goal.** Restore the pod to a serving state with a truncated AOF.
Some tail writes will be lost; this is unavoidable.

**Procedure.**

> **WARNING.** `valkey-check-aof --fix` is destructive — it
> truncates the AOF in place. Snapshot the file first.

1. Run universal preflight.

2. Identify the crash-looping pod and capture its logs and AOF:
   ```bash
   kubectl -n ate-system logs <pod> --previous > /tmp/incident-pod-logs-$(date +%s).txt

   # Copy the AOF off the PVC for postmortem
   kubectl -n ate-system cp <pod>:/data/appendonly.aof \
     /tmp/incident-aof-$(date +%s).aof
   ```
   If the pod is in `CrashLoopBackOff`, `kubectl cp` may need a
   timing window between restarts; alternatively, attach a
   debug container.

3. Stop the StatefulSet-managed restart cycle for the affected
   pod by scaling down its specific behavior is not directly
   possible. Instead, run the fix via a one-shot debug pod that
   mounts the same PVC:
   ```yaml
   apiVersion: v1
   kind: Pod
   metadata:
     name: valkey-aof-fix
     namespace: ate-system
   spec:
     restartPolicy: Never
     containers:
     - name: fix
       image: valkey/valkey:8.0
       command: ["sh", "-c", "sleep 3600"]
       volumeMounts:
       - name: data
         mountPath: /data
     volumes:
     - name: data
       persistentVolumeClaim:
         claimName: data-<affected-pod-name>
   ```
   Apply, then exec in:
   ```bash
   kubectl -n ate-system exec -it valkey-aof-fix -- \
     valkey-check-aof --fix /data/appendonly.aof
   ```
   The tool reports the offset of the first corruption and the
   amount of data that will be truncated. Confirm acceptable
   loss; type `y` to proceed.

4. Delete the debug pod and let the StatefulSet's failed pod
   restart naturally:
   ```bash
   kubectl -n ate-system delete pod valkey-aof-fix
   kubectl -n ate-system delete pod <affected-pod>
   ```

5. Watch the pod come up. Expect normal startup logs (no AOF
   error), then `Ready to accept connections`.

**Verification.** Pod Running and Ready. If the pod was a primary,
run **R-1** to confirm failover/recovery story. If it was a
replica, run **R-2**.

**Postmortem capture.** Cause of the original crash (was the AOF
corrupted by a power loss? K8s evict? OOM?). Amount of data
truncated. Whether `aof-load-truncated yes` (which auto-handles
cleanly truncated tails) was already in effect — if not, enabling
it pre-empts this class of incident for clean truncations.

</details>

### R-5: Expand a full PVC
<details>
<summary><em>Expand</em></summary>

**Triggers.** Disk full
([`failure-modes.md`](./failure-modes.md)). Symptoms: writes failing
with `MISCONF` or `OOM` errors mentioning disk; PVC usage at or
near 100%; AOF rewrite failures in Valkey logs.

**Goal.** Increase available disk on the affected PVC.

**Procedure.**

1. Run universal preflight.

2. Confirm disk full on the affected pod:
   ```bash
   kubectl -n ate-system exec <pod> -- df -h /data
   ```

3. Confirm the storage class supports online PVC expansion:
   ```bash
   kubectl get storageclass <class-name> -o yaml | grep allowVolumeExpansion
   ```
   If `false` or missing → expansion is not possible online;
   escalate to consider migrating the shard.

4. Patch the PVC to a larger size:
   ```bash
   kubectl -n ate-system patch pvc data-<affected-pod> \
     -p '{"spec":{"resources":{"requests":{"storage":"5Gi"}}}}'
   ```
   (Choose a size that gives meaningful headroom; doubling the
   current size is a reasonable default.)

5. Watch the expansion proceed:
   ```bash
   kubectl -n ate-system describe pvc data-<affected-pod>
   ```
   Look for `FileSystemResizePending` then `FileSystemResizeSuccessful`.

6. If the filesystem resize requires a pod restart (depends on CSI
   driver), restart the pod:
   ```bash
   kubectl -n ate-system delete pod <affected-pod>
   ```

7. Confirm the new size is visible inside the pod:
   ```bash
   kubectl -n ate-system exec <affected-pod> -- df -h /data
   ```

**Verification.** New size reflected; writes succeeding; no
disk-related errors in logs.

**Postmortem capture.** What drove the disk usage growth (data
growth? AOF rewrite never completing? oversized backlog?). Whether
the 1 Gi default in [`topology.md`](./topology.md) should be
updated for future deployments.

</details>

### R-6: Respond to IOPS exhaustion
<details>
<summary><em>Expand</em></summary>

**Triggers.** IOPS exhaustion
([`failure-modes.md`](./failure-modes.md)). Symptoms: client p99
latency spike with no corresponding QPS spike; fsync latency
metrics elevated; cloud-provider IOPS / burst-credit metrics
showing exhaustion.

**Goal.** Restore expected latency. Usually self-resolves; the
operator's job is to confirm it's transient or escalate to
provisioning if it's not.

**Procedure.**

1. Run universal preflight.

2. Confirm the symptom is IOPS-bound, not something else:
   - QPS roughly flat or declining (rules out load spike)
   - Memory and CPU not pressured (rules out OOM / CPU starvation)
   - One or a few pods affected (rules out network)

3. Check cloud-provider IOPS metrics for the affected PVC's
   underlying disk. For GCP PD-SSD: `disk/operation_time` and
   `disk/throttled_operations`. For AWS gp3: `VolumeIOPSExceededCheck`
   or burst-balance metrics.

4. Branch:

   **Transient (burst credits exhausted).** Confirm the storage
   class is burst-credit-based, and that credits will replenish.
   The system will recover on its own as credits return. Monitor;
   do not act unless the latency persists beyond expected
   replenishment time.

   **Sustained.** The provisioned IOPS is insufficient for the
   workload. Plan a migration to a higher-IOPS storage class or
   provision explicit IOPS (GCP PD-Extreme, AWS gp3 with
   provisioned IOPS). This is a planned change, not an
   in-incident action — surface the recommendation and exit the
   incident.

**Verification.** p99 latency returns to baseline. IOPS metrics
back below throttling thresholds.

**Postmortem capture.** Whether the storage class choice matches
the workload's actual demand. Whether monitoring should fire
*before* the latency hit (on credit-balance trend, not just on
latency).

</details>

### R-7: Recover from OOM kill / set `maxmemory`
<details>
<summary><em>Expand</em></summary>

**Triggers.** Memory pressure & OOM
([`failure-modes.md`](./failure-modes.md)). Symptoms: K8s
`OOMKilled` event on a Valkey pod; pod restart with empty in-memory
state, AOF replay on restart.

**Goal.** Get the pod back up and prevent recurrence. The pod
restart itself is automatic; the operator's job is the
prevention work.

**Procedure.**

1. Run universal preflight.

2. Identify the OOMKilled pod and confirm:
   ```bash
   kubectl -n ate-system describe pod <pod> | grep -A5 "Last State"
   ```
   Expect `Reason: OOMKilled`.

3. If the killed pod was a primary, expect a failover; run **R-1**
   to verify it completed cleanly. If a replica, run **R-2**.

4. Identify the working-set growth that caused the OOM:
   ```bash
   kubectl -n ate-system exec <any-healthy-pod> -- vcli info memory | \
     grep -E "(used_memory_human|used_memory_peak_human|maxmemory_human)"
   ```
   If `maxmemory` is `0`, no limit is set — the pod can grow until
   the K8s pod limit triggers OOMKill.

5. Set `maxmemory` explicitly. The right value is roughly 75% of
   the K8s pod memory limit (leaves headroom for fork-on-BGSAVE,
   client buffers, replication backlog). For a 4 Gi pod limit,
   set 3 Gi:
   ```bash
   kubectl -n ate-system exec <pod> -- vcli \
     config set maxmemory 3gb
   kubectl -n ate-system exec <pod> -- vcli \
     config set maxmemory-policy noeviction
   ```
   `noeviction` is the correct policy for a persistence store —
   it makes writes fail with a clear OOM error instead of silently
   deleting actor records. **Persist** the config change into
   `valkey.yaml` so it survives pod restarts; the `config set` above
   is in-memory only.

6. Monitor memory trend. If `used_memory` is still growing toward
   the new ceiling, the underlying issue (actor / worker count
   growth) needs structural resolution — see scaling math in
   [`topology.md`](./topology.md).

**Verification.** Pod Running, `maxmemory` set, eviction policy
`noeviction`. No further OOMKilled events.

**Postmortem capture.** Working-set growth rate. Whether the pod
memory limit needs raising or the shard needs splitting. Whether
`valkey.yaml` should be updated to set `maxmemory` and
`maxmemory-policy` for every pod by default.

</details>

### R-8: Stop and contain a resync storm
<details>
<summary><em>Expand</em></summary>

**Triggers.** Replication lag / resync storm
([`failure-modes.md`](./failure-modes.md)). Symptoms: multiple
replicas in full-resync simultaneously; primary CPU and network
elevated; latency spiking on affected shards.

**Goal.** Prevent additional replicas from starting concurrent
full resyncs. Let existing resyncs complete serially.

**Procedure.**

> **WARNING.** Restarting any replica during this incident risks
> escalating the storm. Pause all voluntary pod restarts
> (deploys, autoscaler scale-downs) immediately.

1. Run universal preflight.

2. Pause anything that could trigger more pod restarts:
   ```bash
   # Pause any in-flight deploy
   kubectl -n ate-system rollout pause statefulset valkey-cluster

   # Pause autoscaler if applicable
   # (depends on autoscaler setup; consult your platform)
   ```

3. Identify replicas currently in full resync:
   ```bash
   for pod in $(kubectl -n ate-system get pods -l app=valkey-cluster \
                  -o jsonpath='{.items[*].metadata.name}'); do
     echo "== $pod =="
     kubectl -n ate-system exec $pod -- vcli info replication | \
       grep -E "(role|master_sync_in_progress|master_link_status)"
   done
   ```
   Replicas with `master_sync_in_progress:1` are mid-resync.

4. Wait. Resyncs proceed; do not intervene unless a primary is in
   distress (high CPU or memory pressure spilling toward OOM).

5. If a primary IS in distress and might OOM:
   - Confirm `maxmemory` is set with `noeviction` (R-7).
   - Consider temporarily blocking new client writes to give the
     primary headroom — there is no clean knob for this, so it's
     escalation-only.

6. Once all replicas show `master_sync_in_progress:0` and
   replication lag is bounded, resume normal operations:
   ```bash
   kubectl -n ate-system rollout resume statefulset valkey-cluster
   ```

**Verification.** All replicas
`master_sync_in_progress:0`, lag within `repl-backlog-size`. No
more concurrent full resyncs.

**Postmortem capture.** What triggered the storm (rolling deploy?
node maintenance? autoscaler?). Whether a Pod Disruption Budget
on the StatefulSet would prevent recurrence by enforcing serial
restart.

</details>

## Network recovery

### R-9: Recover from network partition
<details>
<summary><em>Expand</em></summary>

**Triggers.** Network partition / split-brain
([`failure-modes.md`](./failure-modes.md)). Symptoms:
`MASTERDOWN` / `CLUSTERDOWN` from a subset of pods or clients;
cluster-state divergence between subsets.

**Goal.** Let the partition heal on its own. Do **not** intervene
manually unless you have explicit confirmation that one side is
permanently lost.

**Procedure.**

> **WARNING.** Manual promotion (`CLUSTER FAILOVER FORCE`) of a
> replica during a network partition can create split-brain if
> the partition heals. **Do not perform manual failover during a
> live partition.**

1. Run universal preflight from a vantage point that can reach
   the majority side. If your `kubectl` is on the minority side,
   you may not be able to.

2. Identify the partition boundary:
   - Which pods can reach which (gossip state via `CLUSTER NODES`
     on both sides will diverge).
   - Whether the partition is K8s-internal (CNI, NetworkPolicy)
     or external (AZ link, regional).

3. Engage networking / platform on-call. Recovery is not a Valkey
   operation; it's a network operation.

4. Once network is restored, the cluster heals automatically.
   Some writes that succeeded on the minority side and were
   never replicated to majority may be lost on heal.

5. Run **R-1** for any shard whose primary was on the minority
   side (it may or may not still be primary post-heal).

**Verification.** `cluster_state:ok` from any pod. `CLUSTER NODES`
identical from all pods. No `MASTERDOWN` / `CLUSTERDOWN` errors.

**Postmortem capture.** Cause of the partition. Duration. Any
data lost on heal. Whether multi-AZ or single-AZ deployment is
the right tradeoff for the failure rate observed.

</details>

## Security recovery

### R-10: Recover from cert expiry
<details>
<summary><em>Expand</em></summary>

**Triggers.** Pod certificate expiry
([`failure-modes.md`](./failure-modes.md)). Symptoms: TLS handshake
errors in `ate-api-server` and / or Valkey logs; cert-expiry
monitoring alert fired.

**Goal.** Get fresh certs onto the affected pods and verify mTLS
is restored cluster-wide.

**Procedure.**

1. Run universal preflight.

2. Confirm the issue is cert expiry and not something else:
   ```bash
   kubectl -n ate-system logs <pod> --tail=200 | grep -i "tls\|certificate\|expired"
   ```

3. Check the pod-certificate controller is healthy:
   ```bash
   kubectl -n <controller-namespace> get pods -l app=pod-certificate-controller
   kubectl -n <controller-namespace> logs <controller-pod> --tail=200
   ```
   If the controller itself is unhealthy, **fix the controller
   first** — restarting pods without a working signer will not
   help, and may make things worse.

4. Force cert rotation on affected pods. The `podCertificate`
   projected volume re-fetches on pod restart, so:
   ```bash
   kubectl -n ate-system delete pod <affected-pod>
   ```
   The StatefulSet will recreate with fresh certs.

5. If all Valkey pods have expired certs, restart them with
   staggering to avoid simultaneous failover:
   ```bash
   for pod in valkey-cluster-{3,4,5}; do
     kubectl -n ate-system delete pod $pod
     # Wait for the pod to be Ready before continuing
     kubectl -n ate-system wait pod/$pod --for=condition=Ready --timeout=120s
   done
   # Then primaries:
   for pod in valkey-cluster-{0,1,2}; do
     kubectl -n ate-system delete pod $pod
     kubectl -n ate-system wait pod/$pod --for=condition=Ready --timeout=120s
   done
   ```
   Restart replicas first so each primary still has its replica
   when it eventually restarts.

**Verification.** No TLS errors in logs for 5+ minutes after the
last pod restart. `cluster_state:ok`.

**Postmortem capture.** Why monitoring didn't fire earlier
(monitor TTL at 50% remaining, not 10%). Whether the controller's
rotation cadence matches the cert TTL safely.

</details>

### R-11: Recover from a botched CA bundle rotation
<details>
<summary><em>Expand</em></summary>

**Triggers.** CA bundle rotation
([`failure-modes.md`](./failure-modes.md)). Symptoms: TLS
verification failures everywhere immediately after a
`valkey-ca-certs` secret change. Effectively a whole-cluster
TLS outage.

**Goal.** Restore TLS by reverting to the previous CA bundle,
then plan a proper bundle-overlap rotation as a separate change.

**Procedure.**

> **WARNING.** Time-critical. Every minute of cert mismatch is a
> minute of cluster outage.

1. Identify the previous-known-good `valkey-ca-certs` secret
   contents. Sources: secret history (if your platform retains
   it), git config repo, or a fresh export from before the
   rotation.

2. Re-apply the previous secret:
   ```bash
   kubectl -n ate-system apply -f /path/to/known-good-valkey-ca-certs.yaml
   ```

3. The `valkey-ca-certs` secret is mounted as a projected volume.
   Most projection types pick up secret changes within ~60 s
   without a pod restart. If pods do not recover within 2 minutes,
   force restart in the same staggered order as R-10.

4. Verify TLS recovery (R-10 step 5 verification).

5. **Stop**. Do not retry the CA rotation in-incident. Plan a
   bundle-overlap rotation as a separate, scheduled change:
   - Build a CA bundle containing **both** old and new CA certs.
   - Apply the bundle to `valkey-ca-certs`.
   - Wait for all pods to pick up the bundle.
   - Issue new client / pod certs signed by the new CA.
   - Once all certs are new-CA-signed, narrow the bundle to
     new-CA-only.

**Verification.** TLS errors clear. `cluster_state:ok`.

**Postmortem capture.** Why the rotation procedure missed the
bundle-overlap discipline. Whether to enforce a check in the
rotation tooling.

</details>

## Kubernetes-level recovery

### R-12: Unstick a Pending pod
<details>
<summary><em>Expand</em></summary>

**Triggers.** StatefulSet pod stuck Pending
([`failure-modes.md`](./failure-modes.md)). Symptoms: a Valkey pod
in `Pending` state for more than a few minutes.

**Goal.** Identify the scheduling block and clear it.

**Procedure.**

1. Run universal preflight.

2. Read the K8s events for the stuck pod:
   ```bash
   kubectl -n ate-system describe pod <pending-pod>
   ```
   The `Events` section at the bottom names the scheduling
   failure: insufficient resources, PVC binding failure, image
   pull failure, taint mismatch, etc.

3. Branch on the cause:

   **Insufficient resources.** Verify node pool capacity; scale
   the node pool up if applicable.

   **PVC binding failure.** Run **R-13**.

   **Image pull failure.** Check image registry access from the
   target node; verify image pull secret if needed.

   **Taints / tolerations.** Verify the StatefulSet's tolerations
   match the available nodes' taints.

4. Once the underlying cause is resolved, the pod schedules
   automatically. No manual intervention needed beyond fixing the
   block.

**Verification.** Pod Running and Ready. Run **R-1** or **R-2**
depending on whether it was a primary or replica.

**Postmortem capture.** Why the resource / image / taint state
was off. Whether monitoring should fire when a pod is Pending
for > N minutes.

</details>

### R-13: Recover from PVC / PV failure
<details>
<summary><em>Expand</em></summary>

**Triggers.** PVC / PV failures
([`failure-modes.md`](./failure-modes.md)). Symptoms: PVC stuck in
`Pending`, PV in `Failed`, pod can't start because volume can't
mount.

**Goal.** Restore the PV (preserving data) or replace the PVC
(accepting data loss for that pod).

**Procedure.**

> **WARNING.** PVC deletion is permanent unless retained by a
> reclaim policy. **Confirm the PV reclaim policy before any
> destructive action.**

1. Run universal preflight.

2. Diagnose the PV state:
   ```bash
   kubectl get pv | grep ate-system
   kubectl describe pv <pv-name>
   ```

3. Branch on the cause:

   **Underlying storage transient failure.** Wait or retry. Some
   CSI drivers self-heal.

   **Underlying storage permanently lost (node disk failure on a
   non-replicated PV).** The data on that PV is gone. Options:
   - If the affected pod is a replica and the primary is healthy:
     delete the PVC, let the StatefulSet create a fresh one, the
     new pod will full-resync from the primary (no cluster-wide
     data loss).
   - If the affected pod is a primary AND the replica is healthy:
     promote the replica first (cluster will failover automatically
     when this pod is unreachable long enough), then handle as
     above on the new replica side.
   - If both pods of the shard are affected: this is whole-shard
     data loss; run **R-3** Branch C.

   **PV provisioner outage.** Engage platform on-call; not a
   Valkey-side fix.

4. To delete a PVC and force a fresh one (only when accepting
   data loss for that pod):
   ```bash
   # Confirm reclaim policy first
   kubectl get pv <pv-name> -o jsonpath='{.spec.persistentVolumeReclaimPolicy}'
   # If Delete: the PV will be cleaned up when PVC is removed
   # If Retain: the PV survives PVC removal; you must clean up separately

   kubectl -n ate-system delete pvc data-<affected-pod>
   kubectl -n ate-system delete pod <affected-pod>
   ```
   The StatefulSet recreates both PVC and pod. The new pod
   full-resyncs from its primary.

**Verification.** PVC `Bound`, pod Running, replication healthy.

**Postmortem capture.** Whether the storage class survives the
failure mode observed. Whether resilient storage (regional
PD-SSD, EBS with cross-AZ) should be the default.

</details>

### R-14: Recover from multi-pod node loss
<details>
<summary><em>Expand</em></summary>

**Triggers.** Multi-pod node loss
([`failure-modes.md`](./failure-modes.md)). Symptoms: K8s node
`NotReady`; multiple Valkey pods restarting / Pending; potentially
a whole-shard loss if a shard's two pods co-located on the lost
node.

**Goal.** Reschedule the affected pods onto healthy nodes;
recover any shard that lost both members.

**Procedure.**

1. Run universal preflight.

2. Identify the lost node and the pods that were on it:
   ```bash
   kubectl get nodes
   kubectl -n ate-system get pods -l app=valkey-cluster -o wide
   ```

3. For each affected pod, determine if its PVC follows or is
   stranded on the dead node:
   - With cloud-provider block storage (PD-SSD, EBS): PVC follows
     to a new node when the pod is rescheduled.
   - With local-PV: PVC is stranded; the pod can't reschedule
     without losing data on that PV.

4. Per-shard branch:

   **A shard with both pods on the lost node.** This is
   whole-shard loss. Run **R-3**.

   **A shard with one pod on the lost node.** Replica- or
   primary-loss event. Run **R-2** or **R-1**.

5. If the node will not return (hardware failure, decommission),
   cordon and drain to prevent further scheduling and let pods
   reschedule onto other nodes:
   ```bash
   kubectl cordon <dead-node>
   kubectl drain <dead-node> --ignore-daemonsets --delete-emptydir-data
   ```

**Verification.** All pods Running on healthy nodes;
`cluster_state:ok`.

**Postmortem capture.** **Critical**: how many shards lost both
pods to the same node. If any, this is direct evidence that
anti-affinity / topology spread is mandatory for the current
shard count. Update [`topology.md`](./topology.md) and the
StatefulSet manifest before the next deployment.

</details>

## Post-incident artifacts

For every incident handled with a procedure above, capture the
following before closing the ticket:

- All `/tmp/incident-*` files generated by universal preflight.
- Pod logs from before and after recovery (`kubectl logs --previous`
  for the failed pods).
- Timeline: first detection signal, each operator action, recovery
  confirmed.
- Data loss accounting if any.
- Whether the recovery procedure as written worked, was incomplete,
  or was wrong — and what to change in this doc for next time.

The handbook gets better only if every incident updates it. A
procedure that didn't quite work is a higher-priority improvement
than a missing procedure for a not-yet-seen failure.
