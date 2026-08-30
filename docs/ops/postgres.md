# Operating the PostgreSQL Store

This page is for whoever runs an Agent Substrate installation. It covers what
the PostgreSQL store holds, which deployment postures are supported, what
happens if the data is lost, and how to size, tune, and watch the database.

Agent Substrate as a whole is not ready for production use (see the README).
This page describes the store's posture within that: which parts of it are a
development convenience and which parts an operator has to supply themselves.

## What the store holds

`ate-api-server` keeps all of its state in one PostgreSQL database. There is no
second copy anywhere: the Kubernetes API holds the declarative objects
(`WorkerPool`, `ActorTemplate`, `SandboxConfig`, and the worker pods), and the
store holds everything the control plane decides at runtime. The schema is in
`cmd/ateapi/internal/store/atepg/schema.go`:

| Table | Holds |
| --- | --- |
| `atespaces` | Tenancy boundaries. |
| `actors` | Every actor: identity, state, and which worker (if any) hosts it. |
| `actor_egress_policies` | Per-actor egress policy. |
| `actor_templates` | Template records. |
| `actor_snapshots`, `actor_snapshot_tags` | Snapshot manifests and the tags that name them, including the golden snapshot tag. |
| `workers` | Every registered worker pod, its capacity, and its current assignment. |
| `leases` | The locks the resume/suspend workflows take. |
| `worker_outbox` and its `worker_outbox_p<timestamp>` partitions, `worker_outbox_default`, `worker_outbox_trim` | The transactional outbox behind `WatchWorkers`, and its retention high-water mark. |

The `actors` and `workers` tables are the system of record for the
actor-to-worker mapping ([`docs/architecture.md`](../architecture.md)). Nothing
reconstructs them: `atelet` exposes no "what are you hosting?" RPC, so the
control plane cannot re-derive placement by asking the data plane. That
property drives most of the rest of this page.

The outbox is deliberately `UNLOGGED` and partitioned, with a 15-minute
retention floor — partitions are dropped whole at a 15-minute granularity, so a
row survives 15 to 30 minutes.
It is a change feed, not a record: a crash truncates it and every
watcher rebuilds its view from the `workers` table. Losing the outbox costs a
relist. Losing `workers` and `actors` is what matters.

## Supported postures

### In-cluster StatefulSet (development and evaluation)

`manifests/ate-install/postgres.yaml` deploys a **single-replica** StatefulSet
onto one `ReadWriteOnce` PVC. It is what `hack/install-ate.sh` installs by
default, and it is a development and evaluation convenience:

- No backups. No WAL archiving, no `pg_dump` schedule, no PITR. Nothing in this
  repository ever copies the data anywhere.
- No replicas and no failover. A restart is a control-plane outage for its
  duration; a lost disk is permanent data loss.
- Trust authentication. `pg_hba.conf` in that manifest is
  `hostssl all all all trust clientcert=verify-ca`, so any client holding a
  certificate from the pod-identity CA logs in as any role, including the
  `postgres` superuser, without a password. `ate-api-server` itself connects as
  `postgres`. See "Security posture" below.

Use it for dev clusters, CI, benchmarks, and demos. Do not put anything in it
that you would mind re-creating from scratch.

### Externally managed PostgreSQL (production)

The supported posture for an installation whose data matters is a PostgreSQL
instance you manage outside the cluster — Cloud SQL, RDS, or your own — with:

- **Automated backups plus point-in-time recovery** enabled on the instance.
- **Deletion protection** enabled on the instance.
- A tested restore. An untested backup is not a backup.
- The tuning and connection budget below applied as instance flags.

Point the installer at it by exporting `ATE_API_POSTGRES_CONNECTION_STRING`
before running `hack/install-ate.sh`. When that variable is set,
`create_api_server_env_vars` (in `hack/install-ate.sh`) writes it into the
`ate-api-server-envvars` ConfigMap and `ate-api-server` uses it instead of the
in-cluster default; when it is unset, the installer falls back to
`default_postgres_connection_string`, which is the in-cluster StatefulSet above.
`cmd/ate-setup` mirrors the same default in
`DefaultPostgresConnectionString`.

The connection string is a standard libpq/pgx URL, so TLS material and pool
settings both travel in it:

```sh
export ATE_API_POSTGRES_CONNECTION_STRING='postgresql://ateapi@db.example.internal:5432/atepg?sslmode=verify-full&sslrootcert=/path/ca.pem&pool_max_conns=12'
```

Backups, PITR, and deletion protection belong to that instance. This repository
deliberately ships no backup CronJob and no PITR tooling for the in-cluster
StatefulSet: a half-backup in the manifests would invite operators to trust it.

## What happens if you lose the data

### What does and does not lose data

Data lives on the `data-postgres-0` PVC. It survives more than people expect:

- **Deleting and re-applying the StatefulSet does not lose data.** The default
  `persistentVolumeClaimRetentionPolicy` is `Retain`, and `delete_ate_system` in
  `hack/install-ate.sh` deletes the StatefulSet but not the PVC, so a
  re-applied StatefulSet re-adopts `data-postgres-0` with its contents.
- **Deleting the PVC does lose data**, as does deleting the `ate-system`
  namespace (which deletes the PVC, and with the common `Delete` reclaim policy
  the underlying disk too), losing the disk or the zone, or renaming the
  volume claim template.

### Why an empty database is worse than a down database

A down database is loud: `ate-api-server` fails its connection and retries.
An *empty* database is silent, and the system actively papers over it:

1. `ate-api-server` applies the schema with `CREATE TABLE IF NOT EXISTS` on
   every start, so an empty database is indistinguishable from a first install.
   It reports ready with zero rows. There is no install-id or generation marker,
   and no wipe detection.
2. `atecontroller`'s worker syncer re-registers the fleet. In
   `cmd/atecontroller/internal/workersync/syncer.go` (`createOrUpdateWorker`,
   around lines 246-276), a `GetWorker` that returns `NotFound` is treated as
   "this worker is new": the syncer calls `CreateWorker`, and the API sets the
   worker `STATE_ACTIVE` with full capacity. Every live worker pod is re-registered
   as free within one pod event or the informer's 5-minute resync.
3. Those pods are not free. They still host the sandboxes they hosted before the
   wipe, and nothing in the system knows it — there is no RPC that asks a worker
   what it is running.

The result is a fleet the scheduler believes is idle and will place new actors
onto, on top of sandboxes that are still running. Actor records are gone, so
clients see `NotFound` for actors whose sandboxes are still alive and still
consuming memory.

This is not hypothetical, and it is not specific to PostgreSQL: it is the same
mechanism as the P0 filed against the previous Valkey store ("Valkey data loss
orphans all running workloads and enables double-placement") and its P2
follow-up, both closed as obsolete when Valkey was removed rather than fixed.
The syncer path is backend-agnostic and still behaves this way.

### Runbook: the database was lost or restored to an earlier point

**If you lose the store, drain and re-create the worker fleet before serving
traffic again.** The re-registered workers are lying about being free; the only
way to make the store's view true is to make the fleet match it.

1. Stop new traffic reaching `ate-api-server` — at the router, or by whatever
   fronts it — so nothing schedules onto the stale fleet. Leave
   `ate-api-server` itself running: worker registration in step 4 goes through
   it, so scaling it to zero stalls the recovery rather than protecting it.
2. Restore the database if you have a backup. If you do not, continue: you are
   rebuilding from empty.
3. Delete the worker pods (delete the `WorkerPool`s, or delete the pods and let
   them be re-created). Every sandbox on them is an orphan whose actor record no
   longer exists; nothing can reattach to it.
4. Wait for the fresh pods to register. `atecontroller`'s syncer registers a
   worker by calling `CreateWorker` on `ate-api-server`, and
   `ate.workerpool.workers` is emitted by `ate-api-server` — so with
   `ate-api-server` down there is neither registration nor a metric to watch.
   Those `CreateWorker` calls fail and are retried off the syncer's workqueue
   and the informer's 5-minute resync, so re-registration is not instant even
   once it is up. Confirm
   `ate.workerpool.workers{ate.worker.state="idle"}` matches the fleet size and
   that no actor claims a worker that no longer exists.
5. Let traffic back in.

Doing step 3 *before* step 5 is the whole point. Skipping it leaves double
placement in the system with no signal that it is there.

The same procedure applies after restoring to an earlier point in time: any
placement made after the restore point is a placement the store does not know
about.

## Sizing and tuning

The shipped container is limited to 16 CPU and 2Gi of memory with a 500Gi PVC
(`manifests/ate-install/postgres.yaml`); the kind overlay shrinks CPU and the
disk, not memory. Everything below is derived from that memory limit — if you
change the limit, re-derive them, and if you run a managed instance, apply the
same derivation to its size.

The memory **request** equals the memory limit, unlike the CPU request. A
container using more memory than it requests is the first thing the kubelet
evicts under node pressure — which a PodDisruptionBudget does not prevent,
because node-pressure eviction is involuntary — and with `shared_buffers` at
512MB a smaller request would be exceeded in steady state. Requesting the whole
limit means usage can never exceed the request.

| Setting | Value here | Derivation |
| --- | --- | --- |
| `shared_buffers` | `512MB` | 25% of the 2Gi container limit. The compiled-in default is 128MB. |
| `effective_cache_size` | `1GB` | The memory limit less `shared_buffers` and backend headroom. The compiled-in default is 4GB — twice the container limit — which makes the planner over-favor index scans. |
| `max_connections` | `100` | See the connection budget below. |
| `idle_in_transaction_session_timeout` | `60s` | See operational timeouts below. |

Two notes on the memory limit itself. First, PostgreSQL relies on the kernel
page cache beyond `shared_buffers`, and that cache is charged to the same
cgroup, so raising `shared_buffers` past ~25% takes memory away from it rather
than adding cache. Second, `shared_buffers`, `effective_cache_size` and
`max_connections` all require a restart, which on this single-replica
StatefulSet is a control-plane outage plus a full outbox relist — so get them
right at install time rather than under load.

Not tuned here, and worth revisiting when there is a workload to measure:
`max_wal_size` and `checkpoint_completion_target`, per-table autovacuum settings
for `workers` and `actors`, and `work_mem`. None of them is a correctness
concern; all of them are cheap to change later, because they are configuration
rather than schema.

### Applying a change to a running install

**These settings do not reach a running server by themselves.**
`postgresql.conf` is delivered as the `postgres-config` ConfigMap, and the pod
template carries no hash of its contents, so re-running `hack/install-ate.sh`
against a live cluster updates the ConfigMap and leaves the server on its old
values — a cluster upgraded in place keeps its 128MB `shared_buffers` with
nothing to indicate otherwise. Roll the pod deliberately:

```sh
kubectl rollout restart statefulset/postgres -n ate-system
```

That restart is the control-plane outage described above, so schedule it rather
than folding it into an upgrade. Confirm it took with `SHOW shared_buffers;` and
`SHOW max_connections;`.

### Node maintenance

`manifests/ate-install/postgres.yaml` carries a PodDisruptionBudget with
`minAvailable: 1` against the single replica, which blocks voluntary eviction
outright. That is deliberate — routine node maintenance should not take the
system of record down as a side effect — but it means `kubectl drain` on the
node hosting `postgres-0` will not complete, and a managed node-pool upgrade
(GKE, EKS) stalls on that node until someone intervenes. Taking the outage is an
explicit operator decision: delete the pod and let the StatefulSet reschedule
it, or delete the PDB for the duration of the drain. Expect a full outbox relist
across the `ate-api-server` replicas afterwards.

The PDB sets `unhealthyPodEvictionPolicy: AlwaysAllow` so that an
already-unhealthy `postgres-0` can still be evicted: under the default
`IfHealthyBudget` a drain could not even make progress toward replacing a broken
pod, which protects nothing — the outage is already happening. A PDB constrains
voluntary disruption only; node loss and node-pressure eviction are unaffected
by it.

## Connection budgeting

Each `ate-api-server` replica opens two pgxpools against the same DSN
(`cmd/ateapi/internal/store/atepg/atepg.go`, `Connect` and `poolConfig`):

- The **main pool**, for all reads and writes. Its size comes from
  `pool_max_conns` in the connection string. When the DSN omits it, pgxpool
  falls back to `max(4, runtime.NumCPU())` — and `runtime.NumCPU()` reports the
  *node's* cores regardless of the container's CPU limit, so the pool is four
  connections on a small node and sixty-four on a large one, neither of them a
  chosen number.
- The **outbox watch pool**, fixed at `watchPoolMaxConns = 3`: one for the
  `WatchWorkers` poller, one for partition maintenance, one of headroom. It is
  set in code and is not affected by `pool_max_conns`.

So a replica costs `pool_max_conns + 3` connections, and the budget is:

```
max_connections >= (replicas + surge) x (pool_max_conns + 3)
                   + operator sessions
```

The installer's default DSN sets `pool_max_conns=12`. With
`ate-api-server` at 2 replicas and a rolling update that surges one pod, that is
3 x 15 = 45 connections in steady state, and the `max_connections = 100` in the
manifest leaves room to scale to five replicas (6 x 15 = 90, plus ten operator
sessions) without restarting the database.

The formula has no `superuser_reserved_connections` term on purpose. That
setting holds slots back from non-superusers only, and in the shipped posture
`ate-api-server` connects as `postgres` — a superuser — so its pools can consume
the reserved slots too and it buys an operator no headroom. It becomes real
headroom once issue #997 moves `ate-api-server` onto a non-superuser role; add
the term back then.

Three things to keep straight when you change any of this:

- **`pool_max_conns` and `max_connections` move together.** They live in two
  files (`hack/install-ate.sh` and `manifests/ate-install/postgres.yaml`, plus
  the mirror in `cmd/ate-setup/internal/config/config.go`) and the manifest
  comment carries the formula.
- **The pool, not the server, is usually the first limit.** Exhausting the pool
  shows up as latency — requests queue in pgxpool's acquire — while exhausting
  `max_connections` shows up as `FATAL: sorry, too many clients already`
  (SQLSTATE 53300) on new connections. If store latency rises with load but the
  server has connections to spare, raise `pool_max_conns` first.
- **Backends cost memory.** Several MB each on top of `shared_buffers`. Raising
  `max_connections` without raising the container's memory limit trades an
  availability limit for an OOM kill, and an OOM kill of this pod is a
  control-plane outage.

## Monitoring

### What is instrumented today

The store and the outbox emit **no metrics at all**. This is a known blind spot,
recorded as such in [`docs/metrics/substrate.yaml`](../metrics/substrate.yaml)
(`blind_spots`, areas `store`
and `worker cache`), and `cmd/ateapi/internal/workercache` carries a TODO for
it. Do not go looking for a `worker_outbox` instrument; there isn't one.

The instruments in
[`docs/metrics/registry/metrics.yaml`](../metrics/registry/metrics.yaml) that
give you indirect visibility into the store are:

| Metric | What it tells you about the store |
| --- | --- |
| `ate.workerpool.workers` | The fleet as `ate-api-server` believes it to be, read from the worker cache. A sudden jump to "everything idle" with no matching pod churn is what a wiped store looks like from the outside. |
| `ate.scheduler.eligible_workers` | Free workers surviving the constraint filters at each scheduling decision. Drops before rejections start. |
| `ate.scheduler.assignment.duration` | `ate.scheduler.outcome="no_free_worker"` rising without matching pod churn is what a stale worker view looks like. Do not look for it under `ate.scheduler.outcome="error"`: retried version conflicts — the CAS failures a stale pick causes — are deliberately excluded from this instrument (`schedulerRecordable` in `cmd/ateapi/internal/controlapi/workflow_resume.go`), including the exhausted-retry path. |
| `ate.actor.lifecycle.operation.duration` | Store latency has no phase of its own, so it appears as unattributed time here. If this is slow and no phase explains it, suspect the store. |
| `rpc.server.call.duration` | `ate-api-server`'s own RPC latency and error codes. |

Everything specific to the store is **not yet instrumented**: outbox maintenance
success and last-success time, rows in the DEFAULT partition, partition count,
xmin fence lag, pool acquire latency and saturation, and store operation
latency. Until they exist, the checks below are `psql` queries. (`kubectl exec`
into the postgres pod and run `psql -U postgres -d atepg`.)

### Outbox maintenance

A maintenance pass runs every minute
(`cmd/ateapi/internal/store/atepg/outbox.go`): it creates partitions ahead of
the clock, truncates a strayed DEFAULT partition, and drops partitions past the
15-minute retention. Failures log `worker outbox maintenance failed` once per
minute per replica and nothing else reacts. Creation has 30-45 minutes of lead,
after which writes land in the DEFAULT partition; delivery keeps working from
there, so the symptom is disk growth and stopped retention rather than an
outage.

Alert on the log line. Until there are metrics, the two checks are:

```sql
-- Should be 0. Non-zero means partition creation has been failing for
-- 30-45 minutes and writes are landing in the catch-all partition.
SELECT count(*) FROM worker_outbox_default;

-- The partition inventory: expect the current partition, two of lead, and
-- whatever has not yet aged past the 15-minute retention floor (a partition is
-- dropped only once all of it is older than that, so expect one or two).
SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) AS bounds
FROM pg_class c
JOIN pg_inherits i ON i.inhrelid = c.oid
WHERE i.inhparent = 'worker_outbox'::regclass
ORDER BY c.relname;
```

The role running maintenance must **own** `worker_outbox`: `CREATE TABLE ...
PARTITION OF` and `DROP TABLE` require ownership, not merely privileges. Today
`ate-api-server` connects as a superuser and creates the tables itself, so this
holds trivially. It stops holding the moment a migration role is split from the
runtime role — see issue #997, "Define least-privilege role design for
Postgres", which has to preserve partition DDL for the runtime role.

### The xmin fence

`WatchWorkers` polls the outbox for rows below the cluster's xmin horizon
(`xid < pg_snapshot_xmin(pg_current_snapshot())` in `outbox.go`), which is what
makes event delivery ordered and gap-free. The consequence is that **any**
transaction holding an xid, anywhere in the database, holds the horizon back and
stalls worker-event delivery for every replica. It is silent: an empty poll
batch looks exactly like an idle feed.

The blast radius is bounded but not zero. The worker cache relists from the
`workers` table every 5 minutes regardless of watch health
(`workercache.New(persistence, 5*time.Minute)` in `cmd/ateapi/main.go`), so a
stall degrades event latency from ~50ms to that 5-minute floor rather than
freezing the view; and a stale pick is rejected by the version CAS on the claim
and retried, so it costs retries and spurious "no capacity", not corruption.
Past the 15-minute retention the stall becomes loud — a partition drop moves the
trim mark past every watcher's cursor, and each replica logs `worker watch fell
behind outbox retention; closing for resync` and does a full relist.

`ate-api-server` cannot be the culprit: its RPCs are capped at a 10-minute
deadline, and its transactions are a handful of statements issued back-to-back
with no client-side waiting in between — the longest is a write plus the outbox
insert. The
realistic holder is a human's forgotten `BEGIN` in a `psql` session, or a
long-running DDL migration. To find one:

```sql
-- The horizon, and how far behind the newest event it is. A growing gap with a
-- static horizon is a stall.
SELECT pg_snapshot_xmin(pg_current_snapshot()) AS fence,
       (SELECT max(xid) FROM worker_outbox) AS newest_event;

-- Who is holding it back. The oldest row here is the culprit.
SELECT pid, usename, application_name, state,
       now() - xact_start AS xact_age,
       backend_xid, backend_xmin,
       left(query, 100) AS query
FROM pg_stat_activity
WHERE backend_xid IS NOT NULL OR backend_xmin IS NOT NULL
ORDER BY xact_start
LIMIT 10;
```

Connection headroom, while you are in there:

```sql
SELECT count(*) AS in_use,
       current_setting('max_connections')::int AS max_connections
FROM pg_stat_activity;
```

## Operational timeouts

- **`idle_in_transaction_session_timeout = 60s`** is set in the shipped
  manifest, and should be set on a managed instance too. It ends sessions that
  sit idle inside a transaction, which is exactly the forgotten-`BEGIN` case
  above. 60s is far longer than any legitimate `ate-api-server` transaction:
  they are a handful of statements issued back-to-back with no client-side
  waiting in between — the longest is a write plus the outbox insert — and
  the outbox maintenance transactions are never idle either — they are always
  either executing a statement or waiting on a lock, both of which this timeout
  ignores.
  Anything that trips it is a human or a stuck client.
- **`statement_timeout` is deliberately not set globally.** An outbox
  maintenance pass legitimately waits on locks for up to its 5-minute pass
  timeout (`outboxMaintenancePassTimeout`), and `statement_timeout` counts
  lock-wait time, so a global statement timeout would kill exactly those passes.
  The worker relist is not at risk: it is paginated at `relistPageSize = 1000`
  rows per statement (`cmd/ateapi/internal/workercache/workercache.go`), so no
  single statement in it is long-running. Set it per-role instead — on
  human and ad-hoc roles, not on the role `ate-api-server` uses — once issue
  #997 gives us separate roles to set it on. Until then, `SET statement_timeout`
  in your own `psql` session.
- **Run schema migrations outside the serving window.** Versioned migrations
  (issue #1196, "ateapi: add versioned PostgreSQL schema migrations") run inside
  `ate-api-server` at startup, which during a rolling update means a migration
  transaction is open while the old replicas are still serving. A long DDL
  transaction holds an xid, and therefore stalls worker-event delivery on every
  one of those replicas for its duration, on top of whatever locks it takes.
  Schedule non-trivial migrations against a stopped or drained control plane, or
  write them as short autocommit steps.

## Security posture

The in-cluster StatefulSet's authentication is `trust` behind mTLS
(`pg_hba.conf`: `hostssl all all all trust clientcert=verify-ca`, plus
`local all all trust` for the pod's own health checks and TLS-reload sidecar).
The pod-identity CA is the only gate: any workload that can obtain a certificate
from it and reach port 5432 can connect as any role, including `postgres`.
`ate-api-server` itself connects as the `postgres` superuser.

This is the posture the threat model already describes.
[`docs/threat-model.md`](../threat-model.md) T-04 (Critical) covers access to
the internal network allowing arbitrary actions against the backend database,
and states the mitigation: mutual authentication and TLS between components,
and **only `ate-api-server` authorized to connect directly to the backend
database** — enforce that with a NetworkPolicy.
[`docs/authentication.md`](../authentication.md) describes the front-door side:
`ate-api-server` accepts mTLS and JWTs, but authorization and RBAC are not
implemented, so any configured provider's users have full access, including
`DebugClear`.

Two open issues bear directly on the store:

- Issue #997, "Define least-privilege role design for Postgres": replace the
  superuser connection with a role that has only the privileges
  `ate-api-server` needs. Note the ownership constraint above — whatever role
  runs outbox maintenance must own `worker_outbox`.
- Issue #999, "Delete the DebugClear API before release": the `DebugClear` RPC
  truncates the store. It is reachable by any authenticated caller, since
  authorization is not implemented. It must be gone before any installation
  holds data you care about.

## Out of scope for this document

Deliberately not covered, and deliberately not shipped in the manifests:
backup CronJobs and PITR tooling for the in-cluster StatefulSet (bring a managed
instance instead), store-side wipe detection, and high availability for the
store — the last of which belongs to the wider HA discussion in issue #181,
"Define substrate HA story".
