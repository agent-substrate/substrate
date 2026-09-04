# Scaling the Cloud SQL store

Sizing guidance for running the ateapi PostgreSQL store
([tools/setup-gcp/cloud-sql.md](../../tools/setup-gcp/cloud-sql.md)) at large
actor counts and high request rates.

The provisioning defaults (`db-custom-2-8192`, 10 GB disk) suit development
and modest fleets. At large actor counts the store becomes I/O-bound: once
tables and indexes outgrow memory, uniform random reads fall out of cache and
point lookups pay persistent-disk latency (several milliseconds) instead of
microseconds. The knobs below address that, in order of leverage.

## Instance shape

Set with `--tier` / `--edition` at create time, or `gcloud sql instances
patch` later — edition/tier changes restart the instance.

- Memory is the primary lever: reads are served from cache until the working
  set (tables + indexes) outgrows RAM, then p50 degrades to disk latency.
- Once the dataset can't fit RAM on any tier, switch to
  [`--edition=enterprise-plus`](https://docs.cloud.google.com/sql/docs/postgres/editions-intro)
  with `--tier=db-perf-optimized-N-<vCPU>`. The tool
  enables its **local-SSD data cache**, which extends the effective cache
  several times beyond RAM: reads that would miss to persistent disk are
  served from local SSD at a fraction of the latency.

## Storage

Set with `--storage-size` at create time; it only grows afterwards.

- Persistent-disk IOPS and throughput scale with provisioned size — the disk
  is also the I/O knob. Pre-size to ~2× the expected dataset (records +
  indexes + WAL + bloat) rather than relying on auto-resize, which grows in
  small steps and stalls under bulk loads.

## Connection sizing for a target throughput

Set with `ATE_API_POSTGRES_POOL_MAX_CONNS` at deploy time. Target connections
equal throughput multiplied by average query latency:

```
connections ≈ QPS × mean latency in seconds
            ≈ 10,000 req/s × 0.006 s ≈ 60 active connections
```

Provision ~2× headroom for bursts (e.g. 4 replicas × `ATE_API_POSTGRES_POOL_MAX_CONNS=32`).
An undersized pool causes client-side queuing inside `pgx` rather than database errors.
Ensure total connections across all replicas stay within Cloud SQL's limit:

```
replicas × pool_max_conns  ≤  max_connections − slack (superuser, maintenance)
```

Exceeding
[`max_connections`](https://docs.cloud.google.com/sql/docs/postgres/quotas#maximum_concurrent_connections)
does error (`FATAL: sorry, too many clients
already`); raise the flag with `gcloud sql instances patch
--database-flags=…`, remembering the list **replaces all flags**, so always
re-include `cloudsql.iam_authentication=on`. Going far beyond ~2× vCPUs in
*active* connections buys no throughput either — backends are OS processes,
and excess active ones just context-switch. Sweep around the formula's
number rather than maximizing.

## Proxy sidecar resources

The proxy imposes no connection limit and adds
sub-millisecond latency, but it encrypts all database traffic, so its CPU
use scales with throughput. The patch
(`manifests/ate-install/cloudsql/proxy-sidecar-patch.yaml`) requests `100m` — sized
for control-plane traffic. For sustained thousands of ops/s, raise the
sidecar's CPU request so node pressure cannot throttle it into becoming the
bottleneck. Connection *churn* has a separate ceiling: IAM database logins
are quota'd at 12,000/min per instance — irrelevant for steady pools, but a
simultaneous reconnect storm across very many replicas can brush it.

## Managed Connection Pooling

[Managed Connection Pooling](https://docs.cloud.google.com/sql/docs/postgres/managed-connection-pooling)
(Enterprise Plus only) is a server-side pooler
(`gcloud sql instances patch <instance> --enable-connection-pooling`) that
multiplexes up to `max_client_connections` (default 5,000) client
connections onto at most `max_pool_size` (default 50) backends per
database+user pair — the fix when very many ateapi replicas would otherwise
need thousands of real backends. Not usable with atepg's efficient
`transaction` mode today: the worker-watch path uses `LISTEN`, which
transaction pooling doesn't support (`session` mode works but forfeits most
of the multiplexing). If enabled, size `max_pool_size` to the same
formula's number and reserve ~15 server connections per vCPU in
`max_connections` for the pooler.

## Beyond configuration

At billions of rows per table, vacuum duration and index maintenance on
monolithic tables become the operational limit — partitioning the large
tables is schema work, not a configuration change.
