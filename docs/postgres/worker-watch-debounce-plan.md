# PostgreSQL worker-watch debounce plan

## Status

Proposed incremental plan. The immediate goal is to determine whether PostgreSQL
worker notifications materially affect the critical resume/suspend path, then
make one measured debounce change if they do.

The North Star remains useful direction: 1,000 or more concurrent wakeups per
second at very large actor scale. It is not a latency pass/fail number for this
work. The initial decision is based on a controlled before/after comparison of
the current system.

## Why start with resume/suspend

`ResumeActor` claims a worker and `SuspendActor` releases it. Each transition
updates worker state, which currently emits `pg_notify` inside its PostgreSQL
transaction. That makes resume/suspend the direct user-visible path most likely
to benefit from changing the worker watch.

The worker cache uses these events to learn about changes made by other API
servers. Delaying or batching a notification can improve database write
throughput, but it also leaves other caches briefly stale. The benchmark must
therefore report both lifecycle performance and assignment correctness.

## Phase 1: existing lifecycle benchmark baseline

Use the lifecycle benchmark from `benchmark/postgres`, adapted to current
interfaces. It provides:

- a single atelet simulator with a fixed 1 ms runtime delay instead of real
  worker Pods or gVisor;
- a Go Boomer worker that can generate 1,000 concurrent lifecycle users
  without Python/gevent becoming the load bottleneck; and
- synthetic worker/actor seeding and final-state verification.

### Workload

Use only the worker-changing lifecycle operations:

```text
cold ResumeActor -> SuspendActor
```

Do not include an already-running `ResumeActor` in this first test. It does not
claim or release a worker and would dilute the notification load.

Initial configuration:

| Setting | Value |
|---|---:|
| Store | PostgreSQL |
| `ateapi` replicas | 1 |
| Synthetic workers | 1,000 |
| Concurrent Boomer users | 1,000 |
| Pre-seeded suspended actors | at least 1,000 |
| Simulated atelet delay | 1 ms |
| Think time | none |
| Duration | 60–90 seconds |
| Repetitions | 3 |

This is a capacity-style baseline, not a claim about normal production arrival
rates. It provides enough worker claims and releases to make any notification
cost visible. It intentionally starts with 1,000 workers, not 100,000: the
current scheduler scans the worker cache for candidates, and a 100,000-worker
fleet could hide the effect of watch changes behind unrelated scheduling cost.

### Baseline measurements

For every run record:

- successful cold resumes/s, suspends/s, and complete cycles/s;
- ResumeActor and SuspendActor average, p95, and p99 latency;
- failures, timeouts, assignment conflicts, and retries;
- PostgreSQL and `ateapi` CPU; and
- final actor/worker assignment consistency.

If easily available while adapting the benchmark, also record worker updates
and notifications per second. Do not delay the first before/after comparison
solely to add detailed watch telemetry.

Store the full configuration, commit SHA, resource limits, PostgreSQL pool
size, and raw per-run results alongside the benchmark report.

## Phase 2: debounce implementation

Implement one conservative debounce/batching design for PostgreSQL worker
notifications, with unit and integration tests.

The intended direction is:

```text
commit authoritative worker row
    -> enqueue best-effort worker wake-up
    -> coalesce repeated changes for a short interval
    -> flush fewer pg_notify transactions
```

The worker table remains the source of truth. The existing worker-cache relist
after reconnect and periodic relist recover from a notification lost during a
process failure. The implementation must define bounded-buffer and flush-failure
behavior rather than blocking worker writes indefinitely.

Start with one small, configurable debounce interval. Do not add a broad
interval sweep unless the first before/after result makes the tradeoff unclear.

Required tests cover:

- repeated updates coalesce to the latest worker version;
- create, update, delete, and delete/recreate ordering;
- rolled-back writes do not produce usable events;
- notifier flush and shutdown behavior;
- slow/disconnected watchers; and
- eventual cache convergence after a lost notification or reconnect.

## Phase 3: identical verification

Run the exact Phase 1 lifecycle configuration three times after the change.
Compare medians and the per-run variance against the baseline.

The change is justified when it improves successful lifecycle throughput or
latency without increasing assignment failures, retries, or final-state
violations. A result with little or no lifecycle change is also useful: it
means the current watch path is not the dominant bottleneck under this load,
so further watch complexity should not be prioritized on speculation.

## Only if the before/after result is ambiguous

Add one focused PostgreSQL attribution test:

```text
concurrent UpdateWorker calls -> WatchWorkers consumer
```

Run the current path and a benchmark-only no-notify control at the same worker
update rate. Compare worker-update throughput and latency, PostgreSQL commit
waiting, and (for the normal path) notification propagation/convergence.

This test is not required before implementing the first debounce change. It is
the next diagnostic only when the end-to-end resume/suspend comparison neither
clearly improves nor clearly rules out `pg_notify` as a material cost.

## Deferred work

Do not include these in the initial decision:

- Redis comparisons or the broad existing storage matrix;
- multiple API replicas;
- a full debounce-interval sweep;
- saturation sweeps beyond the zero-think-time lifecycle baseline;
- 100,000-worker lifecycle comparison; or
- large actor-cardinality studies.

After a clear lifecycle result, run the selected implementation once at
100,000 workers to characterize target fleet scale. If scheduler scan cost
dominates there, treat worker indexing/candidate selection as a separate next
bottleneck rather than attributing it to the watch mechanism.

