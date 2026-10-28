# Resumes are slow

> Some documents call this a cold start. Substrate says **resume**, because the
> usual path restores a snapshot. It does not boot the actor from nothing.

Start here when the activation of an actor that is not on a worker is slow, or
fails. [requests-are-slow.md](requests-are-slow.md) sends you here when the
request that did the resume was slow.

Read [How to read a step](README.md#how-to-read-a-step) first.

## How a resume works

An actor is idle most of the time, and Substrate suspends or pauses it to free
its place on a worker. The next request must put the actor back on a worker
before the actor can answer:

1. The router asks ateapi to resume the actor, and parks the request.
2. The scheduler in ateapi finds a worker with room.
3. atelet on the node of that worker restores the snapshot of the actor, in
   phases: it gets the snapshot, prepares the image and the sandbox, and asks
   ateom to restore.
4. ateom starts the actor in the sandbox, next to the other actors on that
   worker.

Where the snapshot is changes the cost:

| Before the resume | Where the snapshot is | Cost |
|---|---|---|
| Paused | On the node VM. The resume accepts only a worker on that VM. | Low. No download. |
| Suspended | In object storage, such as GCS or S3 | High. A download and an unpack. |

The `registry.ate.snapshot` group of
[the registry](../metrics/registry/metrics.yaml) says which snapshot a resume
read, and what each restore phase covers. A boot is a start from nothing: it
is not a restore, thus the restore instrument of atelet has no data for it.

---

## Step 1. Where did the time of the resume go?

**Signal:** three instruments that measure different parts of one resume.

| Instrument | Emitted by | What it covers |
|---|---|---|
| `atenet.router.route.duration`, for the request that did the resume | the router | What the user waited for, with the parking time. |
| `ate.actor.lifecycle.operation.duration`, for the resume operation | ateapi | The full operation, with the scheduler. |
| `ate.actor.restore.duration`, phase `total` | atelet | Only the work on the node. |

**Query:**

```promql
histogram_quantile(0.95, sum by (le) (
  rate(atenet_router_route_duration_seconds_bucket{
        ate_router_resume="triggered"}[5m])))

histogram_quantile(0.95, sum by (le) (
  rate(ate_actor_lifecycle_operation_duration_seconds_bucket{
        ate_actor_operation_name="resume"}[5m])))

histogram_quantile(0.95, sum by (le) (
  rate(ate_actor_restore_duration_seconds_bucket{
        ate_snapshot_phase="total"}[5m])))
```

**Read:** each number contains the next one.

* Many resumes fail — go to step 2 first. A failure holds its timer until it
  gives up, thus failures make the ateapi and the node numbers larger. The
  router number counts only the resumes that completed.
* The node time is most of the total — the node is the cause. Go to step 3.
* The ateapi time is much larger than the node time — the time went to the
  scheduler or to the store. Go to step 4 of
  [capacity-is-full.md](capacity-is-full.md).
* The router time is much larger than the ateapi time — the request waited in
  the router. Go to step 3 of [requests-are-slow.md](requests-are-slow.md).
* The node time is empty, and resumes occur — the resumes are boots, not
  restores. The snapshot kind on the lifecycle instrument confirms it.

**A quantile at the last bucket is not a value.** The instruments do not use
the same buckets. A value at or near the end of the range means only that the
true value is above the buckets, thus you cannot compare two numbers there.
Read the mean instead: divide the increase of `_sum` by the increase of
`_count`.

## Step 2. Do the resumes fail?

**Signal:** `error.type` on `ate.actor.lifecycle.operation.duration`. ateapi
sets it only on an operation that failed, and it holds the gRPC status code.
The restore instrument of atelet does not mark a failure.

**Query:**

```promql
sum by (ate_template_name, error_type) (
  rate(ate_actor_lifecycle_operation_duration_seconds_count{
        ate_actor_operation_name="resume", error_type!=""}[5m]))
/ ignoring(error_type) group_left
sum by (ate_template_name) (
  rate(ate_actor_lifecycle_operation_duration_seconds_count{
        ate_actor_operation_name="resume"}[5m]))
```

The result is the fraction of the resumes of each template that failed.

**Read:** the status code tells you the class of the failure, not the
component. To find the component, find where the restores stop. atelet does
not record a phase that did not start. Thus compare the count of each restore
phase with the count of `total`, for one snapshot kind at a time:

| Where the restores stop | Examine |
|---|---|
| Before any phase on the node | ateapi. Read its logs. |
| While atelet gets the snapshot | The storage backend, and the URL of the snapshot. The logs of atelet. |
| While atelet prepares the image or the sandbox | The node. The logs of atelet. |
| In the restore by ateom | The sandbox runtime. The logs of ateom. |

A failed suspend also makes the next resume fail, because it leaves no good
snapshot. `ate.actor.crashes` counts the actors that Substrate lost, by the
operation that lost them. The `ate.actor.crashed` event in
[`events.yaml`](../metrics/registry/events.yaml) names the actor, which no
metric label may. `ate.actor.checkpoint.duration` shows the phases of a
suspend.

## Step 3. Which phase on the node is slow?

**Signal:** the phases of `ate.actor.restore.duration`. The
`registry.ate.snapshot` group says what each phase covers.

**Query:**

```promql
sum by (ate_snapshot_kind, ate_snapshot_phase) (
  increase(ate_actor_restore_duration_seconds_sum[30m]))
/ sum by (ate_snapshot_kind, ate_snapshot_phase) (
  increase(ate_actor_restore_duration_seconds_count[30m]))
```

This query gives the mean, because the buckets are wide at the tail.

**Do not add the phases together.** Some phases occur at the same time. Use
`total` as the denominator.

**Read:** the slowest phase says where to go next.

* atelet gets the snapshot slowly — go to step 4.
* atelet prepares the image slowly — go to step 5.
* atelet prepares the sandbox slowly, or ateom restores slowly — go to step 6,
  and read the logs of ateom.
* atelet mounts the volumes slowly — read the logs of atelet.

## Step 4. Is the snapshot large, or the storage slow?

**Signal:** `atelet.snapshot.size`, which records the size of each image file
that a checkpoint writes.

**Query:**

```promql
histogram_quantile(0.95, sum by (le, ate_template_name, file_name) (
  rate(atelet_snapshot_size_bytes_bucket[1h])))
```

Compare the same file name across the templates, and across time.

**Read:**

* The snapshots of a template became larger — the download takes longer
  because the snapshot is larger. Examine what the actor keeps in memory and
  on disk.
* The size did not change, but the download did — the storage backend is the
  cause.

## Step 5. Does the image cache on the node miss?

**Signal:** `ate.imagecache.requests`. The `registry.ate.imagecache` group
says what each outcome means.

**Query:**

```promql
sum by (ate_imagecache_outcome, error_type) (
  rate(ate_imagecache_requests_total[5m]))
```

**Read:**

* Many misses — each miss adds a pull and an unpack to the resume. Calculate
  the hit ratio as `hit / (hit + miss)`. Do not put the failures in the
  denominator.
* Many failures — `error.type` holds the HTTP status that the image registry
  returned. It tells a credential fault from a rate limit from a fault of the
  registry.

## Step 6. Is the worker or the node busy?

**Signal:** the actor stats instruments of atelet, such as
`ate.actor.stats.memory.working_set`, `ate.actor.stats.cpu.time`, and
`ate.actor.stats.sampled_actors`. The `registry.ate.stats` group says what
each source measures.

**Query:**

```promql
sum by (ate_workerpool_name, ate_template_name, ate_stats_source) (
  ate_actor_stats_memory_working_set_bytes)

sum by (ate_workerpool_name, ate_template_name, ate_stats_source) (
  rate(ate_actor_stats_cpu_time_seconds_total[5m]))
```

**Group by the source, and do not add the sources together.** They measure
different things.

**Use `sum` across the nodes.** Each atelet measures only the actors on its
own node. This is the opposite of `ate.workerpool.workers` in
[capacity-is-full.md](capacity-is-full.md), where each replica of ateapi
reports the whole fleet. Read who emits an instrument before you choose the
operator.

**Read:** one worker holds many actors, thus a resume shares the CPU and the
memory of its worker with the actors that are already there.

* The actors on the worker use most of its CPU or memory — a busy neighbor
  makes the resume slow. The scheduler does not prevent this: it compares the
  limits of the actors with the capacity of the worker, not what the actors
  use.
* The number of measured actors decreases, but the number of actors does not
  — the sweep cannot measure some actors. Read the logs of atelet.

```bash
kubectl ate top workers
```

---

## The blind spots of this scenario

* **The store in ateapi.** A delay looks like unmeasured time in the lifecycle
  and the scheduler instruments.
* **The build of a golden snapshot.** Nothing measures it. A slow first
  activation of a new template has no data.
* **The eviction of the image cache.** You see the hits and the misses, but
  not the disk pressure.

The full list is in `blind_spots` of
[`docs/metrics/substrate.yaml`](../metrics/substrate.yaml).

## Go from a template to one actor

No resume metric names an actor. The cardinality rules forbid it. When the
metrics give you a slow template, use the logs and the traces to find the
actor:

```bash
kubectl ate get actors -a <atespace>
kubectl ate logs actors <actor-name> -a <atespace> -f
kubectl ate resume actor <actor-name> -a <atespace> --trace
```

The `--trace` flag prints a trace ID. Put it in Cloud Trace or Jaeger to see
each step of the one resume.
