# Resumes are slow

> Some documents call this a cold start. Substrate says **resume**, because the
> usual path restores a snapshot. It does not boot the actor from nothing.

## Context

A **resume** is the activation of an actor that is not on a worker. An actor is
idle most of the time, and Substrate suspends it to release its worker. The
next request must put the actor back on a worker before the actor can answer.
The request waits for that. This is the `triggered` series of
[requests-are-slow.md](requests-are-slow.md), measured from the other end.

Two states give a resume, and their cost is not the same:

| State before | Where the snapshot is | Cost |
|---|---|---|
| Paused | On the node VM, and the resume prefers that node | Low. No download. |
| Suspended | In object storage (GCS or S3) | High. A download and an unpack. |

`ate.snapshot.kind` tells you which snapshot the resume read. The permitted
values and their meaning are in the `registry.ate.snapshot` group of
[the registry](../metrics/registry/metrics.yaml). One of them changes what you
can query: a `boot` is a start from nothing, thus it is not a restore and the
atelet restore histogram has no data for it.

Two instruments measure a resume. They do not measure the same part:

| Metric | Emitted by | What it covers |
|---|---|---|
| `ate.actor.lifecycle.operation.duration` with `ate.actor.operation.name="resume"` | ateapi | The full operation, with the scheduler. |
| `ate.actor.restore.duration` | atelet | Only the part on the worker node. |

If the ateapi number is much larger than the atelet `total` phase, the delay is
before the handoff. Examine the scheduler in step 5, and read the blind spots.

**Do not add the phases together.** The download occurs at the same time as the
asset fetch and the OCI unpack. Each phase is an independent measurement. Use
`total` as the denominator. A phase that did not start is absent. It is not
zero.

Each step gives the query in two forms. Refer to
[the naming rules](README.md#the-names-on-your-backend) for which form your
backend needs, and for the reasons a query can return nothing.

---

## Step 1. Confirm that the resume is the cause

Read the two numbers together. The top line is what the user paid. The bottom
line is what the node used.

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le) (
  rate(atenet_router_route_duration_seconds_bucket{
        ate_router_resume="triggered"}[5m])))

histogram_quantile(0.95, sum by (le) (
  rate(ate_actor_restore_duration_seconds_bucket{
        ate_snapshot_phase="total"}[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le) (
  rate({__name__="atenet.router.route.duration_bucket",
        "ate.router.resume"="triggered"}[5m])))

histogram_quantile(0.95, sum by(le) (
  rate({__name__="ate.actor.restore.duration_bucket",
        "ate.snapshot.phase"="total"}[5m])))
```

* The two numbers agree — the node is the cause. Go to step 2.
* The router number is much larger — the time went to the queue or to the
  scheduler. Go to step 5.

**A quantile at the last bucket is saturated.** The two instruments do not use
the same buckets, and the lifecycle histogram of ateapi ends before the restore
histogram of atelet. A value at or near the end of either range means only that
the true value is somewhere above the buckets, thus the two cannot be compared
there. Read the mean instead, as step 2 does.
* The restore query is empty but resumes occur — the resumes are boots. Confirm
  it:

**Prometheus**

```promql
sum by (ate_snapshot_kind) (
  rate(ate_actor_lifecycle_operation_duration_seconds_count{
        ate_actor_operation_name="resume"}[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.snapshot.kind") (
  rate({__name__="ate.actor.lifecycle.operation.duration_count",
        "ate.actor.operation.name"="resume"}[5m]))
```

## Step 2. Find the phase

**Remove the failures before you read the time.** A phase that fails holds its
timer until it gives up. Thus a few failures make the phase look slow, and the
restores that were correct disappear into the tail. `ate.failure.reason` is on
the phase that failed and on `total`, and on no other phase. An empty value
selects the restores that were correct:

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le, ate_snapshot_phase) (
  rate(ate_actor_restore_duration_seconds_bucket{ate_failure_reason=""}[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le, "ate.snapshot.phase") (
  rate({__name__="ate.actor.restore.duration_bucket","ate.failure.reason"=""}[5m])))
```

**Read the mean as well.** These buckets are wide at the tail, thus a quantile
in the last bucket is an interpolation that can be far above the true value:

**Prometheus**

```promql
sum by (ate_snapshot_phase) (
  increase(ate_actor_restore_duration_seconds_sum{ate_failure_reason=""}[30m]))
/ sum by (ate_snapshot_phase) (
  increase(ate_actor_restore_duration_seconds_count{ate_failure_reason=""}[30m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.snapshot.phase") (
  increase({__name__="ate.actor.restore.duration_sum","ate.failure.reason"=""}[30m]))
/ sum by("ate.snapshot.phase") (
  increase({__name__="ate.actor.restore.duration_count","ate.failure.reason"=""}[30m]))
```

The `registry.ate.snapshot` group of
[the registry](../metrics/registry/metrics.yaml) says what each phase covers.
The slowest phase says where to go next:

| Phase | Go to |
|---|---|
| `volume_mount` | The logs of atelet. |
| `manifest_fetch`, `download` | Step 3. |
| `oci_unpack`, `sandbox_assets` | Step 4. |
| `ateom_restore` | The logs of ateom. |

If the filtered numbers are small but the unfiltered numbers are large, the
subject is not the speed of the phase. It is the failures. Go to step 6.

**A phase that disappears under the filter is the phase that failed.** The
reason key is on the failed phase and on `total`, and on no other phase. Thus a
filtered result that lists `download` and `oci_unpack` but not `ateom_restore`
and not `total` says that the restores reached the sandbox runtime and died
there. The phases before it were correct, and they are the ones you can still
read.

## Step 3. Examine the snapshot and the storage

A large snapshot makes a long download. Compare the templates.

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le, ate_template_name) (
  rate(atelet_snapshot_size_bytes_bucket[1h])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le, "ate.template.name") (
  rate({__name__="atelet.snapshot.size_bucket"}[1h])))
```

atelet records one measurement for each image file, not one for each
checkpoint. Use `file.name` to compare the same type of image.

```bash
kubectl ate get actor-snapshots -a <atespace>
```

If the size did not change but the download did, the storage backend is the
cause. Read step 6 for the failure reasons.

## Step 4. Examine the image cache on the node

A miss adds a pull and an unpack to each resume.

**Prometheus**

```promql
sum by (ate_imagecache_outcome) (
  rate(ate_imagecache_requests_total[5m]))

sum by (error_type) (
  rate(ate_imagecache_requests_total{
        ate_imagecache_outcome="error"}[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.imagecache.outcome") (
  rate({__name__="ate.imagecache.requests"}[5m]))

sum by("error.type") (
  rate({__name__="ate.imagecache.requests",
        "ate.imagecache.outcome"="error"}[5m]))
```

Calculate the hit ratio as `hit / (hit + miss)`. Keep the outcomes that are not
a lookup result out of the denominator. The
`registry.ate.imagecache` group of
[the registry](../metrics/registry/metrics.yaml) lists them.

Only the `error` outcome carries `error.type`, which holds the HTTP status that
the registry of the image returned. Group by it to divide a credential fault
from a rate limit from a fault of the registry. The permitted values are on
`metric.ate.imagecache.requests` in the same file.

## Step 5. Examine the control plane

Use this step only if step 1 sent you here.

**Prometheus**

```promql
sum by (ate_scheduler_outcome) (
  rate(ate_scheduler_assignment_duration_seconds_count[5m]))

histogram_quantile(0.95, sum by (le) (
  rate(ate_scheduler_assignment_duration_seconds_bucket{
        ate_scheduler_outcome="assigned"}[5m])))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.scheduler.outcome") (
  rate({__name__="ate.scheduler.assignment.duration_count"}[5m]))

histogram_quantile(0.95, sum by(le) (
  rate({__name__="ate.scheduler.assignment.duration_bucket",
        "ate.scheduler.outcome"="assigned"}[5m])))
```

* The outcome is `no_free_worker` — this is a capacity fault, not a resume
  fault. Read [capacity-is-full.md](capacity-is-full.md).
* The outcome is `assigned` but the time is large — a delay in the store. The
  store has no metrics. Read the logs of ateapi.

The user also pays the parking time with the resume time:

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le, outcome) (
  rate(atenet_router_parking_wait_duration_seconds_bucket[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le, outcome) (
  rate({__name__="atenet.router.parking.wait.duration_bucket"}[5m])))
```

## Step 6. Find out if the resumes fail

A failed resume is not the same fault as a slow resume, but a failure also
makes the phase look slow, thus read this step together with step 2. atelet puts `ate.failure.reason` on the phase that failed and on the
total. It puts the key on no other phase. Thus the phases that were correct
stay queryable as successes.

**Prometheus**

```promql
sum by (ate_snapshot_phase, ate_failure_reason) (
  rate(ate_actor_restore_duration_seconds_count{
        ate_failure_reason!=""}[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.snapshot.phase", "ate.failure.reason") (
  rate({__name__="ate.actor.restore.duration_count",
        "ate.failure.reason"!=""}[5m]))
```

The `registry.ate.failure` group of
[the registry](../metrics/registry/metrics.yaml) says what each reason means.
The reason says which component to examine next:

| Reason | Examine |
|---|---|
| `FAILED_GET_EXTERNAL_OBJECT`, `INVALID_OBJECT_URL` | The storage backend, and the URL of the snapshot object. |
| `TERMINAL_FILE_SYSTEM_ERROR`, `LOCAL_SNAPSHOT_GONE`, `INVALID_SANDBOX_ASSET` | The node. Read the logs of atelet. |
| `INVALID_CHECKPOINT_RESULT`, `FAILED_SAVE_SNAPSHOT` | The suspend path. A bad checkpoint makes the next resume fail. |
| `INVALID_CONTAINER_CONFIG` | The ActorTemplate. |
| `WORKER_POD_GONE`, `WORKER_REASSIGNED`, `CORRUPTED_ASSIGNMENT` | The control plane. Examine ateapi. |
| `UNKNOWN` | Nothing else. The reason is absent, thus read the logs of atelet. |

A slow resume and a failed suspend are related. A suspend that fails leaves no
good snapshot for the next resume. Read the crash counter, which uses the same
taxonomy:

**Prometheus**

```promql
sum by (ate_failure_reason, ate_template_name) (
  rate(ate_actor_crashes_total[5m]))

histogram_quantile(0.95, sum by (le, ate_snapshot_phase) (
  rate(ate_actor_checkpoint_duration_seconds_bucket[5m])))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.failure.reason", "ate.template.name") (
  rate({__name__="ate.actor.crashes"}[5m]))

histogram_quantile(0.95, sum by(le, "ate.snapshot.phase") (
  rate({__name__="ate.actor.checkpoint.duration_bucket"}[5m])))
```

## Step 7. Examine the load on the node

A node with no free memory or no free CPU makes each resume slow.

**Prometheus**

```promql
sum by (ate_template_name, ate_stats_source) (
  ate_actor_stats_memory_working_set_bytes)

sum by (ate_template_name) (
  rate(ate_actor_stats_cpu_time_seconds_total[5m]))

ate_actor_stats_sampled_actors
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.template.name", "ate.stats.source") (
  {__name__="ate.actor.stats.memory.working_set"})

sum by("ate.template.name") (
  rate({__name__="ate.actor.stats.cpu.time"}[5m]))

{__name__="ate.actor.stats.sampled_actors"}
```

Group the data by `ate.stats.source`. Do not add the sources together. The
`cgroup` source includes the load of the sandbox runtime. The `guest-agent`
source includes only the containers of the workload.

**Add these across the nodes with `sum`.** Each atelet measures only the actors
on its own node, thus one series for each node, and the sum is the fleet. This
is the opposite of `ate.workerpool.workers`, where each ateapi reports the whole
fleet and a sum multiplies it. Read the emitter before you choose the operator:

| Metric | Emitter | Each series covers | Operator |
|---|---|---|---|
| `ate.actor.stats.*` | atelet, one for each node | One node | `sum` |
| `ate.workerpool.workers` | ateapi, some replicas | The whole fleet | `max` |

The pool keys are absent when the atelet DaemonSet does not set `NODE_NAME`
through the Downward API. The samples still flow; they carry no
`ate.workerpool.*`.

Use `working_set` against a memory limit. The `usage` value includes the page
cache that the node can release. `sampled_actors` is the denominator: if it
decreases but the number of actors does not, the sweep cannot measure some
actors.

```bash
kubectl ate top workers
kubectl logs -n ate-system <atelet-pod>
```

---

## The blind spots of this scenario

| Area | Effect on a slow resume |
|---|---|
| The store in ateapi | A delay looks like unmeasured time in the lifecycle and the assignment histograms. |
| The worker cache in ateapi | An old view of the fleet gives a worker that is not in operation. This looks like a resume failure with no cause. |
| The eviction of the image cache | You see the hits and the misses, but not the disk pressure from the layer pool. |
| The build of a golden image | Nothing measures it. A slow first activation of a new template has no data. |

## Move from a template to an actor

No resume metric has the name, the UID, or the atespace of an actor. The
cardinality rules forbid these keys. When the metrics give you a slow template,
use the logs and the traces to find the actor:

```bash
kubectl ate get actors -a <atespace>
kubectl ate logs actors <actor-name> -a <atespace> -f
kubectl ate resume actor <actor-name> -a <atespace> --trace
```

The `--trace` flag prints a trace ID. Put it in Cloud Trace or Jaeger to see
each step of the one resume.
