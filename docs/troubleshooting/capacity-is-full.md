# Substrate has no capacity for an actor

## Context

Substrate puts many actors on few workers. A worker is a pod in a WorkerPool.
An actor must hold a worker to run. When each worker is assigned, a new resume
cannot start. The router parks the request, and then it returns
`503 no free workers available`.

Three different states look the same at the edge:

| State | What it means | Where to see it |
|---|---|---|
| The pool is full | Each worker has an actor. | `ate.workerpool.workers`, state `idle` = 0 |
| The pool did not grow | Kubernetes did not give the pods that the pool asked for. | `desired_workers` − `ready_workers` > 0 |
| The workers are hidden | Free workers exist, but a constraint removes them. | `ate.scheduler.eligible_workers` = 0 |

The last state is the one that confuses people. `ate.workerpool.workers` shows
idle workers and the resumes still fail.

Two pool keys identify a pool together: `ate.workerpool.namespace` and
`ate.workerpool.name`. A WorkerPool has a namespace, thus the name alone merges
the pools of different namespaces into one series.

Each step gives the query in two forms. Refer to
[the naming rules](README.md#the-names-on-your-backend) for which form your
backend needs, and for the reasons a query can return nothing.

---

## Step 1. Compare the demand with the supply

**Prometheus**

```promql
max by (ate_workerpool_namespace, ate_workerpool_name, ate_worker_state) (
  ate_workerpool_workers)
```

**Cloud Monitoring / GMP**

```promql
max by("ate.workerpool.namespace", "ate.workerpool.name", "ate.worker.state") (
  {__name__="ate.workerpool.workers"})
```

**Use `max` and not `sum` across the replicas of ateapi.** Each ateapi replica
reports the whole fleet, thus a sum multiplies the fleet by the number of
replicas. Two replicas make 10 idle workers read as 20. The `instance` label
divides the replicas:

**Prometheus**

```promql
sum by (instance, ate_worker_state) (ate_workerpool_workers)
```

**Cloud Monitoring / GMP**

```promql
sum by(instance, "ate.worker.state") ({__name__="ate.workerpool.workers"})
```

After the `max`, you can add the counts together. The sum across the states is
the size of the pool. The sum across the pools is the size of the fleet.

* `idle` = 0 — the pool is full. Go to step 2.
* `idle` > 0 and the resumes still fail — a constraint hides the workers. Go to
  step 3.

## Step 2. Find out if the new capacity arrived

**Prometheus**

```promql
ate_workerpool_desired_workers
  - on(ate_workerpool_namespace, ate_workerpool_name)
    ate_workerpool_ready_workers
```

**Cloud Monitoring / GMP**

```promql
{__name__="ate.workerpool.desired_workers"}
  - on("ate.workerpool.namespace", "ate.workerpool.name")
    {__name__="ate.workerpool.ready_workers"}
```

A value above 0 for more than a few minutes means that Kubernetes did not give
the pods. The usual causes are an empty node pool, a quota, or a worker pod
that cannot start.

```bash
kubectl ate get workers
kubectl get pods -n <workerpool-namespace> -o wide
kubectl describe pod -n <workerpool-namespace> <worker-pod>
kubectl get events -n <workerpool-namespace> --sort-by=.lastTimestamp | tail -20
```

If the difference is 0, the pool has each pod that it asked for. The pool is
too small. Make `spec.replicas` larger, or add nodes.

## Step 3. Find out if a constraint hides the workers

`ate.scheduler.eligible_workers` counts the free workers that remain after each
constraint filter. This is an early sign. It warns you before the first
rejection.

**Prometheus**

```promql
histogram_quantile(0.5, sum by (le, ate_scheduling_constraint) (
  rate(ate_scheduler_eligible_workers_bucket[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.5, sum by(le, "ate.scheduling.constraint") (
  rate({__name__="ate.scheduler.eligible_workers_bucket"}[5m])))
```

The `registry.ate.scheduler` group of
[the registry](../metrics/registry/metrics.yaml) says what each value of
`ate.scheduling.constraint` is.

**The value of this key does not tell you the cause.** An ActorTemplate with a
`workerSelector` makes each of its requests `selector`, thus you never see
`none` for that template. Compare this histogram with the idle count from step
1 instead:

| Idle count (step 1) | Eligible workers | Cause |
|---|---|---|
| 0 | 0 | The pool is full. The constraint is not the subject. Go to step 2. |
| Above 0 | 0 | The constraint hides the free workers. Examine the selector of the actor and of the template, and the labels of the workers. |
| Above 0 | Above 0 | The capacity is available. The fault is elsewhere. Go to step 4. |

A series with **both pool keys empty** means that no pool agreed with the
request. Only this instrument reports that state, as one series with the value
0.

## Step 4. Read the decision of the scheduler

**Prometheus**

```promql
sum by (ate_scheduler_outcome) (
  rate(ate_scheduler_assignment_duration_seconds_count[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.scheduler.outcome") (
  rate({__name__="ate.scheduler.assignment.duration_count"}[5m]))
```

| Outcome | Go to |
|---|---|
| `assigned` | The scheduler took a worker. If the users still get a 503 error, go to step 5. |
| `no_free_worker` | This is capacity pressure and not a failure. Go to step 2 and to step 6. |
| `error` | The query below. Only this outcome has an `error.type` key. |

**Prometheus**

```promql
sum by (ate_scheduler_outcome, error_type) (
  rate(ate_scheduler_assignment_duration_seconds_count{
        ate_scheduler_outcome="error"}[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.scheduler.outcome", "error.type") (
  rate({__name__="ate.scheduler.assignment.duration_count",
        "ate.scheduler.outcome"="error"}[5m]))
```

The scheduler can also be slow and not full:

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le) (
  rate(ate_scheduler_assignment_duration_seconds_bucket{
        ate_scheduler_outcome="assigned"}[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le) (
  rate({__name__="ate.scheduler.assignment.duration_bucket",
        "ate.scheduler.outcome"="assigned"}[5m])))
```

This step is a read of the cache and some writes to the store. A large value
means a delay in the store. The store has no metrics. Read the logs of ateapi.

## Step 5. Measure the cost at the edge

The steps above measure the fleet. This step measures what the users pay.

**Prometheus**

```promql
sum by (ate_router_outcome) (
  rate(atenet_router_route_duration_seconds_count[5m]))

atenet_router_parking_active

sum(rate(atenet_router_parking_rejected_total[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.router.outcome") (
  rate({__name__="atenet.router.route.duration_count"}[5m]))

{__name__="atenet.router.parking.active"}

sum(rate({__name__="atenet.router.parking.rejected"}[5m]))
```

* `no_capacity` on the router — the park budget ended and the fleet stayed
  full. The user got a 503 error.
* `parking.rejected` above zero — the parking area is full. The router sheds
  load before it tries a resume.

Refer to [requests-are-slow.md](requests-are-slow.md) for the full path.

## Step 6. Find out if the pressure is real

A pool that is full of actors that do no work is a different fault. Read the
resource use of the node:

**Prometheus**

```promql
sum by (ate_template_name, ate_stats_source) (
  ate_actor_stats_memory_working_set_bytes)

sum by (ate_template_name) (
  rate(ate_actor_stats_cpu_time_seconds_total[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.template.name", "ate.stats.source") (
  {__name__="ate.actor.stats.memory.working_set"})

sum by("ate.template.name") (
  rate({__name__="ate.actor.stats.cpu.time"}[5m]))
```

Group the data by `ate.stats.source`. Do not add the sources together. The
`cgroup` source includes the load of the sandbox runtime. The `guest-agent`
source includes only the containers of the workload.

```bash
kubectl ate top workers
kubectl ate get actors -A
```

If the workers hold actors that are idle, the suspend policy is the subject,
not the size of the pool.

---

## The blind spots of this scenario

| Area | Effect |
|---|---|
| The worker cache in ateapi | If the view of the fleet is old, the scheduler gives a worker that is not in operation. This looks like a resume failure with no cause in the scheduler data. |
| The actor population | Nothing counts the actors by state. These metrics count the operations, not the actors. |
| The store in ateapi | A delay looks like unmeasured time in the assignment histogram. |
