# Substrate has no capacity for an actor

Start here when a resume fails because no worker has room for the actor. At
the edge, the client gets `503 no free workers available` after the router
parked the request. [requests-are-slow.md](requests-are-slow.md) sends you
here.

Read [How to read a step](README.md#how-to-read-a-step) first.

## How Substrate places an actor

Substrate puts many actors on few workers. A worker is a pod in a WorkerPool,
and one worker holds many actors. Each worker reports its capacity: a number
of actor slots, and CPU and memory. The scheduler in ateapi gives an actor
only a worker that meets the constraints of the actor and that has room for
it. The constraints come from the ActorTemplate and the actor, such as the
sandbox class and the worker selectors. An actor with a local snapshot also
needs a worker on the node that holds the snapshot.

Thus "no capacity" has four different causes, and they need different work:

| Cause | The work it needs |
|---|---|
| Each worker is full. | More workers, or fewer idle actors. |
| The pool asked for more workers, and Kubernetes did not give them. | Fix the nodes, the quota, or the worker pod. |
| Workers have free slots, but not enough CPU or memory for the actor. | Smaller limits on the template, or more resources for each worker. |
| Workers have room, but a constraint removes them. | Fix the selector, the sandbox class, or the node pin. |

---

## Step 1. Does a worker with room exist?

**Signal:** `ate.workerpool.workers`, which counts the workers of each pool by
state. The `registry.ate.workerpool` group of
[the registry](../metrics/registry/metrics.yaml) says what each state means.

**Query:**

```promql
max by (ate_workerpool_namespace, ate_workerpool_name, ate_sandbox_class,
        ate_worker_state) (
  ate_workerpool_workers)
```

**Use `max`, not `sum`, across the replicas of ateapi.** Each replica reports
the whole fleet, thus a sum multiplies the fleet by the number of replicas.

**Keep the sandbox class in the result.** The scheduler gives an actor only a
worker of the same class. A sum across the classes counts workers that the
actor cannot use.

**Read:**

* No worker of the class has a free slot — each worker is full. Go to step 2.
* Workers of the class have free slots, and the resumes still fail — go to
  step 3.
* Many workers are not schedulable — the scheduler cannot use them. A worker
  that drains, and a worker that has not reported its capacity yet, are in
  this state. A rollout makes many of them:

  ```bash
  kubectl ate get workers
  kubectl rollout status deploy -n <workerpool-namespace> <workerpool-name>
  ```

**This instrument counts workers, not slots.** A worker with one free slot and
a worker with many free slots are in the same state. Thus it tells you whether
a worker with a free slot exists, not how full the pool is.

## Step 2. Did Kubernetes give the workers that the pool asked for?

**Signal:** `ate.workerpool.desired_workers` and
`ate.workerpool.ready_workers`.

**Query:**

```promql
max by (ate_workerpool_namespace, ate_workerpool_name) (
  ate_workerpool_desired_workers)
- max by (ate_workerpool_namespace, ate_workerpool_name) (
  ate_workerpool_ready_workers)
```

Reduce each side with `max` before the subtraction. Without it, a second
replica of ateapi gives two series for one pool, and the subtraction fails.

**Read:**

* Above 0 for more than some minutes — Kubernetes did not give the pods. The
  usual causes are an empty node pool, a quota, or a worker pod that cannot
  start:

  ```bash
  kubectl get pods -n <workerpool-namespace> -o wide
  kubectl describe pod -n <workerpool-namespace> <worker-pod>
  kubectl get events -n <workerpool-namespace> --sort-by=.lastTimestamp | tail -20
  ```

* 0 — the pool has each pod that it asked for. Before you make the pool
  larger, go to step 5. A pool can be full of actors that do no work.

## Step 3. Why do the workers with room take no actor?

**Signal:** the `no_capacity` outcome on `ate.scheduler.assignment.duration`.
It names the sandbox class, but no pool. No metric names the constraint that
removed a worker, because the selectors and the nodes are barred from a metric
label.

**Query:**

```promql
sum by (ate_sandbox_class) (
  rate(ate_scheduler_assignment_duration_seconds_count{
        ate_scheduler_outcome="no_capacity"}[5m]))
```

**Read:** compare the class with the workers with room from step 1.

* No worker of that class has room, but workers of a different class do — the
  template asks for a class that has no free worker. Examine the sandbox class
  of the template and of the pools.
* Workers of that class have room — compare the actor with the workers:

  ```bash
  kubectl ate get workers
  kubectl ate top workers
  kubectl ate get actors -A
  ```

  | What you see | Cause |
  |---|---|
  | The workers with free slots have little free CPU or memory. | The limits of the actors fill the worker before the slots do. |
  | The labels of the workers with room do not match the selector of the template or of the actor. | A selector hides the workers. |
  | The actor was paused, and its node has no worker with room. | The actor is pinned to the node of its local snapshot. |

**A node pin has no fallback.** The scheduler does not place a pinned actor on
a different node. Suspend the actor rather than pause it when it must be free
to move, or add capacity to the node that holds the snapshot.

## Step 4. Is the scheduler slow, or does it fail?

**Signal:** `ate.scheduler.assignment.duration`. The `registry.ate.scheduler`
group says what each outcome means.

**Query:**

```promql
sum by (ate_scheduler_outcome, error_type) (
  rate(ate_scheduler_assignment_duration_seconds_count[5m]))

histogram_quantile(0.95, sum by (le, ate_scheduler_outcome) (
  rate(ate_scheduler_assignment_duration_seconds_bucket[5m])))
```

**Read:**

* The scheduler fails — `error.type` holds the gRPC status code. Read the logs
  of ateapi.
* The scheduler assigns workers, but slowly — a placement is a read of a cache
  and some writes to the store. A large time means a delay in the store, which
  has no metrics. Read the logs of ateapi.
* The scheduler assigns workers quickly, and the users still get a 503 error —
  the fault is not capacity. Go back to
  [requests-are-slow.md](requests-are-slow.md).

## Step 5. Is the pressure real?

**Signal:** the actor stats instruments of atelet, such as
`ate.actor.stats.memory.working_set` and `ate.actor.stats.cpu.time`. The
`registry.ate.stats` group says what each source measures.

**Query:**

```promql
sum by (ate_template_name, ate_stats_source) (
  ate_actor_stats_memory_working_set_bytes)

sum by (ate_template_name, ate_stats_source) (
  rate(ate_actor_stats_cpu_time_seconds_total[5m]))
```

**Group by the source, and do not add the sources together.** They measure
different things.

**Read:** compare the use with the limits of the template, and with the
number of actors.

* The actors are idle — each actor holds its slot and its limits, also when
  it is idle. The suspend policy is the subject, not the size of the pool.
* The actors use much less than their limits — the limits of the template are
  too large. They hold resources that the actors do not use.
* The actors use their limits — the load is real. Add workers.

---

## The blind spots of this scenario

* **The use of the slots.** No metric gives the number of slots in use in a
  pool. Step 1 counts workers.
* **The constraint that removed a worker.** No metric names it. Step 3 uses
  kubectl.
* **The worker cache in ateapi.** If the view of the fleet is old, the
  scheduler gives a worker that is not in operation. This looks like a resume
  failure with no cause in the scheduler data.

The full list is in `blind_spots` of
[`docs/metrics/substrate.yaml`](../metrics/substrate.yaml).
