# Requests are slow or return 503

## Context

The requests are the **data-plane requests of the workload**. They are the HTTP
or gRPC calls that a client sends to the address of the actor:

```
<actor-name>.<atespace>.actors.resources.substrate.ate.dev
```

They are not `kubectl ate` commands. Those go to ateapi. Use
`rpc.server.call.duration` for a slow command.

`atenet.router.route.duration` measures only the decision of the router: from
the moment Envoy gives the request to the router, to the moment the router
gives the worker endpoint back. It does **not** include the work of the actor,
and it does not include the response.

| The client is slow | The router metric | Where the cause is |
|---|---|---|
| Yes | Large | In Substrate. Use the steps below. |
| Yes | Small | In the code of the actor, or in the network. Read the logs of the actor. |
| No | Large | Somebody waited, but not this client. For example, an operator did a resume. |

Four outcomes reach the client, and three of them are a 503 error:

| What the client sees | Label | Cause |
|---|---|---|
| A slow but correct response | `ate.router.resume="triggered"` | The actor was not on a worker. The request paid for the resume. |
| `503 no free workers available` | `ate.router.outcome="no_capacity"` | The park budget ended. The fleet stayed full. |
| `503 router at capacity` | Counted in `parking.rejected` | The parking area is full. The router sheds load. |
| A 503 error with no capacity pressure | `ate.router.outcome="resume_error"` | A defect. Examine the router and ateapi. |

**Parking** is why a full fleet does not immediately give an error. The router
holds the request and does the resume again with a backoff. Refer to
[request-parking.md](../request-parking.md). The router also puts the requests
for the **same** actor into one resume. Thus 50 requests on one cold actor make
one `triggered` sample and 49 `joined` samples. Do not read `joined` as 50 slow
activations.

Each step gives the query in two forms. Refer to
[the naming rules](README.md#the-names-on-your-backend) for which form your
backend needs, and for the reasons a query can return nothing.

---

## Step 1. Find the outcome

**Prometheus**

```promql
sum by (ate_router_outcome) (
  rate(atenet_router_route_duration_seconds_count[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.router.outcome") (
  rate({__name__="atenet.router.route.duration_count"}[5m]))
```

| Outcome | Go to |
|---|---|
| `no_capacity` | The fleet has no free worker. Read [capacity-is-full.md](capacity-is-full.md). |
| `resume_error` | Step 3, and then the logs of ateapi. |
| `ok` but slow | Step 2. |
| `ok`, but the client got an error | The fault is after the boundary of the router. The router found the endpoint and Envoy could not use it. Go to step 5. |
| `timeout` | The resume did not finish in the park budget. Go to step 3, and then read the logs of atelet on the worker node. |
| `cancelled` | The client gave up. Read step 2 to find how long it waited. |

`ok` on this metric means only that the router found an endpoint. It does not
mean that the client got an answer.

The table above holds the outcomes that send you to a different step. The
router has more, and the `registry.ate.router` group of
[the registry](../metrics/registry/metrics.yaml) lists each one. An outcome
that is not in the table names its own cause; read it there and then go to
step 4.

## Step 2. Divide the warm route from the resume

**Keep both `ate.router.outcome` and `ate.router.resume` in the `by()` clause.**
The resume state alone does not tell you what it means, because the router also
reports `none` for a request that stopped before it reached a resume state. The
outcome is what separates the two readings, thus the query must group by both.

Keep the resume key for a second reason: if you remove it, the aggregation adds
the warm route to the activation. A warm route is milliseconds and an
activation is hundreds of milliseconds or more, thus one distribution then
holds both and the merged number describes neither.

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le, ate_router_outcome, ate_router_resume) (
  rate(atenet_router_route_duration_seconds_bucket[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le, "ate.router.outcome", "ate.router.resume") (
  rate({__name__="atenet.router.route.duration_bucket"}[5m])))
```

**Read this key only together with the outcome.** `none` is also the value that
the router uses when the request never got as far as a resume state. Thus a
failed route reports `none` although it did try to resume.

| Series | With `outcome="ok"` | With a failed outcome |
|---|---|---|
| `none` | The warm route. The actor was already in operation. This must stay in milliseconds. Go to step 3. | No information. The request stopped before the router set the state. Use step 3 and step 4. |
| `triggered` | This request did the resume. Go to [resumes-are-slow.md](resumes-are-slow.md). | The resume failed. Go to [resumes-are-slow.md](resumes-are-slow.md). |
| `joined` | This request waited for the resume of a different request. Do not count these as separate activations. | The same, and the flight that it joined failed. |

## Step 3. Examine the queue in the router

**Prometheus**

```promql
atenet_router_parking_active

sum(rate(atenet_router_parking_rejected_total[5m]))

histogram_quantile(0.95, sum by (le, outcome) (
  rate(atenet_router_parking_wait_duration_seconds_bucket[5m])))
```

**Cloud Monitoring / GMP**

```promql
{__name__="atenet.router.parking.active"}

sum(rate({__name__="atenet.router.parking.rejected"}[5m]))

histogram_quantile(0.95, sum by(le, outcome) (
  rate({__name__="atenet.router.parking.wait.duration_bucket"}[5m])))
```

* `parking.active` near the configured maximum means that the parking area is
  almost full. `--parked-request-max` sets it. Refer to
  [request-parking.md](../request-parking.md) for the flag and its default.
* `parking.rejected` above zero means that the router refuses requests at the
  edge. Make the pool larger, or make the parking area larger.
* The `outcome` label on the wait histogram does not start with `ate.`. This is
  a known exception in `docs/metrics/substrate.yaml`. Its permitted values are
  in the `registry.ate.deviation` group of
  [the registry](../metrics/registry/metrics.yaml).

A value of `budget_exhausted` below the full budget is normal. The budget is
per flight. A request that joins a flight late shares the remaining budget.

## Step 4. Find out if a component is slow

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le, rpc_method) (
  rate(rpc_server_call_duration_seconds_bucket[5m])))

histogram_quantile(0.95, sum by (le, rpc_method) (
  rate(rpc_client_call_duration_seconds_bucket[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le, "rpc.method") (
  rate({__name__="rpc.server.call.duration_bucket"}[5m])))

histogram_quantile(0.95, sum by(le, "rpc.method") (
  rate({__name__="rpc.client.call.duration_bucket"}[5m])))
```

Compare the two for the same method. If the client number is much larger than
the server number, the delay is not in the handler. It is in the network or in
a queue.

## Step 5. Read the actor

```bash
kubectl ate get actors -a <atespace>
kubectl ate logs actors <actor-name> -a <atespace> -f
```

If the route duration is small but the client is slow, the cause is here. No
Substrate metric covers the work of the actor.

An actor can run more than one container. `--container`, or `-c`, keeps only
the lines of the named container:

```bash
kubectl ate logs actors <actor-name> -a <atespace> -c <container-name>
```

Two things to know before you use it:

* **It also removes the lifecycle records.** `Actor started`, `Actor restored`
  and the others are about the actor, thus no container produced them and no
  container name selects them. Read the logs without `-c` when you need the
  lifecycle of the actor together with its output.
* **A name that does not match gives no output and no error.** The command ends
  with status 0 and an empty result, which looks the same as a silent actor.
  Take the container names from the ActorTemplate.

---

## The blind spots of this scenario

| Area | Effect |
|---|---|
| The work of the actor and the response | Outside the route duration. No metric. |
| atenet-dns | No instruments. If a reload fails, the answers stay old and no signal shows it. |
| The shutdown of the router | Nothing counts a drain that ends with an error. |
| The store in ateapi | A delay looks like unmeasured time inside a resume. |
