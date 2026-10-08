# Requests are slow or return 503

Start here when a client of an actor waited too long, or got a 503 error. The
requests are the **data-plane requests of the workload**: the HTTP or gRPC
calls that a client sends to the address of the actor. A slow `kubectl ate`
command is a different subject. It goes to ateapi, thus read
`rpc.server.call.duration` in step 4.

Read [How to read a step](README.md#how-to-read-a-step) first.

## What the router measures

`atenet.router.route.duration` measures only the decision of the router: from
the moment Envoy gives the request to the router, to the moment the router
gives the worker endpoint back. It does **not** include the work of the actor,
and it does not include the response. Thus compare it with what the client
saw:

| The client is slow | The router time | Where the cause is |
|---|---|---|
| Yes | Large | In Substrate. Use the steps below. |
| Yes | Small | In the code of the actor, or in the network. Go to step 5. |
| No | Large | Somebody waited, but not this client. For example, an operator did a resume. |

When an actor is not on a worker, the router asks ateapi to resume it. While
the resume is in progress, the router **parks** the request: it holds the
request and tries again with a backoff. The router puts the requests for the
same actor into one resume. Refer to
[request-parking.md](../request-parking.md).

---

## Step 1. Did the router find a worker?

**Signal:** the outcome on `atenet.router.route.duration`. The
`registry.ate.router` group of [the registry](../metrics/registry/metrics.yaml)
says what each outcome means.

**Query:**

```promql
sum by (ate_router_outcome) (
  rate(atenet_router_route_duration_seconds_count[5m]))
```

**Read:**

* The router found the endpoint, and the request was slow — go to step 2.
* The router found the endpoint, and the client still got an error — the fault
  is after the router. Envoy could not use the endpoint, or the actor failed.
  Go to step 5. The outcome `ok` means only that the router found an endpoint.
* No worker had room for the actor (`no_capacity`) — go to
  [capacity-is-full.md](capacity-is-full.md).
* The router did not find the endpoint for a different reason — go to step 3.
  The router can shed a request because its parking area is full, and step 3
  shows it. For each other outcome, the registry names the cause.
* The client stopped, or the time limit ended — go to step 2 to see how long
  the request waited.

## Step 2. Was the request a warm route or a resume?

**Signal:** the resume state on `atenet.router.route.duration`, in the same
`registry.ate.router` group.

**Query:**

```promql
histogram_quantile(0.95, sum by (le, ate_router_outcome, ate_router_resume) (
  rate(atenet_router_route_duration_seconds_bucket[5m])))
```

**Keep the resume state in the `by()` clause.** A warm route is milliseconds
and a resume is hundreds of milliseconds or more. A query that adds them
together gives a number that describes neither.

**Read:**

* The actor was already on a worker (the warm route), and the time is large —
  the router itself is slow. Go to step 3 and step 4.
* This request did the resume — the resume is the cost. Go to
  [resumes-are-slow.md](resumes-are-slow.md).
* This request waited for the resume of a different request — count these
  with the resume that they waited for, not as separate resumes. One resume
  with 50 requests is one slow resume, not 50.
* The resume did not complete — this time is not a resume time. Read the
  outcome of the same series, and go to step 3.

## Step 3. Did the router park or shed the request?

**Signal:** the parking instruments of the router:
`atenet.router.parking.active`, `atenet.router.parking.rejected` and
`atenet.router.parking.wait.duration`.

**Query:**

```promql
atenet_router_parking_active

sum(rate(atenet_router_parking_rejected_total[5m]))

histogram_quantile(0.95, sum by (le, outcome) (
  rate(atenet_router_parking_wait_duration_seconds_bucket[5m])))
```

**Read:**

* Requests are rejected — the parking area is full, and the router sheds the
  requests without a wait. Make the parking area larger, or make the resumes
  faster with [resumes-are-slow.md](resumes-are-slow.md). More workers do not
  help a shed request. [request-parking.md](../request-parking.md) names the
  flag and its default.
* No requests are rejected, and the wait is long — the requests waited for a
  resume. The outcome of the wait says why it ended. A wait that ended because
  the budget ended means that capacity, not a fault, blocked the resume. Go to
  [capacity-is-full.md](capacity-is-full.md).
* No requests are rejected, and the wait is short — the router did not hold
  the request. Go to step 4, and read the logs of the router.

An absent rejection series counts as zero: a counter appears only after its
first increase.

## Step 4. Is a component of the control plane slow?

**Signal:** the gRPC instruments: `rpc.server.call.duration` on the server
side of a call, and `rpc.client.call.duration` on the client side.

**Query:**

```promql
histogram_quantile(0.95, sum by (le, rpc_method) (
  rate(rpc_server_call_duration_seconds_bucket[5m])))

histogram_quantile(0.95, sum by (le, rpc_method) (
  rate(rpc_client_call_duration_seconds_bucket[5m])))
```

**Read:** compare the two for the same method.

* The server time is large — the handler is slow. Read the logs of that
  component.
* The client time is much larger than the server time — the delay is not in
  the handler. It is in the network or in a queue.

## Step 5. Is the actor the cause?

**Signal:** none. No Substrate metric measures the work of the actor. Read its
logs.

```bash
kubectl ate get actors -a <atespace>
kubectl ate logs actors <actor-name> -a <atespace> -f
```

An actor can run more than one container. `--container`, or `-c`, keeps only
the lines of the named container. It also removes the lifecycle records of the
actor, because no container wrote them. A name that does not match gives no
output and no error. Take the container names from the ActorTemplate.

---

## The blind spots of this scenario

* **The work of the actor and the response.** They are outside the route time.
  No metric measures them.
* **atenet-dns.** If it gives old answers, no signal shows it.
* **The store in ateapi.** A delay looks like unmeasured time inside a resume.

The full list is in `blind_spots` of
[`docs/metrics/substrate.yaml`](../metrics/substrate.yaml).
