---
title: "Request Parking Walkthrough"
linkTitle: "Request Parking Walkthrough"
weight: 5
estimated_time: "10 mins"
card_icon: "fa-solid fa-traffic-light"
description: >
  Hold and retry inbound requests during momentary WorkerPool saturation instead of failing with 503.
---

# Request Parking Demo

This demo shows the **request parking** feature of the `atenet` router: when the
`WorkerPool` is momentarily saturated, the router *holds* (parks) an inbound
request and retries the resume until a worker frees up — instead of failing fast
with a `503`.

The setup is deliberately **oversubscribed**: a 2-worker pool with several
actors running the `counter` binary from the counter demo. Run
`kubectl ate get actors -a ate-demo-parking` to see which worker pod each actor
is assigned to.

See [docs/concepts/request-parking.md](../../docs/concepts/request-parking.md) for the design.

## Prerequisites

- A Kubernetes cluster with Agent Substrate installed as in the [Quickstart](../../docs/get-started/quickstart.md)
  (with `ATE_INSTALL_KIND=true` exported in your shell on [kind](https://kind.sigs.k8s.io/)) or [Operator Install Guide](../../docs/guides/operator-install.md).
- `atenet-router` configured with `--ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/ate-system/sa/atenet-client`
  (`ATE_INGRESS_ALLOWED_SPIFFE_IDS`, see [Ingress to actors](../../docs/guides/operator-install.md#ingress-to-actors)) so `kubectl ate proxy actor` can authenticate to the router.
- A snapshot location (`SNAPSHOT_LOCATION`, see [Snapshot storage](../../docs/guides/operator-install.md#snapshot-storage)) and image repository (`KO_DOCKER_REPO`, or `ko.dockerRepo` in `--config`). On `kind`, `export ATE_INSTALL_KIND=true` configures local defaults (`gs://ate-snapshots/` and `localhost:5001`) automatically.
- The default Envoy router dataplane (`--atenet-dataplane=envoy`) for Sections C and D, which inspect `:4040/statusz` and patch `--parked-request-max=0` on `atenet-router`.

## How to Run on Agent Substrate

### 1. Build and Deploy

```bash
go run ./cmd/ate-setup deploy demo parking
```

This command will:
- Build the `counter` workload image using `ko`.
- Create the `ate-demo-parking` namespace and a **2-replica** `WorkerPool`
  (`parking`, from `parking.yaml.tmpl`).
- Create the `ate-demo-parking` atespace and the `parking` actor template in it
  (`parking-template.yaml.tmpl`, applied with `kubectl ate create actor-template`).
- Wait until the pool is rolled out and the template's golden snapshot is built.

### 2. Create more actors than workers

Actors live in the demo's **atespace** (`ate-demo-parking`). `--template`
names the template, resolved in the actor's atespace:

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

# 4 actors share a 2-worker pool -> oversubscribed.
for id in p1 p2 p3 p4; do
  kubectl ate create actor "$id" --atespace ate-demo-parking --template parking
done
```

### 3. Proxy to the actors

```bash
kubectl ate proxy actor p1 8001:80 -a ate-demo-parking &
PROXY_P1=$!
kubectl ate proxy actor p2 8002:80 -a ate-demo-parking &
PROXY_P2=$!
kubectl ate proxy actor p3 8003:80 -a ate-demo-parking &
PROXY_P3=$!
```

## How to Use

Parking is **on by default** (`--parked-request-budget=5s`,
`--parked-request-max=1024`), so the cluster you just deployed already parks.

### A. Watch a 503 become a served request

Fill both workers by requesting two actors, leaving them `ACTOR_STATE_RUNNING`:

```bash
curl -s http://localhost:8001
curl -s http://localhost:8002

kubectl ate get workers   # both workers are now bound to p1 and p2
kubectl ate get actors -a ate-demo-parking   # p1,p2 ACTOR_STATE_RUNNING; p3,p4 ACTOR_STATE_SUSPENDED
```

Now request **p3** with timing while a background subshell frees a worker 1s
later by suspending `p1` (within the 5s `--parked-request-budget`). Because the
pool is full when `curl` starts, the router parks the request and retries the
resume until `p1` releases its worker:

```bash
(sleep 1 && kubectl ate suspend actor p1 -a ate-demo-parking >/dev/null) &
curl -s -w '\n-> HTTP %{http_code} in %{time_total}s\n' http://localhost:8003
```

The parked request completes with **`HTTP 200`**, and `time_total` shows how
long it waited for the worker. With parking disabled, that same request returns
**`503`** immediately (see section D).

### B. See it under load

`load.sh` drives one concurrent request→suspend loop per actor. Because there are
more actors than workers, the pool stays saturated; the suspend at the end of each
loop frees a worker for a competitor (standing in for an actor going idle). The
tally shows parking absorbing the contention:

```bash
./demos/parking/load.sh                 # 30s, actors p1 p2 p3 p4
# ==> results
#     total requests : 142
#     200 OK         : 142
#     503 unavailable: 0
#     200 latency    : avg 0.43s, slowest 6.12s  <- parked requests sit here
#     => 0 failures under saturation: parking absorbed the contention.
```

### C. Observe parking state

The router's `/statusz` page has a **Request Parking** card. Port-forward the
status port and read it (run this while `load.sh` is generating load to see a
non-zero `active`):

```bash
kubectl -n ate-system port-forward deployment/atenet-router 4040:4040
curl -s 'http://localhost:4040/statusz?format=json' | jq .parking
# { "enabled": true, "active": 3, "max_parked": 1024, "max_wait": "5s" }
```

The parking metrics are also exported on the router's metrics endpoint
(`--metrics-listen-addr`, container port `9090`): `atenet.router.parking.active`,
`atenet.router.parking.wait.duration` (labeled by `ate.router.outcome`), and
`atenet.router.parking.rejected` (labeled by `ate.router.outcome`).

### D. Compare with parking disabled

Turn parking off to see the old fail-fast behavior. Add the flag to the router
container's args:

```bash
kubectl -n ate-system patch deployment atenet-router --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--parked-request-max=0"}]'
kubectl -n ate-system rollout status deployment/atenet-router
```

Re-run the load test — now transient saturation surfaces as `503`s:

```bash
./demos/parking/load.sh
#     503 unavailable: 37
#     => 37 requests were shed with 503 (parking off, ...).
```

Re-enable parking by removing that flag again:

```bash
kubectl -n ate-system rollout undo deployment/atenet-router
```

> [!TIP]
> You can tune parking instead of disabling it: add `--parked-request-budget=10s` or
> `--parked-request-max=512` to the same args list.

## How to Uninstall

Stop the background proxy processes and remove the demo. This deletes the demo's
actors (suspending running ones first) and then the template, atespace, pool,
and namespace:

```bash
kill "$PROXY_P1" "$PROXY_P2" "$PROXY_P3"
go run ./cmd/ate-setup delete demo parking
```
