# Request Parking Demo

This demo shows the **request parking** feature of the `atenet` router: when the
`WorkerPool` is momentarily saturated, the router *holds* (parks) an inbound
request and retries the resume until a worker frees up — instead of failing fast
with a `503`.

The setup is deliberately **oversubscribed**: a 2-worker pool with several
actors. The workload is the same `counter` binary used by the counter demo; its
reply includes the worker pod IP, so you can see which worker served a request.

See [docs/request-parking.md](../../docs/request-parking.md) for the design.

## Prerequisites

- A k8s cluster with Agent Substrate installed (`./hack/install-ate.sh --deploy-ate-system`).
- `ko` installed for building images.
- A GCS bucket for storing snapshots (configured via `BUCKET_NAME` env var).

## How to Run on Agent Substrate

### 1. Build and Deploy

> [!NOTE]
> Do not manually edit `demos/parking/parking.yaml.tmpl`. The installation script
> automatically injects your `${BUCKET_NAME}` environment variable during deployment.

```bash
./hack/install-ate.sh --deploy-demo-parking
```

This command will:
- Build the `counter` workload image using `ko`.
- Create the `ate-demo-parking` namespace.
- Create a **2-replica** `WorkerPool` (`parking`) and the `parking` `ActorTemplate`.
- Wait until the pool is rolled out and the template is `Ready`.

### 2. Create more actors than workers

Actors live in an **atespace**, and their DNS names embed it
(`<id>.<atespace>.actors.resources.substrate.ate.dev`), so create one first:

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

kubectl ate create atespace parking

# 4 actors share a 2-worker pool -> oversubscribed.
for id in p1 p2 p3 p4; do
  kubectl ate create actor "$id" --atespace parking --template ate-demo-parking/parking
done
```

### 3. Port-forward the atenet router

```bash
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

## How to Use

Parking is **on by default** (`--parking-max-wait=30s`,
`--parking-max-parked=2048`), so the cluster you just deployed already parks.

### A. Watch a 503 become a served request

Fill both workers by requesting two actors, leaving them `RUNNING`:

```bash
curl -s -H "Host: p1.parking.actors.resources.substrate.ate.dev" http://localhost:8000
curl -s -H "Host: p2.parking.actors.resources.substrate.ate.dev" http://localhost:8000

kubectl ate get workers   # both workers are now bound to p1 and p2
kubectl ate get actors    # p1,p2 RUNNING; p3,p4 SUSPENDED
```

Now request **p3** with timing. The pool is full, so this request **parks** —
the `curl` hangs while the router retries the resume:

```bash
curl -s -w '\n-> HTTP %{http_code} in %{time_total}s\n' \
  -H "Host: p3.parking.actors.resources.substrate.ate.dev" http://localhost:8000
```

While that is hanging, in a **second terminal** free a worker by suspending p1
(within the 30s park budget):

```bash
kubectl ate suspend actor p1 --atespace parking
```

Back in the first terminal, the parked request now completes with **`HTTP 200`**,
and `time_total` shows how long it waited for the worker. With parking disabled,
that same request would have returned **`503`** immediately (see section D).

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
# { "enabled": true, "active": 3, "max_parked": 2048, "max_wait": "30s" }
```

The parking metrics are also exported on the router's metrics endpoint
(`--metrics-listen-addr`, container port `9090`): `atenet.router.parking.active`,
`atenet.router.parking.wait.duration` (labeled by `outcome`), and
`atenet.router.parking.rejected`.

### D. Compare with parking disabled

Turn parking off to see the old fail-fast behavior. Add the flag to the router
container's args:

```bash
kubectl -n ate-system patch deployment atenet-router --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--parking-max-parked=0"}]'
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
> You can tune parking instead of disabling it: add `--parking-max-wait=10s` or
> `--parking-max-parked=512` to the same args list.

## How to Uninstall

Remove the demo — this deletes the demo's actors (suspending running ones
first) and then the template, pool, and namespace:

```bash
./hack/install-ate.sh --delete-demo-parking
```
