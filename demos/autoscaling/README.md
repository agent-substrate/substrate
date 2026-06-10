# WorkerPool autoscaling demo

Demonstrates demand-reactive autoscaling of a `WorkerPool` ([issue #198]) on a
running ate cluster:

- **Reactive scale-up** — when a resume finds no free worker, ateapi emits a
  capacity-pressure signal and the autoscaler raises `spec.replicas`
  *immediately*, at the request edge — not at its next poll.
- **Hysteretic scale-down** — once idle workers are in surplus, the pool shrinks
  back to `minReady`, but only after a stabilization window, so a brief lull
  never discards warm capacity.

The pool keeps a small warm **buffer** (`targetBuffer`) of idle workers so a
burst is served by capacity that *already exists*; the autoscaler's job is to
refill that buffer fast and trim it slowly.

[issue #198]: https://github.com/agent-substrate/substrate/issues/198

## What gets deployed

`autoscaling.yaml.tmpl` creates the `ate-demo-autoscaling` namespace, an
autoscaled `WorkerPool`, and an `ActorTemplate` that reuses the `counter`
workload (the workload is irrelevant — the focus is the pool):

```yaml
spec:
  replicas: 2        # starting point; the autoscaler owns it from here
  minReady: 2        # reservation floor — never scale below 2 warm workers
  targetBuffer: 2    # keep ~2 idle workers ready to absorb a burst
  maxReplicas: 8     # ceiling
```

## Prerequisites

- An ate cluster **deployed from this branch** (`worktree-wp-autoscaling`), so
  `ate-controller` runs the WorkerPool autoscaler and the CRD carries the new
  `minReady` / `targetBuffer` / `maxReplicas` fields. A kind cluster via
  `hack/install-ate-kind.sh` works.
- `KO_DOCKER_REPO` and `BUCKET_NAME` set (same as the other demos — `ko`
  resolves the `ko://` images and `BUCKET_NAME` is the GCS bucket for actor
  snapshots).
- The `kubectl-ate` plugin available as `kubectl ate` (or run the demo with
  `ATE=./bin/kubectl-ate ./demo.sh`). If your CLI needs an explicit API
  endpoint, set it the same way you do for other `kubectl ate` calls.

## Run

```sh
# From the repo root:
./demos/autoscaling/demo.sh            # deploys if needed, then runs the scenario
```

Watch it react live in another terminal:

```sh
kubectl get workerpool autoscaling -n ate-demo-autoscaling -w
kubectl logs -n ate-system deploy/ate-controller -f | grep 'autoscaled WorkerPool'
```

The driver also accepts:

```sh
./demos/autoscaling/demo.sh deploy     # just apply the manifest
./demos/autoscaling/demo.sh cleanup    # remove the demo actors + manifest
```

Tunables (env vars): `N` (actors woken, default 6), `SCALE_DOWN_WAIT`
(seconds to wait for the shrink, default 180), `ATE`, `KO`.

## What you should see

1. **Steady** — the pool settles at `DESIRED 2 / REPLICAS 2`: two idle workers
   held warm.
2. **Burst** — the demo wakes 6 actors. The first two consume the warm buffer;
   the next resume finds no free worker and returns `503`. That miss is the
   trigger: `ate-controller` logs `autoscaled WorkerPool` and `spec.replicas`
   jumps up toward `occupied + targetBuffer`, capped at `maxReplicas=8`. The
   `503`'d resumes succeed on retry once the new workers finish booting (cold
   start takes a little while — that delay is *why* the warm buffer exists).
3. **Idle** — the demo suspends all actors, freeing their workers. The buffer is
   now in surplus. After the stabilization window (~60s) the pool shrinks back
   to the floor, `DESIRED 2`.

To see the ceiling enforced, run with `N=12 ./demos/autoscaling/demo.sh`: the
pool grows to `maxReplicas=8` and no further, so the excess resumes keep
getting `503` (there is no capacity left to create).

## How it works

- ateapi publishes a pool-scoped `CapacityPressureEvent` whenever
  `AssignWorkerStep` finds no free worker, exposed via the
  `WatchCapacityPressure` streaming RPC.
- `WorkerPoolAutoscaler` (in `ate-controller`) subscribes to that stream and
  turns each event into an immediate reconcile of the pool. It also re-evaluates
  on a ~10s poll (the scale-down path and a safety net). It reads occupancy via
  `ListWorkers`, runs the decision policy, and is the **single writer** of
  `spec.replicas`; `WorkerPoolReconciler` still owns the Deployment.

> Note: `ate-controller` runs without leader election today, so the
> single-writer guarantee assumes a single controller replica.
