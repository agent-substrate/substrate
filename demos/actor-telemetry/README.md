# Actor Telemetry Continuity Demo

This directory contains a reference demo for **actor telemetry continuity across
suspend/resume**, the follow-up work discussed in
[#503](https://github.com/agent-substrate/substrate/issues/503).

It deploys a small Go HTTP server (`main.go`) that emits OpenTelemetry (OTel)
metrics and trace spans over OTLP on every request, and shows how an actor
author can avoid losing telemetry that is still buffered in the OTel SDK when an
actor is suspended.

## The problem it demonstrates

OTel push exporters batch telemetry in memory and flush on a timer (the
`PeriodicReader` for metrics, the `BatchSpanProcessor` for spans). When the push
interval (e.g. 60s) is longer than the active-execution window before idle
suspension (e.g. 30s), telemetry generated during the active phase is still
sitting in those in-memory queues when the actor is suspended.

With `snapshotsConfig.onPause: Data` (the mode this demo uses), Substrate
excludes process memory from the snapshot and kills the container process on
pause, so that buffered telemetry is **permanently lost** (this is Issue 1,
HIGH, in #503).

## The mitigation it shows

Until the `PreSuspend` lifecycle hook ([#450](https://github.com/agent-substrate/substrate/issues/450))
lands, an actor author can flush the OTel providers themselves before going
idle. The workload runs an idle watcher: after `--idle-flush` of request
inactivity it calls `ForceFlush` on both the `MeterProvider` and
`TracerProvider`, exporting everything buffered before a suspend can kill the
process. When `#450` lands, that same flush moves into the `PreSuspend` hook.

Flags:

| Flag | Default | Purpose |
|------|---------|---------|
| `--push-interval` | `60s` | OTLP export interval. Intentionally long, to reproduce #503. |
| `--idle-flush` | `25s` | Idle duration after which the workload force-flushes. Set to `0` to disable the flush and observe the loss. |

## Prerequisites

- A k8s cluster with Agent Substrate installed (`./hack/install-ate.sh --deploy-ate-system`).
- `ko` installed for building images.
- A GCS bucket for storing snapshots (configured via `BUCKET_NAME` env var).

## How to Run on Agent Substrate

### 1. Build and Deploy

> [!NOTE]
> Do not manually edit `demos/actor-telemetry/actor-telemetry.yaml.tmpl`. The
> installation script automatically injects your `${BUCKET_NAME}` environment
> variable during deployment.

```bash
./hack/install-ate.sh --deploy-demo-actor-telemetry
```

This command will:
- Build the demo server image using `ko`.
- Create the `ate-demo-actor-telemetry` namespace.
- Create the `WorkerPool` and `ActorTemplate`.
- Wait until the template is ready.

### 2. Create an Actor

```bash
# Install the CLI as a kubectl plugin if not already installed.
go install ./cmd/kubectl-ate

# Create the atespace (required before creating actors).
kubectl ate create atespace demo

# Create the actor from the actor-telemetry template.
kubectl ate create actor my-telemetry-1 -a demo --template ate-demo-actor-telemetry/actor-telemetry
```

### 3. Port-Forward Services

```bash
# Router, to send requests to the actor.
kubectl port-forward -n ate-system svc/atenet-router 8000:80

# Jaeger + Prometheus, to see the telemetry arrive (Kind cluster).
kubectl port-forward -n otel-system svc/jaeger 16686:16686
kubectl port-forward -n otel-system svc/prometheus 9090:9090
```

## How to Use

1. Send a few requests through the router:
   ```bash
   curl -X POST -H "Host: my-telemetry-1.demo.actors.resources.substrate.ate.dev" http://localhost:8000
   ```
   Each response reports the actor it was handled for. The metric
   `demo.actor.requests` and a `handle-request` span are now buffered.

2. **Stop sending requests.** After `--idle-flush` (25s) elapses, the workload
   logs `pre-suspend flush complete`. Confirm the buffered telemetry arrived:
   - Prometheus ([http://localhost:9090](http://localhost:9090)): query
     `demo_actor_requests_total`.
   - Jaeger ([http://localhost:16686](http://localhost:16686)): select service
     `ate-demo-actor-telemetry` and find the `handle-request` traces.

3. Suspend and resume the actor, then send more requests and repeat the checks.
   In Jaeger, `handle-request` spans continue to carry the actor's own
   `ate.actor.name` (read fresh from `/run/ate/actor-id`), correlated across the
   worker pods the actor ran on. The `demo_actor_requests_total` metric is
   labeled only by the bounded `ate.actor.template.name`, not per actor, to keep
   metric cardinality bounded at scale (actor identity lives on spans, not
   series).
   ```bash
   kubectl ate suspend actor my-telemetry-1 -a demo
   ```

4. **Observe the loss (optional).** Redeploy with `--idle-flush=0` (edit the
   template's container `args`), send requests, then suspend before the 60s push
   timer fires. With `onPause: Data` the buffered telemetry never reaches the
   backend, reproducing #503.

5. Clean up:
   ```bash
   kubectl ate delete actor my-telemetry-1 -a demo
   kubectl ate delete atespace demo
   ```

## How to Uninstall

```bash
./hack/install-ate.sh --delete-demo-actor-telemetry
```

## Related

- [`docs/observability.md`](../../docs/observability.md) - how Substrate
  surfaces logs, metrics, and traces across suspend/resume.
- [#503](https://github.com/agent-substrate/substrate/issues/503) - the
  telemetry loss and delivery-delay issue this demo addresses.
- [#450](https://github.com/agent-substrate/substrate/issues/450) - actor
  lifecycle hooks (`PostResume` / `PreSuspend`) that will host this flush.
