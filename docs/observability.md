# Actor Observability in Agent Substrate

Agent Substrate manages actors as virtually long-lived entities that can be suspended when idle and resumed on different Kubernetes worker pods over time.

This guide explains how Agent Substrate achieves observability across these suspend/resume cycles, allowing you to monitor logs, metrics, and traces as if an actor has been continuously running on a single dedicated machine.

## The Observability Model

To make underlying infrastructure transitions transparent, Agent Substrate establishes a standardized metadata model to identify actors across worker pods:
* `ate.dev/actor_id`: The unique identifier of the actor (e.g., `my-counter-1` or `test`).
* `ate.dev/actor_template_name`: The name of the actor's ActorTemplate (e.g., `counter`).
* `ate.dev/actor_template_namespace`: The Kubernetes namespace of the actor's ActorTemplate (e.g., `ate-demo-counter`).

Currently, Agent Substrate automatically wraps container output and injects these metadata labels into **container logs**. For metrics and distributed tracing, Agent Substrate provides foundational system telemetry and on-demand request tracing, with roadmap plans to fully integrate actor-level correlation.

---

## 1. Logging

Agent Substrate captures container standard output/error, wraps them into structured JSON log entries, and injects the `ate.dev` metadata labels.

### Active Actor Inspection via CLI
For quick, on-demand debugging of an active actor, use the Agent Substrate CLI:

```bash
kubectl ate logs <actor_id> [--follow / -f]
```

> **Note:** By default, `kubectl ate logs` queries the Kubernetes API of the worker pod where the actor is *currently* running. It is designed for immediate inspection of active actors. To view historical logs across past worker pods and suspension cycles, use a centralized logging backend.

#### Example 1: Actor Not Currently Running
If an actor is suspended or not assigned to a worker pod, the CLI informs you immediately:

```bash
$ kubectl ate logs test
Error: actor test is not currently running on any worker pod
```

#### Example 2: Default Clean JSON Lines Output
When an active actor is assigned to a worker pod, the CLI outputs clean, uniform JSON lines stripped of Substrate metadata, perfectly matching standard `kubectl logs` behavior:

```bash
$ kubectl ate logs test
{"time":"2026-05-22T21:49:15.23700774Z","message":"Actor started"}
{"time":"2026-05-22T21:49:15.23700774Z","level":"INFO","msg":"Starting server on port 80"}
{"time":"2026-05-22T21:49:15.255765354Z","count":0,"fshash":"mCY7G4S318ztOUojPTF2NA/W+ZSmWyr+T5K3udFuP50","level":"INFO","msg":"Count"}
{"time":"2026-05-22T21:49:25.263744806Z","count":1,"fshash":"mCY7G4S318ztOUojPTF2NA/W+ZSmWyr+T5K3udFuP50","level":"INFO","msg":"Count"}
```

#### Example 3: Streaming/Live Logs (`--follow` or `-f`)
To stream actor logs in real-time, append the `--follow` (or `-f`) flag. The CLI is fully actor-aware, automatically resuming the stream if the actor is suspended or migrates to a different worker pod:

```bash
$ kubectl ate logs test -f
Actor is currently running on pod ate-demo-counter/counter-deployment-d8f99-m7d96
{"time":"2026-05-22T21:49:15.255765354Z","count":0,"fshash":"mCY7...","level":"INFO","msg":"Count"}
{"time":"2026-05-22T21:49:25.263744806Z","count":1,"fshash":"mCY7...","level":"INFO","msg":"Count"}
Actor is currently running on pod ate-demo-counter/counter-deployment-ab123-x4y5z
{"time":"2026-05-22T21:50:02.123456789Z","count":2,"fshash":"mCY7...","level":"INFO","msg":"Count"}
```


---

### Centralized Logging Backends (Multi-Dimensional Aggregation)
To view the continuous log history of actors across past and present worker pods, you can integrate Agent Substrate with any centralized logging backend (such as Grafana or Google Cloud Logging) that supports structured JSON indexing.

Because the logging pipeline indexes the core metadata labels, you can query your logs across multiple dimensions using your logging platform's query language (examples below use Google Cloud Log Explorer syntax):

#### 1. Actor-Centric View
To track the unified, continuous lifecycle of a single actor regardless of how many times it migrated across worker pods or was suspended/resumed:

```text
labels.actor_id="test"
```

#### 2. Template-Centric View
To monitor or debug all actor instances created from a specific template (e.g., analyzing the collective behavior or error rates of all counter actors):

```text
labels.actor_template="counter"
```

#### 3. Pod-Centric View
To inspect the physical worker pod's aggregate stream and see all co-located actors multiplexed together (useful for investigating pod-level resource exhaustion or noisy neighbor issues):

```text
resource.labels.pod_name="counter-deployment-c995fdf4c-m7d96"
```

---

## 2. Metrics

Agent Substrate emits foundational OpenTelemetry system and server metrics to monitor the overall health and performance of the control plane services. Every metric below is emitted by a service binary over OTLP and is **independent of the deployment** — a Kind dev cluster gets the same instruments as production; only the backend differs (see [Where Telemetry Goes](#4-where-telemetry-goes)).

| Metric | Emitted by | Type | Measures |
|--------|------------|------|----------|
| `rpc.server.call.duration` | ateapi & atelet (gRPC servers, via `otelgrpc`) | histogram | per-method gRPC latency, request rate, and errors (labels `rpc.method`, `rpc.response.status_code`) |
| `atenet.router.route.duration` | atenet-router | histogram | Substrate E2E — Envoy receiving a request to Envoy forwarding it to the resolved worker, excluding actor compute and the response |
| `atelet.snapshot.size` | atelet | histogram | uncompressed size in bytes of each gVisor snapshot image written during checkpoint (labels `kind`, `actor_template_name`) |

The table lists the OpenTelemetry instrument names. How a name appears in a query depends on the backend (Cloud Monitoring (GMP) / Kind collector).

### Local Metrics with Prometheus (Kind Cluster)

For local development inside a `kind` cluster, Agent Substrate automatically provisions a Prometheus server in the `otel-system` namespace.

To explore metrics locally:

1. **Expose the Prometheus UI** via port forwarding:
   ```bash
   kubectl port-forward -n otel-system svc/prometheus 9090:9090
   ```

2. **Open the Prometheus UI** in your web browser:
   [http://localhost:9090](http://localhost:9090)

3. **Query metrics**: Run `up` to confirm each component is scraped (one series per target, value `1`), then explore the `rpc_*` series via the expression browser's autocomplete. **Status > Targets** lists the discovered pods.

> **Note:** Storage is ephemeral (`emptyDir`), so metrics are lost when the Prometheus pod restarts.

> **Roadmap Note (Actor-Level Metrics):** A comprehensive metrics roadmap is under active development to support both system operators and workload analysis. Planned OpenTelemetry instrumentation focuses on control plane latency, state snapshot performance, fleet utilization density, and enriching metrics with standardized actor labels for seamless aggregation across pod transitions.

---

## 3. Tracing

Distributed tracing tracks the end-to-end flow of requests as they pass through the Agent Substrate gateway, router, worker pods, and external services.

Currently, Agent Substrate supports on-demand request tracing. When initiated by a client (e.g., via the `--trace` flag), Agent Substrate leverages OpenTelemetry (OTel) for context propagation across the call stack. Each traced request generates a unique trace hash/ID, which you can use to inspect the detailed request lifecycle and span hierarchy inside Google Cloud Trace or Jaeger.

### Local Tracing with Jaeger (Kind Cluster)

For local development inside a `kind` cluster, Agent Substrate automatically provisions a local OpenTelemetry Collector and Jaeger instance.

To visualize traces locally:

1. **Expose the Jaeger query UI** via port forwarding:
   ```bash
   kubectl port-forward -n otel-system svc/jaeger 16686:16686
   ```

2. **Open the Jaeger UI** in your web browser:
   [http://localhost:16686](http://localhost:16686)

3. **Generate Traces**: Run a CLI command or API call with the `--trace` flag, e.g.:
   ```bash
   kubectl ate get actor --trace
   # or
   kubectl ate suspend actor <actor-id> --trace
   ```

4. **Search and Inspect**: Copy the printed Trace ID from the CLI output and paste it into the Jaeger search box (top right), or select `ateapi` or `atelet` under the **Service** dropdown and click **Find Traces** to inspect detailed call stacks, DB transactions, state updates, and worker pod handoffs.

> **Developer Guide:** For detailed instructions on configuring OpenTelemetry tracer providers, middleware, and exporters in your servers or clients, please refer to the [Tracing Best Practices](dev/best-practices/tracing.md) guide.

---

## 4. Where Telemetry Goes

Telemetry is emitted the same way everywhere; only the backend differs between a local Kind cluster and a Google Cloud (GKE) deployment. The cloud-side backends below are all **GCP services**.

| | Kind | GKE (Google Cloud) |
|---|---|---|
| Path | service → in-cluster `opentelemetry-collector` | service → Google Managed Prometheus (GMP) |
| Metrics | collector Prometheus exporter on `:8889` | Google Cloud Monitoring |
| Traces | Jaeger UI | Google Cloud Trace |
| Dashboards | Not supported | Google Cloud Monitoring (see [Dashboards](#5-dashboards)) |

> In Kind only `ateapi` and `atelet` are pointed at the in-cluster collector; `atenet-router` still targets the GKE collector endpoint, so `atenet.router.route.duration` is emitted but not collected locally.

---

## 5. Dashboards

> **GCP-specific.** These are **Google Cloud Monitoring** dashboards; they apply only to a GKE / Google Cloud deployment. There is no dashboard support on Kind — use the Prometheus UI in [Metrics](#2-metrics) for local development.

Dashboard definitions live in [`monitoring/dashboards/`](../monitoring/dashboards/) (see its README for the per-dashboard breakdown). They are created and updated **as part of GCP setup**: `tools/setup-gcp` applies each dashboard idempotently (matched and updated by display name), so re-running is safe.

```sh
go run ./tools/setup-gcp --create-monitoring-dashboards   # also part of: --all
```
