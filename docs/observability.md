# Actor Observability in Agent Substrate

Agent Substrate manages actors as virtually long-lived entities that can be suspended when idle and resumed on different Kubernetes worker pods over time.

This guide explains how Agent Substrate achieves observability across these suspend/resume cycles, allowing you to monitor logs, metrics, and traces as if an actor has been continuously running on a single dedicated machine.

## The Observability Model

To make underlying infrastructure transitions transparent, Agent Substrate establishes a standardized metadata model to identify actors across worker pods:
* `ate.dev/actor_id`: The unique identifier of the actor (e.g., `my-counter-1` or `test`).
* `ate.dev/actor_template`: The template used to create the actor (e.g., `counter`).
* `ate.dev/actor_namespace`: The Kubernetes namespace of the actor (e.g., `ate-demo-counter`).

Currently, Agent Substrate automatically wraps container output and injects these metadata labels into **container logs**. For metrics and distributed tracing, Agent Substrate provides foundational system telemetry and on-demand request tracing, with roadmap plans to fully integrate actor-level correlation.

---

## 1. Logging

Agent Substrate captures container standard output/error, wraps them into structured JSON log entries, and injects the `ate.dev` metadata labels.

### Active Actor Inspection via CLI
For quick, on-demand debugging of an active actor, use the Agent Substrate CLI:

```bash
kubectl ate logs <actor_id>
```

> **Note:** `kubectl ate logs` queries the Kubernetes API of the worker pod where the actor is *currently* running. It is designed for immediate inspection of active actors. To view historical logs across past worker pods and suspension cycles, use a centralized logging backend.

#### Example 1: Actor Not Currently Running
If an actor is suspended or not assigned to a worker pod, the CLI informs you immediately:

```bash
$ kubectl ate logs test
Error: actor test is not currently running on any worker pod
```

#### Example 2: Default Pretty-Printed Output
When an active actor is assigned to a worker pod, the CLI formats the log stream for easy reading:

```bash
$ kubectl ate logs test
[INFO] Actor started
[2026-05-19 18:39:24] [INFO] Starting server on port 80
[2026-05-19 18:39:24] [INFO] Count
[2026-05-19 18:39:34] [INFO] Count
[2026-05-19 18:39:44] [INFO] Count
```

#### Example 3: Raw JSON Output (`--raw`)
To inspect the underlying structured JSON log entries, use the `--raw` flag. Note that during logging pipeline ingestion, the `ate.dev/` prefix is typically stripped from the label keys for cleaner indexing:

```bash
$ kubectl ate logs test --raw
{"count":9,"fshash":"JiOzRUA5Ab+aro4YnhADSSMq8gUXhh/DMNSFzl75Q7c","level":"INFO","logging.googleapis.com/labels":{"actor_id":"test","actor_namespace":"ate-demo-counter","actor_template":"counter"},"msg":"Count","time":"2026-05-19T18:40:54.957798659Z"}
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

Agent Substrate currently emits foundational OpenTelemetry system and server metrics (such as `rpc.server.call.duration` and `http.server.request.duration`) to monitor the overall health and performance of the `ateapi` and `atelet` control plane services.

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
