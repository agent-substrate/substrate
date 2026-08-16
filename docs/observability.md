# Actor Observability in Agent Substrate

Agent Substrate manages actors as virtually long-lived entities that can be suspended when idle and resumed on different Kubernetes worker pods over time.

This guide explains how Agent Substrate achieves observability across these suspend/resume cycles, allowing you to monitor logs, metrics, and traces as if an actor has been continuously running on a single dedicated machine.

## The Observability Model

To make underlying infrastructure transitions transparent, Agent Substrate establishes a standardized metadata model to identify actors across worker pods:
* `ate.dev/actor_name`: The name of the actor (e.g., `my-counter-1` or `test`).
* `ate.dev/actor_atespace`: The atespace the actor lives in (e.g., `ate-demo-counter`).
* `ate.dev/actor_uid`: Server-assigned UID of the actor, unique to the lifetime of an actor.
* `ate.dev/actor_template_name`: The name of the actor's ActorTemplate (e.g., `counter`).
* `ate.dev/actor_template_namespace`: The Kubernetes namespace of the actor's ActorTemplate (e.g., `ate-demo-counter`).
* `ate.dev/container_name`: The name of the container within the actor that produced the log line (e.g., `counter`), so a multi-container actor's logs can be demultiplexed by container.

Currently, Agent Substrate automatically wraps container output and injects these metadata labels into **container logs**. For metrics and distributed tracing, Agent Substrate provides foundational system telemetry and on-demand request tracing, with roadmap plans to fully integrate actor-level correlation.

---

## 1. Logging

Agent Substrate captures container standard output/error, wraps them into structured JSON log entries, and injects the `ate.dev` metadata labels.

### Active Actor Inspection via CLI
For quick, on-demand debugging of an active actor, use the Agent Substrate CLI:

```bash
kubectl ate logs actors <actor-name> --atespace <atespace> [--follow / -f]
```

`--atespace` (short form `-a`) is required: actor names are only unique within
an atespace, so an actor is always addressed by `(atespace, name)`.

> **Note:** By default, `kubectl ate logs` queries the Kubernetes API of the worker pod where the actor is *currently* running. It is designed for immediate inspection of active actors. To view historical logs across past worker pods and suspension cycles, use a centralized logging backend.

#### Example 1: Actor Not Currently Running
If an actor is suspended or not assigned to a worker pod, the CLI informs you immediately:

```bash
$ kubectl ate logs actors test -a demo
Error: actor test is not currently running on any worker pod
```

#### Example 2: Default Clean JSON Lines Output
When an active actor is assigned to a worker pod, the CLI outputs clean, uniform JSON lines stripped of Substrate metadata, perfectly matching standard `kubectl logs` behavior:

```bash
$ kubectl ate logs actors test -a demo
{"time":"2026-05-22T21:49:15.23700774Z","message":"Actor started"}
{"time":"2026-05-22T21:49:15.23700774Z","level":"INFO","msg":"Starting counter server on port 80"}
{"time":"2026-05-22T21:49:15.255765354Z","count":0,"fshash":"mCY7G4S318ztOUojPTF2NA/W+ZSmWyr+T5K3udFuP50","level":"INFO","msg":"Count"}
{"time":"2026-05-22T21:49:25.263744806Z","count":1,"fshash":"mCY7G4S318ztOUojPTF2NA/W+ZSmWyr+T5K3udFuP50","level":"INFO","msg":"Count"}
```

#### Example 3: Streaming/Live Logs (`--follow` or `-f`)
To stream actor logs in real-time, append the `--follow` (or `-f`) flag. The CLI is fully actor-aware, automatically resuming the stream if the actor is suspended or migrates to a different worker pod:

```bash
$ kubectl ate logs actors test -a demo -f
Actor is currently running on pod ate-demo-counter/counter-d8f99-m7d96
{"time":"2026-05-22T21:49:15.255765354Z","count":0,"fshash":"mCY7...","level":"INFO","msg":"Count"}
{"time":"2026-05-22T21:49:25.263744806Z","count":1,"fshash":"mCY7...","level":"INFO","msg":"Count"}
Actor is currently running on pod ate-demo-counter/counter-ab123-x4y5z
{"time":"2026-05-22T21:50:02.123456789Z","count":2,"fshash":"mCY7...","level":"INFO","msg":"Count"}
```


---

### Centralized Logging Backends (Multi-Dimensional Aggregation)
To view the continuous log history of actors across past and present worker pods, you can integrate Agent Substrate with any centralized logging backend (such as Grafana or Google Cloud Logging) that supports structured JSON indexing.

Because the logging pipeline indexes the core metadata labels, you can query your logs across multiple dimensions using your logging platform's query language (examples below use Google Cloud Log Explorer syntax):

#### 1. Actor-Centric View
To track the unified, continuous lifecycle of a single actor regardless of how many times it migrated across worker pods or was suspended/resumed:

```text
labels."ate.dev/actor_name"="test"
```

#### 2. Atespace-Centric View
To monitor or debug all actor instances in a specific atespace (e.g., analyzing the collective behavior or error rates of all actors belonging to one tenant):

```text
labels."ate.dev/actor_atespace"="ate-demo-counter"
```

#### 3. Template-Centric View
To monitor or debug all actor instances created from a specific ActorTemplate (e.g., analyzing the collective behavior or error rates of all counter actors). One atespace can run actors from many templates, so this is a distinct dimension from the atespace view above:

```text
labels."ate.dev/actor_template_name"="counter"
```

#### 4. Pod-Centric View
To inspect the physical worker pod's aggregate stream and see all co-located actors multiplexed together (useful for investigating pod-level resource exhaustion or noisy neighbor issues):

```text
resource.labels.pod_name="counter-c995fdf4c-m7d96"
```

---

## 2. Metrics

Agent Substrate emits foundational OpenTelemetry system and server metrics to monitor the overall health and performance of the control plane services. Every metric below is emitted by a service binary over OTLP and is **independent of the deployment** — a Kind dev cluster gets the same instruments as production; only the backend differs (see [Where Telemetry Goes](#4-where-telemetry-goes)).

| Metric | Emitted by | Type | Measures |
|--------|------------|------|----------|
| `rpc.server.call.duration` | ateapi & atelet (gRPC servers, via `otelgrpc`) | histogram | per-method gRPC latency, request rate, and errors (labels `rpc.method`, `rpc.response.status_code`) |
| `ate.actor.crashes` | ateapi | counter | Number of times actors transitioned to `STATUS_CRASHED` with failure reasons (labels `ate.actor.operation.name`, `ate.failure.reason`, `ate.template.namespace`, `ate.template.name`, `ate.workerpool.name`, `ate.sandbox.class`) |
| `atenet.router.route.duration` | atenet-router | histogram | Substrate E2E — Envoy receiving a request to Envoy forwarding it to the resolved worker, excluding actor compute and the response (labels `ate.template.namespace`, `ate.template.name`, `ate.router.outcome`, `ate.router.resume`) |
| `ate.scheduler.eligible_workers` | ateapi | histogram | number of eligible unassigned workers available during scheduling given the constraint filters (labels `ate.workerpool.namespace`, `ate.workerpool.name`, `ate.sandbox.class`, `ate.scheduling.constraint`) |
| `atelet.snapshot.size` | atelet | histogram | uncompressed size in bytes of each gVisor snapshot image written during checkpoint (labels `file.name`, `ate.template.namespace`, `ate.template.name`) |
| `ate.workerpool.desired_workers` | atecontroller | up/down counter | number of worker pods requested for a WorkerPool, from `spec.replicas` (labels
`ate.workerpool.namespace`, `ate.workerpool.name`) |
| `ate.workerpool.ready_workers` | atecontroller | up/down counter | number of worker pods currently ready for a WorkerPool, from `status.readyReplicas` (labels
`ate.workerpool.namespace`, `ate.workerpool.name`) |
| `ate.workerpool.workers` | ateapi | up/down counter | live worker count per pool, split by state (`idle`/`assigned`) and sandbox class to provide fleet capacity and saturation at a glance |
| `ate.actor.lifecycle.operation.duration` | ateapi | histogram | how long each actor operation (create/resume/suspend/pause/delete) takes and whether it failed (`error.type` present = failure, absent = success); labeled by operation, template, pool, sandbox class, and snapshot kind and scope on resume; already-running resume no-ops are not recorded so the histogram tracks actual activations, not router traffic |
| `ate.scheduler.assignment.duration` | ateapi | histogram | time it takes for an actor to be assigned to a worker, per attempt (version-conflict retries record only the final attempt), with the outcome (`assigned` / `no_free_worker` / `error`) and sandbox class to catch scheduling latency and capacity starvation problems |
| `ate.actor.restore.duration` | atelet | histogram | how long each phase of a restore takes on the worker node, which is where cold-start latency actually goes once ateapi hands off (labels `ate.snapshot.phase`, `ate.snapshot.kind`, `ate.snapshot.scope`, `ate.template.namespace`, `ate.template.name`, `ate.sandbox.class`, plus `ate.failure.reason` on failure) |
| `ate.actor.checkpoint.duration` | atelet | histogram | the same phase breakdown for writing a snapshot, so a slow suspend can be attributed to ateom or to the upload (same labels as the restore histogram) |
| `ate.imagecache.requests` | atelet | counter | image lookups in the node-local image cache, by outcome (`ate.imagecache.outcome`), with `error.type` on the `error` outcome. A miss pays for the pull and the unpack, so the hit ratio per node is a leading indicator of resume latency |

The table lists the OpenTelemetry instrument names. How a name appears in a query depends on the backend (Cloud Monitoring (GMP) / Kind collector).

For `ate.workerpool.desired_workers` and `ate.workerpool.ready_workers`:
* **Supply-Side Saturation Golden Signal**: Measures whether commanded capacity was delivered. Dedicated instruments match Kubernetes semantic conventions (`k8s.deployment.desired_pods` / `k8s.deployment.available_pods`) because desired + ready is not a disjoint sum.
* **Autoscaling Control Loop & Anti-Windup**: `desired - ready > 0` sustained beyond a few minutes indicates undelivered capacity due to node pool exhaustion, quota limits, or stuck worker pods, serving as anti-windup input for demand-reactive capacity scaling.

For `atenet.router.route.duration`:
* `ate.router.outcome` categorizes the route attempt result: `ok`, `cancelled`, `timeout`, `no_capacity`, `lock_conflict`, `not_found`, `unavailable`, `rate_limited`, or `resume_error`.
* `ate.router.resume` indicates the singleflight execution state of actor resumption: `none` (actor already running), `triggered` (initiated cold activation), or `joined` (parked on in-flight activation).

For `ate.scheduler.eligible_workers`:
* `ate.scheduling.constraint` categorizes the scheduling request constraint type: `none` (unconstrained), `selector` (actor or template label selectors specified), or `required_nodes` (pinned to specific node VMs).

For `ate.imagecache.requests`:
* `ate.imagecache.outcome` is `hit` when the node holds a complete image record — every layer directory the record names is present — and `miss` when the lookup must pull. A failed lookup is neither: `error` is a failed lookup whatever the cause, and `cancelled` or `timeout` is the caller giving up, as on `ate.router.outcome`. So the hit ratio is `hit / (hit + miss)`, with failures and abandoned lookups out of the denominator.
* `error.type` is present only on the `error` outcome, and carries the registry's own HTTP status for its rejection, from a fixed set: `401`, `403`, `404`, `429`, `500`, `502`, `503`, `504`. The set is an allow-list because the registry client reports whatever the remote returned. Each other status, and each failure that carries no status, reports `_OTHER`.

The three snapshot labels are orthogonal and mean the same thing on every histogram that carries them:
* `ate.snapshot.kind`: which snapshot the operation reads or writes. `local` (node-local, written by a pause), `latest` (the actor's own durable snapshot), `golden` (the template's image), or `boot` (from scratch, so it never appears on the atelet histograms).
* `ate.snapshot.scope`: what content it covers. `full`, `data`, or `data_on_golden` (restore-only: the actor's data combined with the golden guest state).
* `ate.snapshot.phase`: which step was timed. `volume_mount`, `manifest_fetch`, `sandbox_assets`, `download`, `oci_unpack`, `ateom_restore` on restore; `sandbox_assets`, `ateom_checkpoint`, `persist` on checkpoint; `total` on both.

**Phases overlap and do not sum to `total`.** The download runs concurrently with the asset fetch and OCI unpack, so each is an independent observation; use `total` as the denominator. A phase that never started is absent rather than zero.

On a failure, `ate.failure.reason` marks the phase that died and the `total`, and nothing else, so `ate.actor.restore.duration{ate.snapshot.phase="download", ate.failure.reason!=""}` says how often the download is what breaks and why, while the phases that succeeded stay queryable as successes. The atelet histograms classify with substrate's own reason taxonomy (the same one `ate.actor.crashes` uses) rather than `error.type`, because these handlers return wrapped domain errors and the gRPC status is only assigned after the handler returns, so a status code would read `Unknown` for nearly every real failure. Infrastructure failures that carry no reason report `UNKNOWN`.

The `ate.*` control-plane metric labels are either fixed value sets (operation, outcome, state, class, kind, scope, phase) or scoped to the deployment catalog (template and pool names are operator-created, never derived from request payloads), and the label set varies per operation: resume carries the most dimensions, delete only the operation and error type. `ate.sandbox.class` is derived from the template (each template has exactly one class), so it adds no extra series next to the template labels; it exists so dashboards can aggregate by class without enumerating template names. High-cardinality actor identity (name/uid/atespace) stays off metrics entirely and lives on logs and traces instead.

### Bridged controller-runtime metrics (atecontroller)

atecontroller bridges controller-runtime's private Prometheus registry, which the manager serves on an unscraped `:8080`, onto its OTLP reader. So `controller_runtime_*`, `workqueue_*`, `rest_client_*`, `leader_election_*`, `go_*`, and `process_*` reach the collector too, keeping their Prometheus names because they are upstream instruments and renaming them would break existing controller-runtime dashboards.

These can be used to answer whether the controller is keeping up, e.g. rising `workqueue_depth` or `workqueue_queue_duration_seconds` means reconciles are falling behind, and `controller_runtime_reconcile_errors_total` says which controller.

Note that controller-runtime enables native histograms on `controller_runtime_reconcile_time_seconds`, `workqueue_queue_duration_seconds`, and `workqueue_work_duration_seconds`, so those three arrive as OTLP exponential histograms rather than fixed-bucket ones.

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

Agent Substrate samples traces by default. Each component roots parentless requests at a per-component ratio (10% on the control plane components, 1% at the atenet router), overridable per component through the standard `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` environment variables. Every component uses a parent based sampler, so a client can also force a request to be traced end to end (e.g. via the `--trace` flag). Agent Substrate leverages OpenTelemetry (OTel) for context propagation across the call stack. Each traced request generates a unique trace hash/ID, which you can use to inspect the detailed request lifecycle and span hierarchy inside Google Cloud Trace or Jaeger. See the per-component defaults table in [Tracing Best Practices](dev/best-practices/tracing.md).

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
   kubectl ate get actor -A --trace
   # or
   kubectl ate suspend actor <actor-name> -a <atespace> --trace
   ```
   The kind overlay pins `ateapi` to `parentbased_always_on`, so API calls show up even without `--trace`; the flag additionally prints the trace ID and forces sampling on every hop.

4. **Search and Inspect**: Copy the printed Trace ID from the CLI output and paste it into the Jaeger search box (top right), or select `ateapi`, `atelet`, or `ateom-gvisor` under the **Service** dropdown and click **Find Traces** to inspect detailed call stacks, DB transactions, state updates, and worker pod handoffs.

> ateom carries no manual spans — its only instrumentation is the `otelgrpc` interceptor on the gRPC surface `atelet` calls. So it produces a span for an actor lifecycle operation (`suspend`, `resume`) and nothing at all for a read like `kubectl ate get actor`. Its sampler is parent based, so a lifecycle command is traced end to end into ateom whenever `ateapi` roots a sampled trace, which the kind overlay makes unconditional; the per-component ratio never enters into it. To check whether ateom exported its spans through the [OTLP relay](#the-ateom-otlp-relay) rather than falling back to direct network egress, inspect the span's resource attributes: `ate.otlp.relay` will be set to `"relay"` (instead of `"direct"`).

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

> In Kind, `ateapi`, `atelet`, `ate-controller`, and `atenet-router` are pointed at the in-cluster collector, and the controller propagates the endpoint to the ateom worker pods it creates, so all component telemetry lands locally.
>
> Every component reads that endpoint from the shared `ate-otel-config` ConfigMap ([`manifests/ate-install/ate-otel-config.yaml`](../manifests/ate-install/ate-otel-config.yaml), with a Kind replacement of the same name under [`manifests/ate-install/kind/`](../manifests/ate-install/kind/ate-otel-config.yaml)). Editing it does not restart the pods that consume it — follow a change with `kubectl rollout restart`.
>
> ateom workers don't read the ConfigMap at all — `ate-controller` copies the value into each worker pod at creation. A new endpoint reaches them only once the controller itself restarts, and that restart then rolls every WorkerPool Deployment, replacing the running workers along with the actors on them.

### The ateom OTLP relay

ateom is the one component that does not talk to the collector directly. It exports over a unix socket at `/var/lib/ateom-gvisor/atelet-otlp.sock`, which `atelet` serves and forwards to the collector on the node's network ([`internal/otlprelay`](../internal/otlprelay)):

```
ateom ──OTLP/gRPC over unix socket──► atelet relay ──OTLP/gRPC──► collector
```

The socket sits in the `BasePath` hostPath already mounted into both, so nothing new is mounted. `atelet` is a DaemonSet, so every ateom on a node shares one relay, and the many per-pod collector connections collapse into one per node. Four things motivate it: the worker pod runs untrusted agent code and will not need egress to the collector once direct fallback is phased out; the connection count drops; ateom's own telemetry stays clear of the transparent egress redirect it installs for the actor; and `atelet` outlives the worker pod, so spans still queued at teardown are not lost with it.

The relay is best-effort. If the socket is absent when ateom starts — `atelet` not up yet, `--otlp-relay-socket=""`, or no collector configured for the relay to forward to — ateom logs it and exports directly to `OTEL_EXPORTER_OTLP_ENDPOINT` as before. That fallback is decided once at startup, not per export, and is stamped on telemetry as the `ate.otlp.relay` resource attribute (`relay` vs `direct`).

> **Note on Network Egress Lockdown:** Complete network policy lockdown of worker pod egress to the collector is planned as a Phase 2 milestone once the relay path is fully proven and direct fallback is deprecated. While the fallback path remains active, worker pods retain network egress to the collector and `ate-controller` continues to inject `OTEL_EXPORTER_OTLP_ENDPOINT`.

For verified ateom sources, the relay forwards each request verbatim rather than decoding and re-exporting, which is what keeps every ateom its own service in Jaeger/GCP Trace instead of being absorbed into `atelet`'s. `ate-controller` injects `k8s.pod.name`, `k8s.namespace.name`, `k8s.pod.uid`, and `service.instance.id` directly into `OTEL_RESOURCE_ATTRIBUTES` via the Kubernetes Downward API; because the relay preserves resources verbatim, Kubernetes attributes remain intact even though the TCP connection to the collector originates from `atelet` rather than the worker pod IP (bypassing reliance on collector-side IP-based `k8sattributes` enrichment).

Verbatim forwarding is restricted to known ateom sources and refuses anything else with `PermissionDenied`. Actor telemetry is what that excludes: actors share a hostname (`runsc`) and an interior IP, so their series merge unless identity is injected from outside the actor ([#761](https://github.com/agent-substrate/substrate/issues/761)) — a rewrite, which will be implemented as an explicit rewriting path alongside this forwarder.

---

## 5. Dashboards

> **GCP-specific.** These are **Google Cloud Monitoring** dashboards; they apply only to a GKE / Google Cloud deployment. There is no dashboard support on Kind — use the Prometheus UI in [Metrics](#2-metrics) for local development.

Dashboard definitions live in [`tools/setup-gcp/dashboards/`](../tools/setup-gcp/dashboards/) (see its README for the per-dashboard breakdown). They are created and updated **as part of GCP setup**: `tools/setup-gcp` applies each dashboard idempotently (matched and updated by display name), so re-running is safe.

```sh
go run ./tools/setup-gcp create dashboards   # also part of: bootstrap
```
