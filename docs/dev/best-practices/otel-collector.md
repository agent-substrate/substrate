# OpenTelemetry Collector Best Practices

This document covers how to deploy an OpenTelemetry Collector for Agent
Substrate in a Kubernetes cluster, and how to point Substrate's components at
it.

For how to *instrument* a Substrate service, see
[Tracing Best Practices](tracing.md). For what Substrate emits and where it
shows up, see [Actor Observability](../../observability.md).

## What Is Available

| | Topology | Who operates it |
|---|---|---|
| [GKE Managed OpenTelemetry](#gke-managed-opentelemetry-gke-only) | Deployment behind a ClusterIP Service | GKE |
| [Self-managed DaemonSet](#self-managed-daemonset) | One collector per node | You |
| [kind](#local-development-kind) | Single-replica Deployment plus Jaeger | Bundled with `install-ate-kind.sh` |

This guide does not recommend one over another — that depends on your cluster,
your telemetry volume, and who is on call for the collector. It documents what
each option is and what follows from it.

Whichever you run, it must terminate OTLP/gRPC on port **4317**. Substrate
cannot use the HTTP endpoint on 4318 — see
[Constraints](#constraints-you-cannot-configure-around).

## GKE Managed OpenTelemetry (GKE only)

GKE can run and upgrade the collector for you. See
[Managed OpenTelemetry for GKE](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/managed-otel-gke)
for the authoritative documentation.

Enable it on an existing cluster:

```bash
gcloud container clusters update CLUSTER_NAME \
    --location=LOCATION \
    --managed-otel-scope=COLLECTION_AND_INSTRUMENTATION_COMPONENTS
```

(`--managed-otel-scope=NONE` disables it again.)

You get a collector reachable at:

```
opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317
```

When running on GKE, this is the address every Substrate manifest already
defaults to, so no further configuration is needed. Telemetry is forwarded to
Google Cloud Observability and surfaces in Cloud Trace, Cloud Monitoring, and
Cloud Logging.

**The feature is in Preview.** It is subject to Pre-GA Offerings Terms, and
it is GKE-only — nothing about it transfers to EKS, AKS, or on-prem.

### Topology

Managed OTel runs as an HPA-scaled Deployment behind one ClusterIP Service — a
gateway tier that every pod in the cluster sends to over the network. On a GKE
1.36 cluster with the feature enabled it was 4 replicas, HPA bounds 1–5,
targeting 80% CPU and memory.

## Self-Managed DaemonSet

One collector per node, receiving **traces, metrics, and logs** over OTLP.
Pods reach the collector on their own node through a Service with
`internalTrafficPolicy: Local`, which keeps a stable DNS name while never
routing off-node.

All three signals share the single OTLP receiver on port 4317 — there is no
separate port or receiver per signal, only a separate pipeline.

If you want the agent topology, this manifest provides it. Its configuration
mirrors the practices in GKE's own managed collector — per-signal batching, a
memory limiter, layered pod association, gRPC keepalive, and a self-metrics
pipeline — adapted for a node-local agent and a backend-agnostic exporter.

Save as `otel-collector-daemonset.yaml` and `kubectl apply -f` it.

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: otel-system
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: opentelemetry-collector
  namespace: otel-system
---
# The k8s_attributes processor reads pod metadata to tag incoming telemetry.
# No apps/replicasets grant: k8s.deployment.name is derived from the pod's
# ownerReference, so no ReplicaSet informer is started. Add that grant if you
# extract k8s.deployment.uid or any `from: deployment` label/annotation.
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: opentelemetry-collector
rules:
- apiGroups: [""]
  resources: ["pods", "namespaces"]
  verbs: ["get", "watch", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: opentelemetry-collector
subjects:
- kind: ServiceAccount
  name: opentelemetry-collector
  namespace: otel-system
roleRef:
  kind: ClusterRole
  name: opentelemetry-collector
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: opentelemetry-collector-config
  namespace: otel-system
data:
  otel-collector-config.yaml: |
    extensions:
      health_check:
        endpoint: ${env:K8S_POD_IP}:13133

    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: ${env:K8S_POD_IP}:4317
            keepalive:
              server_parameters:
                # Reap idle connections from actor pods that have exited.
                max_connection_idle: 5m
                time: 5m
                timeout: 1m
                # Only matters if you later put a multi-replica gateway
                # behind a Service: gRPC balances at connection time, so
                # recycling connections is what redistributes load.
                max_connection_age: 10m
                max_connection_age_grace: 1m
          http:
            endpoint: ${env:K8S_POD_IP}:4318

      # Self-observability: the collector's own metrics are looped back in
      # through a dedicated receiver so they flow out via the same exporter
      # as everything else. See service::telemetry below.
      otlp/self-metrics:
        protocols:
          grpc:
            endpoint: ${env:K8S_POD_IP}:14317

    processors:
      # Shed load before the container hits its memory limit. Without this a
      # burst becomes an OOMKill, which drops everything queued.
      memory_limiter:
        check_interval: 1s
        limit_percentage: 80
        spike_limit_percentage: 10

      k8s_attributes:
        auth_type: serviceAccount
        passthrough: false
        extract:
          # k8s.deployment.name must be listed for the attribute to be set at
          # all; it is derived from the pod's ReplicaSet ownerReference by
          # trimming the pod-template-hash, which needs no ReplicaSet informer.
          metadata:
          - k8s.namespace.name
          - k8s.deployment.name
          - k8s.statefulset.name
          - k8s.daemonset.name
          - k8s.cronjob.name
          - k8s.job.name
          - k8s.replicaset.name
          - k8s.node.name
          - k8s.pod.name
          - k8s.pod.uid
          - k8s.pod.start_time
        filter:
          # Agent mode: only watch pods on this collector's own node. Without
          # this, every one of the N collectors caches metadata for the whole
          # cluster and watches every pod -- N times the API server load and
          # N times the memory. KUBE_NODE_NAME comes from the downward API
          # below.
          node_from_env_var: KUBE_NODE_NAME
          # Only running pods can be producing telemetry; skipping the rest
          # keeps the informer cache smaller still.
          fields:
          - key: status.phase
            value: Running
            op: equals
        # Layered fallback. Connection-based association is reliable here
        # precisely because the collector is node-local: the peer IP is
        # always a pod on this node. The resource-attribute sources come
        # first so a client that already knows its own identity wins.
        pod_association:
        - sources:
          - from: resource_attribute
            name: k8s.pod.ip
        - sources:
          - from: resource_attribute
            name: k8s.pod.uid
        - sources:
          - from: connection

      # Per-signal batching. Traces are small and numerous, so they batch
      # large; logs and metrics batch small to bound per-request latency and
      # payload size.
      batch/traces:
        send_batch_size: 8192
        send_batch_max_size: 8192
        timeout: 5s
      batch/metrics:
        send_batch_size: 200
        send_batch_max_size: 200
        timeout: 5s
      batch/logs:
        send_batch_size: 200
        send_batch_max_size: 200
        timeout: 5s

      # Keep only the self-metrics that answer "is the collector coping?",
      # and drop the rest before they reach your backend.
      # Keep only the self-metrics that answer "is the collector coping?".
      # filter drops what its conditions match, so the condition is negated:
      # anything NOT in the allowlist is dropped.
      filter/self-metrics:
        error_mode: ignore
        metric_conditions:
        - >-
          not IsMatch(metric.name,
          "^otelcol_(receiver_(accepted|refused)_(spans|metric_points|log_records)|exporter_(sent|send_failed)_(spans|metric_points|log_records)|exporter_queue_(size|capacity)|exporter_enqueue_failed_spans|processor_(incoming|outgoing)_items)$")

    exporters:
      # Replace with your backend. See "Choosing an exporter" below.
      debug:
        verbosity: normal

    service:
      extensions: [health_check]
      pipelines:
        traces:
          receivers: [otlp]
          processors: [memory_limiter, k8s_attributes, batch/traces]
          exporters: [debug]
        metrics:
          receivers: [otlp]
          processors: [memory_limiter, k8s_attributes, batch/metrics]
          exporters: [debug]
        # Logs arrive over the same OTLP receiver on 4317 -- one port serves
        # all three signals. This pipeline is for workloads instrumented
        # with the OTel logs SDK; see "A Note on Logs" below.
        logs:
          receivers: [otlp]
          processors: [memory_limiter, k8s_attributes, batch/logs]
          exporters: [debug]
        metrics/self:
          receivers: [otlp/self-metrics]
          processors: [filter/self-metrics, k8s_attributes, batch/metrics]
          exporters: [debug]
      telemetry:
        logs:
          encoding: json
        metrics:
          level: detailed
          readers:
          - periodic:
              interval: 60000
              exporter:
                otlp:
                  protocol: grpc
                  endpoint: http://${env:K8S_POD_IP}:14317
                  insecure: true
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: opentelemetry-collector
  namespace: otel-system
  labels:
    app: opentelemetry-collector
spec:
  selector:
    matchLabels:
      app: opentelemetry-collector
  template:
    metadata:
      labels:
        app: opentelemetry-collector
    spec:
      serviceAccountName: opentelemetry-collector
      # A node with no collector pod silently blackholes telemetry under
      # internalTrafficPolicy: Local, so tolerate everything. Any tainted pool
      # that runs Substrate workloads -- a dedicated sandbox pool, for
      # instance -- would otherwise lose its actor and ateom spans.
      tolerations:
      - operator: Exists
      containers:
      - name: otel-collector
        image: otel/opentelemetry-collector-contrib:0.153.0@sha256:93aad750175cbf1a973ae1c5886c3371f4d800f61be25cdd26870b8441ffe9fa
        args:
        - --config=/conf/otel-collector-config.yaml
        env:
        - name: K8S_POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        # Scopes k8s_attributes to this node -- see filter.node_from_env_var.
        - name: KUBE_NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        # memory_limiter's limit_percentage is read against the container's
        # cgroup limit below. GOMEMLIMIT makes the Go runtime collect more
        # aggressively before that ceiling, rather than after.
        - name: GOMEMLIMIT
          value: 400MiB
        ports:
        - name: otlp-grpc
          containerPort: 4317
        - name: otlp-http
          containerPort: 4318
        livenessProbe:
          httpGet:
            path: /
            port: 13133
        readinessProbe:
          httpGet:
            path: /
            port: 13133
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            memory: 512Mi
        volumeMounts:
        - name: config
          mountPath: /conf
      volumes:
      - name: config
        configMap:
          name: opentelemetry-collector-config
          items:
          - key: otel-collector-config.yaml
            path: otel-collector-config.yaml
---
apiVersion: v1
kind: Service
metadata:
  name: opentelemetry-collector
  namespace: otel-system
spec:
  type: ClusterIP
  # Route only to the collector on the caller's own node. This is what makes
  # the DaemonSet an agent tier rather than a load-balanced gateway.
  internalTrafficPolicy: Local
  selector:
    app: opentelemetry-collector
  ports:
  - name: otlp-grpc
    port: 4317
    targetPort: 4317
  - name: otlp-http
    port: 4318
    targetPort: 4318
```

Then point Substrate at it:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://opentelemetry-collector.otel-system.svc:4317
```

### Choosing an Exporter

The manifest ships with the `debug` exporter so you can verify delivery
immediately, but it only writes to the collector's own stdout. Replace it
with a real backend:

- **Google Cloud** — `googlecloud` (or `otlp` to `telemetry.googleapis.com:443`
  with the `googleclientauth` extension, which is what managed OTel itself
  does).
- **Jaeger / Tempo / any OTLP backend** — `otlp` pointed at the backend's
  gRPC endpoint.
- **Prometheus** — the `prometheus` exporter, scraped from the collector.

### Verifying Delivery

Send a request through the router, then check the collector:

```bash
kubectl logs -n otel-system daemonset/opentelemetry-collector | grep -A5 ResourceSpans
```

You should see `service.name` values matching Substrate's components —
`ateapi`, `atelet`, `atenet-router-envoy`, and any instrumented actors.

If you have a workload exporting OTLP logs, the same command shows
`ResourceLog` blocks alongside the spans. Records should carry
`k8s.pod.name` and `k8s.namespace.name`; if they do not, `k8s_attributes`
failed to associate them — check that the ClusterRole is bound.

## Pointing Substrate at Your Collector

Every Substrate component reads the standard
`OTEL_EXPORTER_OTLP_ENDPOINT` environment variable. The endpoint appears in
these manifests:

| File | Component |
|---|---|
| `manifests/ate-install/ate-api-server.yaml` | ateapi |
| `manifests/ate-install/ate-controller.yaml` | ate-controller |
| `manifests/ate-install/atelet.yaml` | atelet |
| `manifests/ate-install/atenet-router.yaml` | atenet-router (and, indirectly, its Envoy) |
| `benchmarking/locust/manifests/locust.yaml` | load generators |

Setting the variable is sufficient for every component. Two also expose a flag
that defaults to it, so you should not normally need either:
`ate-controller`'s `--otel-exporter-otlp-endpoint`, which it also propagates to
the `ateom` worker pods it creates, and `atenet-router`'s
`--otlp-collector-address`, which is what its Envoy is given over xDS.

Rather than editing the base manifests, prefer a kustomize overlay that
patches the variable — see `manifests/ate-install/kind/kustomization.yaml`
for a worked example.

### A Note on Logs

The collector configs here include a logs pipeline, but **Substrate does not
currently export logs over OTLP.** `serverboot.InitLogger` writes structured
JSON to stdout, and `atelet` wraps actor container output with the
`ate.dev/*` metadata labels described in
[Actor Observability](../../observability.md) — also on stdout. There is no
`LoggerProvider` or OTLP log exporter in `internal/serverboot`, alongside
`InitTracing` and `InitMetrics`.

Those logs are collected by whatever agent already reads container stdout on
your nodes — Cloud Logging's agent on GKE, or your own. The collector is not
in that path.

The logs pipeline is therefore there for **your** workloads: actors or
services instrumented with the OpenTelemetry logs SDK that push OTLP log
records to the same endpoint. It costs nothing to leave configured, and it
means an actor that adopts OTLP logging needs no collector change.

## Constraints You Cannot Configure Around

These are properties of Substrate's current exporter setup
(`internal/serverboot/serverboot.go`), not of your collector. Tracked in
[#563](https://github.com/agent-substrate/substrate/issues/563).

**TLS is not supported.** The exporters are constructed with
`otlptracegrpc.WithInsecure()`, which overrides scheme inference, so setting
`OTEL_EXPORTER_OTLP_ENDPOINT=https://…` does **not** produce a TLS
connection — it silently stays plaintext. Keep the collector in-cluster and
let it own the authenticated hop to your backend.

TODO: align this with the pod-certificate mTLS the other in-cluster hops
already use.

**The protocol is fixed to gRPC.** `otlptracegrpc` and `otlpmetricgrpc` are
compiled in, so `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` is ignored. Your
collector must expose an OTLP **gRPC** receiver, and the endpoint must name
port **4317**, not 4318 — including on GKE managed OTel, whose documentation
advertises the 4318 HTTP endpoint.

**`atenet-router` disables Envoy-side tracing on an endpoint Envoy cannot
use.** Envoy's tracer cluster is plaintext h2c, so an `https://` endpoint is
neither honored nor silently downgraded: the router logs a warning, turns
Envoy-side tracing off, and starts normally. Its own spans are unaffected.
Taking the xDS control plane down for every ingress Envoy over a tracing
endpoint that works fine for the router's own exporter would be the larger
failure.

## Local Development (kind)

`hack/install-ate-kind.sh` installs a collector plus a Jaeger all-in-one into
the `otel-system` namespace — see
`manifests/ate-install/kind/otel-collector.yaml`. It is a single-replica
Deployment, which is appropriate for a one-node kind cluster. The kind
overlay patches every component's `OTEL_EXPORTER_OTLP_ENDPOINT` to point at
it.

View traces:

```bash
kubectl port-forward -n otel-system svc/jaeger 16686:16686
# then open http://localhost:16686
```

## Sizing and What to Watch

The collector reports on itself. The DaemonSet config above routes its
self-metrics through a dedicated `metrics/self` pipeline so they reach the
same backend as everything else; managed OTel does the equivalent internally.
Regardless of topology, these are the ones that tell you it is struggling:

| Metric | Meaning |
|---|---|
| `otelcol_receiver_refused_spans` | Collector rejected data — it is saturated |
| `otelcol_exporter_send_failed_spans` | Backend rejected or was unreachable |
| `otelcol_exporter_queue_size` vs `_capacity` | Queue filling — shedding is imminent |
| `otelcol_processor_incoming_items` vs `_outgoing_items` | Gap means a processor is dropping data |

Substrate's telemetry has two independent rate drivers, and they scale on
different axes:

- **Metrics** scale with **fleet size** — roughly
  `(2 + nodes + actors) × instruments × cardinality` per minute, pushed on a
  60s unjittered interval.
- **Traces** scale with **request rate**. `ateapi` roots a trace for every
  unparented API call; downstream components are `ParentBased(NeverSample)`,
  so one sampled request fans out into spans across the whole call chain.

A cluster with many idle actors is metrics-bound; a cluster under load is
trace-bound. Size for whichever dominates your workload, and use the
trace sampling probability as the first knob when the collector saturates.
