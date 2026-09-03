<!--
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Autoscaling WorkerPools with an HPA

This describes how to autoscale a `WorkerPool` on how many of its workers are
currently assigned, using a Kubernetes HorizontalPodAutoscaler (HPA) fed by the
`ate_workerpool_workers` metric through prometheus-adapter.

## Prerequisites

- A local kind cluster with Agent Substrate installed (`./hack/install-ate-kind.sh --deploy-ate-system`).
  Note: this demo is currently only supported on kind.
- `ko` installed for building images.
- A GCS bucket for storing snapshots (configured via `BUCKET_NAME` env var).

## Architecture (kind)

```
ate-api-server :9090/metrics          (ate_workerpool_workers, OTel -> Prometheus gauge)
        │  scrape (prometheus.io/scrape annotation, 15s)
        ▼
Prometheus                            manifests/ate-install/monitoring/prometheus.yaml
        │  PromQL
        ▼
prometheus-adapter                    demos/autoscaled-workerpool/prometheus-adapter.yaml
        │  external.metrics.k8s.io
        ▼
HorizontalPodAutoscaler
        │  /scale  (writes WorkerPool.spec.replicas)
        ▼
atecontroller  ──►  Deployment.spec.replicas  ──►  worker pods
```
## HPA Configuration (External + AverageValue)

The example HPA uses an **External** metric with target type **AverageValue**:

```
desiredReplicas = ceil( metricValue / target.averageValue )
```

where `metricValue = max(ate_workerpool_workers{namespace=<ns>, name=<pool>, state=assigned})`:
the pool's assigned-worker count. WorkerPool names are only unique within a
namespace, so the selector must pin both.

`averageValue` is the target **assigned-workers-per-replica**:

| `averageValue` | Meaning                                     | Example (`assigned=7`) |
| -------------- | ------------------------------------------- | ---------------------- |
| `"0.7"` (700m) | ~70% assigned / 30% idle headroom (default) | `ceil(7/0.7) = 10`     |
| `"1"`          | pack to 100%, no idle headroom              | `ceil(7/1) = 7`        |
| `"0.5"` (500m) | lots of headroom, ~2× replicas              | `ceil(7/0.5) = 14`     |

Lower `averageValue` → more idle headroom → more replicas.

### Scale-up/scale-down behavior

The example HPAs set an aggressive `scaleUp` (no stabilization window,
`selectPolicy: Max`, `Percent 100` / `Pods 10` steps) so a burst is served in a
**batch** rather than creeping up one worker at a time, and a slow `scaleDown`
(300s stabilization) to avoid flapping. `behavior` only sets the _rate of change_;
the HPA still scales to what the metric dictates.

## How to Run on Agent Substrate

### 1. Build and Deploy

```sh
# Local dev (kind)
./hack/install-ate-kind.sh --deploy-demo-autoscaled-workerpool
```

This command will:

- Create the `ate-demo-autoscaled-workerpool` namespace and one `WorkerPool`
  (`counter`, starting with 5 replicas).
- Create the `ate-demo-autoscaled-workerpool` atespace and the `counter` actor
  template in it (`autoscaled-workerpool-template.yaml.tmpl`, applied with
  `kubectl ate create actor-template`), waiting until the pool is rolled out
  and the template's golden snapshot is built.
- Deploy `prometheus-adapter` into `ate-demo-autoscaled-workerpool` to serve `ate_workerpool_workers` on `external.metrics.k8s.io`, and one `HorizontalPodAutoscaler` (`counter`).

### 2. Verify Monitoring Stack & External Metric

Confirm that `prometheus-adapter` is serving the external metric:

```sh
# 1. Adapter is serving the External Metrics API
kubectl get apiservice v1beta1.external.metrics.k8s.io          # Available=True

# 2. The external metric resolves for the counter pool
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/ate-demo-autoscaled-workerpool/ate_workerpool_workers?labelSelector=ate_worker_state%3Dassigned,ate_workerpool_namespace%3Date-demo-autoscaled-workerpool,ate_workerpool_name%3Dcounter"
```

## How to Use

We can trigger autoscaling by spawning multiple actors and sending traffic to assign workers in the pool. The actors go in the demo's atespace (`ate-demo-autoscaled-workerpool`) — `--template-ref` names the template, resolved in the actor's atespace:

### 1. Spawn load actors

```sh
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

# Create 15 actors to generate load
for i in {001..015}; do
  kubectl ate create actor c$i -a ate-demo-autoscaled-workerpool --template-ref counter
done
```

### 2. Port-forward the atenet router and send traffic

```sh
kubectl port-forward -n ate-system svc/atenet-router 8000:80 &
```

In a separate terminal, send requests in a retry loop across all hosts to activate the actors and keep them active while the pool scales up:

```sh
for attempt in {1..10}; do
  for i in {001..015}; do
    curl -s -H "Host: c$i.ate-demo-autoscaled-workerpool.actors.resources.substrate.ate.dev" http://localhost:8000 >/dev/null
  done
  sleep 2
done
```

### 3. Watch the HPA scale up

As the assigned worker count increases, watch the HPA scale up the pool's replicas:

```sh
kubectl -n ate-demo-autoscaled-workerpool get hpa counter -w
kubectl -n ate-demo-autoscaled-workerpool get workerpool counter -w
```

### 4. Trigger scale-down

Suspend the actors to drop the assigned worker count. After the 300s stabilization window, the HPA will scale down the pool:

```sh
for i in {001..015}; do
  kubectl ate suspend actor c$i -a ate-demo-autoscaled-workerpool
done
```

## How to Uninstall

Remove the demo — this deletes the actors, the template, the atespace, and
then the pool and its namespace:

```sh
./hack/install-ate-kind.sh --delete-demo-autoscaled-workerpool
```
