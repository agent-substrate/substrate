---
title: "Quickstart & Installation"
linkTitle: "Quickstart"
weight: 2
aliases:
  - "/docs/getting-started/"
description: >
  Deploy Agent Substrate on a local kind cluster and invoke your first stateful actor.
---

# Quickstart & Installation

Deploy the Agent Substrate control plane and the stateful **Counter** sample application on a local [kind](https://kind.sigs.k8s.io/) cluster.

> [!NOTE]
> **Prerequisites:** Ensure [Go](https://go.dev/doc/install), [`kubectl`](https://kubernetes.io/docs/tasks/tools/), and [Docker](https://www.docker.com/) are installed ([`kind`](https://kind.sigs.k8s.io/) is managed automatically via Go). A Linux host that will run more than about a thousand sandboxes also needs a larger kernel neighbor table (`net.ipv4.neigh.default.gc_thresh1/2/3` = 4096/16384/32768); `hack/create-kind-cluster.sh` warns when the host is at the default of 1024.

## 1. Deploy Agent Substrate & Create an Actor

Run the following commands from the repository root to provision a local `kind` cluster, install Agent Substrate, and create a `counter` actor:

```bash
# 1. Create a local kind cluster and image registry (IPv4; IP_FAMILY=dual|ipv6 overrides)
hack/create-kind-cluster.sh

# 2. Target the local kind cluster in every shell that runs ate-setup
export ATE_INSTALL_KIND=true

# 3. Install Agent Substrate, PostgreSQL, and snapshot storage (rustfs)
go run ./cmd/ate-setup deploy ate-system --credential-provider='{"name":"k8s.io"}' \
  --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/ate-system/sa/atenet-client

# 4. Deploy the Counter sample application
go run ./cmd/ate-setup deploy demo counter

# 5. Install the kubectl-ate CLI plugin and create a stateful actor
go install ./cmd/kubectl-ate
kubectl ate create actor my-counter-1 -a ate-demo-counter --template counter

# 6. Serve a local proxy on port 8000 that sends requests to the actor through the router
kubectl ate proxy actor my-counter-1 -a ate-demo-counter
```

> [!IMPORTANT]
> `{"name":"k8s.io"}` deploys the bundled credential provider with an empty, default-deny namespace policy. Before an actor can have a credential injected, grant its atespace the namespaces whose Secrets it may read; see [Egress Credential Injection](../guides/egress-credential-injection.md#enable-it).

**Teardown:** When finished, remove the local cluster and registry with:

```bash
./hack/delete-kind-cluster.sh
```

## 2. Snapshot & Telemetry Configuration

In `kind`, snapshots are stored in the in-cluster S3-compatible `rustfs` service (`BUCKET_NAME=ate-snapshots`), and telemetry exports to the local OpenTelemetry Collector (`manifests/ate-install/kind/ate-otel-config.yaml`):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ate-otel-config
  namespace: ate-system
data:
  OTEL_EXPORTER_OTLP_ENDPOINT: http://opentelemetry-collector.otel-system.svc:4317
  OTEL_METRIC_EXPORT_INTERVAL: "10000"
  OTEL_METRIC_EXPORT_TIMEOUT: "10000"
  OTEL_LOGS_EXPORTER: otlp
---
# ActorTemplate snapshotConfig rendered for kind (local bucket)
snapshotConfig:
  onPause: SNAPSHOT_CONTENT_SCOPE_FULL
  onCommit: SNAPSHOT_CONTENT_SCOPE_FULL
  storageLocation: gs://ate-snapshots/ate-demo-counter/
```

## 3. Verify Your Running Actor

With `kubectl ate proxy actor` running in your first terminal, open a **second terminal** and send an HTTP `POST` request to wake and increment your actor:

```bash
curl -X POST -i http://localhost:8000/
```

Run the `curl` command several times—each request routes to `my-counter-1` and increments its stateful in-memory counter. When the actor becomes idle, Substrate checkpoints its memory and filesystem state to storage and releases the worker Pod; the next `curl` request automatically resumes the actor in under 500ms with its counter state intact.

> [!TIP]
> **Running on Google Kubernetes Engine (GKE)?** Follow the official [GKE Installation Guide](https://docs.cloud.google.com/kubernetes-engine/ai-ml/install-overview-substrate).
