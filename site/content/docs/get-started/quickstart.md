---
title: "Quickstart & Installation"
linkTitle: "Quickstart"
weight: 2
aliases:
  - "/docs/getting-started/"
description: >
  Deploy Agent Substrate on a local Kind cluster or Google Kubernetes Engine (GKE) and invoke your first stateful actor.
---

Choose your target environment below to deploy the Agent Substrate control plane and the stateful **Counter** sample application.

> [!NOTE]
> **Prerequisites:** Ensure [Go](https://go.dev/doc/install) and [`kubectl`](https://kubernetes.io/docs/tasks/tools/) are installed. Local setup also requires [Docker](https://www.docker.com/) (`kind` is managed automatically via Go); cloud setup requires [`gcloud`](https://cloud.google.com/sdk/docs/install).

## 1. Deploy Agent Substrate & Create an Actor

{{< tabpane text=true >}}
{{% tab header="Kind (Local Development)" %}}

Run the following commands from the repository root to provision a local `kind` cluster, install Agent Substrate, and create a `counter` actor:

```bash
# 1. Create a local Kind cluster and image registry
hack/create-kind-cluster.sh

# 2. Install Agent Substrate, PostgreSQL, and snapshot storage (rustfs)
hack/install-ate-kind.sh --deploy-ate-system

# 3. Deploy the Counter sample application
hack/install-ate-kind.sh --deploy-demo-counter

# 4. Install the kubectl-ate CLI plugin and create a stateful actor
go install ./cmd/kubectl-ate
kubectl ate create actor my-counter-1 -a ate-demo-counter --template counter

# 5. Port-forward the Substrate router to localhost:8000
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

**Teardown:** When finished, remove the local cluster and registry with:

```bash
./hack/delete-kind-cluster.sh
```

{{% /tab %}}
{{% tab header="GKE (Cloud Deployment)" %}}

Configure your GCP project environment, bootstrap the cluster and snapshot bucket, and deploy Agent Substrate:

```bash
# 1. Configure and load your GCP environment variables
cp hack/ate-dev-env.sh.example .ate-dev-env.sh
# Edit .ate-dev-env.sh to set PROJECT_ID, then source it:
source .ate-dev-env.sh

# 2. Authenticate application-default credentials
gcloud auth application-default login --project="${PROJECT_ID}"

# 3. Provision GKE cluster, GCS snapshot bucket, and Workload Identity IAM bindings
go run ./tools/setup-gcp bootstrap

# 4. Deploy Agent Substrate and the Counter sample application
./hack/install-ate.sh --deploy-ate-system
./hack/install-ate.sh --deploy-demo-counter

# 5. Install the kubectl-ate CLI plugin, create an actor, and port-forward the router
go install ./cmd/kubectl-ate
kubectl ate create actor my-counter-1 -a ate-demo-counter --template counter
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

**Teardown:** To delete all provisioned GCP resources in reverse order:

```bash
./hack/teardown.sh --all
```

{{% /tab %}}
{{< /tabpane >}}

## 2. Configuration: Kind vs. GKE

Both environments deploy the same `ActorTemplate` and control-plane components, differing only in how snapshot storage and OpenTelemetry collection are configured:

{{< tabpane text=true >}}
{{% tab header="Kind Configuration (YAML)" %}}

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
# ActorTemplate snapshotConfig rendered for Kind (local bucket)
snapshotConfig:
  onPause: SNAPSHOT_CONTENT_SCOPE_FULL
  onCommit: SNAPSHOT_CONTENT_SCOPE_FULL
  storageLocation: gs://ate-snapshots/ate-demo-counter/
```

{{% /tab %}}
{{% tab header="GKE Configuration (YAML)" %}}

On GKE, snapshots are written to your Google Cloud Storage bucket (`gs://${BUCKET_NAME}`) using Workload Identity, and telemetry exports to GKE Managed OpenTelemetry (`manifests/ate-install/ate-otel-config.yaml`):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ate-otel-config
  namespace: ate-system
data:
  OTEL_EXPORTER_OTLP_ENDPOINT: http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317
---
# ActorTemplate snapshotConfig rendered for GKE (GCS bucket)
snapshotConfig:
  onPause: SNAPSHOT_CONTENT_SCOPE_FULL
  onCommit: SNAPSHOT_CONTENT_SCOPE_FULL
  storageLocation: gs://${PROJECT_ID}-ate-snapshots/ate-demo-counter/
```

{{% /tab %}}
{{< /tabpane >}}

## 3. Verify Your Running Actor

With `kubectl port-forward` running in your first terminal, open a **second terminal** and send an HTTP `POST` request through the Substrate router to wake and increment your actor:

```bash
curl -X POST \
  -H "ate-target-actor: ate-demo-counter/my-counter-1" \
  -i http://localhost:8000/
```

Run the `curl` command several times—each request routes to `my-counter-1` and increments its stateful in-memory counter. When the actor becomes idle, Substrate checkpoints its memory and filesystem state to storage and releases the worker Pod; the next `curl` request automatically resumes the actor in under 500ms with its counter state intact.
