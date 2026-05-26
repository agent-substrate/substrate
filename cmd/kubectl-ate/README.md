# `kubectl-ate`

A Kubernetes-native CLI plugin for managing Substrate Actor and Worker lifecycles.

## Running the CLI

There are two ways to run the tool, depending on whether you are developing locally or installing it permanently.

### 1. Install as a native `kubectl` Plugin
You can use `go install` to compile the tool and place the binary directly into your Go bin directory (which should be in your `$PATH`). Because the source folder is named `kubectl-ate`, Kubernetes will automatically recognize the resulting binary!

```bash
go install ./cmd/kubectl-ate
```
You can now run it seamlessly anywhere as a native Kubernetes command: `kubectl ate <command>`.

### 2. Run directly from source (Development)
If you are testing changes to the codebase, you can bypass compilation and run the CLI directly from the source tree:

```bash
go run ./cmd/kubectl-ate <command>
```

## Connection & Auto Port-Forwarding
By default, `kubectl-ate` will automatically read your `~/.kube/config`, discover the `ate-api-server` pods in your cluster, and establish a temporary background port-forward tunnel to execute gRPC calls securely.

If you prefer to route traffic directly (e.g., through a LoadBalancer or when running natively inside a cluster pod), simply provide the `--endpoint` flag to bypass the tunnel.

## Tracing
The CLI supports on-demand tracing using the `--trace` flag. When enabled, the CLI will generate a trace ID and signal to the server that it wants the request to be traced.

**Prerequisites:**

1. The Google Cloud project must have the **Cloud Trace API** enabled. You can enable it using:
```bash
gcloud services enable cloudtrace.googleapis.com --project=PROJECT_ID
```

2. The GKE cluster must have **Managed OpenTelemetry** enabled. If it is not enabled, you can enable it using the following `gcloud` command:

```bash
gcloud beta container clusters update CLUSTER_NAME \
    --project=PROJECT_ID \
    --managed-otel-scope=COLLECTION_AND_INSTRUMENTATION_COMPONENTS \
    --location=LOCATION
```

**Local (kind):**

The kind overlay installed by `hack/install-ate-kind.sh --deploy-ate-system` already provisions an in-cluster OpenTelemetry Collector and a Jaeger all-in-one in the `otel-system` namespace. No additional setup is required.

Port-forward the Jaeger UI and invoke any command with `--trace`:
```bash
kubectl port-forward -n otel-system svc/jaeger 16686:16686 &
kubectl ate get actor my-counter-1 --trace
# open http://localhost:16686 and search for the most recent trace
```

## Global Flags
These flags can be appended to any command:

| Flag | Short | Description | Default |
|---|---|---|---|
| `--kubeconfig` | | Path to your kubeconfig file | `~/.kube/config` |
| `--endpoint` | | Manual gRPC endpoint override (e.g., `localhost:8080`) | |
| `--output` | `-o` | Output format (`table`, `json`, `yaml`) | `table` |
| `--trace` | | Enable on-demand tracing for the request | `false` |

---

## Command Reference & Examples

### Getting Resources
List and inspect the state of actors and workers across the cluster.

```bash
# List all actors in a clean table format
kubectl ate get actors

# Get a specific actor by ID and output as raw YAML
kubectl ate get actor <actor-id> -o yaml

# List all physical workers and see which actors are assigned to them
kubectl ate get workers
```

> **Note:** Actors and workers are not Kubernetes CRDs — they live in the Substrate control plane (valkey/redis), not `etcd`. `kubectl get actor` and `kubectl get worker` will not return anything; only `kubectl ate get …` queries the control plane. `kubectl get actortemplate` and `kubectl get workerpool` *do* work, because those are CRDs.

#### `kubectl ate get actor` output columns

| Column | Meaning |
|---|---|
| `NAMESPACE` | The namespace of the `ActorTemplate` the actor was created from. |
| `TEMPLATE` | The `ActorTemplate` name. |
| `ID` | Actor ID. User-provided for application actors; UUID for the golden actor that each template materialises during `ResumeGoldenActor`. |
| `STATUS` | One of `STATUS_RESUMING`, `STATUS_RUNNING`, `STATUS_SUSPENDING`, `STATUS_SUSPENDED`. |
| `ATEOM POD` | The worker pod (namespace/name) currently hosting the actor. Empty while suspended. |
| `ATEOM IP` | The pod IP of that worker. Empty while suspended. |
| `VERSION` | Monotonic integer that increments on every state transition (resume / suspend / checkpoint). Useful for distinguishing snapshots. |

#### `kubectl ate get worker` output columns

| Column | Meaning |
|---|---|
| `NAMESPACE` | The `WorkerPool` namespace. |
| `POOL` | The `WorkerPool` name. |
| `POD` | The worker pod name. |
| `STATUS` | `FREE` (idle, ready to receive an actor) or `ASSIGNED` (currently hosting an actor). |
| `ASSIGNED ACTOR` | If `STATUS=ASSIGNED`, the actor reference `<namespace>/<template>/<actor-id>`. |

### Actor Lifecycle
Manage the execution state of your workloads. 
*(Note: Actors are identified by a user-provided ID, which must be a valid DNS-1123 label)*

```bash
# Create a new actor deriving from a specific ActorTemplate
kubectl ate create actor my-actor --template=ate-demo-counter/counter-template

# Resume an actor (assigns it to a free worker and restores its state)
kubectl ate resume actor my-actor

# Suspend an actor (snapshots its state to storage and frees the worker)
kubectl ate suspend actor my-actor

# Delete an actor.
kubectl ate delete actor my-actor
```

### Logs

`kubectl ate logs` requires a resource-type subcommand; running `kubectl ate logs <id>` on its own prints help. The only supported resource type is `actors`:

```bash
# Stream logs for an actor (follows by default; aggregated across worker
# reassignments so the same actor is queryable as it teleports between pods).
kubectl ate logs actors my-actor
```

Logs are streamable only while the actor is bound to a worker (i.e., `STATUS_RUNNING`). For history across worker migrations, route through a centralized log backend (Cloud Logging, Loki, etc.); see `docs/observability.md`.

### Administration & Setup
Commands for bootstrapping the Substrate control plane and debugging local environments.

```bash
# Generate a new CA pool and push it directly to a Kubernetes Secret
kubectl ate admin make-ca-pool \
  --name workerpool-ca-certs \
  --secret-namespace ate-system \
  --ca-id "1"

# Generate a new JWT authority pool and push it to a Kubernetes Secret
kubectl ate admin make-jwt-pool \
  --name session-id-jwt-pool \
  --secret-namespace ate-system \
  --key-id "1"

# DANGEROUS: Completely flush all Actor and Worker tracking state from Redis
kubectl ate admin debug-flush-redis
```
