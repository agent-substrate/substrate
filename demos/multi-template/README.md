# Multi-Template Demo

This demo shows that **two different `ActorTemplate`s running two different binaries
can share a single `WorkerPool` — even when all three live in different namespaces**.

Each `ActorTemplate` gates on the pool via `workerSelector`, a label selector matched
against the pool's labels — pool selection is cluster-wide, not scoped by namespace.

## Prerequisites

- A k8s cluster with Agent Substrate installed (`./hack/install-ate.sh --deploy-ate-system`).
- `ko` installed for building images.
- A GCS bucket for storing snapshots (configured via `BUCKET_NAME` env var).

## How to Run on Agent Substrate

### 1. Build and Deploy

> [!NOTE]
> Do not manually edit `demos/multi-template/multi-template.yaml.tmpl`. The installation
> script automatically injects your `${BUCKET_NAME}` environment variable during deployment.

```bash
./hack/install-ate.sh --deploy-demo-multi-template
```

This command will:
- Build the `counter` and `fspersist` images using `ko`.
- Create 3 namespaces: `ate-demo-multi-template-pool`,
  `ate-demo-multi-template-counter`, and `ate-demo-multi-template-fspersist`.
- Create one `WorkerPool` (`shared-pool`) in `ate-demo-multi-template-pool` and two
  `ActorTemplate`s — `counter` in `ate-demo-multi-template-counter` and `fspersist` in
  `ate-demo-multi-template-fspersist`, both selecting the pool via the same
  `workerSelector` label.
- Wait until both templates are `Ready` (golden snapshots built).

### 2. Create one actor per template

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

# Create two actors from different templates.
kubectl ate create actor c1 --template ate-demo-multi-template-counter/counter
kubectl ate create actor f1 --template ate-demo-multi-template-fspersist/fspersist
```

### 3. Port-forward the atenet router

To interact with the router locally:

```bash
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

## How to Use

When you send an HTTP request through the router, Substrate automatically detects the session, activates (resumes) the actor onto an available worker pod, and proxies the traffic.

```bash
# counter binary
curl -s -H "Host: c1.actors.resources.substrate.ate.dev" http://localhost:8000
# -> hello from: <ip> | preserved memory count: 1

# fspersist binary
curl -s -H "Host: f1.actors.resources.substrate.ate.dev" http://localhost:8000
# -> pod: <ip>
#    --- history ---
#    pod=<ip> | count=0 | time=<timestamp>
```

Confirm both actors landed on workers in the one `shared-pool`:

```bash
kubectl ate get workers
```

The `counter` increments its in-memory count on each request, while `fspersist` prepends
a line to its history file on each request. Suspending and re-requesting an actor
preserves that state across the snapshot/restore cycle:

```bash
kubectl ate suspend actor f1
curl -s -H "Host: f1.actors.resources.substrate.ate.dev" http://localhost:8000  # history persists; count keeps climbing
```

## How to Uninstall

Delete the actors first — namespace teardown does not reclaim actor records or their GCS snapshots:

```bash
# For example:
kubectl ate delete actor c1
kubectl ate delete actor f1
```

Then remove the templates, pool, and namespaces:

```bash
./hack/install-ate.sh --delete-demo-multi-template
```
