# Multi-Template Demo

This demo shows that **two different actor templates running two different binaries
can share a single `WorkerPool` — even when the templates live in different atespaces**.

Each template gates on the pool via `workerSelector`, a label selector matched
against the pool's labels — pool selection is cluster-wide, not scoped by atespace
or namespace.

## Prerequisites

- A k8s cluster with Agent Substrate installed (`./hack/install-ate.sh --deploy-ate-system`).
- `ko` installed for building images.
- A GCS bucket for storing snapshots (configured via `BUCKET_NAME` env var).

## How to Run on Agent Substrate

### 1. Build and Deploy

> [!NOTE]
> Do not manually edit the `demos/multi-template/*.yaml.tmpl` manifests. The
> installation script automatically injects your `${BUCKET_NAME}` environment
> variable during deployment.

```bash
./hack/install-ate.sh --deploy-demo-multi-template
```

This command will:
- Build the `counter` and `fspersist` images using `ko`.
- Create one `WorkerPool` (`shared-pool`) in the `ate-demo-multi-template-pool`
  namespace (`multi-template.yaml.tmpl`).
- Create 2 atespaces — `ate-demo-multi-template-counter` and
  `ate-demo-multi-template-fspersist` — and an actor template in each:
  `counter` (`counter-template.yaml.tmpl`) and `fspersist`
  (`fspersist-template.yaml.tmpl`), both selecting the pool via the same
  `workerSelector` label and applied with `kubectl ate create actor-template`.
- Wait until both templates' golden snapshots are built.

### 2. Create one actor per template

Each actor goes in its template's atespace — `--template-ref` names the template,
resolved in the actor's atespace — and their DNS names embed that atespace:

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

# Create two actors from different templates, one per atespace.
kubectl ate create actor c1 -a ate-demo-multi-template-counter --template-ref counter
kubectl ate create actor f1 -a ate-demo-multi-template-fspersist --template-ref fspersist
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
curl -s -H "Host: c1.ate-demo-multi-template-counter.actors.resources.substrate.ate.dev" http://localhost:8000
# -> hello from: <ip> | preserved memory count: 1

# fspersist binary
curl -s -H "Host: f1.ate-demo-multi-template-fspersist.actors.resources.substrate.ate.dev" http://localhost:8000
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
kubectl ate suspend actor f1 -a ate-demo-multi-template-fspersist
curl -s -H "Host: f1.ate-demo-multi-template-fspersist.actors.resources.substrate.ate.dev" http://localhost:8000  # history persists; count keeps climbing
```

## How to Uninstall

Remove the demo — this deletes the actors (suspending running ones first), the
templates, both atespaces, and then the pool and its namespace:

```bash
./hack/install-ate.sh --delete-demo-multi-template
```
