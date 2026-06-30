# Counter Demo

This directory contains a demo of a stateful counter application running on Agent Substrate.

It deploys a simple Go HTTP server (`counter.go`) that increments a counter on every request and preserves state across suspends and resumes.

## Prerequisites

- A k8s cluster with Agent Substrate installed (`./hack/install-ate.sh --deploy-ate-system`).
- `ko` installed for building images.
- A GCS bucket for storing snapshots (configured via `BUCKET_NAME` env var).

## How to Run on Agent Substrate

### 1. Build and Deploy

> [!NOTE]
> Do not manually edit `demos/counter/counter.yaml.tmpl`. The installation script automatically injects your `${BUCKET_NAME}` environment variable during deployment.

Use the core installation script to build the image and apply the resolved manifests to your cluster:

```bash
./hack/install-ate.sh --deploy-demo-counter
```

This command will:
- Build the counter server image using `ko`.
- Create the `ate-demo-counter` namespace.
- Create the `WorkerPool` and `ActorTemplate`.
- Wait until the template is ready.

### 2. Create a Counter Actor

Actors live in an **atespace**, which must exist before you create actors in it. Create one (e.g., `demo`), then create the counter actor with a chosen ID (e.g., `my-counter-1`):

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

# Create the atespace (required before creating actors).
kubectl ate create atespace demo

# Create the actor in the atespace, using the counter template.
kubectl ate create actor my-counter-1 -a demo --template ate-demo-counter/counter
```

### 3. Port-Forward Services

To interact with the router locally:

```bash
# Port-forward the Atenet Router
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

## How to Use

When you send an HTTP request through the router, Substrate automatically detects the session, activates (resumes) the actor onto an available worker pod, and proxies the traffic.

1. Send an HTTP POST request to increment the counter:
```bash
curl -X POST -H "Host: my-counter-1.demo.actors.resources.substrate.ate.dev" http://localhost:8000
```

2. Verify that the actor is now in a `RUNNING` state and assigned to a worker pod:
```bash
kubectl ate get actor my-counter-1 -a demo
```

3. When finished, you can manually suspend the actor back to snapshot storage:
```bash
kubectl ate suspend actor my-counter-1 -a demo
```

4. To permanently delete the suspended actor, then the now-empty atespace:
```bash
kubectl ate delete actor my-counter-1 -a demo
kubectl ate delete atespace demo
```

## Micro-VM variant

The same in-RAM-counter suspend/resume-continuity demo also runs on the micro-VM
sandbox class (`ateom-microvm`: a Kata guest on Cloud Hypervisor), proving that
the guest-memory snapshot round-trips just as gVisor's process snapshot does.

- [`demos/counter/counter-microvm.yaml.tmpl`](counter-microvm.yaml.tmpl) — the
  `WorkerPool` + `ActorTemplate` for the micro-VM sandbox class.
- [`hack/run-microvm-demo.sh`](../../hack/run-microvm-demo.sh) — one-shot bring-up
  that builds the micro-VM worker image, stages the guest assets, deploys the
  control plane, and applies the manifest above. Like the other hack scripts it
  reads `.ate-dev-env.sh` for GKE; use the kind wrapper for a local cluster.

Run it and follow the printed next steps:

```bash
# GKE (uses .ate-dev-env.sh, uploads assets to GCS):
./hack/run-microvm-demo.sh

# local kind (local registry + in-cluster rustfs):
KIND_CLUSTER_NAME=<cluster> ./hack/run-microvm-demo-kind.sh
```

Then create an actor, increment the counter, suspend it, resume it (even on a
different worker), and confirm the count continues — the actor's counter lives in
guest RAM, so a continuing count proves the guest-memory snapshot survived the
round trip.

## How to Uninstall

To remove the counter demo resources from your cluster, run:

```bash
./hack/install-ate.sh --delete-demo-counter
```
