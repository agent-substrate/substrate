# Counter Demo

This directory contains a demo of a stateful counter application running on Agent Substrate.

It deploys a simple Go HTTP server (`counter.go`) that increments two counters on every request — one in process memory, one in a file on a durable volume — and preserves both across suspends and resumes: the template's `onCommit: Full` snapshot captures process memory alongside the durable volumes, so the in-memory count continues from where it left off. (With a `Data`-scope snapshot policy, only the durable-volume counter would survive and the in-memory counter would restart from a cold boot.)

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

To enable validation of reading from an external volume (e.g. `/external-data/test.txt`), run:

```bash
./hack/install-ate.sh --deploy-demo-counter-with-external-volume
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

# Also port-forward the CONNECT listener if you want to reach the actor's
# extra port (see "Reaching a non-default port" below).
kubectl port-forward -n ate-system svc/atenet-router 8001:8081
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

Repeat the `curl` from step 1 and the actor resumes from its snapshot —
possibly on a different worker — with **both** counters continuing from where
they left off: the memory count comes back from the `Full` snapshot's process
memory, the file counter from the durable volume.

4. To permanently delete the suspended actor, then the now-empty atespace:
```bash
kubectl ate delete actor my-counter-1 -a demo
kubectl ate delete atespace demo
```

## Reaching a non-default port

`counter.go` also listens on a second, configurable port (`--extra-port=9090`
in this demo's template) to demonstrate atenet-router's arbitrary-port ingress
support: a client reaches any port an actor listens on, not just its default
(port 80), by naming the port in an HTTP `CONNECT` request's authority rather
than a separate header. The router terminates the `CONNECT` on its own
listener (`--port-connect`, 8081 by default) and tunnels ordinary HTTP requests
through it to the named port.

`curl -p` forces a tunnel even for a plain (non-TLS) target, which its default
proxy behavior wouldn't do:

```bash
curl -p -x http://localhost:8001 http://my-counter-1.demo.actors.resources.substrate.ate.dev:9090/
```

This reaches the same actor's second listener and resumes it exactly like any
other request — the only difference from the default-port examples above is
the port named in the URL and dialing the router's CONNECT listener (8081/8001
here) instead of its plain HTTP one.

**Current limitation**: only HTTP(S) traffic over the tunnel is supported.
Raw TCP or other non-HTTP protocols on a non-default port aren't reachable
this way today — if your use case needs that, please open an issue or reach
out; the CONNECT tunnel itself is protocol-agnostic, so it's a matter of
prioritizing it, not a fundamental limitation of the design.

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

## Substrate-resource (proto) variant

In this variant the `WorkerPool` stays a CRD, but the actor template is an
`ateapipb.ActorTemplate` created through the ate API with
`kubectl ate create actor-template`. Its manifest
([`counter-substrate-template.yaml.tmpl`](counter-substrate-template.yaml.tmpl))
is the message's protojson form — the shape `kubectl ate get actor-template
-o yaml` prints inside its `actorTemplates` list — and lives in an
**atespace** rather than a Kubernetes namespace.

Deploy the gVisor variant (pool + atespace + template, then wait for the
golden snapshot):

```bash
./hack/install-ate.sh --deploy-demo-counter-substrate
```

Create an actor from it with `--template-ref` (the template's name, resolved
in the actor's atespace — so the actor lives in the demo's atespace):

```bash
kubectl ate create actor my-counter-1 -a ate-demo-counter-substrate --template-ref counter
```

Everything else — curling through the router, suspend/resume, delete — works
exactly as in the walkthrough above. Inspect the template with:

```bash
kubectl ate get actor-templates -a ate-demo-counter-substrate
kubectl ate get actor-template counter -a ate-demo-counter-substrate -o yaml
```

For the micro-VM variant, pass `--substrate` to the one-shot bring-up (or run
`./hack/install-ate.sh --deploy-demo-counter-substrate-microvm` on a cluster
that already has the micro-VM deps):

```bash
./hack/run-microvm-demo.sh --substrate
```

> [!NOTE]
> Actor templates are immutable: there is no update RPC, and a re-deploy
> leaves an existing template in place. To change one, delete the demo
> (below) and deploy again — deleting the demo, not the bare template:
> deleting a template does not yet clean up its golden actor, and a RUNNING
> actor left behind can no longer be suspended or deleted until the
> template is recreated.

Uninstall with:

```bash
./hack/install-ate.sh --delete-demo-counter-substrate
./hack/install-ate.sh --delete-demo-counter-substrate-microvm
```

This also deletes the actors created from the demo templates and the demo's
atespace.

## How to Uninstall

To remove the counter demo resources from your cluster, run:

```bash
./hack/install-ate.sh --delete-demo-counter
```
