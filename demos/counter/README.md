# Counter Demo

This directory contains a demo of a stateful counter application running on Agent Substrate.

It deploys a simple Go HTTP server (`counter.go`) that increments two counters on every request — one in process memory, one in a file on a durable volume — and preserves both across suspends and resumes: the template's `Full`-scope `onCommit` snapshot captures process memory alongside the durable volumes, so the in-memory count continues from where it left off. (With a `Data`-scope snapshot policy, only the durable-volume counter would survive and the in-memory counter would restart from a cold boot.)

The demo uses two kinds of resources: the `WorkerPool` is a Kubernetes CRD, while the actor template is a Substrate `ActorTemplate` resource — an `ateapipb.ActorTemplate` living in an **atespace** rather than a Kubernetes namespace, managed through the ate API with `kubectl ate`.

## Prerequisites

- A k8s cluster with Agent Substrate installed (`./hack/install-ate.sh --deploy-ate-system`).
- `ko` installed for building images.
- A GCS bucket for storing snapshots (configured via `BUCKET_NAME` env var).

## How to Run on Agent Substrate

### 1. Build and Deploy

> [!NOTE]
> Do not manually edit `demos/counter/counter-substrate.yaml.tmpl` or
> `demos/counter/counter-substrate-template.yaml.tmpl`. The installation script
> automatically injects your `${BUCKET_NAME}` environment variable during
> deployment.

Use the core installation script to build the image and deploy the demo to your cluster:

```bash
./hack/install-ate.sh --deploy-demo-counter-substrate
```

This command will:
- Build the counter server image using `ko`.
- Apply [`counter-substrate.yaml.tmpl`](counter-substrate.yaml.tmpl): the `ate-demo-counter-substrate` namespace and the `counter-substrate` `WorkerPool`, then wait for the worker rollout.
- Create the `ate-demo-counter-substrate` atespace.
- Create the `counter` actor template through the ate API (`kubectl ate create actor-template`) from [`counter-substrate-template.yaml.tmpl`](counter-substrate-template.yaml.tmpl). The manifest is the message's protojson form — the same shape `kubectl ate get actor-template -o yaml` prints inside its `actorTemplates` list.
- Wait until the template's golden snapshot is ready.

Inspect the deployed template with:

```bash
kubectl ate get actor-templates -a ate-demo-counter-substrate
kubectl ate get actor-template counter -a ate-demo-counter-substrate -o yaml
```

### 2. Create a Counter Actor

Create the counter actor with a chosen ID (e.g., `my-counter-1`) using `--template-ref` (the template's name, resolved in the actor's atespace — so the actor lives in the demo's atespace, which the deploy step already created):

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

# Create the actor from the counter template.
kubectl ate create actor my-counter-1 -a ate-demo-counter-substrate --template-ref counter
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
curl -X POST -H "Host: my-counter-1.ate-demo-counter-substrate.actors.resources.substrate.ate.dev" http://localhost:8000
```

2. Verify that the actor is now in a `RUNNING` state and assigned to a worker pod:
```bash
kubectl ate get actor my-counter-1 -a ate-demo-counter-substrate
```

3. When finished, you can manually suspend the actor back to snapshot storage:
```bash
kubectl ate suspend actor my-counter-1 -a ate-demo-counter-substrate
```

Repeat the `curl` from step 1 and the actor resumes from its snapshot —
possibly on a different worker — with **both** counters continuing from where
they left off: the memory count comes back from the `Full` snapshot's process
memory, the file counter from the durable volume.

4. To permanently delete the suspended actor:
```bash
kubectl ate delete actor my-counter-1 -a ate-demo-counter-substrate
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
curl -p -x http://localhost:8001 http://my-counter-1.ate-demo-counter-substrate.actors.resources.substrate.ate.dev:9090/
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

- [`demos/counter/counter-substrate-microvm.yaml.tmpl`](counter-substrate-microvm.yaml.tmpl) —
  the `WorkerPool` for the micro-VM sandbox class.
- [`demos/counter/counter-substrate-microvm-template.yaml.tmpl`](counter-substrate-microvm-template.yaml.tmpl) —
  the `counter-microvm` actor template, created through the ate API in the
  `ate-demo-counter-substrate-microvm` atespace.
- [`hack/run-microvm-demo.sh`](../../hack/run-microvm-demo.sh) — one-shot bring-up
  that builds the micro-VM worker image, stages the guest assets, deploys the
  control plane, and applies the manifests above. Like the other hack scripts it
  reads `.ate-dev-env.sh` for GKE; use the kind wrapper for a local cluster.

Run it with `--substrate` and follow the printed next steps:

```bash
# GKE (uses .ate-dev-env.sh, uploads assets to GCS):
./hack/run-microvm-demo.sh --substrate

# local kind (local registry + in-cluster rustfs):
KIND_CLUSTER_NAME=<cluster> ./hack/run-microvm-demo-kind.sh --substrate
```

On a cluster that already has the micro-VM deps (`hack/install-microvm-deps.sh`),
deploy directly instead:

```bash
./hack/install-ate.sh --deploy-demo-counter-substrate-microvm
```

Then create an actor (`--template-ref counter-microvm`, in the
`ate-demo-counter-substrate-microvm` atespace), increment the counter, suspend
it, resume it (even on a different worker), and confirm the count continues —
the actor's counter lives in guest RAM, so a continuing count proves the
guest-memory snapshot survived the round trip.

## How to Uninstall

To remove the counter demo resources from your cluster, run:

```bash
./hack/install-ate.sh --delete-demo-counter-substrate
./hack/install-ate.sh --delete-demo-counter-substrate-microvm
```

This deletes the actors created from the demo templates, the templates
themselves (along with their golden actors and golden snapshots, server-side),
the demo atespaces, and the worker pools.
