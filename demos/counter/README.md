# Counter Demo

This directory contains a demo of a stateful counter application running on Agent Substrate.

It deploys a simple Go HTTP server (`counter.go`) that increments two counters on every request — one in process memory, one in a file on a durable volume — and preserves both across suspends and resumes: the template's `MEMORY` `preferredFidelity` snapshot captures process memory alongside the durable volumes, so the in-memory count continues from where it left off. (With a `VOLUMES` fidelity, only the durable-volume counter would survive and the in-memory counter would restart from a cold boot. `ROOTFS` also keeps each container's rootfs writes, but not process memory, so the in-memory counter would restart under it too.)

The demo uses two kinds of resources: the `WorkerPool` is a Kubernetes CRD, while the actor template is a Substrate `ActorTemplate` resource — an `ateapipb.ActorTemplate` living in an **atespace** rather than a Kubernetes namespace, managed through the ate API with `kubectl ate`.

## Prerequisites

- A k8s cluster with Agent Substrate installed.
- `ko` installed for building images.
- A GCS bucket for storing snapshots (configured via `BUCKET_NAME` env var).

## How to Run on Agent Substrate

### 1. Build and Deploy

> [!NOTE]
> Do not manually edit `demos/counter/counter.yaml.tmpl` or
> `demos/counter/counter-template.yaml.tmpl`. The installation script
> automatically injects your `${BUCKET_NAME}` environment variable during
> deployment.

<details>
<summary>Installation on MicroVM</summary>

The in-RAM-counter suspend/resume-continuity demo be run on the micro-VM
sandbox class (`ateworker-microvm`: a Kata guest on Cloud Hypervisor).

- [`demos/counter/counter-microvm.yaml.tmpl`](counter-microvm.yaml.tmpl) —
  the `WorkerPool` for the micro-VM sandbox class.
- [`demos/counter/counter-microvm-template.yaml.tmpl`](counter-microvm-template.yaml.tmpl) —
  the `counter-microvm` actor template, created through the ate API in the
  `ate-demo-counter-microvm` atespace.
- [`hack/run-microvm-demo.sh`](../../hack/run-microvm-demo.sh) — one-shot bring-up
  that builds the micro-VM worker image, stages the guest assets, deploys the
  control plane, and applies the manifests above. Like the other hack scripts it
  reads `.ate-dev-env.sh` for GKE; use the kind wrapper for a local cluster.

Run it and follow the printed next steps:

```bash
# GKE (uses .ate-dev-env.sh, uploads assets to GCS):
./hack/run-microvm-demo.sh

# local kind (local registry + in-cluster rustfs):
KIND_CLUSTER_NAME=<cluster> ./hack/run-microvm-demo-kind.sh
```

On a cluster that already has the micro-VM deps (`hack/install-microvm-deps.sh`),
deploy directly instead:

```bash
go run ./cmd/ate-setup deploy demo counter-microvm
```
</details>

<details>
<summary>Installation on GVisor</summary>
Use the core installation script to build the image and deploy the demo to your cluster:

```bash
./hack/install-ate.sh --deploy-demo-counter
```

To enable validation of reading from an external volume (e.g.
`/external-data/test.txt`), run:

```bash
./hack/install-ate.sh --deploy-demo-counter-with-external-volume --preview=ExternalVolumes
```

This command will:
- Build the counter server image using `ko`.
- Apply [`counter.yaml.tmpl`](counter.yaml.tmpl): the `ate-demo-counter` namespace and the `counter` `WorkerPool`, then wait for the worker rollout.
- Create the `ate-demo-counter` atespace.
- Create the `counter` actor template through the ate API (`kubectl ate create actor-template`) from [`counter-template.yaml.tmpl`](counter-template.yaml.tmpl). The manifest is the message's protojson form — the same shape `kubectl ate get actor-template -o yaml` prints inside its `actorTemplates` list.
- Wait until the template's golden snapshot is ready.

Inspect the deployed template with:

```bash
kubectl ate get actor-templates -a ate-demo-counter
kubectl ate get actor-template counter -a ate-demo-counter -o yaml
```
</details>

### 2. Create a Counter Actor

Create the counter actor with a chosen ID (e.g., `my-counter-1`) using `--template` (the template's name, resolved in the actor's atespace — so the actor lives in the demo's atespace, which the deploy step already created):

<details>
<summary>Creation on MicroVM</summary>

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

# Create the actor from the counter template.
kubectl ate create actor my-counter-microvm-1 -a ate-demo-counter-microvm --template counter-microvm
```
</details>

<details>
<summary>Creation on GVisor</summary>

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

# Create the actor from the counter template.
kubectl ate create actor my-counter-1 -a ate-demo-counter --template counter
```
</details>

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
The two `-H` options below are the routing key; the request URL and `Host` are
ordinary application metadata.

1. Send an HTTP POST request to increment the counter:
<details>
<summary>Send to MicroVM</summary>

```bash
curl -X POST \
  -H "ate-target-actor: ate-demo-counter-microvm/my-counter-microvm-1" \
  http://localhost:8000
```
</details>

<details>
<summary>Send to GVisor</summary>

```bash
curl -X POST \
  -H "ate-target-actor: ate-demo-counter/my-counter-1" \
  http://localhost:8000
```
</details>

2. Verify that the actor is now in a `RUNNING` state and assigned to a worker pod:
<details>
<summary>Verify on MicroVM</summary>

```bash
kubectl ate get actor my-counter-microvm-1 -a ate-demo-counter-microvm
```
</details>

<details>
<summary>Verify on GVisor</summary>

```bash
kubectl ate get actor my-counter-1 -a ate-demo-counter
```
</details>

3. When finished, you can manually suspend the actor:
<details>
<summary>Suspend on MicroVM</summary>

```bash
kubectl ate suspend actor my-counter-microvm-1 -a ate-demo-counter-microvm
```
</details>

<details>
<summary>Suspend on GVisor</summary>

```bash
kubectl ate suspend actor my-counter-1 -a ate-demo-counter
```
</details>

The snapshot stays on the worker's node and is uploaded to snapshot storage in
the background. Add `--mode=sync` to wait for the upload instead.

Repeat the `curl` from step 1 and the actor resumes from its snapshot —
possibly on a different worker — with **both** counters continuing from where
they left off: the memory count comes back from the `MEMORY` snapshot's process
memory, the file counter from the durable volume.

4. To permanently delete the suspended actor:
<details>
<summary>Delete from MicroVM</summary>

```bash
kubectl ate delete actor my-counter-microvm-1 -a ate-demo-counter-microvm
```
</details>

<details>
<summary>Delete from GVisor</summary>

```bash
kubectl ate delete actor my-counter-1 -a ate-demo-counter
```
</details>

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

<details>
<summary>Non standard port with MicroVM</summary>

```bash
curl -p -x http://localhost:8001 \
  --proxy-header "ate-target-actor: ate-demo-counter-microvm/my-counter-microvm-1" \
  http://my-counter-microvm-1:9090/
```
</details>

<details>
<summary>Non standard port with GVisor</summary>

```bash
curl -p -x http://localhost:8001 \
  --proxy-header "ate-target-actor: ate-demo-counter/my-counter-1" \
  http://my-counter-1:9090/
```
</details>

This reaches the same actor's second listener and resumes it exactly like any
other request — the only difference from the default-port examples above is
the port named in the URL and dialing the router's CONNECT listener (8081/8001
here) instead of its plain HTTP one.

**Current limitation**: only HTTP(S) traffic over the tunnel is supported.
Raw TCP or other non-HTTP protocols on a non-default port aren't reachable
this way today — if your use case needs that, please open an issue or reach
out; the CONNECT tunnel itself is protocol-agnostic, so it's a matter of
prioritizing it, not a fundamental limitation of the design.

## Settings

The installer reads these settings in addition to those in
[docs/operator-install.md](../../docs/operator-install.md#settings), through the same
flag, environment variable, and configuration document channels.

| Key                               | Environment variable                    | Flag                     | Default    | Applies to            |
|-----------------------------------|-----------------------------------------|--------------------------|------------|-----------------------|
| `demo.counter.storageClass`       | `STORAGE_CLASS`                         | `--storage-class`        | `standard` | `deploy demo counter` |
| `demo.counter.withExternalVolume` | `ATE_DEMO_COUNTER_WITH_EXTERNAL_VOLUME` | `--with-external-volume` | `false`    | `deploy demo counter` |

## How to Uninstall

To remove the counter demo resources from your cluster, run:

<details>
<summary>Delete from MicroVM</summary>

```bash
go run ./cmd/ate-setup delete demo counter-microvm
```
</details>

<details>
<summary>Delete from GVisor</summary>

```bash
go run ./cmd/ate-setup delete demo counter
```
</details>

This deletes the actors created from the demo templates, the templates
themselves (along with their golden actors and golden snapshots, server-side),
the demo atespaces, and the worker pools.
