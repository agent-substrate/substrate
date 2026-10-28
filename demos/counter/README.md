# Counter Demo

This directory contains a demo of a stateful counter application running on Agent Substrate.

It deploys a simple Go HTTP server (`counter.go`) that increments two counters on every request — one in process memory, one in a file on a durable volume — and preserves both across suspends and resumes: the template's `MEMORY` `preferredFidelity` snapshot captures process memory alongside the durable volumes, so the in-memory count continues from where it left off. (With a `VOLUMES` fidelity, only the durable-volume counter would survive and the in-memory counter would restart from a cold boot. `ROOTFS` also keeps each container's rootfs writes, but not process memory, so the in-memory counter would restart under it too.)

The demo uses two kinds of resources: the `WorkerPool` is a Kubernetes CRD, while the actor template is a Substrate `ActorTemplate` resource — an `ateapi.ActorTemplate` living in an **atespace** rather than a Kubernetes namespace, managed through the ate API with `kubectl ate`.

## Prerequisites

- A k8s cluster with Agent Substrate installed as in the [Quickstart](../../README.md#quickstart-development)
  or [GKE Quickstart](../../README.md#gke-quickstart-development).
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

The in-RAM-counter suspend/resume-continuity demo be run on the microVM
sandbox class (`ateworker-microvm`: a Kata guest on Cloud Hypervisor).

- [`demos/counter/counter-microvm.yaml.tmpl`](counter-microvm.yaml.tmpl) —
  the `WorkerPool` for the microVM sandbox class.
- [`demos/counter/counter-microvm-template.yaml.tmpl`](counter-microvm-template.yaml.tmpl) —
  the `counter-microvm` actor template, created through the ate API in the
  `ate-demo-counter-microvm` atespace.
- [`hack/run-microvm-demo.sh`](../../hack/run-microvm-demo.sh) — one-shot bring-up
  that builds the microVM worker image, stages the guest assets, deploys the
  control plane, and applies the manifests above. Like the other hack scripts it
  reads `.ate-dev-env.sh` for GKE; use the kind wrapper for a local cluster.

Run it and follow the printed next steps:

```bash
# GKE (uses .ate-dev-env.sh, uploads assets to GCS):
./hack/run-microvm-demo.sh

# local kind (local registry + in-cluster rustfs):
KIND_CLUSTER_NAME=<cluster> ./hack/run-microvm-demo-kind.sh
```

On a cluster that already has the microVM deps (`hack/install-microvm-deps.sh`),
deploy directly instead:

```bash
go run ./cmd/ate-setup deploy demo counter-microvm
```
</details>

<details>
<summary>Installation on GVisor</summary>
Use the core installation script to build the image and deploy the demo to your cluster:

```bash
./hack/install-ate.sh deploy demo counter
```

To enable validation of reading from an external volume (e.g.
`/external-data/test.txt`), run:

```bash
./hack/install-ate.sh --preview=ExternalVolumes deploy demo counter --with-external-volume
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

### 3. Proxy to the Actor

Start a local proxy that authenticates with `atenet-router` and forwards local port `8000` to port `80` on the actor:

<details>
<summary>Proxy on MicroVM</summary>

```bash
kubectl ate proxy actor my-counter-microvm-1 -a ate-demo-counter-microvm
```
</details>

<details>
<summary>Proxy on GVisor</summary>

```bash
kubectl ate proxy actor my-counter-1 -a ate-demo-counter
```
</details>

## How to Use

When you send an HTTP request through the proxy, `atenet-router` automatically activates (resumes) the actor onto an available worker pod and routes the traffic.

1. In a separate terminal, send an HTTP POST request to increment the counter:

```bash
curl -X POST http://localhost:8000
```

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
support. Pass `[local-port:]remote-port` arguments to `kubectl ate proxy actor`
to forward additional ports:

<details>
<summary>Non standard port with MicroVM</summary>

```bash
kubectl ate proxy actor my-counter-microvm-1 8000:80 9090:9090 -a ate-demo-counter-microvm
```
</details>

<details>
<summary>Non standard port with GVisor</summary>

```bash
kubectl ate proxy actor my-counter-1 8000:80 9090:9090 -a ate-demo-counter
```
</details>

Then send a request to local port `9090`:

```bash
curl http://localhost:9090/
```

This reaches the same actor's second listener and resumes it like any other
request.

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
