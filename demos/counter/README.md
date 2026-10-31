---
title: "Stateful Counter"
linkTitle: "Stateful Counter"
weight: 1
estimated_time: "5 mins"
card_icon: "fa-solid fa-stopwatch"
description: >
  Preserve in-memory, root filesystem, and durable volume state across actor suspend and resume cycles.
---

# Counter Demo

This demo runs a stateful HTTP server (`counter.go`) on Agent Substrate and shows how actor snapshots preserve state across suspend and resume cycles.

On every HTTP request, `counter.go` increments three counters:
- `preserved memory count` in process memory
- `preserved file counter` in `/home/counter/a.txt` on the `data` `durableDir` volume
- `preserved rootfs counter` in `/rootfs-counter.txt` on the container root filesystem

The demo `ActorTemplate` sets `preferredFidelity` to `SNAPSHOT_FIDELITY_MEMORY`, which captures process memory, root filesystem writes, and durable volumes when an actor suspends. When the actor resumes, all three counters continue from where they left off. Under `SNAPSHOT_FIDELITY_ROOTFS`, the durable volume and root filesystem counters survive while the in-memory counter resets to `1`. Under `SNAPSHOT_FIDELITY_VOLUMES`, only the durable volume counter survives.

The demo creates two resources:
- `WorkerPool` (`counter`), a Kubernetes custom resource in the `ate-demo-counter` namespace.
- `ActorTemplate` (`counter`), a Substrate API resource in the `ate-demo-counter` **atespace**, managed through `kubectl ate`.

## Prerequisites

- A Kubernetes cluster with Agent Substrate installed as in the [Quickstart](../../docs/get-started/quickstart.md) or [Operator Install Guide](../../docs/guides/operator-install.md). On [kind](https://kind.sigs.k8s.io/), keep `export ATE_INSTALL_KIND=true` set in your shell so `ate-setup` targets the local cluster and registry.
- `atenet-router` configured with `--ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/ate-system/sa/atenet-client` (`ATE_INGRESS_ALLOWED_SPIFFE_IDS`) so `kubectl ate proxy actor` can authenticate to the router.
- A snapshot location set at install (`SNAPSHOT_LOCATION`). `ATE_INSTALL_KIND=true` sets this automatically to `gs://ate-snapshots/`.

## How to Run on Agent Substrate

This walkthrough deploys the default `gvisor` sandbox variant (`gvisor-default`). See [Running on microVM](#running-on-microvm) for the `microvm` variant.

The [Quickstart](../../docs/get-started/quickstart.md) already deploys this demo, creates `my-counter-1`, and starts its proxy on port `8000`. If you ran it, skip to [How to Use](#how-to-use).

### 1. Build and Deploy

> [!NOTE]
> Do not manually edit `demos/counter/counter.yaml.tmpl` or `demos/counter/counter-template.yaml.tmpl`. `ate-setup` fills in their `${...}` placeholders during deployment.

Use `ate-setup` to build the image and deploy the demo to your cluster:

```bash
go run ./cmd/ate-setup deploy demo counter
```

This command:
- Builds the counter server image using `ko`.
- Applies [`counter.yaml.tmpl`](counter.yaml.tmpl) to create the `ate-demo-counter` namespace and the `counter` `WorkerPool`, then waits for the worker rollout.
- Creates the `ate-demo-counter` atespace.
- Creates the `counter` actor template through the Substrate API from [`counter-template.yaml.tmpl`](counter-template.yaml.tmpl). The manifest uses the message's protojson form, which matches `kubectl ate get actor-template counter -a ate-demo-counter -o yaml`.
- Waits until the template's golden snapshot is ready.

Inspect the deployed template:

```bash
kubectl ate get actor-templates -a ate-demo-counter
kubectl ate get actor-template counter -a ate-demo-counter -o yaml
```

### 2. Create a Counter Actor

Install the `kubectl-ate` CLI plugin and create an actor named `my-counter-1` in the `ate-demo-counter` atespace from the `counter` template:

```bash
go install ./cmd/kubectl-ate
kubectl ate create actor my-counter-1 -a ate-demo-counter --template counter
```

### 3. Proxy to the Actor

Start a local proxy that authenticates with `atenet-router` and forwards local port `8000` to port `80` on the actor:

```bash
kubectl ate proxy actor my-counter-1 -a ate-demo-counter
```

## How to Use

When you send an HTTP request through the proxy, `atenet-router` resumes the actor onto an available worker pod and routes the traffic.

1. In a separate terminal, send an HTTP POST request to increment the counters:

```bash
curl -X POST http://localhost:8000
```

Expected output:

```text
preserved memory count: 1 | preserved file counter: 1 | preserved rootfs counter: 1
```

2. Verify that the actor is in `ACTOR_STATE_RUNNING` and assigned to a worker pod:

```bash
kubectl ate get actor my-counter-1 -a ate-demo-counter
```

3. Manually suspend the actor:

```bash
kubectl ate suspend actor my-counter-1 -a ate-demo-counter
```

The snapshot stays on the worker's node and uploads to snapshot storage in the background. Pass `--mode=sync` to wait for the upload before returning.

Repeat the `curl` command from step 1:

```bash
curl -X POST http://localhost:8000
```

Expected output:

```text
preserved memory count: 2 | preserved file counter: 2 | preserved rootfs counter: 2
```

The actor resumes from its snapshot, potentially on a different worker pod, and all three counters continue from `1` to `2`. Run `kubectl ate get actor my-counter-1 -a ate-demo-counter` again and check the `WORKER POD` column to see which worker pod it resumed on.

## Reaching a non-default port

`counter.go` also listens on a second configurable port (`--extra-port=9090` in this demo's template) to exercise arbitrary-port ingress through `atenet-router`. Pass `[local-port:]remote-port` arguments to `kubectl ate proxy actor` to forward additional ports. Stop any earlier proxy on port `8000` first:

```bash
kubectl ate proxy actor my-counter-1 8000:80 9090:9090 -a ate-demo-counter
```

Send a request to local port `9090`:

```bash
curl http://localhost:9090/
```

Expected output:

```text
hello from extra port 9090
```

This reaches the same actor's second listener and resumes the actor if it is suspended.

**Current limitation**: only HTTP(S) traffic over the tunnel is supported. Raw TCP or other non-HTTP protocols on a non-default port are not reachable through `atenet-router` yet.

Delete the actor when finished:

```bash
kubectl ate delete actor my-counter-1 -a ate-demo-counter
```

## Running on microVM

You can also run the counter demo on the `microvm` sandbox class (`ateworker-microvm`, which runs a Kata guest on Cloud Hypervisor and requires `/dev/kvm` on worker nodes). This variant uses:
- [`demos/counter/counter-microvm.yaml.tmpl`](counter-microvm.yaml.tmpl): the `WorkerPool` for the `microvm` sandbox class.
- [`demos/counter/counter-microvm-template.yaml.tmpl`](counter-microvm-template.yaml.tmpl): the `counter-microvm` `ActorTemplate` in the `ate-demo-counter-microvm` atespace.

`deploy ate-system` stages the microVM guest assets and applies the cluster-wide `microvm` `SandboxConfig` by default (`--sandbox-classes=gvisor,microvm`), so on a cluster installed as in the [Quickstart](../../docs/get-started/quickstart.md), deploy the demo with:

```bash
go run ./cmd/ate-setup deploy demo counter-microvm
```

Inspect the deployed template:

```bash
kubectl ate get actor-templates -a ate-demo-counter-microvm
kubectl ate get actor-template counter-microvm -a ate-demo-counter-microvm -o yaml
```

Create, proxy, suspend, and delete the microVM actor in the `ate-demo-counter-microvm` atespace:

```bash
kubectl ate create actor my-counter-1 -a ate-demo-counter-microvm --template counter-microvm
kubectl ate proxy actor my-counter-1 -a ate-demo-counter-microvm
```

In a second terminal:

```bash
curl -X POST http://localhost:8000
kubectl ate get actor my-counter-1 -a ate-demo-counter-microvm
kubectl ate suspend actor my-counter-1 -a ate-demo-counter-microvm
curl -X POST http://localhost:8000
kubectl ate delete actor my-counter-1 -a ate-demo-counter-microvm
```

## Attaching an External Volume

Pass `--with-external-volume` to attach a CSI-backed external volume mounted at `/external-data`. That flag sets `--validate-existing-file-path=/external-data/test.txt` on `counter.go`, so `curl` returns `failed to read test file: open /external-data/test.txt: no such file or directory` until `/external-data/test.txt` exists on the volume.

External volumes require the `ExternalVolumes` preview feature gate and an adapted CSI driver with a `CSIDriverConfig` (see [CSI Driver Deployment](../../docs/guides/csi-deployment.md)):

```bash
# 1. Deploy ate-system with the ExternalVolumes preview gate enabled
go run ./cmd/ate-setup deploy ate-system \
  --preview=ExternalVolumes \
  --credential-provider='{"name":"k8s.io"}' \
  --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/ate-system/sa/atenet-client

# 2. Install a bundled CSI driver and its CSIDriverConfig:
#    - NFS (creates StorageClass csi-nfs-sc, requires the nfsd kernel module on nodes):
go run ./cmd/ate-setup setup csi nfs
#    - Or hostpath on kind clusters (creates StorageClass csi-hostpath-sc):
# go run ./cmd/ate-setup setup csi hostpath

# 3. Delete any existing counter demo (templates are immutable), then deploy with the external volume
go run ./cmd/ate-setup delete demo counter
go run ./cmd/ate-setup deploy demo counter \
  --with-external-volume \
  --storage-class=csi-nfs-sc \
  --preview=ExternalVolumes

# 4. Create the actor with --preview enabled on kubectl ate
kubectl ate --preview create actor my-counter-1 -a ate-demo-counter --template counter
```

## Settings

The installer reads these settings in addition to those in [docs/guides/operator-install.md](../../docs/guides/operator-install.md#settings), through the same flag, environment variable, and configuration document channels.

| Key                               | Environment variable                    | Flag                     | Default    | Applies to            |
|-----------------------------------|-----------------------------------------|--------------------------|------------|-----------------------|
| `demo.counter.storageClass`       | `STORAGE_CLASS`                         | `--storage-class`        | `standard` | `deploy demo counter` |
| `demo.counter.withExternalVolume` | `ATE_DEMO_COUNTER_WITH_EXTERNAL_VOLUME` | `--with-external-volume` | `false`    | `deploy demo counter` |

## How to Uninstall

Remove the counter demo resources from your cluster:

```bash
go run ./cmd/ate-setup delete demo counter

# Or for the microVM variant:
go run ./cmd/ate-setup delete demo counter-microvm
```

This deletes the actors created from the demo templates, the templates themselves (along with their golden actors and golden snapshots), the demo atespaces, and the worker pools.
