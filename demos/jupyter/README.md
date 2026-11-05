---
title: "Jupyter Notebook"
linkTitle: "Jupyter Notebook"
weight: 3
estimated_time: "10 mins"
card_icon: "fa-solid fa-book-open"
description: >
  Run an unmodified Jupyter Notebook container that checkpoints on suspend and resumes on browser access.
---

# Jupyter Notebook Demo

This demo runs a Jupyter Notebook on Agent Substrate. It shows how an unmodified container image (`quay.io/jupyter/base-notebook`) runs as a Substrate actor that checkpoints on suspend and resumes when accessed through your browser.

## Prerequisites

- A Kubernetes cluster with Agent Substrate installed as in the [Quickstart](../../docs/get-started/quickstart.md) or [Operator Install Guide](../../docs/guides/operator-install.md).
- `atenet-router` configured with `--ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/ate-system/sa/atenet-client` (`ATE_INGRESS_ALLOWED_SPIFFE_IDS`); see [Ingress to actors](../../docs/guides/operator-install.md#ingress-to-actors).
- A snapshot location (`SNAPSHOT_LOCATION`, see [Snapshot storage](../../docs/guides/operator-install.md#snapshot-storage)) and image repository (`KO_DOCKER_REPO`, or `ko.dockerRepo` in `--config`) for `ate-setup deploy demo`. On [kind](https://kind.sigs.k8s.io/), `export ATE_INSTALL_KIND=true` configures local defaults (`gs://ate-snapshots/` and `localhost:5001`) automatically.

## Deploy the Jupyter Notebook Demo

```bash
go run ./cmd/ate-setup deploy demo jupyter
```

## Creating and Accessing Your Jupyter Actor

Once the infrastructure and template are deployed, you can create instances (actors) of Jupyter Notebook.

### 1. Create the Actor

First, ensure you have the `kubectl-ate` CLI installed:

```bash
go install ./cmd/kubectl-ate
```

The deploy created the `jupyter` actor template in the `ate-demo-jupyter`
atespace. Create your `jupyter-notebook` actor there (`--template` names the
template in the actor's atespace):

```bash
kubectl ate create actor jupyter-notebook -a ate-demo-jupyter --template jupyter
```

### 2. Access Jupyter via the Proxy

1. **Start the local actor proxy:**

   Keep this running in a separate terminal:
   ```bash
   kubectl ate proxy actor jupyter-notebook 8888:80 -a ate-demo-jupyter
   ```

2. **Verify from the command line or open your browser:**

   Query the Jupyter Server `/api` endpoint to wake the actor and confirm it is serving (`HEAD /lab` via `curl -I` returns `405 Method Not Allowed`):
   ```bash
   curl -s http://localhost:8888/api
   ```

   Or open your web browser and visit `http://localhost:8888/lab`.

The JupyterLab interface opens without a password because the template passes `--ServerApp.token=''` and `--ServerApp.password=''`.
Create a new notebook and try running:
```python
print("hello world")
```

### 3. Suspending and Resuming the Notebook

When you are finished using the notebook, checkpoint and suspend the actor:

```bash
kubectl ate suspend actor jupyter-notebook -a ate-demo-jupyter
```

Check the actor status to confirm it is suspended:
```bash
kubectl ate get actor jupyter-notebook -a ate-demo-jupyter
```
The `STATE` column shows `ACTOR_STATE_SUSPENDED`.

To **resume** the notebook, you can either explicitly resume it via CLI:

```bash
kubectl ate resume actor jupyter-notebook -a ate-demo-jupyter
```

Or refresh `http://localhost:8888/lab` in your browser (or run `curl -s http://localhost:8888/api`) while the actor is suspended. `atenet-router` automatically resumes the actor from its snapshot and serves the request.

## Clean up

Stop the `kubectl ate proxy actor` process in your proxy terminal, then uninstall the demo deployment. This deletes the demo's actors, the `jupyter` actor template, the `ate-demo-jupyter` atespace, the `jupyter` `WorkerPool`, and the `ate-demo-jupyter` namespace:
```bash
go run ./cmd/ate-setup delete demo jupyter
```
