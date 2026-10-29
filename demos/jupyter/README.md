---
title: "Jupyter Notebook"
linkTitle: "Jupyter Notebook"
weight: 3
estimated_time: "10 mins"
card_icon: "fa-solid fa-book-open"
description: >
  Run an unmodified Jupyter Notebook container that suspends when idle and resumes on browser access.
---

# Jupyter Notebook Demo

This demo runs a Jupyter Notebook on Agent Substrate. It demonstrates how standard unmodified container images (like `jupyter/base-notebook`) can be deployed as Substrate actors that transparently suspend when idle and resume when you access them via your browser.

## Prerequisites

- A k8s cluster with Agent Substrate installed as in the [Quickstart](../../README.md#quickstart-development)
  (with `ATE_INSTALL_KIND=true` exported in your shell) or [GKE Quickstart](../../README.md#gke-quickstart-development).
- A snapshot location set at install (`SNAPSHOT_LOCATION`). The demo stores its snapshots there.

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
atespace. Create your `jupyter` actor there — `--template` names the
template, resolved in the actor's atespace:

```bash
kubectl ate create actor jupyter-notebook -a ate-demo-jupyter --template jupyter
```

### 2. Access Jupyter via the Proxy!

1. **Start the local actor proxy:**

   Keep this running in a separate terminal:
   ```bash
   kubectl ate proxy actor jupyter-notebook 8888:80 -a ate-demo-jupyter
   ```

2. **Open your browser!**

   Now, open your web browser and visit:
   `http://localhost:8888`

You should see the Jupyter Notebook interface without any password (because we passed `--ServerApp.password=''` in our template config)!
Create a new notebook and try running:
```python
print("hello world")
```

### 4. Suspending and Resuming the Notebook

When you're not using the notebook, instead of leaving the container running, Substrate can checkpoint and suspend it to disk.

```bash
kubectl ate suspend actor jupyter-notebook -a ate-demo-jupyter
```

Check the actor status to confirm it's suspended:
```bash
kubectl ate get actor jupyter-notebook -a ate-demo-jupyter
```
Notice how it shows `STATUS_SUSPENDED`.

To **resume** the notebook, you can either explicitly resume it via CLI:

```bash
kubectl ate resume actor jupyter-notebook -a ate-demo-jupyter
```

Or, even easier, you can rely on "transparent resume" — just refresh the page in your browser or make another request to the URL while it's suspended. Substrate will automatically restore its state and serve your request without any downtime.

### Clean up

Uninstall the demo deployment — this deletes the demo's actors, the actor
template, and the `ate-demo-jupyter` atespace:
```bash
go run ./cmd/ate-setup delete demo jupyter
```
