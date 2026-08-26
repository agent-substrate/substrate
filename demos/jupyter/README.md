# Jupyter Notebook Demo

This demo runs a Jupyter Notebook on Agent Substrate. It demonstrates how standard unmodified container images (like `jupyter/base-notebook`) can be deployed as Substrate actors that transparently suspend when idle and resume when you access them via your browser.

## Prerequisites

- A local Kubernetes cluster via Kind (or GKE).
- `ko` installed for building images.
- A GCS bucket for storing snapshots, or an in-cluster snapshot repository (handled automatically by the kind install script).

## Quickstart on Kind

For local development, Agent Substrate provides helper scripts to get a Kind cluster running and configured.

1. **Create the Kind cluster:**

   ```bash
   ./hack/create-kind-cluster.sh
   # This will create a local cluster named "ate-demo" and configure a local registry.
   ```

2. **Install Agent Substrate core system to the Kind cluster:**

   ```bash
   ./hack/install-ate-kind.sh --deploy-ate-system
   ```

3. **Deploy the Jupyter Notebook Demo:**

   ```bash
   ./hack/install-ate-kind.sh --deploy-demo-jupyter
   ```

## Creating and Accessing Your Jupyter Actor

Once the infrastructure and template are deployed, you can create instances (actors) of Jupyter Notebook.

### 1. Create the Actor

First, ensure you have the `kubectl-ate` CLI installed:

```bash
go install ./cmd/kubectl-ate
```

Then create an `atespace` and your `jupyter` actor:

```bash
# Create an atespace (required before creating actors)
kubectl ate create atespace demo

# Create the actor in the atespace via the jupyter template
kubectl ate create actor jupyter-notebook -a demo --template ate-demo-jupyter/jupyter
```

### 2. Access Jupyter via the Proxy!

Substrate routes HTTP traffic using the `Host` header. To make this easy without modifying local `/etc/hosts` files, this demo includes a lightweight NGINX reverse proxy (`jupyter-proxy`) that automatically injects the proper `Host` header (`jupyter-notebook.demo.actors.resources.substrate.ate.dev`) and forwards traffic internally to the Substrate router.

1. **Port-forward the lightweight proxy to your local machine:**

   Keep this running in a separate terminal:
   ```bash
   kubectl port-forward --address 0.0.0.0 -n ate-demo-jupyter svc/jupyter-proxy 8888:80
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
kubectl ate suspend actor jupyter-notebook -a demo
```

Check the actor status to confirm it's suspended:
```bash
kubectl ate get actor jupyter-notebook -a demo
```
Notice how it shows `STATUS_SUSPENDED`.

To **resume** the notebook, you can either explicitly resume it via CLI:

```bash
kubectl ate resume actor jupyter-notebook -a demo
```

Or, even easier, you can rely on "transparent resume" — just refresh the page in your browser or make another request to the URL while it's suspended. Substrate will automatically restore its state and serve your request without any downtime. 

### Clean up

Delete the actor and atespace:
```bash
kubectl ate delete actor jupyter-notebook -a demo
kubectl ate delete atespace demo
```

Uninstall the demo deployment:
```bash
./hack/install-ate-kind.sh --delete-demo-jupyter
```

Uninstall the entire local Kind cluster:
```bash
./hack/delete-kind-cluster.sh
```
