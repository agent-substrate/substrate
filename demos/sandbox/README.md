# Sandbox Demo

This directory contains a demo of a stateful sandbox execution environment running on Agent Substrate.

It allows you to run arbitrary commands in an sandboxed, isolated container (running Alpine Linux) and preserves the execution state across suspends and resumes.

> [!WARNING]
> **Security Disclaimer:** This demo is not secured by any authorization checks, and the sandbox actor will execute any client-provided commands with no validation. Do not deploy this configuration in production or expose it to untrusted networks.

## Components

1.  **Sandbox Server (`main.go`)**: The application that runs inside the Agent Substrate actor. It exposes a simple, stateless `/process` endpoint to execute commands.
2.  **Sandbox Client (`client/`)**: A CLI REPL tool that allows you to interact with the sandbox actor interactively.

## Prerequisites

- A k8s cluster with Agent Substrate installed.
- `ko` installed for building images.
- A GCS bucket for storing snapshots (configured in `demos/sandbox/sandbox-template.yaml.tmpl`).
- `kubectl-ate` CLI installed (can be installed via `go install ./cmd/kubectl-ate`).
- A kube context with access to the `ate-system` namespace. The client mints its `ate-client` ServiceAccount token and requests an ingress client certificate through it.

## How to Run on Agent Substrate

### 1. Build and Deploy

> [!NOTE]
> Do not manually edit the `demos/sandbox/*.yaml.tmpl` manifests. The installation script automatically injects your `${BUCKET_NAME}` environment variable during deployment.

Use the core installation script to build the image and deploy the demo to your cluster:

```bash
go run ./cmd/ate-setup deploy demo sandbox
```

This command will:
- Build the sandbox server image based on Alpine Linux.
- Create the `ate-demo-sandbox` namespace and `WorkerPool` (`sandbox.yaml.tmpl`).
- Create the `ate-demo-sandbox` atespace and the `sandbox-template` actor
  template in it (`sandbox-template.yaml.tmpl`, applied with
  `kubectl ate create actor-template`).
- Wait until the template's golden snapshot is built.

Inspect the template with:
```bash
kubectl ate get actor-template sandbox-template -a ate-demo-sandbox
```

### 2. Create a Sandbox Actor

Create the sandbox actor in the demo's atespace with a chosen name (e.g., `my-sandbox-1`) — `--template` names the template, resolved in the actor's atespace:

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

kubectl ate create actor my-sandbox-1 -a ate-demo-sandbox --template sandbox-template
```

### 3. Proxy to the Actor

Start a local proxy to the actor in a separate terminal:

```bash
kubectl ate proxy actor my-sandbox-1 -a ate-demo-sandbox
```

## How to Use the Client

Build and run the client REPL:

```bash
go build -o bin/sandbox-client ./demos/sandbox/client

./bin/sandbox-client --atenet=localhost:8000 --atespace=ate-demo-sandbox --name=my-sandbox-1
```

By default, the client automatically port-forwards `svc/api` for control-plane access (authenticating via a minted `ate-client` ServiceAccount token with automatic token refresh) and reaches the actor via the local proxy at `localhost:8000`.

You can also customize connections with flags:
- `--ateapi`: Address of the ateapi gRPC server (defaults to auto port-forwarding `svc/api`).
- `--atenet`: Address of the local actor proxy or atenet router (defaults to `localhost:8000`). If set to `""`, the client automatically port-forwards `svc/atenet-router:443` over mTLS.
- `--token-file`: Path to a bearer token for ateapi authentication (defaults to a minted ServiceAccount token).
- `--kubeconfig`: Path to the kubeconfig file.
- `--context`: Kubeconfig context to use.
- `--router-cert`, `--router-key`: Path to client certificate and private key PEM files for direct router mTLS.
- `--router-ca`: Path to CA certificate PEM file to verify atenet-router (defaults to `servicedns.podcert.ate.dev/identity` ClusterTrustBundle).
- `--router-client-sa`, `--router-client-ns`: ServiceAccount and namespace for the minted SPIFFE client certificate when connecting directly (defaults to `atenet-client` in `ate-system`).

The client sends each `/process` request to the router and automatically sets
the routing header from `--name` and `--atespace`:

```go
req.Header.Set(atenet.TargetActorHeader, actorRef.String())
```

Any replacement HTTP client must send an equivalent `ate-target-actor` header;
the URL and `Host` header do not select the Actor.

Once in the `sandbox>` prompt, you can run commands:

```bash
sandbox> ls -la
sandbox> pwd
sandbox> echo "Hello" > test.txt
sandbox> cat test.txt
```

Type `exit` to leave. This will automatically trigger the suspension of the actor.
