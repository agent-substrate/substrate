---
title: "Interactive Sandbox"
linkTitle: "Interactive Sandbox"
weight: 2
estimated_time: "5 mins"
card_icon: "fa-solid fa-terminal"
description: >
  Execute interactive shell commands inside an isolated Alpine container with stateful snapshots.
---

# Sandbox Demo

This directory contains a demo of a stateful sandbox execution environment running on Agent Substrate.

It runs commands in a sandboxed, isolated container (Alpine Linux) and preserves container filesystem and memory state across suspends and resumes. `sandbox-client` sends each command as `sh -c` to `/process`, which spawns a fresh subprocess per request, so working-directory changes (`cd`) and shell variables reset between commands while files written inside the container persist across requests and snapshots.

> [!WARNING]
> **Security Disclaimer:** This demo is not secured by any authorization checks, and the sandbox actor will execute any client-provided commands with no validation. Do not deploy this configuration in production or expose it to untrusted networks.

## Components

1.  **Sandbox Server (`main.go`)**: The application that runs inside the Agent Substrate actor. It exposes a `/process` endpoint that runs each command in a fresh subprocess, while files in the container persist across requests.
2.  **Sandbox Client (`client/`)**: A CLI REPL tool that allows you to interact with the sandbox actor interactively.

## Prerequisites

- A Kubernetes cluster with Agent Substrate installed as in the [Quickstart](../../docs/get-started/quickstart.md) or [Operator Install Guide](../../docs/guides/operator-install.md).
- `atenet-router` configured with `--ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/ate-system/sa/atenet-client` (`ATE_INGRESS_ALLOWED_SPIFFE_IDS`); see [Ingress to actors](../../docs/guides/operator-install.md#ingress-to-actors).
- A snapshot location (`SNAPSHOT_LOCATION`, see [Snapshot storage](../../docs/guides/operator-install.md#snapshot-storage)) and image repository (`KO_DOCKER_REPO`, or `ko.dockerRepo` in `--config`) for `ate-setup deploy demo`. On [kind](https://kind.sigs.k8s.io/), `export ATE_INSTALL_KIND=true` configures local defaults (`gs://ate-snapshots/` and `localhost:5001`) automatically.
- `kubectl-ate` CLI installed (`go install ./cmd/kubectl-ate`).

## How to Run on Agent Substrate

### 1. Build and Deploy

Use `ate-setup` to build the image and deploy the demo to your cluster:

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

Create the sandbox actor in the demo's atespace with a chosen name (e.g., `my-sandbox-1`). `--template` names the template in the actor's atespace:

```bash
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

Once in the `sandbox>` prompt, you can run commands:

```text
sandbox> ls -la
sandbox> pwd
sandbox> echo "Hello" > test.txt
sandbox> cat test.txt
```

Type `exit` to leave. This will automatically trigger the suspension of the actor.

To resume the actor non-interactively and verify `test.txt` persisted across the snapshot, pipe commands into `sandbox-client`:

```bash
printf 'cat test.txt\nexit\n' | \
  ./bin/sandbox-client --atenet=localhost:8000 --atespace=ate-demo-sandbox --name=my-sandbox-1
```

To permanently delete the suspended actor:
```bash
kubectl ate delete actor my-sandbox-1 -a ate-demo-sandbox
```

## How to Uninstall

To remove the sandbox demo resources (actors, template, atespace, workerpool, and namespace) from your cluster, run:

```bash
go run ./cmd/ate-setup delete demo sandbox
```
