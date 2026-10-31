---
title: "Claude Code Multiplexing"
linkTitle: "Claude Code Multiplexing"
weight: 7
estimated_time: "15 mins"
card_icon: "fa-solid fa-robot"
description: >
  Multiplex three Claude Code agents onto two worker pods with explicit suspend and resume.
---

# Claude Code Multiplex Demo

A demo of three Claude Code agents sharing two Agent Substrate worker pods. The dashboard resumes an agent when a task is assigned and suspends it when the task finishes, so the cluster runs fewer worker pods than agents.

**Walkthrough video (150s):** https://storage.googleapis.com/yojowa-claw-demo-screenshots/multiplex-demo-2026-05-18-captions.webm

The video shows the three-agent rotation end-to-end: `queued` -> `running` -> `completed`, with the third agent waiting while the other two own pods, and Substrate cycling pod ownership as tasks complete.

> [!NOTE]
> This demo provisions **two worker pods for three agents** (each pod capped at `1` CPU and `2Gi` memory, matching one agent's resource limits) to exercise Substrate's suspend and resume path.

## What this shows

- Three Claude Code agents (`luna`, `mars`, `orion`) registered as Substrate actors in the `claude-multiplex-demo` atespace.
- A `WorkerPool` of two pods (`claude-workerpool`), each sized to host one running agent at a time.
- Out-of-band `ANTHROPIC_API_KEY` injection via `EgressPolicy` `replaceHeaders` and `k8s-credential-provider`, keeping container `env` and golden snapshots free of secrets.
- A web UI that assigns tasks to idle agents, calls `ResumeActor` and `SuspendActor` on the control plane, and tails filtered per-pod logs.

This demo uses the `ateapi` control plane to manage Actors and does not send workload HTTP traffic through `atenet-router`, so it does not use the `ate-target-actor` routing header.

## Audience

This guide assumes you know Kubernetes and the general shape of agent runtimes. It does **not** assume prior Substrate experience.

## Prerequisites

- A Kubernetes cluster with Agent Substrate installed as in the [Quickstart](../../docs/get-started/quickstart.md) (with `ATE_INSTALL_KIND=true` exported in your shell for Kind) or [GKE Quickstart](../../README.md#gke-quickstart-development), with the bundled Kubernetes Secrets credential provider enabled (`--credential-provider='{"name":"k8s.io"}'` on `deploy ate-system` or `deploy atenet`; see [Egress Credential Injection](../../docs/guides/egress-credential-injection.md)).
- `kubectl` configured against that cluster (the dashboard uses the operator's kubeconfig via [`client-go`](https://github.com/kubernetes/client-go) for pod-log reads).
- Network reach to the Substrate **api** gRPC service (`api.ate-system:443`). By default (`ATEAPI_ADDR=""`), the dashboard auto-port-forwards to `svc/api` in `ate-system` (the same path `kubectl-ate` uses). If you prefer to run a manual port-forward in a separate terminal (similar to [`demos/sandbox/README.md`](../sandbox/README.md#3-proxy-to-the-actor)), pick a local port that does not collide with the dashboard's default `PORT=8080`:
  ```bash
  # Optional manual api port-forward
  kubectl port-forward svc/api 8443:443 -n ate-system
  ```
  If you run the dashboard as a pod inside the cluster, `claude-code-multiplex.yaml.tmpl` labels `Namespace/claude-multiplex-demo` with `ate.dev/api-access=true` so `ate-system`'s NetworkPolicies admit it (see [`ate-system-networkpolicies.yaml`](../../manifests/ate-install/ate-system-networkpolicies.yaml)).
- An **Anthropic API key** (`ANTHROPIC_API_KEY`). If you do not have a live key, you can pass a placeholder value (such as `ANTHROPIC_API_KEY=sk-ant-demo`) to exercise the full 3-agent / 2-worker multiplexing and `EgressPolicy` flow.
- A snapshot location set at install (`SNAPSHOT_LOCATION`, configured automatically when `ATE_INSTALL_KIND=true`).
- The image registry (`KO_DOCKER_REPO`) that your Substrate install uses. The deploy step builds and pushes the workload image to that registry with a sha256-pinned reference.
- `docker buildx` (the deploy step builds the workload image, a Dockerfile-based Node.js and Bash wrapper around Claude Code, with `docker buildx` rather than `ko`).

## Components

| Path | Purpose |
|---|---|
| `demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl` | Namespace and 2-pod `WorkerPool` manifest |
| `demos/claude-code-multiplex/agent-*-template.yaml.tmpl` | One protojson `ActorTemplate` per agent, mounting the egress trust bundle (`NODE_EXTRA_CA_CERTS`) |
| `cmd/ate-setup/internal/demos/claudemultiplex/` | The installer's demo package, behind `deploy demo claude-code-multiplex` and `delete demo claude-code-multiplex` |
| `demos/claude-code-multiplex/workload/` | The agent container image source (Dockerfile + Bash entrypoint that runs Claude Code on Node.js 20) |
| `demos/claude-code-multiplex/ui/` | Static dashboard (`index.html` + `server.go`) that drives actor lifecycle and reads pod logs |

## How to Run

### 1. Deploy the demo

From the repo root, with your Anthropic key in the environment:

```bash
ANTHROPIC_API_KEY=sk-ant-... \
go run ./cmd/ate-setup deploy demo claude-code-multiplex
```

> [!TIP]
> On non-Kind clusters (where `ATE_INSTALL_KIND=true` is not set), `.ko.yaml` selects `linux/amd64,linux/arm64`. If your local Docker uses the default `docker` driver rather than a multi-platform `docker-container` builder, pass `KO_DEFAULTPLATFORMS=linux/amd64` (or your cluster's node architecture) when running `deploy demo claude-code-multiplex`.

This step:

1. Creates the `claude-multiplex-demo` namespace and its 2-pod `WorkerPool` (`claude-workerpool`, each pod limited to `1` CPU and `2Gi` memory).
2. Builds the workload image with `docker buildx`, pushes it to `${KO_DOCKER_REPO}/claude-multiplex-demo-workload`, and substitutes the sha256-pinned image digest into `agent-luna`, `agent-mars`, and `agent-orion`.
3. Creates the `claude-multiplex-demo` atespace and the three `ActorTemplate`s (with a non-secret placeholder `ANTHROPIC_API_KEY=egress-injected` and the `systemInfo` `trustBundle` mounted for `NODE_EXTRA_CA_CERTS`), then waits for each template's golden snapshot.
4. Stores `ANTHROPIC_API_KEY` in the `anthropic-api-key` Kubernetes `Secret` in `claude-multiplex-demo` and grants `claude-multiplex-demo` access in `k8s-credential-provider-namespace-policy` when `k8s-credential-provider` is deployed.
5. Creates the three `Actor`s (`luna`, `mars`, `orion`) in `ACTOR_STATE_SUSPENDED` and attaches an `EgressPolicy` to each actor that replaces `x-api-key` on requests to `https://api.anthropic.com` with `ate-secret://k8s.io/default/claude-multiplex-demo/anthropic-api-key/api-key`.

### 2. Start the dashboard

The dashboard is a Go HTTP server that reads worker and actor state from the Substrate `ateapi` gRPC service, drives `ResumeActor` and `SuspendActor` transitions when tasks run, and reads pod logs from the Kubernetes API via [`client-go`](https://github.com/kubernetes/client-go).

Using automatic port-forwarding to `svc/api` in `ate-system`:

```bash
cd demos/claude-code-multiplex/ui
PORT=8090 go run .
```

Or targeting a manual port-forward (`kubectl port-forward svc/api 8443:443 -n ate-system`):

```bash
cd demos/claude-code-multiplex/ui
PORT=8090 ATEAPI_ADDR=localhost:8443 go run .
```

Open `http://localhost:8090` in your browser.

Environment variables:

| Var | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | TCP port the dashboard binds (pick a port different from any manual `ATEAPI_ADDR` port-forward). |
| `ATEAPI_ADDR` | `""` (auto-port-forward) | Address of the Substrate `ateapi` gRPC service; when empty, `ateclient` auto-port-forwards to `svc/api` in `ate-system`. |
| `DEMO_NAMESPACE` | `claude-multiplex-demo` | Atespace and Kubernetes namespace the dashboard filters to and reads pod logs from. |

`GET /healthz` reports whether the Kubernetes client picked up a cluster context (`logs:true|false`).

### 3. Drive the demo

Click **Give a task** (three times in a row to saturate the 2-pod pool). Watch:

- The task starts in `queued` while waiting for one of the two worker pods and while the UI server calls `ResumeActor`.
- Once `ResumeActor` restores the agent onto a free worker pod (`ACTOR_STATE_RUNNING`), the task badge flips to `running` and the agent executes a prompt tick inside the container.
- After the task run window completes, the UI server calls `SuspendActor` to checkpoint the agent and release its worker pod, flipping the task badge to `completed`.
- If all two worker pods are busy with active tasks when a third task is assigned, the third task remains `queued` (and its actor stays `Suspended`) until one of the running tasks completes and suspends its actor.

## Settings

The installer reads this setting in addition to those in [docs/guides/operator-install.md](../../docs/guides/operator-install.md#settings), through the same environment variable and configuration document channels.

| Key                    | Environment variable | Flag        | Default    | Applies to |
|------------------------|----------------------|-------------|------------|------------|
| `demo.anthropicAPIKey` | `ANTHROPIC_API_KEY`  | none        | secret     | (global)   |

It is a secret, so it has no flag and the run report prints `<set>` for it.

## Teardown

From the repository root (if your shell is in `demos/claude-code-multiplex/ui`, run `cd ../../..` first):

```bash
cd ../../..
go run ./cmd/ate-setup delete demo claude-code-multiplex
```

This removes the demo's actors, templates, atespace, and the `claude-multiplex-demo` namespace (including the `anthropic-api-key` `Secret` and `claude-workerpool`). Stop the dashboard process (and any manual port-forward) in their respective terminals.
