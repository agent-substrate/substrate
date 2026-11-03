---
title: "Claude Code Multiplexing"
linkTitle: "Claude Code Multiplexing"
weight: 7
estimated_time: "15 mins"
card_icon: "fa-solid fa-robot"
description: >
  Multiplex three Claude Code agents onto two worker pods with automatic suspend and resume.
---

# Claude Code Multiplex Demo

A demo of three Claude-Code-driven agents sharing two Agent Substrate pods. The dashboard resumes an agent when it gets a task and suspends it when the task ends, so the cluster runs *fewer pods than agents*.

**Walkthrough video (150s):** https://storage.googleapis.com/yojowa-claw-demo-screenshots/multiplex-demo-2026-05-18-captions.webm

The video shows the three-agent rotation end-to-end: queued → running → completed, with the third agent suspending while the other two run, and substrate cycling pod ownership as tasks complete.

> [!NOTE]
> This demo intentionally provisions **two pods for three agents** to exercise substrate's suspend/resume path. The same pattern scales — ten agents on three pods, a hundred agents on twenty.

## What this shows

- Three Claude Code agents (`luna`, `mars`, `orion`) registered as Substrate actors.
- A `WorkerPool` of two pods.
- A small web UI that drives "give a task" against random idle agents, renders the queued/running/completed badge state per agent, and resumes and suspends the agents to match.
- Substrate handles the hard parts: state snapshot on suspend, scheduling decisions, resume-correctness when a pod becomes available.

This demo uses the ateapi control plane to manage Actors and does not send
workload HTTP traffic through `atenet-router`, so it does not use
the `ate-target-actor` routing header.

## Audience

This guide assumes you know Kubernetes and the general shape of agent runtimes (autonomy + LLM API access). It does **not** assume prior Substrate experience.

## Prerequisites

- A k8s cluster with Agent Substrate installed as in the [Quickstart](../../README.md#quickstart-development)
  (with `ATE_INSTALL_KIND=true` exported in your shell) or [GKE Quickstart](../../README.md#gke-quickstart-development).
- `kubectl` configured against that cluster (the dashboard uses the operator's kubeconfig via [`client-go`](https://github.com/kubernetes/client-go) for pod-log reads).
- Network reach to the substrate **api** gRPC service (`api.ate-system:443`). From outside the cluster, the dashboard port-forwards to it by itself. If you run the dashboard as a pod in the cluster, label its namespace `ate.dev/api-access=true`, because ate-system's NetworkPolicies admit API clients only from labeled namespaces (see [`ate-system-networkpolicies.yaml`](../../manifests/ate-install/ate-system-networkpolicies.yaml)).
- An **Anthropic API key** (the agents call Claude).
- A snapshot location set at install (`SNAPSHOT_LOCATION`). The agents' snapshots are stored there.
- The image registry (`KO_DOCKER_REPO`) that your Substrate install uses. The deploy step builds and pushes the workload image to that registry with a sha256-pinned reference.
- `docker buildx` (the deploy function builds the workload image — a Dockerfile-based `node:20-slim` + Claude Code wrapper, not a Go binary, so `ko` doesn't apply for the workload itself). `docker` also needs push access to that registry; for `gcr.io`, run `gcloud auth configure-docker gcr.io`.

## Components

| Path | Purpose |
|---|---|
| `demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl` | Namespace and WorkerPool manifest |
| `demos/claude-code-multiplex/agent-*-template.yaml.tmpl` | One protojson ActorTemplate per agent, created through the ate API with `kubectl ate create actor-template` |
| `cmd/ate-setup/internal/demos/claudemultiplex/` | The installer's demo package, behind `deploy demo claude-code-multiplex` / `delete demo claude-code-multiplex` |
| `demos/claude-code-multiplex/workload/` | The agent container image source (Dockerfile + entrypoint that wires Claude Code; built and pushed by the deploy step) |
| `demos/claude-code-multiplex/ui/` | Static dashboard (`index.html` + `server.go`) that talks to the cluster |

## How to Run

### 1. Deploy the demo

From the repo root, with your Anthropic key in the environment:

```bash
ANTHROPIC_API_KEY=sk-ant-... \
go run ./cmd/ate-setup deploy demo claude-code-multiplex
```

This creates the `claude-multiplex-demo` namespace and its 2-pod `WorkerPool`, then the `claude-multiplex-demo` atespace and three actor templates in it named `agent-luna`, `agent-mars`, `agent-orion`, waits for each template's golden snapshot, and creates the three actors (`luna`, `mars`, `orion`), each with an egress policy that lets it reach `api.anthropic.com:443`. The egress gateway passes that TLS through, and denies all traffic from an actor without a policy. The actors start suspended; the dashboard resumes them. Under the hood, the deploy function builds the workload image with `docker buildx`, pushes it to `${KO_DOCKER_REPO}/claude-multiplex-demo-workload`, resolves the pushed sha256 digest, and substitutes the digest-pinned reference and `ANTHROPIC_API_KEY` into the agent templates at apply time.

### 2. Start the dashboard

The dashboard is a small Go HTTP server that reads worker and actor state from the substrate **ateapi** gRPC service (mirroring the pattern in `demos/sandbox/client/main.go`) and pod logs from the Kubernetes API via [`client-go`](https://github.com/kubernetes/client-go), and resumes and suspends the agents as their tasks start and end. No `kubectl` process invocations.

From the repo root:

```bash
cd demos/claude-code-multiplex/ui
PORT=8090 go run .
```

Or build a binary:

```bash
cd demos/claude-code-multiplex/ui
go build -o ui-server .
PORT=8090 ./ui-server
```

Either way, the UI is served on `http://localhost:8090` (or whatever `PORT` you pick).

Env vars:

| Var | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | TCP port the dashboard binds (pick `≠ ATEAPI_ADDR`'s port when both are on the same host). |
| `ATEAPI_ADDR` | empty | Address of the substrate ateapi gRPC service. Empty: the dashboard port-forwards to `svc/api` in `ate-system` itself. |
| `DEMO_NAMESPACE` | `claude-multiplex-demo` | Kubernetes namespace the dashboard filters to and reads pod logs from. |

`GET /healthz` reports whether the kube client picked up a cluster context (`logs:true|false`) — useful for quick smoke-tests after starting the server.

### 3. Drive the demo

Click "Give a task". The UI picks a random idle agent and gives it a task for a fixed time. The task and its timing live only in the dashboard; a resumed agent runs `claude --print` in a loop (see [`workload/run.sh`](workload/run.sh)), and its log shows the replies. Watch:

- The badge shows `queued` until the dashboard has resumed the agent, then `running`. If both pods hold agents with running tasks, the task stays `queued` until one of them finishes.
- When the time is up, the badge flips to `completed` and the dashboard suspends the agent, snapshotting its state and freeing its pod for the next task.

With three agents and two pods, at most two agents run at once, and the third task waits for a pod.

## Settings

The installer reads this setting in addition to those in
[docs/guides/operator-install.md](../../docs/guides/operator-install.md#settings), through the same
environment variable and configuration document channels.

| Key                    | Environment variable | Flag        | Default    | Applies to |
|------------------------|----------------------|-------------|------------|------------|
| `demo.anthropicAPIKey` | `ANTHROPIC_API_KEY`  | — (no flag) | — (secret) | (global)   |

It is a secret, so it has no flag and the run report prints `<set>` for it.

## Upstream blockers worked around for this demo

This demo currently applies workarounds at runtime for two Substrate issues. See the linked issue threads for details and workarounds.

- **`#189`** — Atelet OCI bundle gaps (`Args`, `Secret`, symlinks).
- **`#197` Bug 3** — Atelet symlink resolution.

## Teardown

From the repo root:

```bash
go run ./cmd/ate-setup delete demo claude-code-multiplex
```

This removes the demo's actors, templates, atespace, and the `claude-multiplex-demo` namespace with everything the deploy step created in it. You can also stop the port-forward and the dashboard processes in their respective terminals.
