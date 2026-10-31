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

A demo of three Claude-Code-driven agents sharing two Agent Substrate pods. Substrate suspends idle agents and resumes them on demand, so the cluster runs *fewer pods than agents*.

**Walkthrough video (150s):** https://storage.googleapis.com/yojowa-claw-demo-screenshots/multiplex-demo-2026-05-18-captions.webm

The video shows the three-agent rotation end-to-end: queued → running → completed, with the third agent suspending while the other two run, and substrate cycling pod ownership as tasks complete.

> [!NOTE]
> This demo intentionally provisions **two pods for three agents** to exercise substrate's suspend/resume path. The same pattern scales — ten agents on three pods, a hundred agents on twenty.

## What this shows

- Three Claude Code agents (`luna`, `mars`, `orion`) registered as Substrate actors.
- A `WorkerPool` of two pods.
- A small web UI that drives "give a task" against random idle agents and renders the queued/running/completed badge state per agent.
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
- Network reach to the substrate **api** gRPC service (`api.ate-system:443`). If you run the dashboard as a pod in the cluster, label its namespace `ate.dev/api-access=true`, because ate-system's NetworkPolicies admit API clients only from labeled namespaces (see [`ate-system-networkpolicies.yaml`](../../manifests/ate-install/ate-system-networkpolicies.yaml)). When running the dashboard from outside the cluster, port-forward it in a separate terminal (same convention as [`demos/sandbox/README.md`](../sandbox/README.md#3-port-forward-services)) and keep it running for the lifetime of the demo:
  ```bash
  # Terminal 1: api port-forward
  kubectl port-forward svc/api 8080:443 -n ate-system
  ```
- An **Anthropic API key** (the agents call Claude).
- A snapshot location set at install (`SNAPSHOT_LOCATION`). The agents' snapshots are stored there.
- The image registry (`KO_DOCKER_REPO`) that your Substrate install uses. The deploy step builds and pushes the workload image to that registry with a sha256-pinned reference.
- `docker buildx` (the deploy function builds the workload image — a Dockerfile-based Python + Claude Code wrapper, not a Go binary, so `ko` doesn't apply for the workload itself).

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

This creates the `claude-multiplex-demo` namespace and its 2-pod `WorkerPool`, then the `claude-multiplex-demo` atespace and three actor templates in it named `agent-luna`, `agent-mars`, `agent-orion`, and waits for each template's golden snapshot. Under the hood, the deploy function builds the workload image with `docker buildx`, pushes it to `${KO_DOCKER_REPO}/claude-multiplex-demo-workload`, resolves the pushed sha256 digest, and substitutes the digest-pinned reference and `ANTHROPIC_API_KEY` into the agent templates at apply time.

### 2. Create the agents, allow their egress, and resume them

The deploy step creates the templates; the agents are actors you create here. Each needs an actor, an egress policy, and a resume, in that order.

```bash
for a in luna mars orion; do
  # 1. Create the actor from its template. It starts SUSPENDED and owns no pod.
  kubectl ate create actor agent-$a -a claude-multiplex-demo --template agent-$a

  # 2. Allow the Anthropic API before resuming. The gateway denies all traffic
  #    from an actor with no policy, so without this every `claude --print`
  #    tick fails with `API Error: 403 egress denied`. The gateway also caches
  #    a missing policy as deny for 10s, so a policy created after the resume
  #    can leave the first ticks failing; create it first.
  cat <<'POLICY' | kubectl ate create egress-policy agent-$a -a claude-multiplex-demo -f -
rules:
- https:
    hostnames: ["api.anthropic.com"]
POLICY

  # 3. Resume the agent. Nothing in this demo resumes it for you: no traffic
  #    goes through atenet-router (see the note near the top), and the
  #    dashboard only reads state.
  kubectl ate resume actor agent-$a -a claude-multiplex-demo
done
```

The dashboard's "Give a task" button does none of this. It only drives running agents, so this step comes first.

An `https` rule means the gateway terminates the TLS to `api.anthropic.com`. Claude Code (Node.js) rejects the gateway's certificate unless the actor trusts it, so the agents also need a trust bundle: see [`docs/guides/egress-trust-bundle.md`](../../docs/guides/egress-trust-bundle.md) for wiring a `trustBundle` volume and `NODE_EXTRA_CA_CERTS` into the agent templates.

### 3. Start the dashboard

The dashboard is a small Go HTTP server that reads worker and actor state from the substrate **ateapi** gRPC service (mirroring the pattern in `demos/sandbox/client/main.go`) and pod logs from the Kubernetes API via [`client-go`](https://github.com/kubernetes/client-go). No `kubectl` process invocations.

Make sure the ateapi port-forward from the [Prerequisites](#prerequisites) is still running, then:

```bash
cd demos/claude-code-multiplex/ui
PORT=8090 ATEAPI_ADDR=localhost:8080 go run .
```

Or build a binary:

```bash
cd demos/claude-code-multiplex/ui
go build -o ui-server .
PORT=8090 ATEAPI_ADDR=localhost:8080 ./ui-server
```

Either way, the UI is served on `http://localhost:8090` (or whatever `PORT` you pick — pick something that doesn't collide with the ateapi port-forward).

Env vars:

| Var | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | TCP port the dashboard binds (pick `≠ ATEAPI_ADDR`'s port when both run on the same host). |
| `ATEAPI_ADDR` | `localhost:8080` | Address of the substrate ateapi gRPC service. |
| `DEMO_NAMESPACE` | `claude-multiplex-demo` | Kubernetes namespace the dashboard filters to and reads pod logs from. |

`GET /healthz` reports whether the kube client picked up a cluster context (`logs:true|false`) — useful for quick smoke-tests after starting the server.

### 4. Drive the demo

Click "Give a task". The UI picks a random idle agent and creates a task for it. Watch:

- Badge flips to `queued` (the agent has work but isn't bound to a pod yet).
- Substrate finds a free pod and binds the agent. Badge flips to `running`.
- The agent calls Claude, writes a result, exits. Badge flips to `completed`.
- Substrate notices the inactivity and suspends the agent after a short idle window.
- The released pod becomes available for the next queued task on a different agent.

With three agents and two pods, the third agent stays suspended (state snapshotted) until a pod opens up.

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

```bash
go run ./cmd/ate-setup delete demo claude-code-multiplex
```

This removes the demo's actors, templates, atespace, and the `claude-multiplex-demo` namespace with everything the deploy step created in it. You can also stop the port-forward and the dashboard processes in their respective terminals.
