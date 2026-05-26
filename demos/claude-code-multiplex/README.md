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

## Audience

This guide assumes you know Kubernetes and the general shape of agent runtimes (autonomy + LLM API access). It does **not** assume prior Substrate experience.

## Prerequisites

- A Kubernetes cluster with **Agent Substrate** installed (`./hack/install-ate.sh` from this repo's root).
- `kubectl` configured against that cluster (the dashboard uses the operator's kubeconfig via [`client-go`](https://github.com/kubernetes/client-go) for pod-log reads).
- Network reach to the substrate **ateapi** gRPC service (`ateapi.ate-system:8080`). When running the dashboard from outside the cluster, port-forward it in a separate terminal (same convention as [`demos/sandbox/README.md`](../sandbox/README.md#3-port-forward-services)) and keep it running for the lifetime of the demo:
  ```bash
  # Terminal 1: ateapi port-forward
  kubectl port-forward svc/ateapi 8080:8080 -n ate-system
  ```
- An **Anthropic API key** (the agents call Claude).
- A GCS bucket for substrate state snapshots (configured during Substrate install).
- `KO_DOCKER_REPO` set to a registry you can push to (e.g. `gcr.io/${PROJECT_ID}/ate-images`, same as `hack/ate-dev-env.sh.example`). The deploy step builds and pushes the workload image there with a sha256-pinned reference.
- `docker buildx` (the deploy function builds the workload image — a Dockerfile-based Python + Claude Code wrapper, not a Go binary, so `ko` doesn't apply for the workload itself).

## Components

| Path | Purpose |
|---|---|
| `demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl` | Namespace, WorkerPool, ActorTemplates in a single envsubst template |
| `hack/install-demo-claude-code-multiplex.sh` | Sourced by `install-ate.sh`; registers `--deploy-demo-claude-code-multiplex` and `--delete-demo-claude-code-multiplex` |
| `demos/claude-code-multiplex/workload/` | The agent container image source (Dockerfile + entrypoint that wires Claude Code; built and pushed by the deploy step) |
| `demos/claude-code-multiplex/ui/` | Static dashboard (`index.html` + `server.go`) that talks to the cluster |

(Files will be added across iterations of this PR.)

## How to Run

### 1. Deploy the demo

From the repo root, with your Anthropic key and substrate bucket name in the environment:

```bash
ANTHROPIC_API_KEY=sk-ant-... \
BUCKET_NAME=your-substrate-bucket \
  ./hack/install-ate.sh --deploy-demo-claude-code-multiplex
```

This creates the `claude-multiplex-demo` namespace, a 2-pod `WorkerPool`, and three `ActorTemplate` objects named `luna`, `mars`, `orion`. Under the hood, the deploy function builds the workload image with `docker buildx`, pushes it to `${KO_DOCKER_REPO}/claude-multiplex-demo-workload`, resolves the pushed sha256 digest, and substitutes the digest-pinned reference plus `ANTHROPIC_API_KEY` and `BUCKET_NAME` into the manifest template at apply time.

### 2. Start the dashboard

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

### 3. Drive the demo

Click "Give a task". The UI picks a random idle agent and creates a task for it. Watch:

- Badge flips to `queued` (the agent has work but isn't bound to a pod yet).
- Substrate finds a free pod and binds the agent. Badge flips to `running`.
- The agent calls Claude, writes a result, exits. Badge flips to `completed`.
- Substrate notices the inactivity and suspends the agent after a short idle window.
- The released pod becomes available for the next queued task on a different agent.

With three agents and two pods, the third agent stays suspended (state snapshotted) until a pod opens up.

## Upstream blockers worked around for this demo

This demo currently applies workarounds at runtime for three Substrate issues. Each will be addressed by a separate upstream fix PR; details + workarounds in the linked issue threads.

- **`#189`** — Atelet OCI bundle gaps (`Args`, `Secret`, symlinks). Bundled fix PR forthcoming.
- **`#197` Bug 2a** — `valueFrom.secretKeyRef` on `ActorTemplate` container env is not supported today. `ANTHROPIC_API_KEY` is passed as a plain `value:` env var (envsubst-substituted at apply time) until upstream support lands.
- **`#197` Bug 3** — Atelet symlink resolution. Fix PR forthcoming.

> [!NOTE]
> `@AlexBulankou` asked for this guide as the main deliverable for the Claude-Code-on-Substrate demo work. The fix PRs are filed separately so each blocker has its own upstream review thread.

## Status

**This PR is a draft.** Initial commit lands the structural README. Subsequent commits will add the manifests, the workload Dockerfile/entrypoint, and the UI source. Each addition will keep the README in sync.

Feedback on shape/audience welcome before content fills out.

## Teardown

```bash
./hack/install-ate.sh --delete-demo-claude-code-multiplex
```

This removes the `claude-multiplex-demo` namespace and all the resources created by the deploy step. You can also stop the port-forward and the dashboard processes in their respective terminals.
