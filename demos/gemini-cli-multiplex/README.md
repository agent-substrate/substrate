# Gemini CLI Multiplex Demo

A demo of three Gemini-CLI-driven agents sharing two Agent Substrate pods. Substrate suspends idle agents and resumes them on demand, so the cluster runs *fewer pods than agents*.

> [!NOTE]
> This demo intentionally provisions **two pods for three agents** to exercise substrate's suspend/resume path. The same pattern scales — ten agents on three pods, a hundred agents on twenty.

## What this shows

- Three Gemini CLI agents (`luna`, `mars`, `orion`) registered as Substrate actors.
- A `WorkerPool` of two pods.
- A small web UI that drives "give a task" against random idle agents and renders the queued/running/completed badge state per agent.
- Substrate handles the hard parts: state snapshot on suspend, scheduling decisions, resume-correctness when a pod becomes available.

## Audience

This guide assumes you know Kubernetes and the general shape of agent runtimes (autonomy + LLM API access). It does **not** assume prior Substrate experience.

## Prerequisites

- A Kubernetes cluster with **Agent Substrate** installed (`./hack/install-ate.sh` from this repo's root).
- `kubectl` configured against that cluster (the dashboard uses the operator's kubeconfig via [`client-go`](https://github.com/kubernetes/client-go) for pod-log reads).
- Network reach to the substrate **ateapi** gRPC service (`ateapi.ate-system:8080`). When running the dashboard from outside the cluster, port-forward it in a separate terminal and keep it running for the lifetime of the demo:
  ```bash
  # Terminal 1: ateapi port-forward
  kubectl port-forward svc/ateapi 8080:8080 -n ate-system
  ```
- A **Gemini API key** from Google AI Studio (the agents call Gemini via `@google/gemini-cli`).
- A GCS bucket for substrate state snapshots (configured during Substrate install).
- `KO_DOCKER_REPO` set to a registry you can push to (e.g. `gcr.io/${PROJECT_ID}/ate-images`, same as `hack/ate-dev-env.sh.example`). The deploy step builds and pushes the workload image there with a sha256-pinned reference.
- `docker buildx` (the deploy function builds the workload image — a Dockerfile-based Node + `@google/gemini-cli` wrapper, not a Go binary, so `ko` doesn't apply for the workload itself).

## Components

| Path | Purpose |
|---|---|
| `demos/gemini-cli-multiplex/gemini-cli-multiplex.yaml.tmpl` | Namespace, WorkerPool, ActorTemplates in a single envsubst template |
| `hack/install-demo-gemini-cli-multiplex.sh` | Sourced by `install-ate.sh`; registers `--deploy-demo-gemini-cli-multiplex` and `--delete-demo-gemini-cli-multiplex` |
| `demos/gemini-cli-multiplex/workload/` | The agent container image source (Dockerfile + entrypoint that wires Gemini CLI; built and pushed by the deploy step) |
| `demos/gemini-cli-multiplex/ui/` | Static dashboard (`index.html` + `server.go`) that talks to the cluster |

## How to Run

### 1. Deploy the demo

From the repo root, with your Gemini key and substrate bucket name in the environment:

```bash
GEMINI_API_KEY=... \
BUCKET_NAME=your-substrate-bucket \
  ./hack/install-ate.sh --deploy-demo-gemini-cli-multiplex
```

This creates the `gemini-cli-multiplex-demo` namespace, a 2-pod `WorkerPool`, and three `ActorTemplate` objects named `luna`, `mars`, `orion`. Under the hood, the deploy function builds the workload image with `docker buildx`, pushes it to `${KO_DOCKER_REPO}/gemini-cli-multiplex-demo-workload`, resolves the pushed sha256 digest, and substitutes the digest-pinned reference plus `GEMINI_API_KEY` and `BUCKET_NAME` into the manifest template at apply time.

The model is `gemini-2.5-flash` by default; override per-template by editing the `GEMINI_MODEL` env in `gemini-cli-multiplex.yaml.tmpl` before deploying.

Check that everything is running as expected:

```bash
# k8s-native resources (these work with plain kubectl)
kubectl get pods,workerpool,actortemplate -n gemini-cli-multiplex-demo

# Substrate-native (uses the kubectl-ate plugin against ateapi)
kubectl ate get actors
kubectl ate get workers
```

### 2. Start the dashboard

Make sure the ateapi port-forward from the [Prerequisites](#prerequisites) is still running, then:

```bash
cd demos/gemini-cli-multiplex/ui
PORT=8090 ATEAPI_ADDR=localhost:8080 go run .
```

Or build a binary:

```bash
cd demos/gemini-cli-multiplex/ui
go build -o ui-server .
PORT=8090 ATEAPI_ADDR=localhost:8080 ./ui-server
```

Either way, the UI is served on `http://localhost:8090` (or whatever `PORT` you pick — pick something that doesn't collide with the ateapi port-forward).

Env vars:

| Var | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | TCP port the dashboard binds (pick `≠ ATEAPI_ADDR`'s port when both run on the same host). |
| `ATEAPI_ADDR` | `localhost:8080` | Address of the substrate ateapi gRPC service. |
| `DEMO_NAMESPACE` | `gemini-cli-multiplex-demo` | Kubernetes namespace the dashboard filters to and reads pod logs from. |

`GET /healthz` reports whether the kube client picked up a cluster context (`logs:true|false`) — useful for quick smoke-tests after starting the server.

### 3. Drive the demo

Click "Give a task". The UI picks a random idle agent and creates a task for it. Watch:

- Badge flips to `queued` (the agent has work but isn't bound to a pod yet).
- Substrate finds a free pod and binds the agent. Badge flips to `running`.
- The agent calls Gemini, writes a result, exits. Badge flips to `completed`.
- Substrate notices the inactivity and suspends the agent after a short idle window.
- The released pod becomes available for the next queued task on a different agent.

With three agents and two pods, the third agent stays suspended (state snapshotted) until a pod opens up.

## Upstream blockers worked around for this demo

Same upstream Substrate issues as the Claude Code variant. Each will be addressed by a separate upstream fix PR.

- **`#189`** — Atelet OCI bundle gaps (`Args`, `Secret`, symlinks).
- **`#197` Bug 2a** — `valueFrom.secretKeyRef` on `ActorTemplate` container env is not supported today. `GEMINI_API_KEY` is passed as a plain `value:` env var (envsubst-substituted at apply time) until upstream support lands.
- **`#197` Bug 3** — Atelet symlink resolution.

## Teardown

```bash
./hack/install-ate.sh --delete-demo-gemini-cli-multiplex
```

This removes the `gemini-cli-multiplex-demo` namespace and all the resources created by the deploy step. You can also stop the port-forward and the dashboard processes in their respective terminals.
