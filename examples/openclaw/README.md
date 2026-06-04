# OpenClaw on Agent Substrate: Multiplexing Demo

A high-density demonstration of three stateful **OpenClaw** agents (`Claw-Luna`, `Claw-Mars`, `Claw-Nova`) sharing two physical **Agent Substrate** worker pods. This PoC showcases **Liquid Hardware**: Substrate automatically suspends idle agents and rehydrates them on-demand, allowing a cluster to host significantly more logical agents than physical compute slots.

**Live Demo URL:** [http://<YOUR_DASHBOARD_IP>](http://<YOUR_DASHBOARD_IP>) (Internal/GCP)

> [!NOTE]
> This demo intentionally provisions **two pods for three agents** to force hardware contention. Substrate manages the state teleportation (checkpointing to GCS), ensuring that process memory (task counters) survives migration between physical pods.

## System Information

- **Google Claw Version**: `2026.3.14`
- **Substrate Mode**: Multi-Actor Multiplexing (1.5x oversubscription)
- **Runtime**: Node.js 22 (Debian Slim)
- **Isolation**: gVisor (runsc)

## What this shows

- **High-Density Multiplexing**: Three logical OpenClaw identities running on only two physical pods (1.5x oversubscription).
- **State Persistence**: A `taskCounter` maintained in the Node.js process memory survives multiple suspend/resume cycles.
- **Dynamic Rotation**: Agents finish work at different times (3-6s), forcing Substrate to constantly rotate pod ownership.
- **Visual Identity Tracking**: Color-coded agents (Blue/Pink/Gold) and live log tailing to make infrastructure sharing intuitively obvious.

## Audience

This guide is intended for engineers exploring Agent Substrate for hosting large-scale agentic workloads where cost-efficiency and stateful rehydration are critical.

## Prerequisites

- **Agent Substrate Cluster**: A Kubernetes cluster with Substrate installed.
- **Docker**: For building and pushing the unified actor/UI image.
- **GCS Bucket**: Configured for Substrate state snapshots (e.g., `gs://<YOUR_GCS_BUCKET_NAME>/`).
- **kubectl & kubectl-ate**: The Substrate CLI tool for managing logical actors.

## Components

| Path | Purpose |
|---|---|
| `substrate/src/actor-wrapper.ts` | The workload: A Hono server with persistent memory state. |
| `substrate/src/demo-ui.ts` | The dashboard: A Node.js backend providing live logs, task queueing, and visual tracking. |
| `substrate/manifests/worker-pool.yaml` | The physical pool configuration (2 replicas). |
| `substrate/manifests/actor-template.yaml` | The logical identity definition (snapshots, container spec). |
| `substrate/manifests/valkey-init.yaml` | Utility Job for re-initializing the Valkey metadata store. |
| `substrate/Dockerfile` | Unified OCI image containing both the actor workload and the dashboard UI. |
| `substrate/demo/OpenClaw/DEMO_SCRIPT.md` | The narrative script for the demonstration recording. |

## How to Run

### 1. Provision Hardware
Scale the physical `WorkerPool` to the desired replica count (2 for this demo):
```bash
kubectl apply -f substrate/manifests/worker-pool.yaml
```

### 2. Deploy logical Agents
Create the three "fun-named" actors using the Substrate CLI.
```bash
kubectl-ate create actor agent-luna --template openclaw/openclaw-agent
kubectl-ate create actor agent-mars --template openclaw/openclaw-agent
kubectl-ate create actor agent-nova --template openclaw/openclaw-agent
```

### 3. Launch the Dashboard
The dashboard runs as a standard Kubernetes Deployment with a LoadBalancer.
```bash
kubectl apply -f substrate/manifests/demo-ui.yaml
```

## Drive the Demo

Open the dashboard and use the following interaction patterns:

- **Pulse (10 Tasks)**: The primary demo button. It parallelizes 10 tasks across the registry. Watch the **colored icons** rapidly cycle through the 2 worker slots.
- **Live Logs**: Observe the pod log cards. You will see different Agent IDs appearing in the **same log stream**, proving that physical hardware is being recycled in real-time.

## Integrating a Real LLM API

Integrating an LLM into an OpenClaw logical actor is straightforward. Because Substrate persists the **entire process memory**, any in-memory conversation history or KV-cache will survive multiple suspend/resume cycles without requiring an external database.

### 1. Add the LLM SDK
Add your preferred SDK (e.g., OpenAI or Anthropic) to the `substrate/package.json`:
```bash
npm install openai
```

### 2. Update the Actor Logic
Modify `substrate/src/actor-wrapper.ts` to initialize the client and maintain a local chat history:
```typescript
import OpenAI from "openai";

const openai = new OpenAI({ apiKey: process.env.LLM_API_KEY });
let history: any[] = []; // This array will survive Substrate snapshots!

app.post("/v1/chat", async (c) => {
  const { message } = await c.req.json();
  history.push({ role: "user", content: message });
  
  const response = await openai.chat.completions.create({
    model: "gpt-4",
    messages: history,
  });
  
  const aiMessage = response.choices[0].message;
  history.push(aiMessage);
  return c.json(aiMessage);
});
```

### 3. Provide the API Key
Add the credential to the environment variables in `substrate/manifests/actor-template.yaml`:
```yaml
spec:
  containers:
  - name: agent
    env:
    - name: LLM_API_KEY
      value: "sk-proj-..." # Or use a Kubernetes Secret reference
```

### 4. Rebuild & Deploy
Rebuild the image and Substrate will automatically pick up the new logic for any resumed actors.

## Teardown

```bash
kubectl delete -f substrate/manifests/demo-ui.yaml
kubectl-ate delete actor agent-luna agent-mars agent-nova
kubectl delete -f substrate/manifests/worker-pool.yaml
```

## Nuances & Workarounds

This demo handles several environment-specific challenges to ensure stable multiplexing:

- **Debian-Based Runtime**: Both the builder and runner use `node:22-slim` to ensure `glibc` parity during gVisor checkpointing. Alpine/Musl images are avoided to prevent snapshot corruption.
- **Tini Wrapper**: The `/pause` hook and the Node.js process are wrapped in `tini` to ensure signals are forwarded correctly, preventing zombie processes during the gVisor freeze cycle.
- **Valkey Recovery**: In the event of a "split-brain" cluster state (where Substrate loses track of free workers), the `valkey-init.yaml` Job is provided to reset the metadata hash slots.
- **Hermetic Bundling**: `esbuild` is used to create zero-dependency binaries for the actor and UI, ensuring that rehydration doesn't fail due to missing `node_modules` in the restored process tree.

## Project Structure

This folder is a standalone Node.js package, decoupled from the main Google Claw repository for easy migration to the [OSS Substrate repository](https://github.com/agent-substrate/substrate).

```text
substrate/
├── src/                # Standalone Hono source code (Actor & UI)
├── manifests/          # Kubernetes & Agent Substrate YAMLs
├── scripts/            # Environment-agnostic deployment utilities
├── demo/OpenClaw/      # High-fidelity recording script
├── Dockerfile          # Self-contained build definition
├── package.json        # Decoupled dependencies (Hono, esbuild)
├── tsconfig.json       # Independent TypeScript configuration
└── README.md           # Integrated documentation & System Info
```
