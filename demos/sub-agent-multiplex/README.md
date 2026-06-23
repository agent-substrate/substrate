# Substrate Multiplex Demo: NanoClaw 1.5x Overcommit

This demo demonstrates the extreme efficiency gains possible with Google Substrate by multiplexing **3 logical NanoClaw agents** onto **2 physical substrate workers** (1.5x density ratio).

## System Information

- **Agent Framework**: NanoClaw (v2.x)
- **Source**: `github.com/nanocoai/nanoclaw`
- **Substrate Mode**: Multi-Actor Multiplexing (1.5x oversubscription)
- **Runtime**: Bun (Node.js compatible) inside Debian Slim
- **Isolation**: gVisor (runsc)

## What this shows

- **High-Density Multiplexing**: Three logical agent identities running on only two physical pods (1.5x oversubscription).
- **State Persistence**: A `taskCounter` maintained in the Node.js process memory survives multiple suspend/resume cycles.
- **Dynamic Rotation**: Agents finish work at different times (3-6s), forcing Substrate to constantly rotate pod ownership.
- **Visual Identity Tracking**: Color-coded agents (Blue/Pink/Gold) and live log tailing to make infrastructure sharing intuitively obvious.

## Audience

This guide is intended for engineers exploring Agent Substrate for hosting large-scale agentic workloads where cost-efficiency and stateful rehydration are critical.

## Prerequisites

- **Agent Substrate Cluster**: A Kubernetes cluster with Substrate installed.
- **Docker**: For building and pushing the unified actor/UI image.
- **GCS Bucket**: Configured for Substrate state snapshots (e.g., `gs://snapshot-substrate-gke-ai-eco-dev/`).
- **kubectl & kubectl-ate**: The Substrate CLI tool for managing logical actors.

## Components

| Path | Purpose |
|---|---|
| `workload/agent.ts` | The workload: A NanoClaw/Hono server with persistent memory state. |
| `ui/demo-ui.ts` | The dashboard: A Node.js backend providing live logs, task queueing, and visual tracking. |
| `sub-agent-multiplex.yaml.tmpl` | Kubernetes manifests for ActorTemplates and WorkerPools. |
| `Dockerfile` | Unified OCI image containing both the actor workload and the dashboard UI. |

## How to Run

### 1. Provision Hardware
Scale the physical `WorkerPool` to the desired replica count (2 for this demo):
```bash
kubectl apply -f sub-agent-multiplex.yaml
```

### 2. Deploy Logical Agents
Create the three "fun-named" actors using the Substrate CLI.
```bash
kubectl-ate create actor agent-luna-v12 --template sub-agent/sub-agent-agent
kubectl-ate create actor agent-mars-v12 --template sub-agent/sub-agent-agent
kubectl-ate create actor agent-nova-v11 --template sub-agent/sub-agent-agent
```

### 3. Launch the Dashboard
The dashboard runs as a standard Kubernetes Deployment with a LoadBalancer.
```bash
kubectl apply -f demo-ui.yaml
```

## Drive the Demo

Open the dashboard and use the following interaction patterns:

- **Pulse (Manual Wakeup)**: Trigger tasks across the registry. Watch the **colored icons** rapidly cycle through the 2 worker slots.
- **Live Logs**: Observe the Fleet Decision Stream. You will see agent registrations and task dispatching events proving that physical hardware is being recycled in real-time.
- **Cron Tracker**: Observe real-time countdowns as the automated schedule triggers orchestration events.

## Integrating a Real LLM API

Integrating an LLM into a NanoClaw logical actor is straightforward. Because Substrate persists the **entire process memory**, any in-memory conversation history or KV-cache will survive multiple suspend/resume cycles without requiring an external database.

### 1. Add the LLM SDK
Add your preferred SDK (e.g., OpenAI or Anthropic) to the `package.json`:
```bash
npm install openai
```

### 2. Update the Actor Logic
Modify `workload/agent.ts` to initialize the client and maintain a local chat history:
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
  // ... process response
});
```
