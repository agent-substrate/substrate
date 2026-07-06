# Secret Agent Demo: Self-Suspension & Persistent Identity

This demo showcases a "Zero-Idle" agentic lifecycle using a specialized Go server. It demonstrates how Agent Substrate treats process state as a portable asset, enabling both **Self-Suspension** and **Persistent Working Memory** (via a RAM-based secret).

## Key Concepts Demonstrated
*   **Self-Suspension:** The agent process calls the Substrate Control Plane API (`SuspendActor`) automatically after responding to a request, yielding compute resources as soon as it becomes idle.
*   **Persistent Working Memory:** The agent generates a random secret in its volatile RAM on process start. Substrate preserves this exact memory state across physical resumptions, proving it doesn't just "restart" containers but actually rehydrates living processes.
*   **High-Density Multiplexing:** Demonstrates how dozens of isolated, stateful sessions can coexist on a small pool of physical hardware.

---

## 🛠️ Step-by-Step Setup

### 1. Prerequisites
*   A GKE cluster with **Agent Substrate** installed.
*   `ko` installed (for building and deploying Go images).
*   `kubectl-ate` CLI installed (can be installed via `go install ./cmd/kubectl-ate`).

### 2. Deploy the Infrastructure
Use the core installation script to build the image and apply the manifests:
```bash
./hack/install-ate.sh --deploy-demo-agent-secret
```

### 3. Scale the Worker Pool
To demonstrate oversubscription and density, we recommend scaling the physical pool to 8 workers:
```bash
kubectl patch workerpool agent-secret -n ate-demo-secret-agent-v2 \
  --type='merge' -p '{"spec":{"replicas":8}}'
```

---

## 📽️ Interaction Guide

### 1. Basic Interaction
Actors live in an **atespace**, which must exist first. Create one, then create a single actor and watch it automatically yield compute after use:
```bash
# Create the atespace (required before creating actors).
kubectl ate create atespace demo

# Create the actor
kubectl ate create actor my-agent -a demo --template ate-demo-secret-agent-v2/agent-secret

# Send a request via the Substrate Router (Note the official DNS suffix)
curl -H "Host: my-agent.demo.actors.resources.substrate.ate.dev" http://localhost:8000
```

**What to observe:**
*   In a separate terminal, run `watch kubectl ate get actors`.
*   Notice that the actor status flips to `STATUS_RUNNING` instantly upon the request, and then **automatically** flips back to `STATUS_SUSPENDED` after the 7-second "visibility linger" period.

### 2. Verify Identity Persistence
Send another request to the same actor:
```bash
curl -H "Host: my-agent.demo.actors.resources.substrate.ate.dev" http://localhost:8000
```
The "Identity" secret returned will be identical to the first response, even if the actor was resumed on a different physical pod. This proves the volatile RAM survived the hibernation cycle.

### 3. Demonstrating Massive Density (The "Wave" Swarm)
To show the scale at which Substrate can manage sessions, run this loop to populate the registry with 23 additional stateful sessions:

```bash
for i in {001..023}; do
  kubectl ate create actor session-$i -a demo --template ate-demo-secret-agent-v2/agent-secret
done
```

Now, trigger a "Wave Pulse" to show multiplexing in action. This command sends requests in groups of 8, allowing you to see the physical grid fill and clear in a rhythmic "conveyor belt" fashion:

```bash
# Pulse in 3 waves of 8
for wave in 0 1 2; do
  echo "Triggering Wave $((wave + 1))..."
  for i in {1..8}; do
    num=$(printf "%03d" $((wave * 8 + i)))
    curl -s -H "Host: session-$num.demo.actors.resources.substrate.ate.dev" http://localhost:8000 &
  done
  sleep 8 # 7s linger + 1s buffer
done
```

### 4. Clean Up
Delete the actors, then the now-empty atespace:
```bash
kubectl ate delete actor my-agent -a demo
for i in {001..023}; do kubectl ate delete actor session-$i -a demo; done
kubectl ate delete atespace demo

# Remove the demo infrastructure entirely:
./hack/install-ate.sh --delete-demo-agent-secret
```
