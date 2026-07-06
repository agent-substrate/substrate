# NanoClaw Reborn: Substrate Multiplexing Ultimate PoC

This demo showcases **Agent Substrate**'s definitive ability to multiplex high-latency logical agents onto a limited set of physical workers with sub-second rehydration. It transforms the monolithic "always-on" agent sandbox model into a **Decoupled, Broker-Triggered Event System**.

## 🚀 Key Features & Capabilities

### 1. Decoupled Broker Architecture
The system utilizes a standalone Kubernetes-based **External Broker** that handles all persistent connections and state management. Logical agents (`luna`, `mars`, `nova`) remain in a dormant, suspended state until the broker issues a `kubectl-ate resume` signal based on external events.

### 2. Persistent WhatsApp Bridge
A 24/7 multi-tenant bridge that allows users to interact with the Substrate fleet directly from their mobile devices. The bridge maintains a stable WebSocket connection, ensuring that agents are only "woken up" when a genuine message is received.

### 3. Staggered Cron Engine (Autonomous Fleet)
To demonstrate persistent multiplexing without user intervention, the broker runs a staggered cron engine:
- **Agent Luna:** Pulses every 2 minutes.
- **Agent Mars:** Pulses every 5 minutes.
- **Agent Nova:** Pulses every 10 minutes.
This creates a continuous "Workflow Baseline" that tests logical-to-physical density over time.

### 4. Agentic Burst Mode
Triggered via the `/burst [count]` command, this mode simulates a production "flood" of reasoning-heavy jobs:
- **Security Forensics:** Vulnerability scanning in Dockerfiles.
- **Architecture Tracing:** Dependency graph and circular import analysis.
- **Compliance Auditing:** GDPR and data-sovereignty contract reviews.
These tasks force agents into deep "Thinking" modes, showcasing Substrate's ability to handle high-latency reasoning while maintaining high physical density (3 agents per 2 physical workers).

### 5. Precision Telemetry Dashboard (V1.5.2)
A real-time visualization suite that provides:
- **Oversubscription Efficiency:** A live ratio of logical active time vs. physical resource usage.
- **Physical Resource Map:** Dynamic highlighting of logical agents "landing" on physical pods.
- **Reasoning History:** Full transparency into the "Thinking" and "Tools" used by the live Gemini 1.5 Flash model.

## 🏗 Components
1. **Broker:** A Node.js orchestrator that bridges WhatsApp/Cron triggers to Substrate orchestration.
2. **Dashboard:** A high-fidelity telemetry UI showing cluster state and reasoning history.
3. **Agent:** A lightweight Node.js reasoning engine running inside a gVisor sandbox.

## 🛠 Prerequisites
- A Kubernetes cluster with **Agent Substrate** installed.
- **gVisor** configured as a `RuntimeClass`.
- Google Cloud Storage bucket for snapshot storage.
- A **Gemini API Key** (for real-world reasoning).

## 🚀 Deployment

1. **Configure the Namespace:**
   Ensure the `nanoclaw-rotated` namespace exists.

2. **Apply Manifests:**
   ```bash
   kubectl apply -f k8s-manifests.yaml
   ```

3. **Initialize Logic:**
   Create ConfigMaps for the broker (v66+) and dashboard logic (V1.5.2):
   ```bash
   kubectl create configmap broker-logic-v11 --from-file=broker.js=broker.js -n nanoclaw-rotated
   kubectl create configmap dashboard-logic --from-file=dashboard.js=dashboard.js -n nanoclaw-rotated
   ```

4. **Link WhatsApp:**
   - Check the broker logs for the **Persistent Link Code**: `kubectl logs -l app=nano-broker -n nanoclaw-rotated`
   - In WhatsApp: Settings > Linked Devices > Link with phone number.
   - Once linked, the session persists through broker restarts.

5. **Access Dashboard:**
   Get the LoadBalancer IP for `nano-dashboard` and open it to monitor **Oversubscription Efficiency**.

## 📱 Commands
- Send any message: Triggers a single agent pulse with a reasoning task.
- `/burst <N>`: Dispatches <N> parallel **Agentic Analysis** tasks to demonstrate worker contention and multiplexing.

---
*Developed as part of the Agent Substrate project to prove the scalability of AI agent infrastructure.*
