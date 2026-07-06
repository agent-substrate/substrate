# NanoClaw Substrate: Decoupled Multiplexing Ultimate PoC

This demonstration showcases **Agent Substrate's** ability to multiplex high-latency logical agents onto a limited set of physical workers with sub-second rehydration. It implements a **Decoupled, Broker-Triggered Event System**, proving that agents can remain dormant in snapshots until an external trigger (WhatsApp or Cron) necessitates their activation.

---

## 📂 File Structure

The demo is contained within `demos/nanoclaw-substrate/`:

- **`broker.js`**: The central orchestrator. It maintains persistent 24/7 connections (WhatsApp + Staggered Cron Engine) and communicates with the Substrate control plane to resume/suspend actors.
- **`dashboard.js`**: A high-fidelity telemetry UI that visualizes the logical-to-physical mapping, operational efficiency metrics, and real-time reasoning logs.
- **`k8s-manifests.yaml`**: The Kubernetes definitions for the Broker, Dashboard, and the `nanoclaw-rotated-pool` worker nodes.
- **`README.md`**: This documentation.

---

## 🚀 How to Reproduce the Demo

Follow these steps to deploy and validate the Substrate multiplexing proof.

### 1. Prerequisites
- A GKE cluster with **Agent Substrate** installed.
- **gVisor** configured as a `RuntimeClass`.
- A Google Cloud Storage bucket for snapshot storage.
- A **Gemini API Key** from [aistudio.google.com](https://aistudio.google.com/app/apikey).

### 2. Configure Logic
Update the `GEMINI_KEY` variable in `broker.js` with your API key. (For security, use `REDACTED` if committing to public repos).

### 3. Deploy the Infrastructure
Apply the manifests to create the worker pool and services:
```bash
kubectl apply -f k8s-manifests.yaml
```

### 4. Initialize Orchestration Logic
Create ConfigMaps to feed the broker and dashboard logic into the cluster:
```bash
kubectl create configmap broker-logic-v11 --from-file=broker.js=broker.js -n nanoclaw-rotated
kubectl create configmap dashboard-logic --from-file=dashboard.js=dashboard.js -n nanoclaw-rotated
```

### 5. Link WhatsApp Gateway
The broker acts as a persistent multi-tenant gateway.
- Retrieve the link code from the logs:
  ```bash
  kubectl logs -l app=nano-broker -n nanoclaw-rotated | grep "PERSISTENT LINK CODE"
  ```
- Open WhatsApp > Settings > Linked Devices > Link with phone number.
- Enter the code displayed in the logs.

### 6. Validate Multiplexing
Open the Dashboard (LoadBalancer IP) and perform the following validations:

- **Staggered Cron:** Observe the "Background Task Orchestrator". Every 2, 5, and 10 minutes, logical agents will automatically rehydrate, process a pulse, and suspend back to snapshots.
- **Agentic Burst:** In your WhatsApp chat, send `/burst 5`.
  - **Observe Contention:** Watch the "Infrastructure Resource Map". You will see logical agents competing for the 2 physical workers.
  - **Verify Rehydration:** Note how agents switch between `SUSPENDED` and `RUNNING` in sub-seconds.
  - **Audit Reasoning:** Review the "Reasoning Audit Log" to see live Gemini 1.5 Flash outputs.

---

## 📊 Key Metrics for Review
- **Oversubscription Ratio:** Proof of 3:2 logical-to-physical density.
- **Worker Swap Latency:** Sub-second transition from snapshot to active processing.
- **Economic Savings:** Projected 10x cost reduction compared to always-on sandbox models.

---
*Developed for the Agent Substrate project.*
