# NanoClaw Reborn: Substrate Multiplexing Demo

This demo showcases **Agent Substrate**'s ability to multiplex multiple logical agents onto a limited set of physical workers with sub-second rehydration.

## Features
- **WhatsApp Integration:** Control agents via WhatsApp messages.
- **Multiplexing:** 3 Logical Agents (`luna`, `mars`, `nova`) sharing 2 Physical Workers.
- **Sub-second Rehydration:** Agents are suspended when idle and resumed instantly upon receiving a task.
- **Live Dashboard:** Real-time visualization of agent-to-worker IP mapping and decision streams.

## Components
1. **Broker:** A Node.js service that bridges WhatsApp to Substrate orchestration.
2. **Dashboard:** A real-time UI showing the cluster state and reasoning history.
3. **Agent:** A lightweight Node.js reasoning engine that runs inside a gVisor sandbox.

## Prerequisites
- A Kubernetes cluster with **Substrate** installed.
- **gVisor** configured as a RuntimeClass.
- Google Cloud Storage bucket for snapshots.

## Deployment

1. **Configure the Namespace:**
   Ensure the `nanoclaw-rotated` namespace exists or update the manifests.

2. **Apply Manifests:**
   ```bash
   kubectl apply -f k8s-manifests.yaml
   ```

3. **Initialize Logic:**
   Create ConfigMaps for the broker and dashboard logic:
   ```bash
   kubectl create configmap broker-logic --from-file=broker.js=broker.js -n nanoclaw-rotated
   kubectl create configmap dashboard-logic --from-file=dashboard.js=dashboard.js -n nanoclaw-rotated
   ```

4. **Link WhatsApp:**
   - Check the broker logs for the pairing code: `kubectl logs -l app=nano-broker -n nanoclaw-rotated`
   - In WhatsApp: Settings > Linked Devices > Link with phone number.
   - Enter the code.

5. **Access Dashboard:**
   Get the LoadBalancer IP for `nano-dashboard` and open it in your browser.

## Commands
- `Ready`: Triggers a single agent pulse.
- `/burst <N>`: Triggers <N> parallel tasks to demonstrate worker contention and multiplexing.
