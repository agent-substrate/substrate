# OpenClaw on Substrate: "Liquid Hardware" Demo Script

This document provides a structured narrative for recording the OpenClaw-on-Substrate PoC demonstration.

## **Metadata**
*   **Environment**: `http://<YOUR_DASHBOARD_IP>`
*   **Logical Identities**: Claw-Luna (Blue 🟦), Claw-Mars (Pink 🟪), Claw-Nova (Gold 🟨)
*   **Physical Constraint**: 2 Worker Pods (Replica Pool)
*   **Core Value**: 1.5x Hardware Oversubscription without state loss.

---

## **Phase 1: The Static Constraint (Setup)**
*   **Action**: Open the dashboard. Ensure history is clear (Click **Reset Dashboard** if needed).
*   **Narrative**: 
    > "Welcome to the OpenClaw Substrate PoC. Today we're demonstrating the next evolution of AI infrastructure: **Liquid Hardware**. 
    > 
    > Look at the bottom of the screen. We have **three logical agents**—Luna, Mars, and Nova—but we're only paying for **two physical worker pods**. In a traditional cloud setup, one agent would be permanently offline or require a slow cold-boot. With Substrate, hardware flows where the tasks are."

## **Phase 2: Individual Process Rehydration**
*   **Action**: Click **Give a task**. Wait for the agent to transition to `RESUMING`.
*   **Narrative**:
    > "I'll assign a task to Claw-Luna. Watch the 'Actors' panel. Luna is currently **RESUMING**. 
    >
    > Substrate is reaching into Google Cloud Storage, pulling Luna's exact memory snapshot, and rehydrating it into one of our two worker pods. This isn't just starting a container—it's restoring a live process state in about 5 seconds."
*   **Action**: Wait for task to move to `RUNNING`. Point to the **Live Logs**.
    > "Now Luna is **RUNNING**. You can see the live telemetry in the pod log. Once the task completes, Substrate will automatically checkpoint the state and free the pod for the next agent."

## **Phase 3: High-Concurrency Contention (The Pulse)**
*   **Action**: Click **Pulse (10 Tasks)**.
*   **Narrative**:
    > "Now, let's put the system under pressure. I'm assigning 10 parallel tasks across all three agents. 
    >
    > Watch the dashboard come alive. With 3 agents fighting for 2 slots, Substrate is performing a high-speed multiplex. Luna, Mars, and Nova are constantly swapping positions. When one agent finishes a short 3-second job, Substrate immediately 'hot-swaps' it for a queued agent."
*   **Visual Cue**: Point out the **`SUSPENDING` (Orange)** and **`RESUMING` (Yellow)** badges flashing as the rotation happens.

## **Phase 4: Latency & Cost Efficiency**
*   **Action**: Scroll to the **Approximate Cost** card.
*   **Narrative**:
    > "This fluidity is made possible by our snapshot performance. We're currently seeing a **1.2-second suspend latency**. While resume is currently 5 seconds from a cold GCS fetch, moving this to a local SSD cache would bring us to sub-second rehydration.
    > 
    > The business impact is clear: We are hosting **1.5x more agents** on the same physical hardware, reducing our simulated OpenClaw infrastructure costs by 33% while maintaining 100% state persistence. 
    >
    > This is Liquid Hardware. This is OpenClaw on Substrate."

---

## **Recording Tips**
1.  **Cursor Movement**: Use slow, deliberate mouse movements to highlight the panels you are discussing.
2.  **Timing**: Don't rush Phase 2. Let the viewer see the `RESUMING` -> `RUNNING` transition clearly before hitting the Pulse.
3.  **The Reveal**: Ensure the **Live Logs** are visible during the Pulse so the viewer sees the agent ownership (telemetry) switching on the same pod name.
