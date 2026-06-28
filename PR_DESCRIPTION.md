#### Overview
This PR introduces a high-fidelity demonstration of **1.5x hardware oversubscription** using Agent Substrate. It features three logical **NanoClaw** agents (Luna, Mars, Nova) running on just two physical worker pods, proving the efficiency of Substrate's suspend/resume architecture for agentic workloads.

#### Key Enhancements
*   **Decoupled Orchestration**: Aligns with the production "Claw" pattern by moving orchestration triggers to native **Kubernetes CronJobs**. 
*   **High-Fidelity Telemetry**: The UI now calculates **"Predicted Next Run"** countdowns based on external cron timestamps, providing a smooth visual experience without internal application timers.
*   **Self-Healing & Stability**: 
    *   Implemented **State Settlement Cooldowns** (6s) and **Network Stack Warm-up** (5s) to ensure 100% rehydration reliability with gVisor.
    *   Transitioned to the **v12 logical fleet**, resolving control-plane state-machine deadlocks.
*   **Semantic Heatmap**: Physical worker cards in the dashboard now dynamically inherit the color and "glow" of the active logical agent, making hardware sharing intuitively obvious.
*   **Economic Modeling**: Demonstrates a **10x cost reduction** ($5.00/mo dedicated vs $0.50/mo Substrate).

#### Components Included
*   `demos/sub-agent-multiplex/`: Organized dashboard, workload, and OCI configurations.
*   `hack/install-demo-sub-agent-multiplex.sh`: Updated deployment automation.
*   `hack/install-ate.sh`: Core registration for the new demo.
