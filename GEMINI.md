# Project Conventions & Mandates

## Terminology
- **NO "Singapore"**: Do not use the word "Singapore" in the code, logs, or UI to describe architecture.
- **Functional Naming**: Use descriptive terms for the demo components:
  - **Fleet Management Broker**: The centralized orchestration service.
  - **Managed Fleet Flow**: The decoupled orchestration pattern.
  - **Local Infrastructure Proxy**: The Substrate-aware layer within the actor container.
  - **End User Agent App**: The Substrate-ignorant agent logic.
  - **Synthetic Injection**: The process of pushing external triggers into the local agent message queue.

## UI Standards
- **Typography**: Professional Bold/Mono styling; italics forbidden.
- **Palette**: 
  - Luna (Cyan: #79c0ff)
  - Mars (Pink: #ff79c6)
  - Nova (Yellow: #f1fa8c)
  - Status: Running (Green), Working (Pink), Error (Red), Retrying (Orange), Healing (Red/Alert).
