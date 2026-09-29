---
title: "Overview"
linkTitle: "Overview"
weight: 1
aliases:
  - "/docs/overview/"
description: >
  A high-density, low-latency execution runtime for stateful agent workloads on Kubernetes.
---

**Agent Substrate** (internally prefixed **`ATE`** across binaries and CRDs) is a secure-by-default execution runtime engineered to run millions of stateful sandboxes on Kubernetes at **10x+ higher density** than standard container runtimes. Because autonomous agents spend most of their lifecycle idle waiting on user input, model responses, or external events, dedicating a full Kubernetes Pod to every session leaves clusters heavily underutilized.

Substrate decouples the **Actor** (a stateful agent or workload instance addressed within an **atespace**) from the **Worker** (a pre-warmed Kubernetes Pod in a `WorkerPool`). Idle actors are checkpointed to object storage and evicted from compute; when a request arrives at **atenet**, the control plane assigns a ready worker and the node-level **atelet** supervisor coordinates with **ateom** inside the worker Pod to restore the actor's full memory and filesystem state in **under 500ms**—taking the Kubernetes control plane out of the critical path.

<figure class="as-diagram-card as-overview-arch">
  <div class="as-diagram-card__top">
    <span class="mono as-diagram-card__label">Agent Substrate Platform (ate-system)</span>
    <span class="as-diagram-card__meta">top-level mental model · control plane, atelet &amp; worker pod</span>
  </div>
  <div class="as-diagram-card__row">
    <div class="as-diagram-node as-diagram-node--blue">
      <span class="as-diagram-node__tag as-diagram-node__tag--blue">Agent Substrate Control &amp; Data Plane</span>
      <span class="mono as-diagram-node__title">ate-api-server + atenet</span>
      <span class="mono as-diagram-node__sub">Atespace registry · routes ate-target-actor · schedules Millions of Actors</span>
    </div>
    <div class="as-diagram-edge">
      <span class="mono as-diagram-edge__label">assigns &amp; wakes</span>
      <div class="as-diagram-edge__line"></div>
    </div>
    <div class="as-diagram-node as-diagram-node--highlight">
      <span class="as-diagram-node__tag as-diagram-node__tag--green">Kubernetes Node DaemonSet</span>
      <span class="mono as-diagram-node__title">atelet (Node Supervisor)</span>
      <span class="mono as-diagram-node__sub">streams GCS/S3 snapshots · drives checkpoint &amp; restore (&lt;500ms)</span>
    </div>
    <div class="as-diagram-edge">
      <span class="mono as-diagram-edge__label">gRPC to pod</span>
      <div class="as-diagram-edge__line"></div>
    </div>
    <div class="as-diagram-node as-diagram-node--stack">
      <span class="as-diagram-node__tag">Pre-Warmed Worker Pod (WorkerPool)</span>
      <span class="mono as-diagram-node__title">Worker (ateom + Sandbox)</span>
      <span class="mono as-diagram-node__sub">ateom herder · atunnel mTLS · gVisor / Kata microVM hosting Actor</span>
    </div>
  </div>
  <figcaption class="as-diagram-card__caption">
    <span>How Agent Substrate, <code>atelet</code>, and a <code>Worker</code> interact — idle actors hibernate to snapshot storage and resume into any warm worker sandbox on demand.</span>
    <a href="../../concepts/architecture/">Read full architecture &rarr;</a>
  </figcaption>
</figure>

* **Core Platform Jargon at a Glance:** Each actor lives in an `atespace` (`<atespace>/<actor>`), traffic enters via `atenet`, node supervision and snapshot streaming are handled by `atelet`, and sandbox execution inside each warm `Worker` Pod is managed by `ateom`. See the **[Glossary](../../reference/glossary/)** for complete definitions.
* **Framework Agnostic:** Runs standard OCI containers inside zero-trust sandboxes—supporting ADK, LangChain, MCP servers, and stateful coding environments out of the box.
* **Ready to try it?** Jump straight to **[Quickstart](../quickstart/)** to launch your first actor on Kind or GKE, or explore the **[Architecture](../../concepts/architecture/)**.
