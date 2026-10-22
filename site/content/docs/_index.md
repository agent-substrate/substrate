---
title: "Documentation"
linkTitle: "Documentation"
weight: 20
menu:
  main:
    weight: 20
---

Agent Substrate runs agent-like workloads on Kubernetes at higher density and
lower latency than Kubernetes alone. It maps a large set of *actors*
(applications such as agents) onto a smaller set of ready *workers*
(Kubernetes Pods), and takes the Kubernetes control plane out of the critical
path when actors are created, suspended, and resumed.

Start with the [Overview](overview/), then read about the
[Architecture](concepts/architecture/).
