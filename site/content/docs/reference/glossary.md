---
title: "Glossary"
linkTitle: "Glossary"
weight: 3
repo_source: "docs/glossary.md"
card_icon: "fa-solid fa-book"
aliases:
  - "/docs/concepts/glossary/"
description: >
  Definitions of core terms, resources, and components across Agent Substrate.
---

{{% include-file %}}

## Naming & Prefix {#ate}

- **ATE vs. Agent Substrate**: **Agent Substrate** is the name of the overall platform and runtime, while **`ate`** (originally *Agent Task / Execution Engine*) is the internal prefix used across Kubernetes namespaces (`ate-system`), control-plane binaries (`ate-api-server`, `atecontroller`), node and pod daemons (`atelet`, `ateom`), networking components (`atenet`, `atunnel`), HTTP routing headers (`ate-target-actor`), and the `kubectl-ate` CLI plugin.

