---
title: "Multi-Template WorkerPool"
linkTitle: "Multi-Template WorkerPool"
weight: 4
estimated_time: "5 mins"
card_icon: "fa-solid fa-layer-group"
description: >
  Share a single WorkerPool across multiple ActorTemplates and atespaces using label selectors.
---

# Multi-Template Demo

This demo shows that **two different actor templates running two different binaries
can share a single `WorkerPool` — even when the templates live in different atespaces**.

Each template gates on the pool via `workerSelector`, a label selector matched
against the pool's labels — pool selection is cluster-wide, not scoped by atespace
or namespace.

## Prerequisites

- A k8s cluster with Agent Substrate installed as in the [Quickstart](../../README.md#quickstart-development)
  (with `ATE_INSTALL_KIND=true` exported in your shell) or [GKE Quickstart](../../README.md#gke-quickstart-development).
- A snapshot location set at install (`SNAPSHOT_LOCATION`). The demo stores its snapshots there.

## How to Run on Agent Substrate

### 1. Build and Deploy

```bash
go run ./cmd/ate-setup deploy demo multi-template
```

This command will:
- Build the `counter` and `fspersist` images using `ko`.
- Create one `WorkerPool` (`shared-pool`) in the `ate-demo-multi-template-pool`
  namespace (`multi-template.yaml.tmpl`).
- Create 2 atespaces — `ate-demo-multi-template-counter` and
  `ate-demo-multi-template-fspersist` — and an actor template in each:
  `counter` (`counter-template.yaml.tmpl`) and `fspersist`
  (`fspersist-template.yaml.tmpl`), both selecting the pool via the same
  `workerSelector` label and applied with `kubectl ate create actor-template`.
- Wait until both templates' golden snapshots are built.

### 2. Create one actor per template

Each actor goes in its template's atespace — `--template` names the template,
resolved in the actor's atespace — and their DNS names embed that atespace:

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

# Create two actors from different templates, one per atespace.
kubectl ate create actor c1 -a ate-demo-multi-template-counter --template counter
kubectl ate create actor f1 -a ate-demo-multi-template-fspersist --template fspersist
```

### 3. Proxy to the actors

Start a local proxy for each actor:

```bash
kubectl ate proxy actor c1 8000:80 -a ate-demo-multi-template-counter &
kubectl ate proxy actor f1 8001:80 -a ate-demo-multi-template-fspersist &
```

## How to Use

When you send an HTTP request through the proxy, `atenet-router` automatically activates (resumes) the actor onto an available worker pod and routes the traffic:

```bash
# counter binary
curl -s http://localhost:8000
# -> preserved memory count: 1 | preserved file counter: 1 | preserved rootfs counter: 1

# fspersist binary
curl -s http://localhost:8001
# -> pod: <ip>
#    --- history ---
#    pod=<ip> | count=0 | time=<timestamp>
```

Confirm both actors landed on workers in the one `shared-pool`:

```bash
kubectl ate get workers
```

The `counter` actor increments its in-memory, durable-volume (`/home/counter/a.txt`), and rootfs (`/rootfs-counter.txt`) counters on each request, while `fspersist` prepends a line to its history file on each request. Suspending and re-requesting an actor preserves that state across the snapshot/restore cycle:

```bash
kubectl ate suspend actor f1 -a ate-demo-multi-template-fspersist
curl -s http://localhost:8001  # history persists; count keeps climbing
```

## How to Uninstall

Remove the demo — this deletes the actors (suspending running ones first), the
templates, both atespaces, and then the pool and its namespace:

```bash
go run ./cmd/ate-setup delete demo multi-template
```
