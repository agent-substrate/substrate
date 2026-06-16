# Valkey Operations Handbook

Operational reference for Agent Substrate's persistence tier — the Valkey
cluster that stores actor and worker state, plus the in-process worker
cache that sits in front of it inside `ate-api-server`.

This handbook is for operators, on-call engineers, and contributors who
need to reason about the storage tier without first reading the code. It
is intentionally narrow: it covers what's deployed, how state moves
through it, and what to do when it breaks. It does not retell Valkey's
own documentation.

## Pages

- [`topology.md`](./topology.md) — what's deployed, how the pieces fit
  together, sizing math for MVP and target scale, identity / TLS wiring,
  and the configuration knobs we have not set yet.
- [`lifecycle.md`](./lifecycle.md) — the actor state machine, the four
  lifecycle workflows (Create, Resume, Suspend, Pause), the
  worker-cache subscription model, and the per-actor scheduling
  eligibility rules.
- [`operations.md`](./operations.md) — failure modes with inline
  recovery for each, common admin operations, and the short list of
  open operational risks.

## Conventions

- Source citations use **path + symbol name** (e.g.
  `cmd/ateapi/internal/store/store.go` (`Interface`)), never line
  numbers. Line numbers drift on every refactor; symbol references
  survive.
- Code examples assume `valkey-cli` is invoked with the full TLS flag
  set; for brevity the pages use a `vcli` alias defined in
  [`operations.md`](./operations.md).
- Diagrams are mermaid blocks; GitHub renders them inline.

## Updating

Update the handbook whenever you learn something new about how the
storage tier behaves — new failure modes, recovery steps that worked
(or didn't) during a real incident, configuration changes, surprising
behavior from upgrades. A handbook that captures the *why* of a past
decision is more valuable than one that perfectly describes the
current code, because the code is already self-describing.

Substantive changes belong in their own PR rather than folded into a
feature change, so the doc review gets focused attention. Trivial
touch-ups (renamed flag, updated path) can ride with the code change
that prompted them.
