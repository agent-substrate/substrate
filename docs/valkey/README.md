# Valkey Operations Handbook

This handbook documents how Agent Substrate's Valkey deployment behaves under
normal conditions, how it fails, and how to recover it. It is written for
operators, on-call engineers, and future maintainers who need to reason about
the storage tier without first reading the application code.

## Scope

The handbook is intentionally **API-agnostic**. The Substrate API surface will
change; the operational properties of a six-node Valkey Cluster — hash-slot
ownership, asynchronous replication, AOF durability windows, failover
behavior, cluster coverage rules — will not. Sections that touch the Substrate
API at all do so only to ground a failure mode in a concrete code path; the
recovery guidance should remain valid as the API evolves.

In scope:

- Topology and configuration of the current Valkey deployment.
- Critical read/write paths through the storage tier.
- Every failure mode we have identified, with detection signals and recovery
  procedures.
- Risk register: known sharp edges, missing safeguards, and the tradeoffs of
  the levers available to tighten them.

Out of scope:

- Tutorials on Valkey itself. Link to upstream docs rather than restate them.
- Application-level behavior that does not interact with the storage tier.
- Alternative storage backends. The evaluation in
  [issue #12](https://github.com/agent-substrate/substrate/issues/12) is a
  separate document.

## How to read this handbook

The handbook is organized so that each page stands alone. There is no
required reading order. During an incident, jump straight to the failure mode
page that matches the symptom. Outside an incident, the topology and critical
paths pages are the most useful starting points for building a mental model.

## Contributing

**Update this handbook whenever you learn something new about how Valkey
behaves in this deployment.** That includes:

- New failure modes observed in development, staging, or production.
- Recovery steps that worked (or did not) during a real incident.
- Configuration changes to `manifests/ate-install/valkey.yaml` or the
  cluster-client wiring in `cmd/ateapi/main.go`.
- Surprising behavior from Valkey upgrades, client-library upgrades, or
  Kubernetes platform changes.

Prefer a dedicated handbook PR over folding a substantive update into a
feature PR — it keeps reviews focused and gives the handbook change its own
review attention. Trivial touch-ups (a renamed flag, an updated path) can
ride with the code change that prompted them.

When in doubt, write it down. A page that captures the *why* of a past
decision is more valuable than a page that perfectly describes the current
code, because the code is already self-describing.

## Future direction: agent-assisted drift detection

The cost of a living handbook is drift: code changes, the page does not, and
trust in the document erodes. The temptation is to close that gap with
automation — webhooks that fire on push, agents that regenerate the handbook
from the current source of truth. We do not intend to take that path.

The value of this handbook is not the factual description of what the code
does — that is derivable from the code itself and can be regenerated on
demand. The value is the synthesized judgment about failure modes, recovery
tradeoffs, and the institutional memory of what has bitten us before.
Autonomous agent-authored commits to operational documentation are a poor
fit for that work: agents are bad at distinguishing "I am confident about
this operational claim" from "this sentence reads plausibly," and a single
wrong-but-confident handbook entry quietly poisons reviewer trust in every
other entry.

The shape we do intend to pursue:

- A scheduled or webhook-triggered job that diffs recently touched code
  paths against the handbook sections that reference them, and opens an
  **issue** (not a PR) flagging sections that may be stale, optionally with
  a suggested draft for a human to curate.
- Agent-assisted *drafting* of new sections, where a human prompts the agent
  with the relevant context and reviews the output before it lands.
- No autonomous commits to anything under `docs/valkey/`.

This direction is recorded here so that future contributors do not have to
re-litigate the decision when the temptation to "just hook up an agent"
returns.

## Planned subdocuments

The following pages will be added under this directory as the handbook is
built out. This list is the working table of contents; update it as pages
land or as new topics surface.

- [`topology.md`](./topology.md) — current deployment shape, scaling
  projection to target scale, sharding model, TLS / identity wiring, and
  the explicit go/no-go verdict for MVP and target scale.
- [`actor-lifecycle.md`](./actor-lifecycle.md) — actor state machine,
  per-transition sequence diagrams with Valkey round-trip counts, locking
  model, stranded-state recovery, and a lifecycle-specific risk register.
- [`admin-operations.md`](./admin-operations.md) — non-critical-path
  operations (`ListActors`, `ListWorkers`, syncer-driven worker
  bookkeeping, lock primitives, debug utilities) with special attention
  to where they cross into the critical path.
- [`critical-questions.md`](./critical-questions.md) — open
  architectural questions specific to the storage tier, each tracked
  with its design space, constraints, and the handbook pages that
  would change once decided.
- [`failure-modes.md`](./failure-modes.md) — catalog of Valkey-deployment
  failure modes (pod, data, network, security, and Kubernetes-level)
  with detection signals, blast radius, recovery semantics, and a
  cross-cutting table mapping each failure to its actor-lifecycle
  impact.
- [`recovery-procedures.md`](./recovery-procedures.md) — runbook-style
  procedures (R-1 through R-14) keyed to the failure modes above, with
  trigger / goal / commands / verification / postmortem capture for
  each. Written for the on-call engineer at 3 am.
- [`risk-register.md`](./risk-register.md) — consolidated catalogue of
  every sharp edge surfaced across the handbook (R-1 through R-23),
  each with severity, status, MVP / target-scale impact, mitigation
  levers and their costs, and recommended action.
