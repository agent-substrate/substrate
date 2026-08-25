# Integration Repositories: Structure and Naming

## Summary

Substrate is acquiring its first end-to-end integrations — workloads that *run
on* Substrate rather than demonstrate it. This document records where that code
lives, how the repositories are named, and how fixes flow back into core.

In short: trivial demos stay in the core repository, each non-trivial
integration gets one dedicated repository under the `agent-substrate`
organization, and gaps in core are closed by landing the change in core
first — never by patching core downstream.

## Motivation

Until now every in-repo example has been small enough to live beside the code it
exercises. The first real integrations are not: they carry their own container
images, dependencies, release cadence, and potentially their own maintainers.

Without a written convention, whichever repository happens to be created first
sets the precedent for everything after it. This document makes the convention
explicit instead, so that the choice is deliberate.

## Where code lives

**Stays in the core repository.** Trivial demos and keyless API exercisers — the
counter demo, and the lifecycle mocks that CI uses to drive create, resume, and
suspend. The test is roughly: no API keys, no external services, no third-party
accounts required to run it.

**Gets its own repository under `agent-substrate`.** Non-trivial, end-to-end
integrations, including their code, container images, manifests, and SDKs. One
repository per integration. These are too large to carry in core, and a
dedicated repository lets them have their own maintainers without granting
access to core.

**Lives outside the organization otherwise.** Everything under `agent-substrate`
is official and held to the standards in this document. Anyone is welcome to
build an integration and host it themselves, and we are glad when they do —
but carrying something under the project's brand that the project does not
consider official is confusing to users, so the organization holds only
integrations the project maintains.

**Not a second organization.** GitHub supports only one level of organization
parentage, so a nested org is not actually available; a sibling org would add
onboarding and access-management overhead that a small number of peer
repositories under `agent-substrate` does not.

| Thing | Where it goes |
|---|---|
| Counter demo, keyless lifecycle mocks used by CI | Core repository |
| Non-trivial end-to-end integration (code, images, manifests, SDKs) | `agent-substrate/<name>` |
| A fix or new knob in Substrate that an integration needs | PR to the core repository |
| A community-maintained integration or proof of concept | Outside the `agent-substrate` organization |

## Naming

Two cases, depending on what the repository actually is:

**Capability-named**, when it provides a general Substrate capability that
happens to have one implementation today. Prefer `execution-sandbox` over
`sandbox` (too broad) or a vendor's product name (too narrow).

**Integration-named**, when it integrates one specific *open-source* project.
Name it for the project, not the vendor behind it — `hermes`, not the name of
the organization that publishes Hermes.

Integration-naming is limited to open-source projects. An open-source project's
name refers to something anyone can read, run, and fork, so using it
descriptively claims nothing. A proprietary product's name is a brand we would
be borrowing, and borrowing it implies an endorsement or a compatibility promise
that neither side has made. When the thing being integrated is proprietary, use
a capability name instead: `execution-sandbox`, not the vendor's product
name.

Avoid:

- **Generic names** such as `sandbox` or `plugins`, which claim far more ground
  than any one repository covers.
- **Over-specific names**, which have the opposite failure: they steer people
  away from a solution that would have worked for them. A sandbox that is often
  used for code execution is still a general execution sandbox, and naming it
  `code-execution-sandbox` invites a reader with a different workload to
  conclude it is not for them. A name should be just long enough to describe
  the thing, and no longer.
- **Names that clone a proprietary API or brand**, which quietly commits the
  project to chasing someone else's naming decisions.
- **The `-integration` suffix.** Every repository in this category is an
  integration, so the suffix carries no information: `hermes`, not
  `hermes-integration`.

Even for an open-source project, the name is used descriptively and not as a
claim of affiliation: the repository README should say that the project is not
affiliated with the upstream project and that trademarks belong to their
respective owners. Clear brand and policy edge cases before the repository is
created, not after.

## Upstreaming: the core change lands first

Real integrations surface real gaps in core. Building a long-running agent on
Substrate turned up the need for configurable timeouts and golden-snapshot
warmup ([#487](https://github.com/agent-substrate/substrate/pull/487)), and ran
into suspend-safe actor networking
([#465](https://github.com/agent-substrate/substrate/issues/465)).

Repositories under `agent-substrate` do not carry those gaps as patches. An
integration repository builds and runs against core as released: no fork of
core, no vendored patch, no dependency on an unmerged change. When an
integration needs something core does not do, the core change lands first and
the integration then depends on a released version that has it.

We are our own upstream here — the same project hosts both repositories, so
there is no window in which a patch has to wait downstream for someone else to
act. An official integration that depends on a change missing from our own core
is embarrassing rather than merely inconvenient, so the rule is absolute rather
than a preference. Independent proofs of concept, which live outside the
organization, are free to carry whatever patches they need; that freedom is part
of why they live outside it.

The corollary is a design preference for core: when core behavior blocks an
integration, prefer making that behavior **configurable with defaults
unchanged** over special-casing the caller. That is what keeps "land it in core
first" a short path rather than a blocking one.

## Worked examples

These two validate the convention rather than merely following it:

- **`agent-substrate/execution-sandbox`** — capability-named. A sandboxed
  execution service built on Substrate, in the spirit of existing sandbox
  products but not modeled on any one of their APIs. The name stops at the
  capability on purpose: code execution is one workload it serves, not the
  boundary of what it is.

- **`agent-substrate/always-on-agent`** — a connection-holding agent: a
  multi-tenant gateway plus a suspendable per-conversation actor. Its first
  implementation is built on a third-party agent runtime, but the capability
  outlives any one runtime and the repository is capability-named rather than
  named for that runtime. It is the edge case the open-source-only rule above is
  meant to catch.

## Not settled yet

One open question for the maintainers, deliberately left out of scope here so it
does not block the first repositories:

- **Repository creation and access.** Who creates integration repositories, and
  who grants per-integration maintainer access.
