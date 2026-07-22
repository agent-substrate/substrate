# Security Policy

Agent Substrate is in a pre-release (preview) state. This policy is
intentionally minimal for that stage and will be revisited as part of the
General Availability (GA) milestone, see [Road to GA](#road-to-ga) below.

## Reporting a Vulnerability

**Do not open a public GitHub issue or pull request for security
vulnerabilities.**

Use [GitHub Private Vulnerability Reporting](https://github.com/agent-substrate/substrate/security/advisories/new)
to report privately. This is the only supported reporting channel; there is
no security mailing list at this stage. There is no bug bounty, and this
project is [not eligible](README.md) for the
[Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security).

Please include:

- The affected component and version (commit SHA).
- Steps to reproduce, or a proof of concept.
- The potential impact, and mitigations if known.

### When to report

- You think you found a potential security vulnerability in Agent Substrate.
- You are unsure whether an issue affects Agent Substrate.
- You found a vulnerability in a project Agent Substrate depends on and it
  is exploitable through Agent Substrate. If that project has its own
  disclosure process, please report it there first.

### When not to report

- You need help securing or tuning a deployment (open a regular issue or
  discussion).
- The issue is not security related.

## What Counts as a Vulnerability

Agent Substrate multiplexes many actors onto a smaller pool of shared
workers, so its most important security boundaries are between actors and
between actors and the platform. Reports in these classes are the most
valuable, listed roughly from most to least severe:

- **Cross-actor access**: an actor reading or modifying another actor's
  state (memory, filesystem, working memory) on a shared worker, or acting
  across atespace boundaries.
- **Snapshot integrity and confidentiality**: reading another actor's
  suspend/resume snapshot, or resuming an actor from a tampered snapshot.
- **Routing and identity**: traffic intended for one actor delivered to
  another, or forging session identity (for example session JWTs).
- **Control-plane authentication bypass**: reaching `ateapi` RPCs without
  valid credentials (mTLS or JWT auth modes), or misusing workerpool client
  certificates.
- **Actor-to-platform escalation**: an actor influencing `atelet`, `ateom`,
  or the control plane beyond its own lifecycle.
- **Network isolation bypass**: an actor sending or receiving traffic that
  `atenet` policy was meant to prevent.

### What usually does not qualify

- Denial of service against workloads deployed without resource limits.
- Issues that require cluster-admin permissions or root on the node; that
  level of access already controls the platform.
- Vulnerability scanner or CVE reports about dependencies without an
  exploitable path through Agent Substrate (see
  [Dependencies](#dependencies)).
- Sandbox escapes contained entirely within gVisor or Kata Containers;
  report those upstream (see [Scope](#scope)).

If you are unsure whether something qualifies, report it privately anyway.

## Triage and Response

While the project is in preview there is no dedicated security team or
response committee. Reports are triaged and fixed by the project maintainers,
who own the repository.

What to expect at this stage:

- We aim to acknowledge reports within a few business days, but there are no
  formal response or fix time SLOs yet.
- Fixes are best effort and land on `main`. All valid reports will be
  patched by GA.
- There is no embargo process and no pre-disclosure notification to
  downstream users or vendors. Details stay in the private advisory until a
  fix is merged.

We ask reporters to keep vulnerability details private until an advisory is
published; we prefer to disclose as soon as a fix is available.

## Good-Faith Research

We will not pursue legal action against research conducted in good faith and
in line with this policy. Good faith means:

- Test only against clusters and deployments you own or are authorized to
  test.
- Do not access, modify, or destroy data that is not yours; use test
  workloads to demonstrate impact.
- Do not degrade service for others while testing.

This policy covers the Agent Substrate open source project only. It does not
authorize testing of anyone else's deployment of Agent Substrate; those are
governed by the respective operator's own policies.

## Dependencies

Dependency vulnerabilities are tracked with `govulncheck` in CI, and fixes
are applied by updating the dependency on `main`. If a dependency
vulnerability is exploitable through Agent Substrate, report it to us
privately as well as upstream; otherwise a regular issue or pull request
bumping the dependency is enough.

## Supported Versions

There are no stable releases yet. Security fixes are applied to `main` only;
there are no release branches and no backports.

## Scope

In scope: the Agent Substrate control plane (`ateapi`), node supervisor
(`atelet`, `ateom`), networking stack (`atenet`), and CLI (`kubectl-ate`).

Out of scope:

- The underlying Kubernetes cluster or cloud infrastructure.
- The sandbox runtimes (gVisor, Kata Containers); report those to their
  respective projects.
- The demo applications under `demos/`; they are examples, not production
  code.
- Known limitations listed in [docs/roadmap.md](docs/roadmap.md) and
  [AGENTS.md](AGENTS.md).

## Disclosure

After a fix is merged, we publish a
[GitHub Security Advisory](https://github.com/agent-substrate/substrate/security/advisories)
describing the vulnerability and the fix, and request a CVE through GitHub
when warranted. Timing is coordinated with the reporter.

Reporters are credited in the advisory unless they prefer to remain
anonymous.

## Road to GA

As the project matures we expect to define, at or before GA:

- What triggers the switch from this preview policy to the full process.
- A defined triage process and ownership (for example a security response
  committee or rotation).
- Response and fix SLOs by severity.
- A security announcements channel (for example a mailing list).
- An embargo and coordinated disclosure process, including pre-disclosure
  notification for downstream distributors.
- A patch and release process for supported release branches, including
  backports.
- Whether to incentivize reports with a bug bounty program.

Until then, the preview-state process above applies.
