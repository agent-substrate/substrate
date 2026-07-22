# Security Policy

Agent Substrate is in a pre-release (preview) state. This policy is
intentionally minimal for that stage and will be revisited as part of the
General Availability (GA) milestone, see [Road to GA](#road-to-ga) below.

## Reporting a Vulnerability

**Do not open a public GitHub issue or pull request for security
vulnerabilities.**

Use [GitHub Private Vulnerability Reporting](https://github.com/agent-substrate/substrate/security/advisories/new)
to report privately. This is the only supported reporting channel; there is
no security mailing list and no bug bounty program at this stage.

Please include:

- The affected component and version (commit SHA).
- Steps to reproduce, or a proof of concept.
- The potential impact, and mitigations if known.

### When to report

- You think you found a potential security vulnerability in Agent Substrate.
- You are unsure whether an issue affects Agent Substrate.
- You found a vulnerability in a project Agent Substrate depends on and it
  affects Agent Substrate. If that project has its own disclosure process,
  please report it there first.

### When not to report

- You need help securing or tuning a deployment (open a regular issue or
  discussion).
- The issue is not security related.

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
- There is no bug bounty.

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
- Known limitations listed in [docs/roadmap.md](docs/roadmap.md) and
  [AGENTS.md](AGENTS.md).

## Disclosure

After a fix is merged, we publish a
[GitHub Security Advisory](https://github.com/agent-substrate/substrate/security/advisories)
describing the vulnerability and the fix, and request a CVE through GitHub
when warranted. Timing is coordinated with the reporter; we prefer to
disclose as soon as a fix is available.

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
