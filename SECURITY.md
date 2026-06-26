# Security Policy

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Use [GitHub Private Vulnerability Reporting](https://github.com/agent-substrate/substrate/security/advisories/new)
to report privately. Alternatively, email the maintainers at
[ate-dev@googlegroups.com](mailto:ate-dev@googlegroups.com) with the subject
line `[SECURITY]`.

Include the affected component, reproduction steps, and potential impact.

## Response Times

| Severity | Acknowledgment  | Target Fix  |
| -------- | --------------- | ----------- |
| Critical | 1 business day  | 7 days      |
| High     | 2 business days | 30 days     |
| Medium   | 5 business days | 90 days     |
| Low      | 5 business days | Best effort |

These are targets, not guarantees. Agent Substrate does not have a dedicated
security team.

## Supported Versions

There are no stable releases yet. Security fixes are applied to `main` only.

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
describing the vulnerability and the fix. Timing is coordinated with the
reporter.
