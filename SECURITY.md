# Security Policy

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Use [GitHub Private Vulnerability Reporting](https://github.com/google/agent-substrate/security/advisories/new) to report a vulnerability privately. The maintainers will acknowledge your report within 5 business days and aim to provide a fix or workaround within 90 days, depending on severity and complexity.

Alternatively, you can reach the maintainers via the [ate-dev](https://groups.google.com/g/ate-dev) mailing list.

## Supported Versions

Agent Substrate is in early development and does not yet have stable releases. Security fixes are applied to the `main` branch only.

| Version | Supported |
| ------- | --------- |
| main    | Yes       |
| older   | No        |

## Scope

This policy covers the Agent Substrate control plane, node agents, and networking components in this repository.

**Out of scope:**
- The Kubernetes cluster or cloud infrastructure running Agent Substrate (report those to the respective vendors).
- Known limitations documented in [docs/roadmap.md](docs/roadmap.md) and [AGENTS.md](AGENTS.md).

## Disclosure

Once a fix is merged, we will publish a [GitHub Security Advisory](https://github.com/google/agent-substrate/security/advisories) describing the vulnerability and the fix.
