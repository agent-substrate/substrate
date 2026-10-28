# Security Policy

## Reporting a Vulnerability

**Do not open a public GitHub issue or pull request for security
vulnerabilities.**

Use [GitHub Private Vulnerability Reporting](https://github.com/agent-substrate/substrate/security/advisories/new)
to report security vulnerabilities privately. Verify that there is no existing
open GitHub issue covering the problem before creating a report.

Please ensure your report is concise and clearly articulates the bug. It should
include:

- How the issue is reachable and exploitable by untrusted code running inside an
  actor sandbox. Or a statement that it isn't exploitable from inside an actor
  sandbox.
- The security impact resulting from exploitation of the bug.
- The affected component and version or commit SHA.
- Detailed steps to fully reproduce, or a proof of concept.
- Mitigations if known.

### When to report

- You found a potential security vulnerability in Agent Substrate.
- You found a vulnerability in a project Agent Substrate depends on and it
  is exploitable through Agent Substrate. If that project has its own
  disclosure process, please report it there as well.

### When not to report

- Theoretical bugs from static analysis without demonstration of an exploitable
  security issue.
- You need help securing or tuning a deployment (open a regular issue or
  discussion).
- The issue is not security related.

## What Counts as a Vulnerability

See the [Security Guide](../docs/security.md) and
[Threat Model](../docs/threat-model.md) for the security architecture, trust
boundaries, and assumptions this section is based on.

In scope: the Agent Substrate control plane (`ateapi`, `atecontroller`,
`podcertcontroller`), node and sandbox supervisors (`atelet`, `ateworker-gvisor`,
`ateworker-microvm`, `objectstore-plugin`), networking and credential plane
(`atenet`, `cmd/credential-provider/kubernetes-secrets`), and CLI
(`kubectl-ate`, `ate-setup`).

Agent Substrate multiplexes untrusted actors onto a shared pool of workers, so
its primary security boundaries are between actors and between actors and the
platform. Reports in these classes are the highest priority, listed roughly
from most to least severe:

- **Cross-actor access**: an actor reading or modifying another actor's
  state (memory, filesystem, working memory) on a shared worker, or acting
  across atespace boundaries.
- **Snapshot integrity and confidentiality**: reading another actor's
  suspend/resume snapshot, or resuming an actor from a tampered snapshot.
- **Routing**: traffic intended for one actor delivered to another.
- **Identity**: forging actor identity (such as actor SPIFFE X.509
  certificates or JWTs) or component PodCertificates.
- **Control-plane authentication and authorization bypass**: reaching
  `ateapi` RPCs without valid credentials (mTLS or JWT auth modes),
  bypassing OpenFGA authorization checks, misusing worker or node client
  certificates, or acting beyond what valid credentials are authorized to do.
- **Actor-to-platform escalation**: an actor compromising `atelet`, `ateworker`,
  `atenet`, or the control plane, whether while the actor is actively running
  or across lifecycle transitions.
- **Node lateral movement**: using a compromised worker node to reach other
  nodes, unscheduled actors, other atespaces, or the control plane.
- **Network or credential isolation bypass**: an actor sending or receiving
  traffic that `atenet` policy blocks, or accessing credentials outside its
  authorized `EgressPolicy`.

### Out of scope

- The underlying Kubernetes cluster or cloud infrastructure.
- Upstream sandbox runtimes (gVisor, Cloud Hypervisor); report vulnerabilities
  contained within those runtimes to their respective projects.
- Sample applications under `demos/` and load-testing harnesses under
  `benchmarking/`.
- Documented scope boundaries and future capabilities listed in
  [docs/security.md](../docs/security.md) at the time of your report. If you are
  unsure whether a finding is covered by a documented limitation, report it
  privately anyway and let a maintainer make the call.

### What usually does not qualify

- Denial of service against workloads deployed without resource limits.
- Issues that require cluster-admin permissions or root on the node; that
  level of access already controls the platform.
- Issues that require attacker control of the ActorTemplate.
- Vulnerability scanner or CVE reports about dependencies without an
  exploitable path through Agent Substrate (see
  [Dependencies](#dependencies)).

If you are unsure whether something qualifies, report it privately anyway.

## Triage, Coordinated Disclosure, and Advisories

Reports are promptly triaged by the Agent Substrate maintainers. When a valid
vulnerability is confirmed:

- We work with the reporter on the private advisory thread to validate the
  issue and develop a remediation.
- Fixes are prepared and patched on supported releases.
- We coordinate disclosure timing with the reporter and publish a
  [GitHub Security Advisory](https://github.com/agent-substrate/substrate/security/advisories)
  describing the vulnerability, affected versions, and remediation, requesting
  a CVE through GitHub when warranted.

We ask reporters to keep vulnerability details private until the coordinated
advisory is published. Reporters are credited in the advisory unless they
prefer to remain anonymous.

## Supported Versions

After release 1.0.0, security fixes will be applied to `main` and to supported
stable releases.

## Dependencies

Container images are built on distroless bases. If a dependency vulnerability
is exploitable through Agent Substrate, report it to us privately as well as
upstream; otherwise a regular issue or pull request bumping the dependency is
sufficient.

## Good-Faith Research

We expect security research against Agent Substrate to be conducted in good
faith:

- Test only against clusters and deployments you own or are authorized to
  test.
- Do not access, modify, or destroy data that is not yours; use test
  workloads to demonstrate impact.
- Do not degrade service for others while testing.

This policy covers the Agent Substrate open source project only. It does not
authorize testing of third-party deployments of Agent Substrate, which are
governed by their respective operators' policies.
