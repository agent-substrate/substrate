# Substrate Security

Agent Substrate executes untrusted actors at scale in a multi-tenant environment where workloads are dynamic and memory states migrate continuously across physical infrastructure. All security controls must operate within Substrate's sub-100ms cold-start and resume latency budget. This document defines the high-level security principles. More detailed security risks are documented [in the threat model](threat-model.md).

## Security Scope

Substrate provides the foundational base for agent security:

- Sandboxing: built-in compute, storage, and networking separation.
- Identity: identity issuance is built-in and designed to federate with other identity systems. See the [Substrate Identity Critical User Journeys](https://docs.google.com/document/d/1seJNYQmsQYLCrYYzB9AmuxpSGSPCC3-u3tPNFsK2edc/edit?resourcekey=0-D6q8_0vqF6LPEk0BxWMTaA&tab=t.0#heading=h.6qjmtncwtihi).
- Gateway: built-in ingress and egress filtering that can do destination filtering and credential injection.

We expect the application layer creating actors using substrate to be responsible for:

- Handling human-to-actor authentication and authorization.
- Registering and maintaining information about agents in an agent registry.
- Maintaining agent hierarchy/state to be able to reconstruct agent lineage: who made this sub-sub-sub agent.

## Security Extensibility

More advanced security can be integrated using the ingress and egress extension points. This is where advanced prompt injection scanning, dynamic LLM-driven authorization decisions, data leak prevention (DLP), or cloud IAM policy enforcement can be added:

- **Egress Extensibility**: The outbound path is a fully pluggable Policy Enforcement Point (PEP). Operators can either extend the existing egress proxy using Envoy's `ext_proc` callout mechanism to invoke external inspection and authorization services, or completely replace the egress gateway (`atenet-egress`) with an external solution.
- **Ingress Extensibility**: Ingress similarly supports `ext_proc` callouts for external authentication, authorization, and payload inspection. However, unlike egress, the ingress gateway (`atenet-router`) cannot be completely replaced from the outside: Substrate's core orchestration relies on ingress routing hooks to manage actor state, resolve worker assignments, and trigger on-demand cold starts or resume operations from snapshots to wake up agents.

## Core Security Principles

**Status Legend:**

- Met: Fully implemented in the current codebase.
- In Progress: Partially implemented or under active development.
- Future: Planned architecture and roadmap capabilities not yet implemented.
- Out of Scope: Not substrate’s job, handled by a layer outside substrate.

### Defensible Multi-Tenancy

Substrate is designed to safely run mutually hostile, untrusted actors from disparate tenants side-by-side on the same physical host to maximize resource utilization and bin-packing efficiency.

| Principle | Description | Status |
| :--- | :--- | :--- |
| **Always Sandboxed** | Every actor is required to run inside a strong compute sandbox. A linux container is not sufficient or supported. Our security design relies on strong containment. | Met (gVisor, CHV micro-VM) |
| **Per-Actor Storage Segregation** | Storage and persistent volume state must be strictly segregated per actor. Actors cannot traverse, enumerate, or access persistence belonging to other actors. Restoring a snapshot must re-validate the actor's current tenant authorization before execution resumes. | Met (Segregation)<br>Future (Per-Actor Encryption, Restore Authorization) |
| **Per-Actor Network Isolation** | Guest network stacks are segregated per actor. Lateral peer-to-peer network communication between co-located actors on the same host or across the cluster is blocked by default; inter-actor communication must route through authenticated, policy-enforced gateways. | Met (Private netns, atunnel) |
| **Delete Guarantee** | Reset, scrub, and sanitize local ephemeral volumes, cached state, and worker runtime environments between actor scheduling cycles to prevent cross-tenant data leakage. | In Progress (atelet resetActorDirs) |
| **Node Blast Radius Containment** | A compromise of an individual sandbox guest or node supervisor restricts impact to the actors and identities actively co-located on that node. | Future |
| **Atespace Separation** | The Substrate equivalent of a Kubernetes namespace that is used to group resources and partition object storage snapshot paths. It does not yet enforce runtime API authorization, inter-atespace network policies, or resource quotas, but an embedded OpenFGA relationship-based access control (ReBAC) model is actively being integrated. | In-Progress (OpenFGA) |

Other forms of tenancy:

- **[Possible, but not currently supported] Multiple Substrate installations inside a single Kubernetes cluster**, with nodes separated by taints/tolerations. This gets you Substrate API separation but gives up some node bin-packing efficiency. Care needs to be taken at the Kubernetes layer to keep everything separate.
- **[May be possible, but not currently supported] Attaching multiple Kubernetes clusters to a single Substrate installation**. This may be possible with some work today, but is not supported. We’re expecting a single large K8s cluster to be capable of running 100s of millions of mostly-idle agents.
- **[Out of Scope] Multi-tenancy inside an actor**. There is no meaningful security boundary inside an actor. Actors are designed to be cheap and efficient enough to run many, including one per human user, so that it’s possible to run them without handling content for multiple users inside the same actor.
- **[Not Supported] Per-Actor ingress/egress**. Actor-facing traffic proxies (`atenet-router` for ingress, `atenet-egress` for egress) are shared across all actors in the substrate installation by default. Due to the high cardinality and rapid lifecycle of actors, dedicated 1:1 proxies per actor are operationally and economically infeasible. While we may partition proxies further in the future to reduce sharing, by atespace for example, proxies will remain multi-tenant by default.

### Identity

| Principle | Description | Status |
| :--- | :--- | :--- |
| **Zero Ambient Credentials** | Default actor sandboxes execute completely credential-free. Actors may hold credential identifiers, encrypted credentials, or unprivileged tokens that need to be hydrated or decrypted out-of-band by Substrate infrastructure before accessing external services. | Met (no credentials inside sandbox) |
| **Distinct Workload Identity** | Every actor receives a short lifetime (1h) SPIFFE ID X.509 certificate. It is possible to distinguish an actor that was deleted and then re-created with the same name using their UID. | Met (X.509)<br>In-Progress (OIDC discovery) |
| **Universal Component mTLS** | All control plane and data plane components (`ate-api-server`, `atelet`, `ateom`, routers, tunnels) authenticate and communicate exclusively over mutual TLS using node- and service-attested identities. Unauthenticated traffic is rejected, exempting only local unauthenticated health/liveness probes (`/healthz`, `/readyz`). | Met (Mutual TLS)<br>In-Progress (Authz) |
| **Sub-Actor Provenance** | Delegated sub-actors inherit cryptographically signed lineage. Every tool invocation, data access, and egress transaction can be deterministically resolved to the root actor and execution chain. This provenance/hierarchy needs to be recorded in the system that calls substrate. | Out of Scope |

### Storage

| Principle | Description | Status |
| :--- | :--- | :--- |
| **Untrusted Storage** | Persistence layers are treated as untrusted environments. Remote storage is protected with encryption. Local snapshot caching on storage attached to the node is multi-tenant and protected from cross-tenant attacks with encryption and strict filesystem permissions. | Future (Storage is currently partitioned but is unencrypted) |
| **Dual-Party Snapshot Authorization** | Reading or writing snapshot storage requires dual-identity verification proving both the actor's identity and the node supervisor (`atelet`) operating on its behalf. Sandboxed guests cannot directly interact with snapshot storage. | Future |

### Network

| Principle | Description | Status |
| :--- | :--- | :--- |
| **No Access by Default** | Sandboxed actors and worker nodes launch with default-deny networking and have zero network or API access to control plane services (`ate-api-server`, Kubernetes API servers, PostgreSQL state store). Substrate provisions an isolated synthetic DNS resolver exposing only approved endpoints to conceal internal cluster topology and IP allocation schemes. | Met (Synthetic DNS, loopback) |
| **Authenticated and Policy-Enforced Ingress** | Inbound invocations to actors terminate at authenticated edge routers (`atenet-router`), which authenticate callers and verify invocation policies before triggering on-demand actor cold-start or resume. | Met (atenet-router)<br>In-Progress (authentication) |
| **Fail-Closed Policy-Enforced Egress Gateway** | Outbound traffic is funneled through the multi-tenant egress proxy (`atenet-egress`), which validates actor identity, enforces destination host allowlists, executes TLS inspection or prompt filtering, and dynamically injects target credentials. If an egress policy enforcement point (PEP) is unconfigured/fails egress is dropped. Unsupported protocols are dropped, see [Egress Policy Table](https://docs.google.com/document/d/1XLCo9SMROddQzwsyEhdBNWuLYFtWWZWfYvGlIO1BcUI/edit?resourcekey=0-st2SqKp-O68nv5LeIUfA-g&tab=t.0#bookmark=id.pjnob1gppauk) for specifics. | Met (atenet-egress)<br>In-Progress (fail closed, UDP currently passthrough) |

### Authorization

| Principle | Description | Status |
| :--- | :--- | :--- |
| **Actor to Actor Authorization** | Built-in authorization policy for agent to agent communication will be added in the future. Before that exists users can install an authenticating reverse proxy, or implement authorization at the application level. | Future |
| **Atespace and Node-level containment** | Actors are grouped into logical Atespaces for declarative RBAC, resource quotas, and independent metadata/label authorization to prevent privilege escalation. | In Progress (embedded OpenFGA) |
| **Comprehensive Quotas & Anti-DoS Throttling** | The platform enforces strict limits across API call rates, memory/CPU allocations, and egress bandwidth to eliminate runaway actors and shared resource exhaustion. | Future |

### Auditability and Response

| Principle | Description | Status |
| :--- | :--- | :--- |
| **Structured Audit Trail** | All lifecycle transitions (start, suspend, resume, terminate), snapshot access, gateway routing events, and policy violations generate structured telemetry linked to cryptographic actor identity and parent lineage. Token log leaks are prevented with sanitization. | In-Progress (OTLP logging, shared principle)<br>Future (enhanced identity attribution, log leak protection) |
| **Dynamic Quarantine & Snapshot Tainting** | The control plane supports immediate revocation and network blackholing of misbehaving/compromised actors. Suspended states from compromised actors receive an immutable cryptographic taint, preventing resumption across the fleet. | Future |
