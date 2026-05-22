# Substrate API Guide: WorkerPool & ActorTemplate

This guide explains how to configure Substrate resources to deploy high-density, stateful agents.

## 1. WorkerPool: The Physical Capacity

The `WorkerPool` defines the pool of physical "warm" compute capacity. It manages a fleet of standby pods (herders) that are ready to receive and execute actor states.

### Specification (`WorkerPoolSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `replicas` | `int32` | **Required.** Number of physical standby pods to maintain in the cluster. |
| `ateomImage` | `string` | **Required.** The container image for the `ateom` herder process (e.g. `ko://github.com/agent-substrate/substrate/cmd/servers/ateom-gvisor`). |

### Example

```yaml
apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: agent-pool
  namespace: ate-demo
spec:
  replicas: 10
  ateomImage: ko://github.com/agent-substrate/substrate/cmd/servers/ateom-gvisor
```

---

## 2. ActorTemplate: The Workload Blueprint

The `ActorTemplate` defines the code, environment, and state-management policies for a specific type of agent. It is used to generate the "Golden Snapshot" from which all actors of this type are derived.

### Specification (`ActorTemplateSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `containers` | `[]Container` | **Required.** The workload definition (image, command, env, ports). |
| `workerPoolRef` | `ObjectReference` | **Required.** Pointer to the `WorkerPool` that provides the physical pods for this template. |
| `snapshotsConfig` | `SnapshotsConfig` | **Required.** GCS bucket and folder where memory snapshots are stored. |
| `pauseImage` | `string` | **Required.** The image used for the sandbox root (e.g. `gcr.io/gke-release/pause`). |
| `runsc` | `RunscConfig` | **Required.** Multi-platform configuration for fetching the gVisor binary. |

### Workload Connectivity (Uniform DNS)
Substrate has standardized on a **Uniform DNS Mesh**. You no longer need to define `SessionDiscovery` rules. Every actor created from a template is automatically reachable through the **Substrate Router** via its unique ID:

**Format:** `<actor-id>.actors.resources.substrate.ate.dev`

### Example

```yaml
apiVersion: ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: secret-agent
  namespace: ate-demo
spec:
  runsc:
    amd64:
      # Note: These values are from the 2026-05-19 nightly. 
      # For the latest verified versions, see: demos/counter/counter.yaml.tmpl
      url: "gs://gvisor/releases/nightly/2026-05-19/x86_64/runsc"
      sha256Hash: "a397be1abc2420d26bce6c70e6e2ff96c73aaaab929756c56f5e2089ea842b63"
    arm64:
      url: "gs://gvisor/releases/nightly/2026-05-19/aarch64/runsc"
      sha256Hash: "1ba2366ae2efceba166046f51a4104f9261c9cb72c6db8f5b3fe2dc57dea86b9"
  pauseImage: "gcr.io/gke-release/pause@sha256:bcbd57ba5653580ec647b16d8163cdd1112df3609129b01f912a8032e48265da"
  containers:
  - name: agent
    image: gcr.io/my-project/my-agent:latest
    command: ["/app/server"]
    ports:
    - containerPort: 80
  workerPoolRef:
    name: agent-pool
    namespace: ate-demo
  snapshotsConfig:
    location: gs://my-bucket/snapshots/secret-agent/
```

---

## 3. Operational Workflow

### The Golden Snapshot
When an `ActorTemplate` is created:
1.  Substrate starts a temporary **Golden Pod**.
2.  It executes your workload containers as defined in the template.
3.  Once the process is initialized, gVisor takes a **Golden Snapshot** (Version 0).
4.  The template enters the `Ready` phase.

### Resumption Lifecycle
Once a template is `Ready`, creating an actor logically (via `kubectl-ate create actor`) allows it to be resumed instantly on any free worker in the referenced `WorkerPool`. Substrate bypasses the standard container boot and restores the process directly from its last saved state.

---

## 4. Best Practices
*   **Startup Logic:** Place expensive initialization (loading large models, establishing baseline connections) in your application's entry point. These will be captured in the Golden Snapshot and won't need to be repeated on every resumption.
*   **Symmetry:** Ensure your `ActorTemplate` and `WorkerPool` are in the same namespace or have appropriate RBAC permissions to reference each other.
*   **Version Management:** When updating code, create a new `ActorTemplate` (e.g. `v2`). Substrate treats each template as an immutable state root.

---

## 5. Control Plane gRPC API

The Substrate Control Plane (`ate-api-server`) exposes a gRPC interface for managing actors and workers. This is the primary API used by the `kubectl-ate` CLI and higher-level frameworks.

### Service: `ateapi.Control`

#### `CreateActor`
Registers a new logical actor in the system.
*   **Request:** `CreateActorRequest`
    *   `actor_id`: Unique identifier (DNS-1123 label).
    *   `actor_template_namespace`: Namespace of the `ActorTemplate`.
    *   `actor_template_name`: Name of the `ActorTemplate`.
*   **Response:** `CreateActorResponse` containing the initialized `Actor` object.

#### `ResumeActor`
Activates a suspended actor by restoring it onto a physical worker.
*   **Request:** `ResumeActorRequest`
    *   `actor_id`: ID of the actor to resume.
    *   `boot`: (Optional) If `true`, bypasses snapshots and performs a cold boot.
*   **Response:** `ResumeActorResponse` containing the updated `Actor` object (including the physical `worker_ip`).

#### `SuspendActor`
Hibernate a running actor, capturing its current RAM and disk state into a snapshot.
*   **Request:** `SuspendActorRequest`
    *   `actor_id`: ID of the actor to suspend.
*   **Response:** `SuspendActorResponse` containing the `Actor` object in `STATUS_SUSPENDED`.

#### `DeleteActor`
Removes an actor from the registry.
*   **Constraints:** Only actors in `STATUS_SUSPENDED` can be deleted.
*   **Request:** `DeleteActorRequest`
*   **Response:** `DeleteActorResponse` (empty).

#### `GetActor` / `ListActors`
Query the state of logical actors.
*   **GetActor:** Retrieves a single actor by ID.
*   **ListActors:** Lists all actors currently tracked in the database.

#### `ListWorkers`
Query the physical resource pool.
*   **Request:** `ListWorkersRequest`
*   **Response:** `ListWorkersResponse` containing a list of `Worker` objects (Pods) and their current assignment status.

---

## 6. Advanced: Session Identity

Workloads can exchange their ephemeral Kubernetes credentials for stable **Session Identity** credentials that persist even as the process migrates between different physical workers.

### Service: `ateapi.SessionIdentity`
*   **`MintJWT`:** Generates an OIDC-compatible JWT identifying the Substrate Actor.
*   **`MintCert`:** Signs a Certificate Signing Request (CSR) to provide an mTLS identity for the actor.

---

## 7. Framework & Ecosystem Integration

Agent Substrate is designed to be the foundational execution layer for any agentic framework. 

### Agent Development Kit (ADK)
Substrate provides native support for ADK-compatible identities. Workloads can use the `SessionIdentity` service to mint JWTs that align with ADK's security model, ensuring seamless integration with ADK-managed tools and memory.

### LangChain
Substrate is an ideal runtime for stateful LangChain agents. By defining a LangChain agent as an `ActorTemplate`, you can preserve the agent's internal "thought process" and conversation history in memory across hibernations, while sandboxing its tool execution for security.

### Claude Code & CodeX
For developer-focused agents, Substrate enables massive multiplexing of coding environments. Each developer can have a dedicated, persistent terminal session (Actor) that preserves filesystem deltas, while the cluster only runs physical pods for active users.
