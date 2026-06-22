# Substrate API Guide: WorkerPool & ActorTemplate

This guide explains how to configure Substrate resources to deploy high-density, stateful agents.

## 1. WorkerPool: The Physical Capacity

The `WorkerPool` defines the pool of physical "warm" compute capacity. It manages a fleet of standby pods (herders) that are ready to receive and execute actor states.

### Specification (`WorkerPoolSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `replicas` | `int32` | **Required.** Number of physical standby pods to maintain in the cluster. |
| `ateomImage` | `string` | **Required.** The container image for the `ateom` herder process (e.g. `ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor`). |
| `sandboxClass` | `string` | Optional. The sandbox runtime family for the pool: `gvisor` (default) or `microvm`. Drives the worker pod shape (e.g. KVM device mounts, node placement) and which `SandboxConfig`s are eligible. |
| `sandboxConfigName` | `string` | Optional. Name of a cluster-scoped [`SandboxConfig`](#3-sandboxconfig-sandbox-binaries) providing the sandbox binaries. If empty, the cluster default `SandboxConfig` for the pool's `sandboxClass` is used. |
| `template` | `WorkerPoolPodTemplate` | **Optional.** Pod scheduling and resource settings for worker pods. |

#### `WorkerPoolPodTemplate` (`spec.template`)

| Field | Type | Pod mapping |
| :--- | :--- | :--- |
| `nodeSelector` | `map[string]string` | `spec.nodeSelector` |
| `tolerations` | `[]Toleration` | `spec.tolerations` (max 16) |
| `priorityClassName` | `string` | `spec.priorityClassName` |
| `nodeAffinity` | `NodeAffinity` | `spec.affinity.nodeAffinity` |

### Example

```yaml
apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: agent-pool
  namespace: ate-demo
  labels:
    workload: secret-agent
spec:
  replicas: 10
  ateomImage: ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor
  # sandboxClass defaults to gvisor; the pool resolves to the cluster's default
  # gvisor SandboxConfig unless sandboxConfigName is set.
```

### Example with GPU node scheduling

```yaml
apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: gpu-pool
  namespace: ate-demo
spec:
  replicas: 5
  ateomImage: ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor
  template:
    nodeSelector:
      cloud.google.com/gke-accelerator: nvidia-tesla-t4
    tolerations:
    - key: nvidia.com/gpu
      operator: Exists
      effect: NoSchedule
    priorityClassName: substrate-workers
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: workload
            operator: In
            values: [substrate]
```

---

## 2. ActorTemplate: The Workload Blueprint

The `ActorTemplate` defines the code, environment, and state-management policies for a specific type of agent. It is used to generate the "Golden Snapshot" from which all actors of this type are derived.

### Specification (`ActorTemplateSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `containers` | `[]Container` | **Required.** The workload definition (image, command, env, ports). |
| `sandboxClass` | `string` | Optional. The sandbox runtime family this template's actors require: `gvisor` (default) or `microvm`. Only `WorkerPool`s whose `sandboxClass` matches are eligible. |
| `workerSelector` | `*LabelSelector` | Optional. Gates which `WorkerPool`s actors from this template may use, by matching against each pool's labels. If unset, all pools are eligible (subject to the actor's own `worker_selector`). |
| `snapshotsConfig` | `SnapshotsConfig` | **Required.** GCS bucket and folder where memory snapshots are stored. |
| `pauseImage` | `string` | **Required.** The image used for the sandbox root (e.g. `gcr.io/gke-release/pause`). |

The sandbox binaries (e.g. the gVisor `runsc` binary) are **no longer configured on the `ActorTemplate`**. They are resolved from the referenced `WorkerPool`'s [`SandboxConfig`](#3-sandboxconfig-sandbox-binaries) — by name (`workerPool.spec.sandboxConfigName`) or, by default, the cluster default `SandboxConfig` for the pool's `sandboxClass`.

Because a snapshot is not restorable across sandbox runtimes, `sandboxClass` is a **hard scheduling gate**: an actor is only ever placed on a `WorkerPool` of the matching class. It is AND'd with `workerSelector` (and the actor's `worker_selector`), which can only narrow the eligible pools further. It defaults to `gvisor` and, like the rest of the spec, is immutable, so each template's class is fixed at creation.

Container environment variables support literal `value` entries and `valueFrom.secretKeyRef`. Secret references are resolved by `ate-api-server` from the `ActorTemplate` namespace when a workload spec is materialized. For the golden actor, the resolved values are captured in the golden snapshot and future actors inherit those values until the golden snapshot is recreated. For an actor that bypasses the golden snapshot and boots from the current template spec, the resolved values are sent to atelet but are not serialized into the public Actor API. Other Kubernetes `valueFrom` sources are not supported yet. Secret changes do not automatically restart actors or invalidate snapshots; rotating a Secret requires an explicit actor or template lifecycle action.

### Workload Connectivity (Uniform DNS)
Substrate has standardized on a **Uniform DNS Mesh**. You no longer need to define `SessionDiscovery` rules. Every actor created from a template is automatically reachable through the **Substrate Router** via its unique ID:

**Format:** `<actor-id>.actors.resources.substrate.ate.dev`

### Actor Identity
Substrate bind-mounts a read-only, per-actor identity directory at **`/run/ate`** into each of the actor's containers. An actor can learn its own ID without parsing the `Host` header by reading the file **`/run/ate/actor-id`** inside it, which contains the raw actor ID with no trailing newline. Further identity and configuration data may appear in this directory over time.

Read it fresh rather than caching it at process start. It is delivered as a per-actor bind mount, not an environment variable, precisely so it carries the correct ID after a resume from the golden snapshot — an env var (or a file baked into the image) would be frozen at the *golden* actor's ID, since it lives in the checkpointed process memory, and would therefore be identical for every actor of the template.

### Example

```yaml
apiVersion: ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: secret-agent
  namespace: ate-demo
spec:
  # No sandbox/runsc config here — the binaries come from the WorkerPool's
  # SandboxConfig (see section 3).
  pauseImage: "gcr.io/gke-release/pause@sha256:bcbd57ba5653580ec647b16d8163cdd1112df3609129b01f912a8032e48265da"
  containers:
  - name: agent
    image: gcr.io/my-project/my-agent:latest
    command: ["/app/server"]
    ports:
    - containerPort: 80
  # sandboxClass defaults to gvisor; set to microvm to require micro-VM pools.
  sandboxClass: gvisor
  workerSelector:
    matchLabels:
      workload: secret-agent
  snapshotsConfig:
    location: gs://my-bucket/snapshots/secret-agent/
```

---

## 3. SandboxConfig: Sandbox Binaries

`SandboxConfig` is a **cluster-scoped** resource that decouples the sandbox binaries (the gVisor `runsc` binary, or a micro-VM kernel/firmware/config) from the `ActorTemplate`. A `WorkerPool` resolves its binaries from a `SandboxConfig` — either the one named by `spec.sandboxConfigName`, or the cluster default for the pool's `sandboxClass`.

This means a single, cluster-managed config pins the sandbox runtime version for many templates: snapshots stay restorable because the version is recorded in each snapshot's manifest, and operators upgrade the runtime in one place.

### Specification (`SandboxConfigSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `sandboxClass` | `string` | **Required.** Runtime family this config applies to: `gvisor` (default) or `microvm`. A `WorkerPool` only uses `SandboxConfig`s whose `sandboxClass` matches its own. |
| `default` | `bool` | Optional. Marks this as the cluster default for its `sandboxClass`. A `WorkerPool` with no `sandboxConfigName` resolves to the default for its class. At most one default per class. |
| `assets` | `map[arch]map[name]AssetFile` | Optional. Content-addressed files atelet fetches, keyed by architecture (`amd64`, `arm64`) then asset name. gVisor expects a `runsc` asset; a micro-VM backend expects several. Each `AssetFile` is a `{ url, sha256 }` pair. |

A default cluster-wide gVisor `SandboxConfig` (`gvisor-default`) is installed with the platform, so gVisor pools work out of the box.

### Example

```yaml
apiVersion: ate.dev/v1alpha1
kind: SandboxConfig
metadata:
  name: gvisor-default
spec:
  sandboxClass: gvisor
  default: true
  assets:
    amd64:
      runsc:
        url: "gs://gvisor/releases/nightly/2026-05-19/x86_64/runsc"
        sha256: "a397be1abc2420d26bce6c70e6e2ff96c73aaaab929756c56f5e2089ea842b63"
    arm64:
      runsc:
        url: "gs://gvisor/releases/nightly/2026-05-19/aarch64/runsc"
        sha256: "1ba2366ae2efceba166046f51a4104f9261c9cb72c6db8f5b3fe2dc57dea86b9"
```

---

## 4. Operational Workflow

### The Golden Snapshot
When an `ActorTemplate` is created:
1.  Substrate starts a temporary **Golden Pod**.
2.  It executes your workload containers as defined in the template.
3.  Once the process is initialized, gVisor takes a **Golden Snapshot** (Version 0).
4.  The template enters the `Ready` phase.

### Resumption Lifecycle
Once a template is `Ready`, creating an actor logically (via `kubectl-ate create actor`) allows it to be resumed instantly on any free worker in the referenced `WorkerPool`. Substrate bypasses the standard container boot and restores the process directly from its last saved state.

---

## 5. Best Practices
*   **Startup Logic:** Place expensive initialization (loading large models, establishing baseline connections) in your application's entry point. These will be captured in the Golden Snapshot and won't need to be repeated on every resumption.
*   **Symmetry:** Ensure your `ActorTemplate` and `WorkerPool` are in the same namespace or have appropriate RBAC permissions to reference each other.
*   **Version Management:** When updating code, create a new `ActorTemplate` (e.g. `v2`). Substrate treats each template as an immutable state root.

---

## 6. Control Plane gRPC API

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

## 7. Advanced: Session Identity

Workloads can exchange their ephemeral Kubernetes credentials for stable **Session Identity** credentials that persist even as the process migrates between different physical workers.

### Service: `ateapi.SessionIdentity`
*   **`MintJWT`:** Generates an OIDC-compatible JWT identifying the Substrate Actor.
*   **`MintCert`:** Signs a Certificate Signing Request (CSR) to provide an mTLS identity for the actor.

---

## 8. Framework & Ecosystem Integration

Agent Substrate is designed to be the foundational execution layer for any agentic framework.

### Agent Development Kit (ADK)
Substrate provides native support for ADK-compatible identities. Workloads can use the `SessionIdentity` service to mint JWTs that align with ADK's security model, ensuring seamless integration with ADK-managed tools and memory.

### LangChain
Substrate is an ideal runtime for stateful LangChain agents. By defining a LangChain agent as an `ActorTemplate`, you can preserve the agent's internal "thought process" and conversation history in memory across hibernations, while sandboxing its tool execution for security.

### Claude Code & CodeX
For developer-focused agents, Substrate enables massive multiplexing of coding environments. Each developer can have a dedicated, persistent terminal session (Actor) that preserves filesystem deltas, while the cluster only runs physical pods for active users.
