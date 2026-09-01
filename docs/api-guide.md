# Substrate API Guide: WorkerPool & ActorTemplate

This guide explains how to configure Substrate resources to deploy high-density, stateful agents.

## 1. WorkerPool: The Physical Capacity

The `WorkerPool` defines the pool of physical "warm" compute capacity. It manages a fleet of standby pods (herders) that are ready to receive and execute actor states.

### Specification (`WorkerPoolSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `replicas` | `int32` | **Required.** Number of physical standby pods to maintain in the cluster. |
| `workerImage` | `string` | **Required.** The container image for the `ateom` herder process (e.g. `ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor`). |
| `sandboxClass` | `string` | Optional. The sandbox runtime family for the pool: `gvisor` (default) or `microvm`. Drives the worker pod shape (e.g. KVM device mounts, node placement) and which `SandboxConfig`s are eligible. |
| `sandboxConfigName` | `string` | Optional. Name of a cluster-scoped [`SandboxConfig`](#3-sandboxconfig-the-sandbox-itself) providing the sandbox binaries and pause image. If empty, the cluster default `SandboxConfig` for the pool's `sandboxClass` is used. |
| `template` | `WorkerPoolPodTemplate` | **Optional.** Metadata, scheduling, and resource settings for worker workloads. |

#### `WorkerPoolPodTemplate` (`spec.template`)

| Field | Type | Workload mapping |
| :--- | :--- | :--- |
| `labels` | `map[string]string` | Generated Deployment and `spec.template.metadata.labels` (max 64) |
| `annotations` | `map[string]string` | Generated Deployment and `spec.template.metadata.annotations` (max 64) |
| `nodeSelector` | `map[string]string` | `spec.nodeSelector` |
| `tolerations` | `[]Toleration` | `spec.tolerations` (max 16) |
| `priorityClassName` | `string` | `spec.priorityClassName` |
| `nodeAffinity` | `NodeAffinity` | `spec.affinity.nodeAffinity` |
| `resources` | `ResourceRequirements` | `spec.containers[].resources` |

Keys in `ate.dev/` and its subdomains (for example, `policy.ate.dev/`) are
reserved for controllers and cannot be set in `template.labels` or
`template.annotations`. Metadata keys and label values must follow Kubernetes
syntax.

`template.labels` and `template.annotations` only configure Kubernetes workload
metadata; they do not affect actor scheduling. Actor selectors match
`WorkerPool.metadata.labels`, not `WorkerPool.spec.template.labels`.

#### Pin pools to the installed substrate version (`template.nodeSelector`)

The installation labels every node with the substrate build version it deployed.
Set the same label as a `nodeSelector` on every pool, so its workers only run
on nodes of that version:

```yaml
spec:
  template:
    nodeSelector:
      ate.dev/substrate-version: "<installed version>"
```

Read the installed version off the atelet DaemonSet:

```bash
kubectl get ds -n ate-system -l app=atelet -L ate.dev/substrate-version
```

The pin is critical to make a rolling upgrade possible. An upgrade moves nodes to
the new version one at a time, deleting each node's old worker pods once it
moves. A pinned pool cannot put those pods back on a moved node, so the old
version drains away node by node. An unpinned pool breaks this constraints.
#### Worker Capacity (`spec.template.resources`)

Setting `resources.limits` (CPU and Memory) on a `WorkerPool` establishes each worker pod's **capacity** — the envelope available to host an actor sandbox, taken from the `ateom` container's limits. The scheduler only places an actor on a worker whose capacity is `>=` the actor's declared resource limits (see [Sandbox Right-Sizing](#sandbox-right-sizing-specresources) on the `ActorTemplate`).

- Size a pool's `limits` to the largest actor it should host. An actor occupies its whole worker, so worker capacity is the per-actor ceiling, not a shared budget.
- Capacity is advisory for placement only: a worker that declares no CPU/memory limit reports zero capacity for that dimension, which the scheduler treats as **unconstrained** (placement is never blocked by missing data). The actual sandbox size still comes from the `ActorTemplate`.

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
  workerImage: ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor
  template:
    labels:
      project: agent-platform
    annotations:
      policy.example.com/exemption: sandbox-host
  # sandboxClass defaults to gvisor; the pool resolves to the cluster's default
  # gvisor SandboxConfig unless sandboxConfigName is set.
```

### GPU worker pools

A GPU pool needs two things: (1) scheduling onto GPU nodes, and (2) a
`nvidia.com/gpu` request in `template.resources`. The request does double duty —
it makes the device plugin assign a GPU to the worker pod **and** triggers
Substrate to pass that GPU **through to each actor's sandbox**. No per-actor
configuration is needed.

```yaml
apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: gpu-pool
  namespace: ate-demo
spec:
  replicas: 5
  # GPU pools need a glibc ateom-gvisor build — see Requirements below.
  workerImage: <your-registry>/ateom-gvisor-glibc@sha256:...
  template:
    # (1) schedule onto GPU nodes
    nodeSelector:
      cloud.google.com/gke-accelerator: nvidia-tesla-t4
    tolerations:
    - key: nvidia.com/gpu
      operator: Exists
      effect: NoSchedule
    priorityClassName: substrate-workers
    resources:
      requests:
        cpu: 500m
        memory: 1Gi
      limits:
        cpu: "1"
        memory: 2Gi
        # (2) claim a GPU — this request is what triggers GPU passthrough
        nvidia.com/gpu: "1"
```

`atecontroller` propagates the request onto the `ateom` container and mounts the
host NVIDIA toolkit into the pod. `ateom-gvisor` then generates a CDI spec with
`nvidia-ctk` and injects the GPU device nodes, driver libraries, and env into
each actor container's OCI spec, and runs `runsc` with `--nvproxy` so CUDA and NVML
work inside the sandbox. A worker requesting `nvidia.com/gpu: N` passes all N
through.

Every container in the actor gets the GPU. An actor's containers share one sandbox
and the worker's whole device set, and `ActorTemplate` has no per-container resource
fields, so GPUs are shared at the actor level rather than assigned to one container,
the same as cpu and memory. This differs from a Kubernetes Pod, where the GPU goes
only to the container that requests it. If per-container resource limits are added
later, GPU assignment should follow them.

The driver library directory is prepended to each container's `LD_LIBRARY_PATH` so an
image does not have to set it to find `libcuda.so.1`; any existing value is kept after
it rather than replaced.

**Requirements**

- **A glibc `ateom-gvisor` image**, set as `spec.workerImage`. The distroless default
  cannot exec `nvidia-ctk`. Build one with
  `KO_DEFAULTBASEIMAGE=debian:stable-slim ko build ./cmd/ateom-gvisor`.
- **`nvidia-ctk` on the node**, at the path mounted into the worker — by default
  `/usr/local/nvidia/toolkit`, overridable with the controller's
  `ATE_NVIDIA_TOOLKIT_HOST_PATH`. gpu-operator installs it; GKE's built-in GPU
  support does not.
- **The driver mounted into the pod by the device plugin**, at `/usr/local/nvidia`
  by default. `nvidia-ctk` needs its libraries to enumerate the GPUs at all, so a
  cluster whose plugin uses a different layout must set the controller's
  `ATE_NVIDIA_DRIVER_ROOT`.
- **`atelet` must run on the GPU nodes**, so add a matching toleration to its
  DaemonSet if those nodes are tainted (for example `nvidia.com/gpu`).
- **gVisor only.** `microvm` pools would need VFIO PCI passthrough instead.

**Known limitation: a GPU actor can only be suspended while no CUDA context is
open.** gVisor cannot serialize GPU state, so a checkpoint taken while the workload
holds a context fails with an nvproxy encoding error and terminates the
sandbox. Workloads that run CUDA and exit snapshot normally; one that keeps a
context alive (a model resident in device memory, say) cannot be suspended or have a
golden snapshot taken.

### Status (`WorkerPoolStatus`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `replicas` | `int32` | Total number of worker pods, mirrored from the managed Deployment. |
| `selector` | `string` | Label selector for the worker pods. |

---

## 2. ActorTemplate: The Workload Blueprint

The `ActorTemplate` defines the code, environment, and state-management policies for a specific type of agent. It is used to generate the "Golden Snapshot" from which all actors of this type are derived.

### Specification (`ActorTemplateSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `containers` | `[]Container` | **Required.** The workload definition — see [Container Fields](#container-fields) below. Each container may also declare an optional `readyz` HTTP probe — see [Container Readiness Probe](#container-readiness-probe-readyz). |
| `sandboxConfig` | `SandboxConfig` | **Required.** The sandbox runtime selection: `sandboxClass` (**required**, `SANDBOX_CLASS_GVISOR` or `SANDBOX_CLASS_MICROVM`) picks the runtime family this template's actors require — only `WorkerPool`s whose `sandboxClass` matches are eligible — and `configName` (**required**) names the cluster-scoped [`SandboxConfig`](#3-sandboxconfig-the-sandbox-itself) object supplying the sandbox binaries. |
| `workerSelector` | `*LabelSelector` | Optional. Gates which `WorkerPool`s actors from this template may use, by matching against each pool's labels. If unset, all pools are eligible (subject to the actor's own `worker_selector`). |
| `snapshotsConfig` | `SnapshotsConfig` | **Required.** The base object-storage location snapshots are written under, plus the pause/commit/resume scopes. See [Snapshot Storage Layout](#snapshot-storage-layout). |
| `volumes` | `[]Volume` | Optional. Volumes the containers may mount, each a `durableDir`, an `externalVolumeTemplate` (see [CSI Volumes Guide](csi-volumes.md)), or a `systemInfo` volume (see [SystemInfo Volumes](#systeminfo-volumes)). Every declared volume must be mounted by at least one container. A `microvm` template may declare several `durableDir` volumes; a `gvisor` template is limited to one. |
| `resources` | `*ResourceRequirements` | Optional. Declares each actor's compute size via `limits` — see [Sandbox Right-Sizing](#sandbox-right-sizing-specresources). Immutable, like the rest of the spec. |

The sandbox itself — the binaries (e.g. the gVisor `runsc` binary) and the `pauseImage` holding the sandbox's namespaces — comes from the cluster-scoped [`SandboxConfig`](#3-sandboxconfig-the-sandbox-itself) object named by `sandboxConfig.configName`. At runtime the workers resolve it from the `WorkerPool` side — by name (`workerPool.spec.sandboxConfigName`) or, by default, the cluster default `SandboxConfig` for the pool's `sandboxClass`.

Because a snapshot is not restorable across sandbox runtimes, `sandboxClass` is a **hard scheduling gate**: an actor is only ever placed on a `WorkerPool` of the matching class. It is AND'd with `workerSelector` (and the actor's `worker_selector`), which can only narrow the eligible pools further. It has no default — `sandboxConfig` is required — and, like the rest of the spec, is immutable, so each template's class is fixed at creation.

### Sandbox Right-Sizing (`spec.resources`)

Unlike a Pod, an actor is sized by its **`limits`** (CPU and Memory): the size is a property of the template, baked into snapshots, so it lives on the immutable `ActorTemplate` spec. Declared limits do three things:

1. **Size the sandbox.** The limits are supplied to the sandbox over the actor RPCs (control plane → atelet → ateom):
   - **gVisor (`ateom-gvisor`)** — applied to the container OCI spec: `limits.cpu` sets the cgroup v2 CPU quota (`cpu.max`) and the Sentry vCPU count (`--cpu-num-from-quota`); `limits.memory` sets the cgroup v2 memory limit (`memory.max`) and bounds the virtual total memory the sandbox reports (so JVM/Go do not over-allocate from host RAM).
   - **Micro-VM (`ateom-microvm`)** — `limits.cpu` sets Cloud Hypervisor `BootVcpus` / `MaxVcpus` (rounded up to whole vCPUs); `limits.memory` sets guest RAM, reserving a small configurable margin (default 256 MiB, `--vmm-mem-reserve-mib`) for the VMM and virtiofsd so the pod cgroup does not OOM.
2. **Gate scheduling.** An actor is only placed on a `WorkerPool` whose [worker capacity](#worker-capacity-spectemplateresources) is `>=` these limits.
3. **Fall back to runtime defaults.** A zero or absent limit leaves that dimension at the runtime default — unlimited for gVisor, the kata config for the micro-VM.

`requests` are not consulted today (an actor occupies its whole worker). Because the size is baked into snapshots, a **micro-VM FULL-scope restore reuses the size in the snapshot**; changing an actor's limits takes effect on its next cold boot.

Container environment variables support literal `value` entries only. Values are not interpolated (`$(VAR)` references are not expanded), and Kubernetes `envFrom`/`valueFrom` sources are not supported.

### Workload Connectivity (Uniform DNS)
Substrate uses a **Uniform DNS Mesh**: every actor created from a template is automatically reachable through the **Substrate Router** via its atespace and name:

**Format:** `<actor-name>.<atespace>.actors.resources.substrate.ate.dev`

### SystemInfo Volumes

To deliver identity information, including credentials, to a running actor, you can use a SystemInfo volume. Define it in `spec.volumes`, and mount it into each container that needs it.

Available information sources:

#### actorMetadata
The actorMetadata data source projects the actor's identity fields to files, one per item, analogous to the [Kubernetes downwardAPI volume](https://kubernetes.io/docs/concepts/storage/downward-api/). Each item selects a `field` — `name` (unique within an atespace), `atespace` (together with the name, the actor's full identity and DNS name), or `uid` (server-generated, distinguishes incarnations of the same name) — and the relative `path` the value is written to, raw with no trailing newline.

```yaml
spec:
  volumes:
  - name: system-info
    systemInfo:
      dataSources:
      - actorMetadata:
          items:
          - field: name
            path: actor-name
          - field: atespace
            path: atespace
          - field: uid
            path: actor-uid
  containers:
  - name: main
    # ...
    volumeMounts:
    - name: system-info
      mountPath: /run/ate   # the actor reads e.g. /run/ate/actor-name
```

The values are delivered as files on a read-only per-actor bind mount, not environment variables, precisely so they carry the correct values after a resume from a shared snapshot — an env var (or a file baked into the image) would be frozen at the snapshot-source actor's values, since it lives in the checkpointed process memory, and would therefore be identical for every actor restored from that snapshot. The metadata fields themselves are fixed for the actor's lifetime, so workloads may cache them; future data sources that rotate (identity tokens and certificates) must be re-read at time of use.

#### trustBundle
The trustBundle data source projects the trust anchors of a named trust bundle to a single PEM file — inspired by the [Kubernetes clusterTrustBundle projected volume source](https://kubernetes.io/docs/concepts/storage/projected-volumes/#clustertrustbundle), but source-neutral: the name selects a bundle substrate knows how to fetch, and where it is fetched from is a deployment concern, not part of the API.

Supported names are allowlisted. Today the only supported bundle is `egress-mitm.ate.dev` — the egress gateway CA bundle — resolved from the [ClusterTrustBundle](https://kubernetes.io/docs/reference/access-authn-authz/certificate-signing-requests/#cluster-trust-bundles) (`certificates.k8s.io/v1beta1`) that atecontroller's reconciler derives from the `egress-mitm-ca-pool` Secret in the `ate-system` namespace. A configurable backend registry may widen the allowlist later.

```yaml
spec:
  volumes:
  - name: trust
    systemInfo:
      dataSources:
      - trustBundle:
          name: egress-mitm.ate.dev
          path: ca.pem
  containers:
  - name: main
    # ...
    volumeMounts:
    - name: trust
      mountPath: /run/substrate/certs   # the actor reads /run/substrate/certs/ca.pem
```

atelet resolves the bundle on the node when the actor starts, reading the backing object through a cluster-wide watch (the same informer dynamic refresh will later hang off) and sanitizing it the way kubelet does for projections: only `CERTIFICATE` PEM blocks are kept, deduplicated, with block headers stripped and the anchors deliberately shuffled — order carries no meaning, so consumers must not depend on it. The actor itself never talks to any bundle backend. Starting the actor fails, with an error naming the bundle, if the name is not on the allowlist, the bundle's backend is unavailable in this deployment, or the resolved bundle is missing, empty, or contains no certificates. Bundle contents are re-resolved on every Run/Restore.

### Container Fields

Each entry in `containers` describes one process to run in the actor's sandbox.

| Field | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | **Required.** DNS-label-safe container name. |
| `image` | `string` | **Required.** Must be pinned by digest (`...@sha256:...`) — changing the image invalidates snapshots. |
| `command` | `[]string` | Optional. Entrypoint array. If unset, the image's `ENTRYPOINT` is used. If set, it replaces **both** the image's `ENTRYPOINT` and `CMD`. |
| `args` | `[]string` | Optional. Arguments to the entrypoint. If unset, the image's `CMD` is used (unless `command` is set, which discards the image's `CMD`). If set, it replaces the image's `CMD`. |
| `env` | `[]EnvVar` | Optional. Literal `value` entries. |
| `readyz` | `ContainerReadyz` | Optional. HTTP readiness probe — see [Container Readiness Probe](#container-readiness-probe-readyz). |
| `volumeMounts` | `[]VolumeMount` | Optional. Mounts a `spec.volumes` entry (e.g. `durableDir`) into this container. |
| `securityContext` | `SecurityContext` | Optional. Security settings for the container process — see [Container Capabilities](#container-capabilities-securitycontextcapabilities). |
| `resources` | `ContainerResources` | Optional. Compute limits for this container, enforced inside the actor's sandbox. Only `limits` is supported, and only `cpu` and `memory`. See [Per-container limits](#per-container-limits). |

`command` and `args` resolve against the container image's `ENTRYPOINT`/`CMD` the same way [Kubernetes Pod `command`/`args`](https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/) resolve against `ENTRYPOINT`/`CMD`. If the resolved argv is empty — the image sets neither `ENTRYPOINT` nor `CMD`, and the container sets neither `command` nor `args` — `Run`/`Restore` fails.

### Container Capabilities (`securityContext.capabilities`)

Each container runs with a default set of Linux capabilities — `AUDIT_WRITE`, `KILL` and `NET_BIND_SERVICE`. `securityContext.capabilities` adjusts that set, mirroring `securityContext.capabilities` on a Kubernetes Pod container.

| Field | Type | Description |
| :--- | :--- | :--- |
| `securityContext.capabilities.add` | `[]string` | Optional. Capabilities to grant on top of the default set. `ALL` is **not** accepted here. |
| `securityContext.capabilities.drop` | `[]string` | Optional. Capabilities to remove from the default set. `ALL` drops the whole set. |

- **Naming.** Capabilities are named **without** the `CAP_` prefix, as in Kubernetes — `NET_BIND_SERVICE`, not `CAP_NET_BIND_SERVICE`. The prefixed spelling is rejected at admission rather than silently granting nothing.
- **Order.** `drop` is applied first, then `add`. A capability named in both is therefore **granted**.
- **Exact sets.** Because `drop: ["ALL"]` clears the default set, combining it with `add` expresses an exact capability set rather than a relative one:

  ```yaml
  securityContext:
    capabilities:
      drop: ["ALL"]
      add: ["NET_BIND_SERVICE"]
  ```

- **`ALL` in `add` is rejected.** Kubernetes accepts it in the API and relies on PodSecurity admission to deny it; Substrate has no equivalent policy layer yet, so it is refused at admission instead. Name the capabilities the container needs.
- **Ambient capabilities are not supported** ([gvisor#3166](https://github.com/google/gvisor/issues/3166)).

The sandbox — gVisor or micro-VM — remains the isolation boundary; capabilities constrain the workload *inside* it.

### Per-container limits

A container may cap its own CPU and memory so it cannot starve or kill its siblings in the same actor:

```yaml
sandboxClass: microvm
containers:
  - name: trainer
    resources:
      limits: {memory: 1500Mi}
  - name: sidecar
    resources:
      limits: {memory: 256Mi, cpu: "0.2"}
```

A container that exceeds its memory limit is OOM-killed on its own; the actor's other containers are unaffected. A `cpu` limit below `10m` is raised to `10m`, because the kernel rejects a CFS quota under 1ms.

Per-container limits are micro-VM only today. gVisor applies cgroup limits at the sandbox level: one sentry backs every container in the actor, so a per-container cgroup is created and then stays empty ([google/gvisor#190](https://github.com/google/gvisor/issues/190)). A template that sets `resources` with `sandboxClass: gvisor` is rejected.

These limits subdivide the sandbox that [`spec.resources`](#sandbox-right-sizing-specresources) already sized; a container that declares none is bounded by the guest as a whole, not by a copy of the actor's total. A micro-VM guest is sized from `spec.resources.limits.memory` minus the VMM reserve, or from the pool's [`SandboxConfig`](#3-sandboxconfig-the-sandbox-itself) when the template declares no actor-level limit. The CPU ceiling is the guest's vCPU count, which falls back to the pool's `default_vcpus` (1 unless the `SandboxConfig` raises it), so a template that declares no `spec.resources.limits.cpu` caps each container, and their sum, at `1000m`. A limit above either ceiling can never bind, so the actor fails to start with an error naming both the limit and the ceiling.

Each limit is validated on its own at apply, but the sum across the actor's containers is only checked when the actor first runs, against the real guest size. A template whose limits do not fit is accepted by the API server and fails on its first actor.

### Container Readiness Probe (`readyz`)

Each entry in `containers` may declare an optional **HTTP readiness probe** so the platform only treats the actor as "started" once the workload is actually serving traffic. This mirrors the role of `readinessProbe.httpGet` on a Kubernetes Pod container, but the gate is enforced inside ateom (the in-pod sandbox driver) rather than by the kubelet.

| Field | Type | Description |
| :--- | :--- | :--- |
| `readyz.httpGet.path` | `string` | Optional. URL path to GET. Defaults to `/readyz`. Must begin with `/` and contain only RFC 3986 path characters (no query string `?` or fragment `#`). |
| `readyz.httpGet.port` | `int32` | **Required.** TCP port on the container to probe (`1..65535`). |

How it behaves:

- **Where the probe runs.** ateom (gVisor or microvm) reaches the container at the actor's interior IP (`169.254.17.2` today) — one network hop, no DNS, no router involved.
- **Block-until-ready semantics.** `RunWorkload` (cold start) and `RestoreWorkload` (resume from snapshot) only return successfully after every container with a `readyz` block returns HTTP 200. A failure surfaces as a Run/Restore error and is retried by the control plane; the overall wait is bounded by an internal 30s deadline.
- **Aggressive polling.** The poll loop is tuned for single-millisecond detection latency: a keep-alive HTTP client with a ~500µs interval and 250ms per-request timeout. While the workload is still booting, kernel `RST`s return in microseconds, so the loop spends almost no time blocked; once the listener is up, the next attempt completes on veth-local latency.
- **Golden snapshot warm-up shortcut.** When **every** container in a template declares `readyz`, the actor template controller skips its default ~20s "give the workload time to settle" delay before taking the golden snapshot — `ResumeActor` already blocked until the workload reported 200, so the workload is known to be initialized. Templates that omit `readyz` on any container keep the 20s warm-up as a safety net.
- **Snapshot/restore interaction.** The TCP listener is part of the checkpointed RAM, so on resume `readyz` typically returns 200 on the first attempt, with no observable latency penalty.

If `readyz` is omitted from a container, the prior "started == ready" behavior is preserved — the platform considers the container ready as soon as `runsc start` / `vm.boot` returns.

### Example

A protojson-shaped `ateapipb.ActorTemplate`, created through the ate API with
`kubectl ate create actor-template -f secret-agent.yaml` (the `ate-demo`
atespace must exist):

```yaml
metadata:
  atespace: ate-demo
  name: secret-agent
containers:
- name: agent
  image: gcr.io/my-project/my-agent:latest
  # Optional: gate Run/Restore on the agent's HTTP readiness endpoint.
  # See "Container Readiness Probe (readyz)" above.
  readyz:
    httpGet:
      path: /readyz
      port: 80
workerSelector:
  matchLabels:
    workload: secret-agent
# Both fields are required: sandboxClass picks the runtime family (set
# SANDBOX_CLASS_MICROVM to require micro-VM pools) and configName names the
# cluster-scoped SandboxConfig supplying the sandbox binaries (see section 3);
# gvisor-default is the cluster-wide default that manifests/ate-install ships.
sandboxConfig:
  sandboxClass: SANDBOX_CLASS_GVISOR
  configName: gvisor-default
snapshotsConfig:
  storageLocation: gs://my-bucket/secret-agent
```

### Snapshot Storage Layout

`snapshotsConfig.storageLocation` is a **base prefix**, not the address of any one snapshot. Every snapshot taken from the template lands at:

```
<location>/snapshots/<atespace>/<snapshot name>
```

and the objects of that snapshot (its manifest, memory image, durable-data tar) are named below it. So for the template above, a snapshot named `f47ac10b-…` of an actor in atespace `team-a` is stored at `gs://my-bucket/secret-agent/snapshots/team-a/f47ac10b-…`, and the template's golden snapshot — the golden actor lives in the reserved `ate-golden` atespace — at `gs://my-bucket/secret-agent/snapshots/ate-golden/<name>`.

Each `ActorSnapshot` reports its own address in the server-managed `status.snapshotUri` field. It is recorded when the snapshot is written, not recomputed on read, so the layout can change in future versions without stranding existing snapshots. Do not send it on input; parse it only against the scheme above.

An `ActorTemplate` belongs to one atespace, but one `storageLocation` still holds snapshots for many atespaces: the golden actor lives in the reserved `ate-golden` atespace, and a `PUBLISHED` snapshot may be cloned from other atespaces. The `<atespace>` level exists so that access can be granted per tenant: an object-storage policy can only condition on an **object-name prefix**, and cannot read the identity recorded inside a snapshot's manifest. Binding a per-atespace grant on GCS looks like:

```yaml
# Read-only on team-a's snapshots for this template, and nothing else.
- members: ["serviceAccount:node-runtime@my-project.iam.gserviceaccount.com"]
  role: roles/storage.objectViewer
  condition:
    title: team-a-snapshots
    expression: >
      resource.name.startsWith(
        "projects/_/buckets/my-bucket/objects/secret-agent/snapshots/team-a/")
```

Two consequences worth planning for:

- **A published snapshot is read from the atespace that took it.** Cloning across atespaces via a `PUBLISHED` tag reads the source atespace's prefix, so the reader needs a grant covering it — the target atespace's grant is not enough.
- **A location containing its own `snapshots` segment is legal but confusing.** `gs://my-bucket/snapshots/secret-agent` yields `gs://my-bucket/snapshots/secret-agent/snapshots/<atespace>/<name>`. It parses correctly; it just reads badly in a policy.

---

## 3. SandboxConfig: The Sandbox Itself

`SandboxConfig` is a **cluster-scoped** resource that decouples the sandbox — its binaries (the gVisor `runsc` binary, or a micro-VM kernel/firmware/config) and the `pauseImage` that holds the sandbox's namespaces — from the `ActorTemplate`. A `WorkerPool` resolves its sandbox from a `SandboxConfig` — either the one named by `spec.sandboxConfigName`, or the cluster default for the pool's `sandboxClass`.

This means a single, cluster-managed config pins the sandbox runtime version for many templates: snapshots stay restorable because the version is recorded in each snapshot's manifest, and operators upgrade the runtime in one place.

### Specification (`SandboxConfigSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `sandboxClass` | `string` | **Required.** Runtime family this config applies to: `gvisor` (default) or `microvm`. A `WorkerPool` only uses `SandboxConfig`s whose `sandboxClass` matches its own. |
| `pauseImage` | `string` | **Required.** The image for the sandbox's root container (e.g. `registry.k8s.io/pause`, or `gcr.io/gke-release/pause` on GKE). Must be pinned by digest (`...@sha256:...`) — it is recorded in each snapshot's manifest so a restore rebuilds the sandbox from the same image. |
| `default` | `bool` | Optional. Marks this as the cluster default for its `sandboxClass`. A `WorkerPool` with no `sandboxConfigName` resolves to the default for its class. At most one default per class. |
| `assets` | `map[arch]map[name]AssetFile` | Optional. Content-addressed files atelet fetches, keyed by architecture (`amd64`, `arm64`) then asset name. gVisor expects a `gvisor` asset (the release's `gvisor.tar.bz2`), which atelet auto-extracts. A micro-VM backend expects several. Each `AssetFile` is a `{ url, sha256 }` pair. |

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
  pauseImage: "registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4"
  assets:
    amd64:
      gvisor:
        url: "gs://gvisor/releases/nightly/2026-08-28/x86_64/gvisor.tar.bz2"
        sha256: "97f83fa5f352f2c6337d792b1c23c4e73a9c47529c08f6531029f8e0722cfe2c"
    arm64:
      gvisor:
        url: "gs://gvisor/releases/nightly/2026-08-28/aarch64/gvisor.tar.bz2"
        sha256: "561e281b7f8af95205b1df140c453a795a7fbc0db348c63b305e6521350734ef"
```

### Micro-VM SandboxConfig

A `microvm` `SandboxConfig` supplies the [Kata Containers](https://katacontainers.io/) + [Cloud Hypervisor](https://www.cloudhypervisor.org/) toolchain instead of `runsc`. Each architecture must define the full asset set — `kata-shim`, `cloud-hypervisor`, `virtiofsd`, `kata-kernel`, `kata-image`, and `kata-config` — which a `ValidatingAdmissionPolicy` enforces at apply time. Worker pods for a micro-VM pool require `/dev/kvm` and nested-virtualization-capable nodes. The controller requests those devices on the pod automatically, and atelet advertises them only where they exist, so placement follows the hardware rather than a node label. Clusters that reserve nested-virt nodes with an `ate.dev/sandboxClass=microvm` taint are still tolerated: advertising a device attracts these pods to capable nodes but repels nothing else from them.

See [`hack/microvm-assets/`](../hack/microvm-assets/) for scripts that assemble and stage these assets, plus a worked counter demo (`demos/counter/counter-microvm.yaml.tmpl`) that suspends and resumes an in-RAM counter across worker pods.

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
    *   `actor`: `Actor` — the actor to create. Its `metadata` carries the atespace and name (name must be a DNS-1123 label); the `actor_template` ref (atespace + name) selects the `ActorTemplate`.
*   **Response:** the initialized `Actor`.

#### `UpdateActor`
Replaces the mutable fields of an existing actor with the ones in the request.
*   **Request:** `UpdateActorRequest`
    *   `actor`: `Actor` — the complete replacement actor. `metadata.atespace` and `metadata.name` identify the resource; `metadata.uid` and `metadata.version` are **required** preconditions. `metadata` and `status` are server-owned and whatever the request carries in them is ignored. `source_snapshot_tag` is immutable.
*   **Response:** the updated `Actor`.
*   **Errors:** `INVALID_ARGUMENT` if `uid` or `version` is unset, or if the request changes an immutable field — including by leaving one unset; `ABORTED` if either guard no longer matches the stored resource.

Because the guards are required and only a read supplies them, an update is always a read-modify-write. To Update an `Actor`, you must first `GetActor`/`CreateActor`, instead of building a new one — see [§7.2 of the API style guide](api-style-guide.md#72-using-version-and-uid-to-guard-writes) for why reconstructing the message can silently drop data.

#### `ResumeActor`
Activates a suspended actor by restoring it onto a physical worker.
*   **Request:** `ResumeActorRequest`
    *   `actor`: `ObjectRef` of the actor to resume.
    *   `boot`: (Optional) If `true`, bypasses snapshots and performs a cold boot.
*   **Response:** `ResumeActorResponse` containing the updated `Actor` object (including the physical worker placement in `status.worker_assignment`).

#### `SuspendActor`
Hibernate a running actor, capturing its current RAM and disk state into a snapshot.
*   **Request:** `SuspendActorRequest`
    *   `actor`: `ObjectRef` of the actor to suspend.
*   **Response:** `SuspendActorResponse` containing the `Actor` object in `ACTOR_STATE_SUSPENDED`.

#### `DeleteActor`
Removes an actor from the registry and cleans up associated resources.
*   **Request:** `DeleteActorRequest`
    *   `actor`: `ObjectRef` of the actor to delete. Delete takes no preconditions today, so it is last-writer-wins.
    *   `any_state`: (Optional) If `true`, allows deleting the actor from any state (e.g. `RUNNING`, `PAUSED`), terminating active workloads, detaching volumes, and releasing worker allocations. By default (`false`), only actors in `ACTOR_STATE_SUSPENDED` or `ACTOR_STATE_CRASHED` (or already `ACTOR_STATE_DELETING`) can be deleted.
*   **Response:** the deleted `Actor`, as it was immediately before removal.

#### `GetActor` / `ListActors`
Query the state of logical actors.
*   **GetActor:** Retrieves a single actor by ID.
*   **ListActors:** Lists all actors currently tracked in the database.

#### `ListWorkers`
Query the physical resource pool.
*   **Request:** `ListWorkersRequest`
*   **Response:** `ListWorkersResponse` containing a list of `Worker` objects (Pods) and their current assignment status.

---

## 7. Advanced: Actor Identity Credentials

Workloads can exchange their ephemeral Kubernetes credentials for stable **Actor Identity** credentials that persist even as the process migrates between different physical workers. This is distinct from the `actorMetadata` data source described under [SystemInfo Volumes](#systeminfo-volumes), which only tells an actor its own identity fields (name, atespace, uid).

### Service: `ateapi.ActorIdentity`
*   **`MintJWT`:** Generates an OIDC-compatible JWT identifying the Substrate Actor.
*   **`MintCert`:** Signs a Certificate Signing Request (CSR) to provide an mTLS identity for the actor.

Both RPCs identify the actor the same way the rest of the API does, by `atespace` and `actor_name`.

#### Who may call `MintCert` and `MintJWT`

`MintCert` and `MintJWT` are not callable by actors directly. They must be called over mTLS with a
Pod Certificate, and the broker only signs a CSR when all of the following hold:

1.  The client certificate identifies the **`atelet`** service account
    (`spiffe://cluster.local/ns/ate-system/sa/atelet`) and carries a Pod Identity
    extension, which pins the calling atelet to a node.
2.  The requested actor is **currently running**, per the actor database.
3.  The worker Pod hosting that actor is on the **same node** as the calling
    atelet, and is still assigned to that actor.

An atelet is therefore confined to minting credentials for the actors it is
actually hosting. Callers that fail any of these checks receive
`PERMISSION_DENIED` with no detail, so the RPC cannot be used to discover
whether an actor exists or where it is running. An actor that exists but is
suspended, paused, or crashed yields `FAILED_PRECONDITION`.

The minted leaf certificate carries the SPIFFE URI
`spiffe://substrate-actor.local/atespace/${atespace}/actor/${actor_name}`.

---

## 8. Framework & Ecosystem Integration

Agent Substrate is designed to be the foundational execution layer for any agentic framework.

### Agent Development Kit (ADK)
Substrate provides native support for ADK-compatible identities. Workloads can use the `ActorIdentity` service to mint JWTs that align with ADK's security model, ensuring seamless integration with ADK-managed tools and memory.

### LangChain
Substrate is an ideal runtime for stateful LangChain agents. By defining a LangChain agent as an `ActorTemplate`, you can preserve the agent's internal "thought process" and conversation history in memory across hibernations, while sandboxing its tool execution for security.

### Claude Code & CodeX
For developer-focused agents, Substrate enables massive multiplexing of coding environments. Each developer can have a dedicated, persistent terminal session (Actor) that preserves filesystem deltas, while the cluster only runs physical pods for active users.
