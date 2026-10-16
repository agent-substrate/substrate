# Substrate API Guide: WorkerPool & ActorTemplate

This guide explains how to configure Substrate resources to deploy high-density, stateful agents.

## 1. WorkerPool: The Physical Capacity

The `WorkerPool` defines the pool of physical "warm" compute capacity. It manages a fleet of standby pods (herders) that are ready to receive and execute actor states.

### Specification (`WorkerPoolSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `replicas` | `int32` | **Required.** Number of physical standby pods to maintain in the cluster. |
| `workerImage` | `string` | **Required.** The container image for the `ateworker` herder process (e.g. `ko://go.ate.dev/substrate/cmd/ateworker-gvisor`). |
| `sandboxClasses` | `[]WorkerPoolSandboxClass` | **Required.** The sandbox runtime families the pool runs; exactly one entry today. Each entry has `name` (**required**, `gvisor` or `microvm`, immutable), which drives the worker pod shape (e.g. KVM device mounts, node placement), and `configRef.name` (optional), which names a cluster-scoped [`SandboxConfig`](#3-sandboxconfig-the-sandbox-itself) of the same class. Without `configRef`, the pool uses the class default (see [Class default](#class-default)). The controller records the result in `status.sandboxClasses`. The sandbox binaries an actor boots with come from the `SandboxConfig` the pool of its assigned worker resolved. |
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
| `serviceAccountName` | `string` | `spec.serviceAccountName`; defaults to `ateworker`, which the controller creates |

Keys in `ate.dev/` and its subdomains (for example, `policy.ate.dev/`) are
reserved for controllers and cannot be set in `template.labels` or
`template.annotations`. Metadata keys and label values must follow Kubernetes
syntax.

`template.labels` and `template.annotations` only configure Kubernetes workload
metadata; they do not affect actor scheduling. Actor selectors match
`WorkerPool.metadata.labels`, not `WorkerPool.spec.template.labels`.

#### Worker Capacity (`spec.template.resources`)

Setting `resources.limits` (CPU and Memory) on a `WorkerPool` establishes each worker pod's **capacity** — the envelope its actor sandboxes share, taken from the `ateworker` container's limits. The scheduler only places an actor on a worker whose remaining capacity is `>=` the actor's declared resource limits (see [Sandbox Right-Sizing](#sandbox-right-sizing-resources) on the `ActorTemplate`).

- Worker capacity is a shared budget: each actor placed on a worker subtracts its declared limits from what is left. Size a pool's `limits` for the actors it should host together.
- A worker also has an actor limit, set by the ateworker's `--max-actors` flag (default 1000). Placement stops at whichever runs out first.
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
  workerImage: ko://go.ate.dev/substrate/cmd/ateworker-gvisor
  sandboxClasses:
  - name: gvisor
    configRef:
      name: gvisor-default
  template:
    labels:
      project: agent-platform
    annotations:
      policy.example.com/exemption: sandbox-host
```

### Devices (GPUs) — temporarily unsupported

Substrate does not pass devices through to actors. A GPU worker pod's devices were
injected into every actor container; that was removed while the device API is
designed, since a resource name and a count cannot express which container gets a
device, sharing one, or resuming onto compatible hardware.

A `nvidia.com/gpu` pool limit is now an ordinary extended resource: it places the
worker pod on a GPU node and reserves the device, and nothing else reads it.


### Status (`WorkerPoolStatus`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `replicas` | `int32` | Total number of worker pods, mirrored from the managed Deployment. |
| `selector` | `string` | Label selector for the worker pods. |
| `sandboxClasses` | `[]WorkerPoolSandboxClassStatus` | One entry per sandbox class, with `name`, `configRef.name` (the `SandboxConfig` in use), `pendingConfigRef.name` (transient, the config the class is switching to; see [Deletion protection](#deletion-protection)), and `conditions`. The `Resolved` condition is `True` (reason `Resolved`) when the class's spec resolves. Otherwise it is `False` with reason `SandboxConfigNotFound`, `SandboxConfigDeleting`, `SandboxClassMismatch`, or `DefaultConfigNotFound`, the controller emits a Warning event, and `configRef` keeps the last resolved config. `configRef` is omitted if the class has never resolved. Each time `configRef` changes, the controller emits a Normal `SandboxConfigSwitched` event naming the old and new config, so `kubectl events --for workerpool/NAME` shows the configs the pool used recently. The API server keeps an event for one hour by default, so the full history is only in a logging backend that stores events. The controller creates the worker Deployment only once every class has a config, and keeps updating an existing Deployment while a class does not resolve. |

---

## 2. ActorTemplate: The Workload Blueprint

The `ActorTemplate` defines the code, environment, and state-management policies for a specific type of agent. By default, it is used to generate a "Golden Snapshot" that new actors can start from.

### Specification (`ActorTemplate`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `containers` | `[]Container` | **Required.** The workload definition — see [Container Fields](#container-fields) below. Each container may also declare an optional `wakeupProbe` HTTP probe — see [Container Wakeup Probe](#container-wakeup-probe-wakeupprobe). |
| `workerSelector` | `*Selector` | Optional. Gates which `WorkerPool`s actors from this template may use, by matching against each pool's labels (`matchLabels`). If unset, all pools are eligible (subject to the actor's own `worker_selector`). The pool decides the sandbox class, so this is also how a template pins one: if your cluster has pools of more than one sandbox class and the workload needs a particular class, make sure the selector points only at pools of that class. An actor booting with no snapshot and no selector may land on any class, and keeps it from its first checkpoint on. |
| `snapshotConfig` | `SnapshotConfig` | Optional; omitted fields take their defaults. The base object-storage location snapshots are written under (a preview field, ignored without the `PerTemplateSnapshotLocation` gate), the snapshot fidelity (`preferredFidelity`) every Pause and Suspend captures, and automated golden snapshot creation (`goldenSnapshotConfig.mode`). See [Snapshot Storage Layout](#snapshot-storage-layout). |
| `volumes` | `[]Volume` | Optional. Volumes the containers may mount, each a `durableDir`, an `external` CSI volume source (see [CSI Volumes Guide](csi-volumes.md)), or a `systemInfo` volume (see [SystemInfo Volumes](#systeminfo-volumes)). Every declared volume must be mounted by at least one container. A `durableDir` is created `0777`, as Kubernetes creates an emptyDir, so a container running as any user can write it. A snapshot restores it with the mode it had, so the container's own chmod survives suspend and resume. |
| `resources` | `*ResourceRequirements` | Required. Declares each actor's compute size via `limits`, which must set both `cpuMillicores` and `memoryBytes` — see [Sandbox Right-Sizing](#sandbox-right-sizing-resources). Immutable, like the rest of the template. |

The sandbox runtime and its binaries (e.g. the gVisor `runsc` binary, or a micro-VM kernel/firmware/config) are configured on the [`WorkerPool`](#1-workerpool-the-compute-foundation) via its `sandboxClasses` and the cluster-scoped [`SandboxConfig`](#3-sandboxconfig-the-sandbox-itself) it resolves, rather than on the `ActorTemplate`. An `ActorTemplate` selects its `WorkerPool`s via `workerSelector` (AND'd with the actor's `worker_selector`). When an actor runs on a worker, the worker's active `SandboxRuntime` is stamped onto the actor's assignment and snapshots so subsequent resumes prefer runtime-compatible workers.

### Sandbox Right-Sizing (`resources`)

Unlike a Pod, an actor is sized by its **`limits`** (CPU and Memory): the size is a property of the template, baked into snapshots, so it lives on the immutable `ActorTemplate`. Both are required. Declared limits do two things:

1. **Size the sandbox.** The limits are supplied to the sandbox over the actor RPCs (control plane → atelet → ateworker):
   - **gVisor (`ateworker-gvisor`)** — applied to the container OCI spec: `limits.cpuMillicores` sets the cgroup v2 CPU quota (`cpu.max`) and the Sentry vCPU count (`--cpu-num-from-quota`); `limits.memoryBytes` sets the cgroup v2 memory limit (`memory.max`) and bounds the virtual total memory the sandbox reports (so JVM/Go do not over-allocate from host RAM).
   - **Micro-VM (`ateworker-microvm`)** — `limits.cpuMillicores` sets Cloud Hypervisor `BootVcpus` / `MaxVcpus` (rounded up to whole vCPUs); `limits.memoryBytes` sets guest RAM, reserving a small configurable margin (default 256 MiB, `--vmm-mem-reserve-mib`) for the VMM and virtiofsd so the pod cgroup does not OOM.
2. **Gate scheduling.** An actor is only placed on a `WorkerPool` whose [worker capacity](#worker-capacity-spectemplateresources) is `>=` these limits.

`requests` are not consulted today (an actor occupies its whole worker). Because the size is baked into snapshots, a **micro-VM MEMORY-fidelity restore reuses the size in the snapshot**; changing an actor's limits takes effect on its next cold boot.

Container environment variables support literal `value` entries only. Values are not interpolated (`$(VAR)` references are not expanded), and Kubernetes `envFrom`/`valueFrom` sources are not supported.

### Workload Connectivity

A higher-order system reaches an actor through the **Substrate Router** by
setting `ate-target-actor` to `<atespace>/<actor>`. This value selects the Actor;
`Host` and HTTP/2 `:authority` remain application metadata. Normal HTTP requirements still apply:
clients must send a valid `Host` or `:authority`, usually derived automatically
from the request URL, and reverse proxies should preserve it when the application
depends on the original authority.

Clients that construct HTTP requests should add the routing header directly.
For example, curl uses `-H`, Go uses `request.Header.Set`, and Python clients use
their request `headers` mapping. WebSocket clients add the same header to the
opening HTTP upgrade request. gRPC clients send it as outgoing metadata using
the lowercase name `ate-target-actor`.

For an HTTP `CONNECT` tunnel to a non-default Actor port, put the routing
header on the outer `CONNECT` request and keep the target port in its
authority. With curl, use `--proxy-header` instead of `-H`:

```bash
curl --proxytunnel --proxy http://localhost:8001 \
  --proxy-header "ate-target-actor: my-atespace/my-actor" \
  http://actor-upstream:9090/
```

Browser navigation cannot add custom request headers. Browser-based and other
fixed clients must therefore connect through a user-controlled reverse proxy or
policy enforcement point that overwrites the routing header before forwarding
to `atenet-router`. The [Jupyter demo](../demos/jupyter/README.md) shows this
pattern with NGINX. The proxy must derive the value from trusted configuration
or authenticated request context rather than forwarding values supplied by an
untrusted caller.

### SystemInfo Volumes

To deliver identity information, including credentials, to a running actor, you can use a SystemInfo volume. Define it in `volumes`, and mount it into each container that needs it.

Available information sources:

#### actorMetadata
The actorMetadata data source projects the actor's identity fields to files, one per item, analogous to the [Kubernetes downwardAPI volume](https://kubernetes.io/docs/concepts/storage/volumes/#downwardapi). Each item selects a `field` — `ACTOR_METADATA_FIELD_NAME` (unique within an atespace), `ACTOR_METADATA_FIELD_ATESPACE` (together with the name, the actor's full identity), or `ACTOR_METADATA_FIELD_UID` (server-generated, distinguishes incarnations of the same name) — and the `path` the value is written to, raw with no trailing newline. `path` is a clean relative path from the root of the volume (no leading `/`, no `.` or `..` segments, at most 16 segments) and must not repeat another path projected into the same volume.

```yaml
volumes:
- name: system-info
  systemInfo:
    dataSources:
    - actorMetadata:
        items:
        - field: ACTOR_METADATA_FIELD_NAME
          path: actor-name
        - field: ACTOR_METADATA_FIELD_ATESPACE
          path: atespace
        - field: ACTOR_METADATA_FIELD_UID
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
The trustBundle data source projects the union of the trust anchors of one or more named trust bundles (at most 8) to a single PEM file — inspired by the [Kubernetes clusterTrustBundle projected volume source](https://kubernetes.io/docs/concepts/storage/projected-volumes/#clustertrustbundle), but source-neutral: the name selects a bundle substrate knows how to fetch, and where it is fetched from is a deployment concern, not part of the API.

Supported names are allowlisted:

* `egress-mitm.ate.dev` — the egress gateway man-in-the-middle CA bundle.
* `system-roots.ate.dev` — A selection of public CA root certificates provided
  by Substrate.  In official Substrate images, this is the Mozilla Root Store as
  shipped on Debian (consumed via the distroless-static base image).

```yaml
volumes:
- name: trust
  systemInfo:
    dataSources:
    - trustBundle:
        names:
        - egress-mitm.ate.dev
        path: ca.pem
containers:
- name: main
  # ...
  volumeMounts:
  - name: trust
    mountPath: /run/substrate/certs   # the actor reads /run/substrate/certs/ca.pem
```

atelet resolves the bundle on the node when the actor starts, reading the backing object through a cluster-wide watch (the same informer that drives live refresh) and sanitizing it the way kubelet does for projections: only `CERTIFICATE` PEM blocks are kept, deduplicated across all the named bundles, with block headers stripped and the anchors deliberately shuffled, so consumers must not depend on their order. The actor itself never talks to any bundle backend. Starting the actor fails, with an error naming the bundle, if any name is not on the allowlist, the bundle's backend is unavailable in this deployment, or the resolved bundle is missing, empty, or contains no certificates.

Bundle contents are re-resolved on every Run/Restore and refreshed while the actor runs: when any backing bundle changes, atelet rewrites the projected file atomically at the same path. As with the Kubernetes clusterTrustBundle projection, the application must re-read the file to pick up a rotation; a runtime that loads trust anchors once at startup sees the change at its next start or resume. If a refresh fails because the backing object was deleted or is unusable, the file keeps its last good contents. Bundle publishers should rotate with overlap: add the new CA before minting leaves under it, and keep the old CA until its last leaf expires.

### Container Fields

Each entry in `containers` describes one process to run in the actor's sandbox.

| Field | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | **Required.** DNS-label-safe container name. |
| `image` | `string` | **Required.** Container image name; must include a digest (`name@sha256:...`). |
| `command` | `[]string` | Optional. Entrypoint array. If unset, the image's `ENTRYPOINT` is used. If set, it replaces **both** the image's `ENTRYPOINT` and `CMD`. |
| `args` | `[]string` | Optional. Arguments to the entrypoint. If unset, the image's `CMD` is used (unless `command` is set, which discards the image's `CMD`). If set, it replaces the image's `CMD`. |
| `env` | `[]EnvVar` | Optional. Literal `value` entries. |
| `wakeupProbe` | `WakeupProbe` | Optional. HTTP wakeup probe — see [Container Wakeup Probe](#container-wakeup-probe-wakeupprobe). |
| `volumeMounts` | `[]VolumeMount` | Optional. Mounts a `volumes` entry (e.g. `durableDir`) into this container. |
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

A container may cap its own CPU and memory so it cannot starve or kill its siblings in the same actor (on a template whose `workerSelector` targets a micro-VM pool):

```yaml
containers:
  - name: trainer
    resources:
      limits: {memory: 1500Mi}
  - name: sidecar
    resources:
      limits: {memory: 256Mi, cpu: "0.2"}
```

A container that exceeds its memory limit is OOM-killed on its own; the actor's other containers are unaffected. A `cpu` limit below `10m` is raised to `10m`, because the kernel rejects a CFS quota under 1ms.

Per-container limits are micro-VM only today. gVisor applies cgroup limits at the sandbox level: one sentry backs every container in the actor, so a per-container cgroup is created and then stays empty ([google/gvisor#190](https://github.com/google/gvisor/issues/190)). On a gVisor worker the per-container limits are accepted but not enforced; only the sandbox-level size applies.

These limits subdivide the sandbox that [`resources`](#sandbox-right-sizing-resources) already sized; a container that declares none is bounded by the guest as a whole, not by a copy of the actor's total. A micro-VM guest's memory is `resources.limits.memoryBytes` minus the VMM reserve, and its vCPU count is `resources.limits.cpuMillicores` rounded up to whole cores. A container limit above either ceiling can never bind, so the actor fails to start with an error naming both the limit and the ceiling.

Each limit is validated on its own at apply, but the sum across the actor's containers is only checked when the actor first runs, against the real guest size. A template whose limits do not fit is accepted by the API server and fails on its first actor.

### Container Wakeup Probe (`wakeupProbe`)

Each entry in `containers` may declare an optional **HTTP wakeup probe** so `ResumeActor` only returns once the workload is actually serving traffic. Unlike a Kubernetes `readinessProbe`, which reruns for the life of the pod to gate traffic to it, this is a one-shot gate on each wakeup: `ResumeActor` blocks until the endpoint returns 200, and the probe does not run again while the actor stays `RUNNING`.

| Field | Type | Description |
| :--- | :--- | :--- |
| `wakeupProbe.httpGet.path` | `string` | Optional. URL path to GET. Defaults to `/`. Must begin with `/` and contain only RFC 3986 path characters (no query string `?` or fragment `#`). |
| `wakeupProbe.httpGet.port` | `int32` | **Required.** TCP port on the container to probe (`1..65535`). |
| `wakeupProbe.timeoutSeconds` | `int32` | Optional. How long to keep polling before the wakeup fails (`1..3600`). Defaults to `30`. |

How it behaves:

- **When it runs.** On every `ResumeActor` that actually wakes the actor. A `ResumeActor` on an actor that is already `RUNNING` is a no-op and does not probe.
- **Block-until-ready semantics.** `ResumeActor` returns successfully only after every container with a `wakeupProbe` has returned HTTP 200. If any container has not done so within `timeoutSeconds` (30s by default), `ResumeActor` fails and the actor moves to `ACTOR_STATE_CRASHED`, as it does for any other error while waking. A crashed actor cannot be resumed directly: `ResumeActor` rejects it with `FAILED_PRECONDITION`. Call `RevertActor` first, which returns it to `ACTOR_STATE_SUSPENDED` with its last snapshot, then resume it again.
- **Aggressive polling.** The poll loop is tuned for single-millisecond detection latency: a keep-alive HTTP client with a 1ms interval and 250ms per-request timeout. While the workload is still booting, kernel `RST`s return in microseconds, so the loop spends almost no time blocked; once the listener is up, the next attempt completes on veth-local latency.
- **Golden snapshot warm-up shortcut.** When **every** container in a template declares `wakeupProbe`, the actor template controller skips its default ~20s "give the workload time to settle" delay before taking the golden snapshot — `ResumeActor` already blocked until the workload reported 200, so the workload is known to be initialized. Templates that omit `wakeupProbe` on any container keep the 20s warm-up as a safety net.
- **Snapshot/restore interaction.** The TCP listener is part of the checkpointed RAM, so on resume `wakeupProbe` typically returns 200 on the first attempt, with no observable latency penalty.

If `wakeupProbe` is omitted from a container, `ResumeActor` returns as soon as the sandbox has started that container, which can be before the workload is listening.

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
  image: gcr.io/my-project/my-agent@sha256:7f28ab0e...
  # Optional: gate ResumeActor on the agent's HTTP wakeup probe endpoint.
  # See "Container Wakeup Probe (wakeupProbe)" above.
  wakeupProbe:
    httpGet:
      path: /readyz
      port: 80
workerSelector:
  matchLabels:
    workload: secret-agent
# The actor's size, booked against worker capacity.
resources:
  limits:
    cpuMillicores: 1000
    memoryBytes: 1073741824 # 1Gi
snapshotConfig:
  storageLocation: gs://my-bucket/secret-agent
```

### Snapshot Storage Layout

`snapshotConfig.storageLocation` is a **base prefix**, not the address of any one snapshot. Every external snapshot has exactly one owner: the actor that took it, or the tag that copied it. And the owner is part of the path, so an object's name says who it belongs to:

```
<location>/atespaces/<atespace>/actors/<actor uid>/snapshots/<snapshot name>
<location>/atespaces/<atespace>/tags/<tag uid>
```

> [!NOTE]
> Snapshots are stored under ate-api-server's cluster-wide `--snapshot-location`. `snapshotConfig.storageLocation` is an optional preview field: with the `PerTemplateSnapshotLocation` preview gate (`--preview=PerTemplateSnapshotLocation`), a template that sets it stores its snapshots there instead. Without the gate, the field is ignored. `<location>` above is whichever of the two applies.
>
> With the gate enabled, only a client that opts in to preview features (`kubectl ate --preview`, or the `Ate-Preview-Allowed: true` header) may set `storageLocation`. `CreateActorTemplate` fails with `FailedPrecondition` when the template leaves it empty and ate-api-server has no `--snapshot-location`.
>
> Changing the flag or toggling the gate moves `<location>` for every template it affects. Snapshots already taken stay where they are and can still be resumed from. An actor's next suspend writes under the new location and releases its previous snapshot from the old one. Until then, the actor cannot change its `actor_template` (see `UpdateActor` below), and deleting it collects its prefix under the old location. If that release fails, the previous snapshot's objects stay under the old location, and deleting the actor later does not collect them.

The objects of a snapshot (its manifest, memory image, durable-data tar) are named below it. So for the template above, a snapshot of an actor in atespace `team-a` is stored at `gs://my-bucket/secret-agent/atespaces/team-a/actors/3f8b…/snapshots/f47ac10b-…`, and the template's golden snapshot under `gs://my-bucket/secret-agent/atespaces/team-a/tags/<tag uid>`.

An actor takes a series of snapshots over its life, so it gets a prefix of its
own and each snapshot sits below it. A tag holds exactly one, so the tag's
prefix *is* its snapshot's. Both owners are keyed on their UID, so recreating an
actor or tag under the same name never inherits its predecessor's objects. A tag
records its snapshot's URI in `status.snapshot` right after it is reserved,
before anything is copied, so a tag whose creation failed still names any
partial copy to collect.

An owner is collected by deleting everything under its prefix, and it can delete nothing else. That is what makes a borrowed snapshot safe: an actor created from a tag points at a URI under `tags/`, which its own prefix does not cover. See [Snapshot lifetime](#snapshot-lifetime).

An `Actor` reports its snapshots in the server-managed `status.snapshots` (and
`status.lastAssignedGeneration`) and a `Tag` in `status.snapshot`. Each entry is
a `Snapshot` with the following fields:

- `uuid`: The snapshot's server-generated name. It names the node-local
  checkpoint and forms the last segment of the snapshot's URI in object storage
  (so a tag's snapshot is named by the tag's UID).
- `generation`: The monotonically increasing snapshot generation on the owner.
- `owner`: The resource kind that owns the snapshot (`SNAPSHOT_OWNER_ACTOR` or
  `SNAPSHOT_OWNER_TAG`).
- `fidelity`: The state layers the snapshot holds (`SNAPSHOT_FIDELITY_VOLUMES`,
  `SNAPSHOT_FIDELITY_ROOTFS`, or `SNAPSHOT_FIDELITY_MEMORY`).
- `actorTemplateUid`: The UID of the `ActorTemplate` used to capture this snapshot.
  An actor can change templates while `SUSPENDED`. A resume checks the snapshot
  it restores. A snapshot captured under a replaced template restores volume
  data and boots fresh.
- `locality`: The node holding the snapshot's local files. For attachment rules,
  see `ActorStatus.assigned_node` in the [API schema](../pkg/proto/ateapipb/ateapi.proto).
- `durableSnapshot`: The copy in object storage, with its URI in
  `object.snapshotUri` and a `status` that is `PENDING` while the copy is being
  written and `COMPLETED` once it can be restored from. A pause records the copy
  as `PENDING`, and atelet uploads it in the background. A tag's snapshot always
  has one, and the tag can seed actors only once it is `COMPLETED`.
- `sandboxRuntime`: The sandbox runtime and hardware compatibility attributes the
  guest state was captured under, copied from `status.workerAssignment.sandboxRuntime`
  when the checkpoint is taken. A resume that restores memory state
  (`SNAPSHOT_FIDELITY_MEMORY`) first schedules onto
  an eligible worker whose `defaultRuntime` or `restorableRuntimes` is compatible
  with `sandboxRuntime` (same sandbox class, same schema version, and every
  attribute recorded on the snapshot present on the worker with the same
  value, except `cpu_features`, whose hexadecimal bitmask on the snapshot must be
  a bitwise subset of the worker's; attributes only the worker reports are
  ignored). When no compatible worker has
  capacity:
  - A durable snapshot — the actor's own, a `sourceTag` clone, or the
    template's golden snapshot a new actor starts from — falls back to any eligible
    worker and downgrades the restore to `SNAPSHOT_FIDELITY_ROOTFS`, preserving
    the snapshot's rootfs and volume data while booting the guest fresh under the
    new worker's `defaultRuntime`.
  - A local checkpoint first tries compatible workers on its attached node.
    If that fails, `ResumeActor` checks capacity without the node and runtime
    restrictions. When that capacity exists, it detaches the checkpoint and
    retries placement. Detach uploads it if needed, deletes the local files,
    and releases the binding. The retry prefers compatible workers before
    using the durable fallback above. A detach error leaves the actor attached.

  A snapshot without `sandboxRuntime` or with `SNAPSHOT_FIDELITY_ROOTFS` or
  `SNAPSHOT_FIDELITY_VOLUMES` imposes no runtime placement constraint. Because a
  `ROOTFS` restore reuses the container's rootfs upper layer over the template's
  image, pools selected by an `ActorTemplate` must share a CPU architecture.

An `ActorTemplate` references its golden tag with the `ObjectRef` in
`status.goldenSnapshotStatus.goldenTag`. These status fields are server-owned
and ignored on input. Parse a URI only against the scheme above.

An `ActorTemplate` belongs to one atespace, but one `storageLocation` still holds snapshots for many atespaces: the golden actor and tag live in the template's atespace, and a `PUBLISHED` snapshot may be cloned from other atespaces. The `<atespace>` level exists so that access can be granted per tenant: an object-storage policy can only condition on an **object-name prefix**, and cannot read the identity recorded inside a snapshot's manifest. Binding a per-atespace grant on GCS looks like:

```yaml
# Read-only on team-a's snapshots for this template, and nothing else.
- members: ["serviceAccount:node-runtime@my-project.iam.gserviceaccount.com"]
  role: roles/storage.objectViewer
  condition:
    title: team-a-snapshots
    expression: >
      resource.name.startsWith(
        "projects/_/buckets/my-bucket/objects/secret-agent/atespaces/team-a/")
```

One consequence worth planning for: **a published snapshot is read from the atespace that took it.** Cloning across atespaces via a `PUBLISHED` tag reads the source atespace's prefix, so the reader needs a grant covering it — the target atespace's grant is not enough.

---

## 3. SandboxConfig: The Sandbox Itself

`SandboxConfig` is a **cluster-scoped** resource that decouples the sandbox binaries (the gVisor `runsc` binary, or a micro-VM kernel/firmware/config) from the workload definition in the `ActorTemplate`. A `WorkerPool`'s `sandboxClasses` entry resolves its `SandboxConfig` (either by explicit `configRef` or via the class default), and `atelet` provisions each worker pod with the enabled versions of that config, using `defaultVersion` for cold boots and keeping other enabled versions in `restorableRuntimes` so existing snapshots can still restore across upgrades.

### Specification (`SandboxConfigSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `sandboxClass` | `string` | Optional. Runtime family this config applies to: `gvisor` (default) or `microvm`. A `WorkerPool` only uses `SandboxConfig`s whose `sandboxClass` matches its entry's `name`. |
| `defaultVersion` | `string` | **Required.** Name of the entry in `versions` that actors boot with. It must name an existing version that is not `Disabled`. |
| `versions` | `[]SandboxVersionConfig` | **Required.** 1–16 sandbox versions, unique by `name`. |

Each entry of `versions` (`SandboxVersionConfig`):

| Field | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | **Required.** Identifies the version within the config. A DNS label (lower-case alphanumerics and `-`, at most 63 characters). |
| `state` | `string` | Optional. `Enabled` (default) or `Disabled`. The version named by `defaultVersion` cannot be `Disabled`. |
| `assets` | `map[arch]map[name]AssetFile` | Content-addressed files atelet fetches, keyed by architecture (`amd64`, `arm64`) then asset name. gVisor expects a `gvisor` asset (the release's `gvisor.tar.zstd`), which atelet auto-extracts. A micro-VM backend expects several. Each `AssetFile` is a `{ url, sha256 }` pair; `url` is at most 2048 bytes. A `ValidatingAdmissionPolicy` enforces each class's required assets on every version. atelet holds no storage credentials: it fetches every URL through the node's object-store plugin (`AssetProvider`), with the plugin's credentials. Which schemes work depends on the plugin; the in-tree one serves only its storage backend's scheme, `gs://` on GCS and `s3://` on S3, as `<bucket>/<object>`, and rejects other schemes without reading them. A `gs://` URL the plugin cannot serve (it has no such object, rejects the URL, does not implement `AssetProvider`, is unreachable, or fails with an error it does not recognize, such as GCS refusing its credentials for a public bucket) is then read anonymously, which serves public buckets such as `gs://gvisor`; any other refusal by the plugin is final. The plugin returns an object only if its content matches `sha256`, and refuses any URL under a snapshot location (an `ActorTemplate`'s `snapshotConfig.location` followed by `/atespaces/`), so assets may share the snapshot bucket but must not be stored there. atelet verifies the sha256 again before using an asset. Its logs and errors show an asset URL as `scheme://host/path`. |

A cluster-wide gVisor `SandboxConfig` (`gvisor-default`, annotated as the class default) is installed with the platform, so gVisor `WorkerPool`s use it automatically without any extra setup.

### Class default

A `SandboxConfig` annotated `sandboxconfig.ate.dev/is-class-default: "true"` is a default for its `sandboxClass`. A `WorkerPool` entry without `configRef` uses the newest such config of its class that is not being deleted. Among configs created in the same second, the name that sorts first wins. More than one config of a class may carry the annotation, so a new default can be rolled out by creating it, and pools move to it without restarting their workers.

While no default is available, the class reports `Resolved=False` with reason `DefaultConfigNotFound`, or `SandboxConfigDeleting` if the only defaults are being deleted. A pool that already uses a config keeps it meanwhile, even if that config is being deleted or has lost the annotation.

### Deletion protection

atecontroller keeps a `SandboxConfig` from being deleted while a `WorkerPool` uses it.

- When a `WorkerPool` resolves a `SandboxConfig`, atecontroller puts the finalizer `sandboxconfig.ate.dev/workerpool-protection` on the config before it records the config in the pool's `status.sandboxClasses`.
- A pool that switches to another config first records the new config in `pendingConfigRef` and keeps its current config in `configRef`. It then reads the new config from the API server, and moves `configRef` only if the config is not being deleted. Otherwise it keeps its current config.
- The finalizer stays until the config is deleted. Deleting the config marks it for deletion. atecontroller removes the finalizer once no `WorkerPool`'s `status.sandboxClasses` names the config. It confirms that against the API server, not only its cache.
- A config that pools stopped using is gone within a reconcile. A config that pools still use stays `Terminating` until they move or are deleted. A config no pool has ever used carries no finalizer and is deleted at once.
- A config that is being deleted takes no new pools. A `configRef` to it reports `SandboxConfigDeleting`, and it stops being a class default. A pool already on it keeps it and reports `Resolved=False`. A pinned pool does so until its `configRef` changes, an unpinned pool until another default exists. So a `Terminating` config only loses users.
- `WorkerPool`s carry no finalizer and delete without waiting.

A delete cannot be undone, and an object that is being deleted cannot be recreated under the same name. Applying its manifest again only updates the terminating object. To recover from deleting a class default that pools still use:

1. Create a replacement under a different name with the `is-class-default` annotation. Pools move to it on their own, the finalizer is released, and the old config disappears.
2. Once the old config is gone, the next `ate-setup` upgrade recreates the shipped config under its original name. Pools move back to it, because it is then the newest default.

atecontroller alone adds and releases the finalizer. While it is down, `kubectl delete sandboxconfig` of a config that carries the finalizer hangs until atecontroller is back. `ate-setup` strips the finalizer from every config when it deletes the control plane.

### Example

```yaml
apiVersion: ate.dev/v1alpha1
kind: SandboxConfig
metadata:
  name: gvisor-default
  annotations:
    sandboxconfig.ate.dev/is-class-default: "true"
spec:
  sandboxClass: gvisor
  defaultVersion: v4
  versions:
  - name: v4
    state: Enabled
    assets:
      amd64:
        gvisor:
          url: "gs://gvisor/releases/nightly/2026-10-17/x86_64/gvisor.tar.zstd"
          sha256: "fa72f0d45756c69a30beec21222c78241b0d330c36f4f2185648617cc23415c8"
      arm64:
        gvisor:
          url: "gs://gvisor/releases/nightly/2026-10-17/aarch64/gvisor.tar.zstd"
          sha256: "238634978d99f0dd3e631c4f88eae7b19fb36937065428467f0b6d6b10697689"
```

### Micro-VM SandboxConfig

A `microvm` `SandboxConfig` supplies the [Kata Containers](https://katacontainers.io/) + [Cloud Hypervisor](https://www.cloudhypervisor.org/) toolchain instead of `runsc`. Each architecture of each version must define the full asset set — `cloud-hypervisor`, `virtiofsd`, `kata-kernel`, and `kata-image` — which a `ValidatingAdmissionPolicy` enforces at apply time. Worker pods for a micro-VM pool require `/dev/kvm` and nested-virtualization-capable nodes. The controller requests those devices on the pod automatically, and atelet advertises them only where they exist, so placement follows the hardware rather than a node label. Clusters that reserve nested-virt nodes with an `ate.dev/sandboxClass=microvm` taint are still tolerated: advertising a device attracts these pods to capable nodes but repels nothing else from them. The same convention applies to every class: worker pods of a pool tolerate `ate.dev/sandboxClass=<its class>:NoSchedule`, so a cluster can reserve a node pool per sandbox class with that taint, and the atelet DaemonSet tolerates the key for any value. On GKE, a pool can also select a ComputeClass with `template.nodeSelector` (`cloud.google.com/compute-class: <class>`): GKE taints the nodes it creates for the class `cloud.google.com/compute-class=<class>:NoSchedule` and adds the matching toleration to the worker pods, and the atelet DaemonSet tolerates that key as well.

See [`hack/microvm-assets/`](../hack/microvm-assets/) for scripts that assemble and stage these assets, plus a worked counter demo (`demos/counter/counter-microvm.yaml.tmpl`) that suspends and resumes an in-RAM counter across worker pods.

---

## 4. Operational Workflow

### The Golden Snapshot

Golden snapshot creation is enabled by default. To disable it, set
`snapshotConfig.goldenSnapshotConfig.mode: GOLDEN_SNAPSHOT_MODE_DISABLED` on the
template. Omitting the config or leaving `mode` unspecified defaults to
`GOLDEN_SNAPSHOT_MODE_ENABLED`. A disabled template creates no
golden actor or tag. Actors created without a `sourceTag` cold-boot and can still
pause, suspend, and resume using their own snapshots.

`kubectl ate get actor-templates` shows `DISABLED` in the `GOLDEN TAG` column
for these templates.

When an `ActorTemplate` is created with golden snapshots enabled:
1. Substrate creates and resumes a temporary golden actor in the template's atespace, using the same atespace-scoped policies and storage grants as ordinary actors.
2. It waits for readiness plus a 20-second warmup, then suspends the actor at the template's `preferredFidelity`. The extra interval is zero when every container has a wakeup probe, since Resume already waits for readiness.
3. It creates a published tag named after the template UID, copying the snapshot into tag-owned storage in the same atespace. The golden actor uses that name too.
4. It deletes the golden actor and records the tag reference in the template status.

Golden actors follow ordinary lifecycle and ingress policies and appear in
Actor lists. Leave these resources to the controller: modifying or deleting
them can interrupt Golden creation, and deleting a published Golden tag
invalidates the template's recorded reference. `DeleteActorTemplate` collects
its Golden tag and any unfinished Golden actor.

Golden capture follows `preferredFidelity`. MEMORY preserves processes and memory;
VOLUMES preserves startup data in mounted volumes and starts new processes on
restore. ROOTFS preserves the sandbox's root filesystem writes and also starts new
processes.

`CreateActor` uses an explicit `sourceTag` when supplied; otherwise it resolves the template's golden tag and records that snapshot on the new actor. If the golden tag is not ready yet, the actor starts without a snapshot and cold-boots even if the tag becomes ready before its first resume. The default does not populate the caller-owned `sourceTag` field.

### Resumption Lifecycle
CreateActor fixes the Actor's initial snapshot source at creation. Resume places
it on an eligible worker. A MEMORY snapshot restores saved processes; VOLUMES and
ROOTFS snapshots restore their persisted data and start new processes. An Actor
created without a snapshot boots from its template's images.

### Eviction
A worker pod can go away while it hosts running actors: when its `WorkerPool` rolls to a new worker image or pod template, as in an [upgrade](upgrade.md), or scales down, when its node is drained, or when its Spot VM is reclaimed.

When the pod gets `SIGTERM`, its worker stops accepting actors and sends `SIGTERM` to the main process of each container of every actor it hosts. Each actor then has 30 minutes, counted from the pod's `SIGTERM`, to be suspended. The 30 minutes is the worker's shutdown budget, fixed in ateworker and kept shorter than the pod's one-hour termination grace period so that the worker, not the kubelet, kills what is left. An actor suspended in that window keeps its state and resumes on another worker like any suspended actor.

Substrate does not suspend the actor for you. Whatever drives the actor, usually its harness, has to call `SuspendActor` with `DURABILITY_MODE_SYNCHRONOUS` (`kubectl ate suspend actor --mode=sync`) before those 30 minutes are up. An asynchronous suspend can lose the snapshot if the node goes away:

- It returns once the snapshot is on the node.
- atelet schedules the upload to object storage about `--snapshot-upload-delay` (5 minutes by default) after the snapshot is taken. Neither when the upload starts nor when it finishes is promised.
- Until atelet detaches the actor, `--snapshot-detach-delay` (30 minutes by default, unrelated to the worker's 30 minutes), the actor stays pinned to the node and resumes there.
- If the node goes away before the upload completes, the snapshot goes with it.

Only the actor gets the `SIGTERM`, so it has to pass that on, for example on an endpoint the harness polls.

Every actor that must keep its state through an eviction needs a `SIGTERM` handler that:

- **does not exit.** A process that has exited cannot be suspended.
- **runs in the container's main process.** Substrate signals only to the main process.
- **gets the actor suspended within the 30 minutes.**

An actor still running when the 30 minutes are up is killed. Once the pod is gone, the actor moves to `ACTOR_STATE_CRASHED`, and everything since its last snapshot is lost. `RevertActor` (`kubectl ate revert`) returns a crashed actor to `ACTOR_STATE_SUSPENDED` at its last external snapshot.

The node can cut the 30 minutes short. GKE node upgrades and cluster autoscaler scale-downs wait up to an hour for a pod, so they leave the full 30 minutes. A reclaimed GKE [Spot VM](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/spot-vms) gives pods 15 seconds by default and at most 2 minutes, so an actor there has to be suspended in that time or it crashes.

---

## 5. Best Practices
*   **Startup Logic:** Place expensive initialization (loading large models, establishing baseline connections) in your application's entry point. When golden snapshots are enabled, this initialization can be captured in the Golden Snapshot. Actors that cold-boot run initialization again.
*   **Placement:** The `WorkerPool` decides the sandbox class (`sandboxClasses[].name`), so use the template's `workerSelector` to target the pools whose class your workload needs — pool selection is by label match, not by namespace or RBAC. Without a selector an actor may land on any pool.
*   **Version Management:** When updating code, create a new `ActorTemplate` (e.g. `v2`). Substrate treats each template as an immutable state root. Suspended actors can be repointed to the new template via `UpdateActor` (`kubectl ate update actor --template`).
*   **Eviction:** Give every actor a `SIGTERM` handler that does not exit, and have the actor suspended within 30 minutes of it. See [Eviction](#eviction).

---

## 6. Control Plane gRPC API

The Substrate Control Plane (`ate-api-server`) exposes a gRPC interface for managing actors and workers. This is the primary API used by the `kubectl-ate` CLI and higher-level frameworks.

### Service: `ateapi.Control`

#### `CreateActor`
Registers a new logical actor in the system.
*   **Request:** `CreateActorRequest`
    *   `actor`: `Actor` — the actor to create. Its `metadata` carries the atespace and name (name must be a DNS-1123 label); the `actor_template` ref (atespace + name) selects the `ActorTemplate`.
    *   `actor.source_tag`: (Optional) `ObjectRef` of a `Tag` to seed the actor from. The tag must be taken under the same `ActorTemplate`, and either in the actor's own atespace or `PUBLISHED`. Nothing is copied: the new actor's `status.snapshots` points at the tag's snapshot, under the tag's prefix, until its own first suspend.
    *   `actor.worker_selector`: (Optional) Per-actor `WorkerSelector` merged with the template's `worker_selector` at resume time (actor keys override template keys).
*   **Response:** the initialized `Actor`.

#### `UpdateActor`
Replaces the mutable fields of an existing actor with the ones in the request (`kubectl ate update actor`).
*   **Request:** `UpdateActorRequest`
    *   `actor`: `Actor` — the complete replacement actor. `metadata.atespace` and `metadata.name` identify the resource; `metadata.uid` and `metadata.version` are **required** preconditions. `metadata` and `status` are server-owned and whatever the request carries in them is ignored. `source_tag` is immutable.
    *   `actor_template` may change only while the actor is `SUSPENDED`. The new template must declare the same volumes and volume mounts. Once the actor owns an external snapshot, the new template must also store snapshots under the same location: its `snapshotConfig.storageLocation` with the `PerTemplateSnapshotLocation` preview gate, otherwise ate-api-server's `--snapshot-location` (see [Snapshot Storage Layout](#snapshot-storage-layout)).
    *   `worker_selector` may be updated or cleared (`nil`) in any state; the updated selector takes effect on the next `ResumeActor`.
    *   A paused actor is initially pinned to its node (see `locality` in [Snapshot Storage Layout](#snapshot-storage-layout)). `ResumeActor` first looks for a worker there matching the template and actor selectors. If none is available but a worker satisfies the remaining constraints without node or runtime restrictions, resume detaches the checkpoint and retries placement. If no eligible worker has room, it returns `RESOURCE_EXHAUSTED`.
*   **Response:** the updated `Actor`.
*   **Errors:** `INVALID_ARGUMENT` if `uid` or `version` is unset, or if the request changes an immutable field — including by leaving one unset; `FAILED_PRECONDITION` if a change to `actor_template` breaks one of the rules above; `ABORTED` if either guard no longer matches the stored resource.

Because the guards are required and only a read supplies them, an update is always a read-modify-write. To Update an `Actor`, you must first `GetActor`/`CreateActor`, instead of building a new one — see [§7.2 of the API style guide](api-style-guide.md#72-using-version-and-uid-to-guard-writes) for why reconstructing the message can silently drop data.

#### `ResumeActor`
Activates a suspended or paused actor by restoring it onto a physical worker.
*   **Request:** `ResumeActorRequest`
    *   `actor`: `ObjectRef` of the actor to resume.
*   **Response:** `ResumeActorResponse` containing the updated `Actor` object (including the physical worker placement in `status.worker_assignment`).

#### `SuspendActor`
Hibernate a running actor, capturing its current RAM and disk state into a snapshot.
*   **Request:** `SuspendActorRequest`
    *   `actor`: `ObjectRef` of the actor to suspend.
    *   `options.mode`: (Optional) `DURABILITY_MODE_SYNCHRONOUS` returns once the snapshot is `COMPLETED` in object storage. `DURABILITY_MODE_ASYNCHRONOUS` (a pause) returns once the snapshot is captured on the worker's node, and atelet uploads it in the background. Unset runs an asynchronous suspend.
*   **Response:** `SuspendActorResponse` containing the `Actor` object in `ACTOR_STATE_SUSPENDED`, with its snapshot in `status.snapshots`.
*   Called on an actor that is already `ACTOR_STATE_SUSPENDED`, it returns the actor as it is, in either mode. atelet uploads a paused actor's node-local snapshot in the background, so `status.snapshots` may still show it with `locality` and a `PENDING` `durableSnapshot`.
*   A snapshot generation conflict returns `ABORTED` without crashing the actor. The actor can remain `ACTOR_STATE_SUSPENDING`, with its worker assignment and previous completed snapshot kept for recovery. Read the actor's latest state before retrying; repeating a stale request cannot resolve a persistent disagreement with atelet.
*   A successful suspend releases the actor's previous external snapshot: an actor keeps one, and only tags outlive it. To keep the snapshot a suspend just wrote, tag it with `CreateTag` while the actor is still suspended. After an asynchronous suspend, `CreateTag` first has atelet upload the snapshot if it has not already, so the call can take as long as that upload. If the upload fails, so does `CreateTag`, and the actor is left as it was.

#### Snapshot lifetime

Every external snapshot has exactly one owner, and the control plane deletes it when that owner lets go:

| Owner | Released when |
| :--- | :--- |
| The actor that took it (`status.snapshots`) | The actor's next successful suspend replaces it, or the actor is deleted. |
| The tag that copied it (`status.snapshot`) | The tag is deleted. |

An actor created from a tag borrows the tag's copy instead of taking one of its own. The borrowed URI sits under the tag's prefix, which the actor's own prefix does not cover, so neither suspending nor deleting the actor can reach it; its first own suspend writes a snapshot under the actor's prefix, and it owns its snapshots from then on.

Deletion always runs before the database reference is dropped, and a failure fails the whole RPC. Clients are expected to retry with the same arguments: destinations are deterministic and every phase tolerates a partly-completed predecessor, so a retry resumes rather than duplicating work. The cost of that ordering is that a crash between the two can leave an external snapshot no row names; the reverse order would instead lose the handle needed to ever delete it.

> **Do not delete a tag while actors created from it exist.** A clone borrows the tag's snapshot rather than copying it, and only stops borrowing at its own first suspend (its `status.snapshots` entry still names the tag's prefix while it is). Deleting the tag leaves such a clone unable to resume. This is not prevented today.

#### `RevertActor`
Discards an actor's live or crashed execution and transitions it to `ACTOR_STATE_SUSPENDED` at its last completed external snapshot (`status.snapshots`).
*   **Request:** `RevertActorRequest`
    *   `actor`: `ObjectRef` of the actor to revert. Accepted from `ACTOR_STATE_RUNNING` and `ACTOR_STATE_CRASHED` (plus `ACTOR_STATE_REVERTING` for idempotent retries), and from a paused actor: `ACTOR_STATE_SUSPENDED` whose latest snapshot is node-local. Calling `RevertActor` on an `ACTOR_STATE_SUSPENDED` actor with no node-local snapshot returns `FAILED_PRECONDITION`.
*   **Response:** `RevertActorResponse` containing the reverted `Actor` in `ACTOR_STATE_SUSPENDED`.
*   Reverting terminates any bound worker sandbox, clears node-local pause checkpoints, and garbage-collects any partial external snapshot left by an interrupted suspend while preserving the last committed durable snapshot in `status.snapshots`.
*   External volumes are not reverted. Their contents are never part of a snapshot, so a reverted actor comes back with its memory and root filesystem rewound but its volumes exactly as the discarded execution left them.

#### `DeleteActor`
Removes an actor in any state from the registry and cleans up associated resources, terminating active workloads, detaching volumes, and releasing worker allocations.
*   **Request:** `DeleteActorRequest`
    *   `actor`: `ObjectRef` of the actor to delete.
    *   `options`: (Optional) `DeleteOptions`. `uid` and `version` are preconditions checked against the actor as the caller last read it: a mismatch returns `ABORTED`, an omitted guard is skipped. They are checked before the workflow starts, so a guarded delete that fails part-way leaves the actor `ACTOR_STATE_DELETING` at a higher version. Retry it with the `uid` guard alone, or re-read first.
*   **Response:** the deleted `Actor`, as it was immediately before removal.
*   Deleting an actor also deletes the external snapshot it owns, along with one an interrupted suspend left behind. Snapshots it only borrows from a tag are left alone, and its tags are unaffected — they hold their own copies.

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

ate-api-server mints credentials that identify an actor to services outside Substrate. They name the logical actor, so they stay the same as the actor moves between workers. This is distinct from the `actorMetadata` data source described under [SystemInfo Volumes](#systeminfo-volumes), which only tells an actor its own identity fields (name, atespace, uid).

Both RPCs are on `ateapi.Control`. Only the egress gateway may call them; see [authorization.md](authorization.md). It calls `MintActorJwt` when an egress policy injects an actor JWT into a request.

### `MintActorJwt`

*   **Request:** `actor`, the actor's atespace and name; `audiences`, one to 16 values for the token's `aud`; and `expiration_seconds`, the lifetime, from 300 to 3600. The actor must exist, but it doesn't need to be running.
*   **Response:** `actor_jwt`, the signed token, and `expire_time`, which equals its `exp` claim.

The token's header carries `typ: JWT`, the algorithm of the pool's active signing key (ES256 or RS256), and that key's ID as `kid`. Its claims are:

*   `iss`: ate-api-server's `--actor-jwt-issuer`. ate-setup sets it from the `actorJWT.issuer` setting, which defaults to `https://idp.<namespace>.svc`.
*   `sub`: `actor/<atespace>/<name>`.
*   `aud`: the requested audiences, always a JSON array.
*   `exp`, `nbf`, `iat`: Unix seconds. `nbf` is five minutes before `iat`, to allow for clock skew.
*   `jti`: a random token ID.
*   `actor.ate.dev`: an object with the actor's `atespace`, `actorName`, and `actorUID`.

Relying parties find the signing keys through OpenID Connect Discovery at `<iss>/.well-known/openid-configuration`, which ate-idp-server serves in every install.

### `MintActorCertificate`

*   **Request:** `actor`; `actor_uid`, which must match the actor's current UID, so that a deleted and recreated actor isn't mistaken for the old one (`ABORTED` otherwise); and `certificate_signing_request`, a DER-encoded CSR. The signer checks the CSR's signature and uses only its public key.
*   **Response:** `actor_certificates`, DER certificates with the leaf first, followed by any intermediates.

The caller must connect with a client certificate; a bearer token is not enough. The leaf is valid for one hour, allows client and server authentication, carries the SPIFFE URI `spiffe://substrate-actor.local/actor/<atespace>/<name>`, and has the `ActorIdentity` X.509 extension with the actor's atespace, name, and UID.

---

## 8. Framework & Ecosystem Integration

Agent Substrate is designed to be the foundational execution layer for any agentic framework.

### Agent Development Kit (ADK)
An ADK agent running as an actor can have the egress gateway attach an actor JWT ([section 7](#7-advanced-actor-identity-credentials)) to its outbound requests, so the tools and services it calls can tell which actor is calling.

### LangChain
Substrate is an ideal runtime for stateful LangChain agents. By defining a LangChain agent as an `ActorTemplate`, you can preserve the agent's internal "thought process" and conversation history in memory across hibernations, while sandboxing its tool execution for security.

### Claude Code & CodeX
For developer-focused agents, Substrate enables massive multiplexing of coding environments. Each developer can have a dedicated, persistent terminal session (Actor) that preserves filesystem deltas, while the cluster only runs physical pods for active users.
