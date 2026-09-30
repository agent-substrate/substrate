# CSI Volumes for Actors in Agent Substrate

Substrate integrates with the **Container Storage Interface (CSI)** to provide dynamically provisioned, per-actor external volumes that seamlessly attach and detach as actors transition through their lifecycle.

---

## 1. CSI in Substrate vs. Standard Kubernetes

In Kubernetes, volumes are reconciled asynchronously via standard Kubernetes objects (e.g. `PersistentVolumeClaim`, `PersistentVolume`). Agent Substrate takes a different approach tailored for actor lifecycle operations:

* **No PV or PVC Objects:** External volumes are declaratively defined in the [`ActorTemplate`](api-guide.md#2-actortemplate-the-workload-blueprint) via `externalVolumeTemplate` and provisioned dynamically for each actor instance. Volume operations are coupled directly with the actor lifecycle.
* **Direct Network-Based CSI Controller:** The Substrate control plane (`ateapi`) communicates directly with the CSI Controller gRPC service over the network (via TCP or DNS endpoints, optionally secured with TLS/mTLS).

---

## 2. Dynamic CSI Driver Discovery (`CSIDriverConfig`)

To discover and communicate with CSI drivers, Substrate uses dynamic discovery driven by the cluster-scoped **`CSIDriverConfig`** Custom Resource Definition (CRD).

### The `CSIDriverConfig` Resource

`CSIDriverConfig` defines the gRPC connection parameters for a specific CSI driver. It bridges the Kubernetes `StorageClass` (referenced in the `ActorTemplate`) to the network endpoint of the CSI Controller service and the local socket path of the CSI Node plugin.

```yaml
apiVersion: ate.dev/v1alpha1
kind: CSIDriverConfig
metadata:
  name: nfs.csi.k8s.io
spec:
  driverName: nfs.csi.k8s.io
  controllerEndpoint: tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock
  tls:
    enabled: true
    usePodIdentity: true
    serverName: csi-nfs-controller.kube-system.svc.cluster.local
```

### Specification (`CSIDriverConfigSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `driverName` | `string` | **Required.** The standard CSI driver name (e.g. `nfs.csi.k8s.io`, `hostpath.csi.k8s.io`, `pd.csi.storage.gke.io`). Matches the `provisioner` field on the referenced Kubernetes `StorageClass`. |
| `controllerEndpoint` | `string` | **Required.** The gRPC endpoint for the CSI Controller service. Must be a valid URI starting with `tcp://`, `dns:///`, or `unix://` (e.g., `tcp://csi-controller.kube-system.svc:50051` or `dns:///csi-svc.default.svc:9000`). |
| `nodeSocketOverride` | `string` | **Optional.** Override for the CSI Node service Unix domain socket on worker nodes. Must begin with `unix://`. If omitted, Substrate defaults to `unix:///var/lib/kubelet/plugins/<driverName>/csi.sock`. |
| `tls` | `*CSIDriverTLSConfig` | **Optional.** Configures TLS or mTLS for the gRPC connection to the `controllerEndpoint`. |

#### TLS / mTLS Configuration (`spec.tls`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `enabled` | `bool` | **Required.** Enables TLS/mTLS for the gRPC connection. |
| `usePodIdentity` | `bool` | **Optional.** When `true`, reuses Substrate's SPIFFE Pod Identity certificates for mutual TLS (mTLS) with dynamic CA trust bundle verification and rotation. Must be `true` when `enabled` is `true`. |
| `serverName` | `string` | **Optional.** Server name override for TLS certificate verification. |

> [!NOTE]
> For details on exposing CSI controller endpoints over the network and configuring CSI node DaemonSets with required mount propagations, see the [CSI Driver Deployment Guide](csi-deployment.md).

---

## 3. ActorTemplate: Configuring CSI Volumes

External volumes are declared on the `ActorTemplate` resource. For complete details on actor templates, see the [ActorTemplate: The Workload Blueprint](api-guide.md#2-actortemplate-the-workload-blueprint) section in the Substrate API Guide.

### Volume Configuration Fields

To attach a CSI volume to an actor:

1. Define the volume under `volumes` with an `externalVolumeTemplate`.
2. Mount the volume inside one or more containers under `containers[].volumeMounts`.

#### `volumes[]`

```yaml
volumes:
- name: my-data-volume
  externalVolumeTemplate:
    capacity: 10Gi
    storageClassName: standard-rwx
```

* `name`: Unique DNS-label-compliant volume name.
* `externalVolumeTemplate.capacity`: Quantity string representing the requested volume size (e.g. `1Gi`, `50Gi`).
* `externalVolumeTemplate.storageClassName`: Name of a Kubernetes `StorageClass` present in the cluster whose `provisioner` matches a registered `CSIDriverConfig`.

#### `containers[].volumeMounts[]`

```yaml
volumeMounts:
- name: my-data-volume
  mountPath: /var/data
```

* `name`: Must match the declared `volumes[].name`.
* `mountPath`: Unix path inside the container sandbox where the volume will be mounted.

> [!NOTE]
> All declared volumes in `volumes` must be mounted by at least one container.

---

## 4. End-to-End Example

The following example demonstrates setting up an NFS CSI driver with Substrate and deploying an `ActorTemplate` that mounts an external NFS volume.

### Step 1: Create the StorageClass

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: csi-nfs-sc
provisioner: nfs.csi.k8s.io
parameters:
  server: nfs-server.default.svc.cluster.local
  share: /
reclaimPolicy: Delete
volumeBindingMode: Immediate
mountOptions:
  - nfsvers=4.1
```

### Step 2: Register the CSIDriverConfig

```yaml
apiVersion: ate.dev/v1alpha1
kind: CSIDriverConfig
metadata:
  name: nfs.csi.k8s.io
spec:
  driverName: nfs.csi.k8s.io
  controllerEndpoint: tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock
  tls:
    enabled: true
    usePodIdentity: true
    serverName: csi-nfs-controller.kube-system.svc.cluster.local
```

### Step 3: Define WorkerPool and ActorTemplate

Refer to [ActorTemplate: The Workload Blueprint](api-guide.md#2-actortemplate-the-workload-blueprint) for general template options.

The `WorkerPool` is a Kubernetes resource, applied with `kubectl apply`:

```yaml
apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: agent-pool
  namespace: ate-demo
  labels:
    workload: stateful-agent
spec:
  replicas: 5
  workerImage: ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor
```

The `ActorTemplate` is a protojson-shaped `ateapipb.ActorTemplate`, created
through the ate API with `kubectl ate create actor-template -f -` (the
`ate-demo` atespace must exist):

```yaml
metadata:
  atespace: ate-demo
  name: stateful-agent-template
workerSelector:
  matchLabels:
    workload: stateful-agent
containers:
- name: agent
  image: gcr.io/my-project/agent-app@sha256:7f28ab0...
  volumeMounts:
  - name: shared-storage
    mountPath: /mnt/shared
  wakeupProbe:
    httpGet:
      path: /readyz
      port: 8080
sandboxConfig:
  sandboxClass: SANDBOX_CLASS_GVISOR
  configName: gvisor-default
snapshotConfig:
  storageLocation: gs://my-snapshots-bucket/stateful-agent
volumes:
- name: shared-storage
  externalVolumeTemplate:
    capacity: 5Gi
    storageClassName: csi-nfs-sc
```

---

## 5. Storage Isolation & Dynamic Node Scoping (Threat T-37)

In multi-tenant environments where actors multiplex across shared Kubernetes nodes, external network filesystems (such as Cloud Filestore NFS shares) present a lateral traversal risk: a compromised worker node could attempt to mount filesystems belonging to actors on other nodes (documented under threat **T-37**).

### Dynamic Node Scoping via CSI Publish

Substrate addresses this by coupling network filesystem access directly with actor scheduling and lifecycle:

1. **Scheduled Node Scoping (`NodeId`):**  
   When an actor is scheduled onto a worker node, the control plane (`ateapi`) passes the worker's hosting node (`worker.GetNodeName()`) as `NodeId` in `csi.ControllerPublishVolumeRequest`. A driver that supports dynamic network ACLs (such as Google Cloud Filestore, once its driver implements `ControllerPublishVolume`) can then restrict the share's export rules (`nfsExportOptions`) so NFS access is permitted only from that node.
2. **Wire Delivery of Attachment Metadata (`publish_context`):**  
   Attachment metadata returned by the driver's `ControllerPublishVolume` response is collected in memory during `ensureVolumesAttached` and passed directly to `atelet` over gRPC on the `RestoreRequest` (`WorkloadSpec.Volumes.ExternalVolume.PublishContext`). The node plugin on the destination worker node uses this metadata (e.g. device path or export options) to mount the volume into the sandbox. Attachment metadata is scoped to the active execution run and is not persisted in the actor status database record.
3. **Revocation on Lifecycle Transitions:**  
   When an actor is paused, suspended, reverted, or deleted, the control plane calls `csi.ControllerUnpublishVolumeRequest` for the node of the actor's currently assigned worker, allowing the driver to remove that node from the share's ACLs.
4. **Cross-Node Migration:**  
   When an actor migrates to a different node upon resume, `ensureVolumesAttached` publishes the volume to the new worker node and hands the updated `publish_context` to the new node's `atelet`. The previous node was already unpublished during pause or suspend.

> [!NOTE]
> If a worker node dies abruptly, no unpublish is issued for it and its access persists until cleaned up out of band. Crash-path revocation is tracked separately in [#1715](https://github.com/agent-substrate/substrate/issues/1715).

### Latency Budget & OpenTelemetry Profiling

Because `ControllerPublishVolume` executes synchronously on the actor resume path, volume attachment latency directly impacts cold-start and resume latency:

* **OpenTelemetry Instrumentation:** The attachment phase is measured under the OpenTelemetry span **`step.AttachVolumes`** on the `controlapi` tracer (fully sampled on kind).
* **Latency Budget:** The atenet ingress router parks a request for at most `--parked-request-budget` (default 5s) while its actor resumes. Control-plane attachment, including any storage ACL mutation, should target under 500 ms–1 s to leave margin for worker assignment and restore.
* **Querying Spans in Jaeger:**
  ```bash
  kubectl port-forward -n otel-system svc/jaeger 16686:16686
  curl -s "http://localhost:16686/api/traces?service=ateapi&operation=step.AttachVolumes&limit=20" | \
    jq '.data[].spans[] | select(.operationName=="step.AttachVolumes") | {duration: .duration, tags: .tags}'
  ```

