# CSI Volumes for Actors in Agent Substrate

Substrate integrates with the **Container Storage Interface (CSI)** to provide dynamically provisioned, per-actor external volumes that seamlessly attach and detach as actors transition through their lifecycle.

External volumes are a preview feature. The ate API server accepts them only
when it runs with `--preview=ExternalVolumes` (ate-setup's `preview` setting)
and the client opts in to preview features: `kubectl ate --preview`, or the
`Ate-Preview-Allowed: true` gRPC metadata header. The opt-in is needed to
create an actor template with external volumes, or an actor from such a
template. Later operations on that actor, such as resume, suspend and delete,
do not need it.

---

## 1. CSI in Substrate vs. Standard Kubernetes

In Kubernetes, volumes are reconciled asynchronously via standard Kubernetes objects (e.g. `PersistentVolumeClaim`, `PersistentVolume`). Agent Substrate takes a different approach tailored for actor lifecycle operations:

* **No PV or PVC Objects:** External volumes are declaratively defined in the [`ActorTemplate`](api-guide.md#2-actortemplate-the-workload-blueprint) via `external.template` and provisioned dynamically for each actor instance. Volume operations are coupled directly with the actor lifecycle.
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
| `serverName` | `string` | **Required.** Expected server name (SAN) for TLS certificate verification when TLS is enabled. |

> [!NOTE]
> For details on exposing CSI controller endpoints over the network and configuring CSI node DaemonSets with required mount propagations, see the [CSI Driver Deployment Guide](csi-deployment.md).

---

## 3. ActorTemplate: Configuring CSI Volumes

External volumes are declared on the `ActorTemplate` resource. For complete details on actor templates, see the [ActorTemplate: The Workload Blueprint](api-guide.md#2-actortemplate-the-workload-blueprint) section in the Substrate API Guide.

### Volume Configuration Fields

To attach a CSI volume to an actor:

1. Define the volume under `volumes` with an `external.template`.
2. Mount the volume inside one or more containers under `containers[].volumeMounts`.

#### `volumes[]`

```yaml
volumes:
- name: my-data-volume
  external:
    template:
      capacity: 10Gi
      storageClassName: standard-rwx
```

* `name`: Unique DNS-label-compliant volume name.
* `external.template.capacity`: Quantity string representing the requested volume size (e.g. `1Gi`, `50Gi`).
* `external.template.storageClassName`: Name of a Kubernetes `StorageClass` present in the cluster whose `provisioner` matches a registered `CSIDriverConfig`.

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
  workerImage: ko://github.com/agent-substrate/substrate/cmd/ateworker-gvisor
  sandboxClasses:
  - name: gvisor
```

The `ActorTemplate` is a protojson-shaped `ateapipb.ActorTemplate`, created
through the ate API with `kubectl ate --preview create actor-template -f -` (the
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
  sandboxClass: gvisor
  configName: gvisor-default
resources:
  limits:
  - name: cpu
    quantity: "1"
  - name: memory
    quantity: 1Gi
snapshotConfig:
  storageLocation: gs://my-snapshots-bucket/stateful-agent
volumes:
- name: shared-storage
  external:
    template:
      capacity: 5Gi
      storageClassName: csi-nfs-sc
```

## 5. Shared Volumes

A `SharedVolume` is an atespace-scoped CSI volume whose lifetime is
independent of any actor, so several actors can mount the same storage in turn.
Create one from a StorageClass, or register a volume that already exists:

```bash
kubectl ate create sharedvolume ctx-42 -a demo --storage-class csi-nfs-sc --capacity 10Gi
kubectl ate create sharedvolume imported -a demo --volume-id <csi-volume-id> --volume-type nfs.csi.k8s.io
```

Each volume records:

- `accessMode`: fixed at create. Only `READ_WRITE_ONCE` is accepted today, so
  at most one resumed actor holds the volume; a resume that would take a held
  volume fails and names the holder.
- `deleteTrigger`: `MANUAL` removes the volume only on an explicit delete;
  `LAST_ACTOR` removes it when the last actor referencing it is deleted.
  Provisioned volumes default to `LAST_ACTOR`, registered ones to `MANUAL`.
  Deleting the volume also deletes the storage.
- `status.refs`: every actor referencing the volume, with `active` set while
  the actor is resumed. A referenced volume cannot be deleted.

A template mounts a `SharedVolume` with `external.sharedVolume`. Name the volume
in the template, or leave the name empty and bind it per actor at creation:

```yaml
volumes:
- name: shared
  external: {sharedVolume: {}}
```

```bash
kubectl ate create actor producer-42 -a demo --template producer --volume shared=ctx-42
kubectl ate create actor consumer-42 -a demo --template consumer --volume shared=ctx-42
```

Bindings are validated and recorded when the actor is created and cannot change
afterwards.

Templates referencing a `SharedVolume` cold-boot by default and restore their own
snapshots on later resumes. Explicit `sourceTag` cloning remains refused.

MicroVM templates can opt into `snapshotConfig.goldenSnapshotConfig.mode:
GOLDEN_SNAPSHOT_MODE_RUNTIME`. This snapshots a booted kernel and agent **before
CreateSandbox mounts virtio-fs and before any application process exists**.
On restore, the host stages the actor-selected real volumes, resumes the kernel,
reseeds its random generator, and only then creates the sandbox and application
containers. Readiness is published after the application probes succeed. This
warms VM startup, not application memory. Ordinary own snapshots continue to
capture the running application; the runtime-only marker is not propagated to
those snapshots.

Application-warm goldens cannot transparently substitute arbitrary filesystem
state: applications may retain bytes from the source volume in their heap.
Ordinary own-snapshot handoffs also retain application memory; changed backing
data and deleted-open handles need application-specific semantics. Runtime
goldens do not change those semantics or claim to repair those cases.

gVisor templates also support `GOLDEN_SNAPSHOT_MODE_RUNTIME`. They capture only
the running pause container. Application containers and their filesystem handles
do not exist at capture time. Restore resumes that sandbox, then creates and
starts application containers against the actor-selected volumes. This avoids
carrying placeholder-filesystem options into an application restore. Later own
snapshots restore application memory normally.

### Runtime golden regression coverage

The preview-enabled microVM and gVisor CI lanes require the runtime golden
correctness cases and NFS CSI; missing prerequisites fail those lanes. Local cluster runs opt in with
`E2E_RUNTIME_GOLDEN=true`, `E2E_SANDBOX_CLASS=microvm` or `gvisor`, and
`E2E_RUNTIME_LOG_DIR` pointing to an evidence directory. For microVM continuity
checks, build/deploy the worker with `GOFLAGS=-tags=e2e_runtime_identity`, as the
CI E2E jobs do. Only those instrumented builds add the eager kernel boot marker;
production builds do not alter the kernel command line for this test. Run
`TestRuntimeGoldenVolumes` in `internal/e2e/suites/combinedvolumes` through
`hack/run-e2e-kind.sh`. This checks distinct volume bindings, guest/process
identity, and retained state over two own-snapshot generations. The eager guest
boot marker is checked only for microVM: gVisor regenerates its procfs boot ID
on restore, so that ID is diagnostic rather than evidence of continuity.

The probe contract checks directory entries and negative lookups as well as
cached, open-file, and memory-mapped contents. Only the known microVM
deleted-open-file `EIO` on the second generation is tolerated; other errors fail
the test. gVisor must preserve deleted-open contents in both generations.
MicroVM deleted-open recovery remains limited by virtiofsd's inode migration;
runtime-only capture does not repair a later own snapshot's orphaned inode.

Latency collection is separate: select `TestRuntimeGoldenLatency`, additionally
set `E2E_RUNTIME_GOLDEN_BENCHMARK=true` and `E2E_LATENCY_OUTPUT` to a JSONL file.
`E2E_LATENCY_SAMPLES` defaults to 100 positive measured samples per mode, with
five warmups. It does not disable or replace the correctness test.

### Shared-volume failure recovery

Actor creation and its inactive SharedVolume references commit in one database
transaction. Concurrent creates cannot silently lose references. Deletion marks
the volume `DELETING` before contacting CSI; new bindings and claims are refused
from that point onward. If CSI deletion fails, retry `DeleteSharedVolume` to
finish the same deletion. Fetch the latest version before retrying a request
with a version precondition.

A failed resume rolls back newly acquired claims before attachment. Once node
access may have started, claims are released only after termination and detach
succeed. Failed cleanup keeps the claim active. A crashed actor can therefore
retain its claim: crash detection alone does not establish that the old node
has stopped writing.

To recover while retaining data, first update the SharedVolume's `deleteTrigger`
to `DELETE_TRIGGER_MANUAL` using its current version, then delete the failed
actor. Repair node/CSI reachability and retry deletion if termination or detach
fails. Only after cleanup succeeds should a replacement actor mount the volume.
Do not clear active references directly in the database. If the original node
is permanently unreachable, fence its storage access at the infrastructure
level before attempting operator-assisted recovery; this API does not provide
a force-release shortcut. With `LAST_ACTOR`, deleting the final actor deletes
storage after successful node cleanup.

SharedVolume RPC authorization follows atespace roles: viewers can get/list;
editors can create/update/delete; global viewers can list across atespaces.
This uses the same authorization enforcement configuration as the other
resource RPCs.

### Upgrade from the old external-volume template field

This pre-1.0 change replaces `volumes[].externalVolumeTemplate` (protobuf field
3) with `volumes[].external.template`. It is a breaking stored-template change,
not an automatic migration. Do not roll this binary into a deployment with old
stored templates and expect their volume sources to survive decoding.

Before upgrading, export the affected templates and inventory/back up their
storage using the old binary. Rewrite manifests to the new nested field and
plan recreation of affected templates and actors in a fresh development
installation. Reattach retained storage through registered SharedVolumes after
validating the backups and CSI metadata. Existing actor memory snapshots are
not an upgrade mechanism. Keep the old installation intact until the replacement
is validated. The repository's pre-1.0 policy deliberately provides no legacy
field decoder, deprecated alias, or reserved-field compatibility guarantee.
