---
title: "CSI Volumes"
linkTitle: "CSI Volumes"
weight: 5
status: preview
status_note: >
  External CSI volumes are disabled by default behind the `ExternalVolumes`
  preview gate, and APIs or behavior may change.
description: >
  Attach CSI volumes to actors.
---

# CSI Volumes for Actors in Agent Substrate

Substrate integrates with the **Container Storage Interface (CSI)** to provide dynamically provisioned, per-actor external volumes that seamlessly attach and detach as actors transition through their lifecycle.

> [!IMPORTANT]
> External volumes are a preview feature. The ate API server accepts them only
> when it runs with `--preview=ExternalVolumes` (ate-setup's `preview` setting)
> and the client opts in to preview features: `kubectl ate --preview`, or the
> `Ate-Preview-Allowed: true` gRPC metadata header. The opt-in is needed to
> create an actor template with external volumes, or an actor from such a
> template. Later operations on that actor, such as resume, suspend and delete,
> do not need it.

---

## 1. CSI in Substrate vs. Standard Kubernetes

In Kubernetes, volumes are reconciled asynchronously via standard Kubernetes objects (e.g. `PersistentVolumeClaim`, `PersistentVolume`). Agent Substrate takes a different approach tailored for actor lifecycle operations:

* **No PV or PVC Objects:** External volumes are declaratively defined in the [`ActorTemplate`](../reference/api-guide.md#2-actortemplate-the-workload-blueprint) via `external.template` and provisioned dynamically for each actor instance. Volume operations are coupled directly with the actor lifecycle.
* **Direct Network-Based CSI Controller:** The Substrate control plane (`ateapi`) communicates directly with the CSI Controller gRPC service over the network (via TCP or DNS endpoints, optionally secured with TLS/mTLS).

---

## 2. Dynamic CSI Driver Discovery (`CSIDriverConfig`)

To discover and communicate with CSI drivers, Substrate uses dynamic discovery driven by the cluster-scoped **`CSIDriverConfig`** Custom Resource Definition (CRD).

### The `CSIDriverConfig` Resource

`CSIDriverConfig` defines the gRPC connection parameters for a specific CSI driver. It bridges the Kubernetes `StorageClass` (referenced in the `ActorTemplate`) to the network endpoint of the CSI Controller service and the local socket path of the CSI Node plugin.

```yaml
apiVersion: ate.dev/v1
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
    serverName: csi-nfs-controller.kube-system.svc
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

External volumes are declared on the `ActorTemplate` resource. For complete details on actor templates, see the [ActorTemplate: The Workload Blueprint](../reference/api-guide.md#2-actortemplate-the-workload-blueprint) section in the Substrate API Guide.

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
      capacityBytes: 10737418240 # 10Gi
      storageClassName: standard-rwx
```

* `name`: Unique DNS-label-compliant volume name.
* `external.template.capacityBytes`: The requested volume size in bytes (e.g. `1073741824` for 1Gi).
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
apiVersion: ate.dev/v1
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
    serverName: csi-nfs-controller.kube-system.svc
```

### Step 3: Define WorkerPool and ActorTemplate

Refer to [ActorTemplate: The Workload Blueprint](../reference/api-guide.md#2-actortemplate-the-workload-blueprint) for general template options.

The `WorkerPool` is a Kubernetes resource, applied with `kubectl apply`:

```yaml
apiVersion: ate.dev/v1
kind: WorkerPool
metadata:
  name: agent-pool
  namespace: ate-demo
  labels:
    workload: stateful-agent
spec:
  replicas: 5
  workerImage: ko://go.ate.dev/substrate/cmd/ateworker-gvisor
  sandboxClasses:
  - name: gvisor
```

The `ActorTemplate` is a protojson-shaped `ateapi.ActorTemplate`, created
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
resources:
  limits:
    cpuMillicores: 1000
    memoryBytes: 1073741824 # 1Gi
volumes:
- name: shared-storage
  external:
    template:
      capacityBytes: 5368709120 # 5Gi
      storageClassName: csi-nfs-sc
```

### Starting volumes from an existing snapshot

`contentSource` fills each actor's volume from a snapshot in the storage
system, which may have been taken outside Substrate. Every actor of the
template starts from the same content, in its own volume:

```yaml
volumes:
- name: shared-storage
  external:
    template:
      capacityBytes: 5368709120 # 5Gi
      storageClassName: csi-nfs-sc
      contentSource:
        snapshot:
          storageSnapshotId: <snapshot-id>
```

`storageSnapshotId` is the handle the StorageClass's CSI driver uses for the
snapshot. `capacityBytes` must be at least the snapshot's size.

- The driver must support CSI snapshots, or `create actor` fails with
  `FAILED_PRECONDITION`.
- `create actor` does not check that the snapshot exists. A missing snapshot
  fails the actor's first resume with `FAILED_PRECONDITION`.

## 5. Snapshotting Volumes with a Tag

A tag can also snapshot a suspended actor's external volumes. An actor created
from that tag restores them on its first resume:

```bash
kubectl ate suspend actor <actor-name> -a <atespace>
# All external volumes, or only the named ones.
kubectl ate --preview create tag <tag-name> -a <atespace> --actor <actor-name> --with-volumes
kubectl ate --preview create tag <tag-name> -a <atespace> --actor <actor-name> --with-volumes=shared-storage
kubectl ate --preview create actor <new-actor> -a <atespace> --template <template-name> --tag <tag-name>
kubectl ate resume actor <new-actor> -a <atespace>
```

- Each restored volume is a new volume with its own copy of the data. Writes
  to it don't reach the source actor or other actors created from the tag.
- A volume the tag did not capture starts from its template's `contentSource`,
  or empty without one.
- The driver of every captured volume must support CSI snapshots, or
  `create tag` fails.
- `create tag` does not wait for the storage system to finish the snapshots.
  `create actor` fails with `FAILED_PRECONDITION` until they are ready to use.
  `kubectl ate get tag <tag-name> -a <atespace> -o yaml` shows `readyToUse`
  for each volume snapshot.
- Deleting the tag deletes its volume snapshots. Actors that already resumed
  keep their volumes. An actor created from the tag that never resumed can no
  longer restore them.

## 6. Known limitations

- **Restoring volumes from a tag needs every ate API server on the same
  release.** `kubectl ate --preview create tag --with-volumes` snapshots an
  actor's external volumes, and an actor created from that tag restores them
  on its first resume. An ate API server from a release without this feature
  drops the tag's volume snapshots from the actor and provisions its volumes
  empty, without an error. This can happen while ate API servers roll to a new
  release, or after a rollback. Until every ate API server runs the new
  release, don't resume actors created from a tag with volumes. A rollback
  loses the volume contents of those actors that never resumed. Actors that
  already resumed keep their volumes.
- **`contentSource` needs every ate API server on the same release.** An ate
  API server from a release without this feature ignores `contentSource` and
  provisions the volume empty, without an error. When it updates the
  template's status, it also removes `contentSource` from the stored template
  for good. Don't create templates with `contentSource` until every ate API
  server runs the new release, and recreate them after a rollback.
