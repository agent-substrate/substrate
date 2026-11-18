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

## 2. Dynamic CSI Driver Discovery (`CsiDriverConfig`)

Substrate discovers and connects to CSI drivers through the global-scoped **`CsiDriverConfig`** resource (`ateapi.CsiDriverConfig`), stored in the control plane and managed with `kubectl ate --preview`.

### The `CsiDriverConfig` Resource

`CsiDriverConfig` defines the gRPC connection settings for a specific CSI driver (`driverName`). It connects the Kubernetes `StorageClass` referenced in the `ActorTemplate` (`provisioner` matching `driverName`) to the network endpoint of the CSI Controller service and the local socket path of the CSI Node plugin.

```yaml
metadata:
  name: csi-nfs
driverName: nfs.csi.k8s.io
controllerEndpoint: tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052
nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock
tls:
  enabled: true
  usePodIdentity: true
  serverName: csi-nfs-controller.kube-system.svc
```

### Fields (`ateapi.CsiDriverConfig`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `metadata.name` | `string` | **Required.** Unique DNS-1123 label (no dots) for this `CsiDriverConfig` resource (e.g. `csi-nfs`). Dotted names such as `nfs.csi.k8s.io` are rejected. |
| `driverName` | `string` | **Required.** Immutable CSI driver name (DNS-1123 subdomain with an optional `substrate.io/` prefix, e.g. `nfs.csi.k8s.io`, `hostpath.csi.k8s.io`, `pd.csi.storage.gke.io`). Must be unique across `CsiDriverConfig` resources and match the `provisioner` field on the referenced Kubernetes `StorageClass`. |
| `controllerEndpoint` | `string` | **Required.** The gRPC endpoint for the CSI Controller service. Must be `tcp://<host>:<port>` or `dns://[<authority>]/<host>:<port>` (e.g. `tcp://csi-controller.kube-system.svc:50051` or `dns:///csi-svc.default.svc:9000`). |
| `nodeSocketOverride` | `string` | **Optional.** Clean absolute `unix://` socket URI for the CSI Node service on worker nodes. When omitted, `atelet` defaults to `unix:///var/lib/kubelet/plugins/<driverName>/csi.sock`. |
| `tls` | `CsiDriverTlsConfig` | **Optional.** Configures TLS or mTLS for the gRPC connection to `controllerEndpoint`. |

#### TLS / mTLS Configuration (`tls`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `enabled` | `bool` | **Optional.** Enables TLS/mTLS for the controller gRPC connection (defaults to `false`). When `false`, `usePodIdentity` and `serverName` must be unset. |
| `usePodIdentity` | `bool` | **Optional.** When `true`, uses Substrate's SPIFFE Pod Identity certificates for mutual TLS (mTLS) with dynamic CA trust bundle verification and rotation. Must be `true` when `enabled` is `true`, and forbidden when `false`. |
| `serverName` | `string` | **Optional.** Expected server name (SAN) for TLS certificate verification. Required when `enabled` is `true`, and forbidden when `false`. |

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
* `external.template.storageClassName`: Name of a Kubernetes `StorageClass` present in the cluster whose `provisioner` matches a registered `CsiDriverConfig`.

#### `containers[].volumeMounts[]`

```yaml
volumeMounts:
- name: my-data-volume
  mountPath: /var/data
```

* `name`: Must match the declared `volumes[].name`.
* `mountPath`: Unix path inside the container sandbox where the volume will be mounted.

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

### Step 2: Register the CsiDriverConfig

Register the `CsiDriverConfig` through the ate API with `kubectl ate --preview create csi-driver-config -f -`:

```yaml
metadata:
  name: csi-nfs
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
