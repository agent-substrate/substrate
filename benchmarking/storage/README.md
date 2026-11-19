# Storage benchmarks for external volumes

## Overview

This guide measures actors that use external volumes on Agent Substrate. It
answers two questions:

1. **Latency:** how fast does one actor with a volume suspend and resume?
2. **Density:** how many actors with volumes fit on one node, and what runs
   out first?

The steps work with any CSI driver that Substrate supports. Only the
StorageClass changes between backends. The example uses
[Filestore agent volumes](https://docs.cloud.google.com/filestore/docs/agent-substrate).

| Backend | Setup | StorageClass |
|---|---|---|
| Filestore agent volumes | [manifests/backends/filestore](manifests/backends/filestore/README.md) | [storage_class.yaml](manifests/backends/filestore/storage_class.yaml) |

Both scenarios call the Substrate API through
[`wakestorm`](cmd/wakestorm/README.md), which times every call.

## Supported runtimes

| Runtime | Status |
|---|---|
| gVisor | Validated with the reference results below |
| microVM | Not validated yet |

## Prerequisites

- Install `gcloud`, `kubectl`, `kubectl ate`, `go`, `jq` and `envsubst`.

- Plan for one node pool of `c3-standard-4` nodes: two infra nodes plus one
  node per worker (see [Size the nodes](#3-size-the-nodes)).

- For Filestore, get volume pool access for your project, as the
  [Filestore setup](manifests/backends/filestore/README.md) describes.

### Get the guide

Run every command from a clone of this repository:

```bash
git clone https://github.com/agent-substrate/substrate.git && cd substrate
```

### Configure the environment

```bash
export PROJECT_ID=<project>
export REGION=<region>            # for example us-west1
export ZONE=<zone>                # for example us-west1-c
export CLUSTER=<cluster>
export POOL=<volume-pool-name>
export STORAGE_CLASS=storage-bench
```

## Installation Instructions

### 1. Install Agent Substrate

Create the cluster with the
[GKE installation guide](https://docs.cloud.google.com/kubernetes-engine/ai-ml/install-overview-substrate).
Then create the snapshot bucket and the snapshot transfer service account with
`tools/setup-gcp create bucket` and `create iam` from
[substrate-gke](https://github.com/ai-on-gke/substrate-gke/tree/main/tools/setup-gcp).

Install Substrate from this checkout with the `ExternalVolumes` preview gate
on. The install makes the `ate-client` ServiceAccount an owner, and
`wakestorm` calls the API as that account.

```bash
gcloud container clusters get-credentials "${CLUSTER}" --location "${ZONE}" --project "${PROJECT_ID}"
export CLUSTER_NAME="${CLUSTER}" CLUSTER_LOCATION="${ZONE}"
export KO_DOCKER_REPO=<registry>             # where ko pushes the images it builds
export SNAPSHOT_LOCATION=gs://<bucket>/      # the bucket setup-gcp created
export SNAPSHOT_TRANSFER_GSA_NAME=ate-snapshot-transfer
ATE_PREVIEW=ExternalVolumes go run ./cmd/ate-setup deploy ate-system \
  --sandbox-classes=gvisor --credential-provider='{"enabled":false}'
```

[ate-setup](../../cmd/ate-setup/README.md) lists the other install settings.

### 2. Set up the storage backend

Follow the backend's README. It installs the CSI driver, creates the storage
and applies `${STORAGE_CLASS}`. For Filestore, see
[manifests/backends/filestore](manifests/backends/filestore/README.md).

> [!IMPORTANT]
> Substrate passes only the StorageClass `parameters` to the CSI driver and
> ignores `mountOptions`. Put NFS mount options in the driver's own parameter,
> which for Filestore is `parameters.mount-options`.

### 3. Size the nodes

Resize the node pool to `NODES` plus two infra nodes. Pin the autoscaler first
so it does not move actors during a run:

```bash
export NODES=1          # one worker node for both scenarios
export NODE_POOL=<node-pool>
gcloud container node-pools update "${NODE_POOL}" --cluster "${CLUSTER}" --zone "${ZONE}" --project "${PROJECT_ID}" \
  --enable-autoscaling --min-nodes $((NODES + 2)) --max-nodes $((NODES + 2)) --quiet
gcloud container clusters resize "${CLUSTER}" --node-pool "${NODE_POOL}" --num-nodes $((NODES + 2)) \
  --zone "${ZONE}" --project "${PROJECT_ID}" --quiet
```

### 4. Deploy the actor template and workers

[`workloads/deploy.sh`](../workloads/deploy.sh) creates the `glutton-storage`
actor template on your StorageClass and the `benchmark-worker` WorkerPool:

```bash
./benchmarking/workloads/deploy.sh --deploy --storage-class-name "${STORAGE_CLASS}" \
  --actor-cpu 95m --actor-memory 256Mi --worker-count "${NODES}"
```

Then size the workers. A 2 CPU request fits one worker per `c3-standard-4`.
A worker reports its limits as its capacity, and Substrate places actors by
that capacity. Limits far above the node let actors fill the node itself.
Without them, 95m actors stop at about 40 per worker with `no worker has room`.

```bash
kubectl patch workerpool benchmark-worker -n benchmark-workloads --type merge -p '{"spec":{"template":{
  "resources":{"requests":{"cpu":"2","memory":"1Gi"},"limits":{"cpu":"128","memory":"256Gi"}}}}}'
```

Create the `benchmark` atespace, where the verification and both scenarios put
their actors:

```bash
kubectl ate create atespace benchmark
```

### 5. Keep the control plane off the worker nodes

Pick two nodes for infra. [`isolate_infra.sh`](scenarios/isolate_infra.sh)
labels them, moves the control plane and the CSI controller onto them, and
keeps workers off them:

```bash
kubectl get nodes
benchmarking/storage/scenarios/isolate_infra.sh <infra-node-1> <infra-node-2>
kubectl get pods -n benchmark-workloads -l ate.dev/worker-pool=benchmark-worker -o wide
```

Each worker pod should run on its own node, and no worker should run on an
infra node.

### 6. Set up the runner

The latency scenario runs `wakestorm` in a pod on an infra node, so call times
do not include a port forward from your machine:

```bash
benchmarking/storage/scenarios/runner_setup.sh
```

The runner's token lasts 6 hours. Run the script again to renew it.

## Verification

### 1. One actor suspends and resumes

```bash
kubectl ate --preview create actor storage-check -a benchmark --template benchmark-workloads/glutton-storage
kubectl ate resume actor storage-check -a benchmark
kubectl ate suspend actor storage-check -a benchmark
kubectl ate resume actor storage-check -a benchmark
kubectl ate get actor storage-check -a benchmark
```

Every call should succeed, and the actor should end in `ACTOR_STATE_RUNNING`.

### 2. The CSI driver mounted the volume

The first resume creates the volume, and every resume mounts it. For
Filestore:

```bash
kubectl logs -n gcp-filestore-csi-driver -l app=gcp-filestore-csi-driver -c gcp-filestore-driver --since 10m \
  | grep -E 'NodeStageVolume|NodePublishVolume' | tail
kubectl ate delete actor storage-check -a benchmark
```

The log should show `NodeStageVolume` and `NodePublishVolume` for the actor's
volume.

## Scenario 1: Latency

One actor runs 30 cycles of create, first resume, suspend, resume and delete.
Each cycle uses a new actor, so every first resume creates a new volume.

| Input | Value |
|---|---|
| Actors | 1 |
| Cycles | 30 |
| Actor resources | 95m CPU, 256 MiB memory |
| Volume | 1 GiB from `${STORAGE_CLASS}` |
| Suspend mode | `DURABILITY_MODE_ASYNCHRONOUS`, which returns once the node has the snapshot |
| Pause between steps | 15 s, excluded from the call times |
| Retries | `-retry-contention`, for volume pool contention on first resume |
| Client | `wakestorm` in the runner pod |

```bash
benchmarking/storage/scenarios/latency.sh /tmp/latency 30
```

The script prints P50, P90 and max for each call. `/tmp/latency/calls.csv`
lists every call.

### Reference results

P50 / P90 over 30 cycles:

| Call | Time |
|---|---|
| CreateActor | 0.01 / 0.09 s |
| First ResumeActor, which creates the volume | 0.28 / 0.41 s |
| SuspendActor | 0.23 / 0.28 s |
| ResumeActor | 0.15 / 0.20 s |
| DeleteActor | 0.20 / 0.22 s |

The CSI driver logs from the same run give mount (`NodeStageVolume`) at
37 / 53 ms and unmount (`NodeUnstageVolume`) at 15 / 17 ms. A resume mounts
the volume but does not create it.

With only the default options (`vers=3,proto=tcp,mountproto=tcp,nolock`) and
the kernel's 128 KiB read ahead, the same pool gave a first resume of
0.25 / 0.34 s, a resume of 0.13 / 0.15 s and a mount of 28 / 46 ms. The tuned
options cost about 9 ms per mount here, because this workload reads no files.
They are the Filestore recommendation for workloads that read and write data.

Environment: Substrate at `905e2a65`, gVisor `release-20261102.0`,
`c3-standard-4` nodes in `us-west1-c` (two infra, one worker), Filestore agent
volumes from a zone aware pool with the
[reference layout](manifests/backends/filestore/README.md#2-create-a-volume-pool),
`mount-options: vers=3,proto=tcp,mountproto=tcp,nolock,nconnect=8,rsize=524288,wsize=524288,actimeo=60`,
and a 16 MiB read ahead (`read_ahead_kb=16384`).

## Scenario 2: Density

Actors with volumes are added in steps and kept running until something fails.
Each step records the first resume times, the running actors, the NFS mounts,
and the memory and CPU of the worker nodes.

| Input | Value |
|---|---|
| Worker nodes | `${NODES}` `c3-standard-4`, one worker each, sized as in [step 4](#4-deploy-the-actor-template-and-workers) |
| First step | 40 actors (`START`), created and resumed at once |
| Later steps | 20 more actors (`STEP`) at once |
| Wait after each step | 20 s before sampling the nodes |
| Stop when | a first resume fails, an actor stops running, or a worker node has less than 1 GiB free (`MIN_FREE_MIB`) |
| Actor resources | 95m CPU, 256 MiB memory |
| Volume | 1 GiB from `${STORAGE_CLASS}` |
| Retries | only for `Aborted` conflicts. Any other failed first resume ends the run |
| Client | `wakestorm` on your machine, through `kubectl port-forward` |

```bash
benchmarking/storage/scenarios/density.sh /tmp/density 1000
go run ./benchmarking/storage/cmd/wakestorm -phase delete -prefix density   # clean up
```

`/tmp/density/steps.csv` has one row per step. The last row and the stop
message show what ran out.

### Reference results

One `c3-standard-4` worker node with 16 GiB of memory:

| Running actors | First resume P50 / P90 | NFS mounts | Node memory free | Worker node working set |
|---|---|---|---|---|
| 40 | 2.49 / 3.13 s | 80 | 13,439 MiB | 2,556 MiB |
| 100 | 1.36 / 1.71 s | 200 | 12,509 MiB | 3,486 MiB |
| 200 | 1.65 / 1.95 s | 400 | 10,890 MiB | 5,105 MiB |
| 300 | 1.86 / 2.22 s | 600 | 9,332 MiB | 6,664 MiB |
| 400 | 2.20 / 2.59 s | 800 | 7,780 MiB | 8,215 MiB |
| 500 | 1.74 / 2.55 s | 1,000 | 6,275 MiB | 9,721 MiB |
| 600 | 2.22 / 2.91 s | 1,200 | 4,615 MiB | 11,381 MiB |
| 700 | 5.28 / 7.42 s | 1,400 | 3,180 MiB | 12,815 MiB |
| 720 | 20 failed with `Unavailable` | worker evicted | | |

**700 actors fit, and node memory is the limit.** Both the tuned and default
mount settings reached 700 actors and evicted at 720. Each running actor uses
about 17 MiB. At 720, the node fell below kubelet's 100 MiB eviction threshold
while the new actors resumed. Kubelet evicted the worker pod, which held about
12.5 GiB, and all its actors crashed. Each actor has two NFS mounts, and neither
the mount count nor the mount time was a limit.

The free memory that the kubelet summary API reports stopped falling at about
3.2 GiB after 600 actors, so the `MIN_FREE_MIB` check never stopped the run.
The working set column shows the node filling up.

Environment: Substrate at `905e2a65`, gVisor `release-20261102.0`, one worker
on one `c3-standard-4` in `us-west1-c` plus two infra nodes, and the same pool,
StorageClass and read ahead as scenario 1.

## Troubleshooting

### Common failure modes

- **Density stops at about 40 actors per worker with `no worker has room`**:
  the worker's limits cap its capacity. Apply the limits from
  [step 4](#4-deploy-the-actor-template-and-workers).
- **Creating `glutton-storage` fails with a preview error**: the template has
  an external volume, which needs the `ExternalVolumes` gate on the server and
  `--preview` on the client.
- **First resume fails with `concurrency contention ... please retry`**: many
  first resumes are claiming volumes from the pool at once. Latency retries
  with `-retry-contention`. Density counts it as a failure.
- **First resume fails with `429` or `no available volumes`**: the volume pool
  is empty. If it never had volumes, your project is not set up for volume
  pools, so ask the Filestore team. Otherwise wait for it to refill or ask for
  more spare volumes.
- **NFS mount options have no effect**: they are in `mountOptions`. Move them
  to `parameters.mount-options`.
- **Changing `nconnect` has no effect on an existing node**: the Linux NFS
  client reuses the first active mount's connection count per Filestore IP.
  Suspend or delete running actors on the node before mounting with new options.
- **Volume calls fail after about a day**: the Filestore CSI controller's mTLS
  certificate expired. Restart `gcp-filestore-csi-controller`.
- **Volume calls fail with `Unavailable` after you reinstall Substrate**: the
  Filestore CSI controller still trusts the old install's CA. Restart
  `gcp-filestore-csi-controller`.
- **`wsr.sh` fails with `Unauthenticated`**: the runner token expired after
  6 hours. Run `runner_setup.sh` again.
- **Cleanup fails with `ResourceExhausted` from `DeleteVolume`**: Filestore
  limits how fast volumes are deleted. Wait a minute and run the delete again.
- **Every actor on a node crashes at once during density**: this is the limit.
  Kubelet evicted the worker for low memory, and the step before is the
  result.

## Cleanup

```bash
go run ./benchmarking/storage/cmd/wakestorm -phase delete -prefix lat
go run ./benchmarking/storage/cmd/wakestorm -phase delete -prefix density
kubectl ate delete atespace benchmark
kubectl delete pod wakestorm-runner -n benchmarking
kubectl delete secret wakestorm-token -n benchmarking   # an ate-client token
kubectl delete serviceaccount wakestorm -n benchmarking
kubectl delete clusterrolebinding,clusterrole wakestorm-trust-bundles
./benchmarking/workloads/deploy.sh --delete
benchmarking/storage/scenarios/isolate_infra.sh --undo
```

Then delete the backend's storage as its README describes, and scale the node
pool back down.
