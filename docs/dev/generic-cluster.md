# Running Substrate on a cluster you provide

This guide covers installing Substrate on a Kubernetes cluster that you
manage yourself. Examples include kubeadm or k3s on Proxmox VMs, a bare-metal
cluster, or a managed offering outside GCP. The GKE path lives in
[ai-on-gke/substrate-gke](https://github.com/ai-on-gke/substrate-gke). The
local Kind path lives in
[hack/create-kind-cluster.sh](../../hack/create-kind-cluster.sh). This path is
everything else: you provide the cluster, the installer does the rest.

You own the layer below Kubernetes. Provision your VMs and install your
Kubernetes distribution however you already do. Substrate owns the layer above
the nodes. The installer takes a kubeconfig and deploys the CRDs, the control
plane, the atelet DaemonSet, snapshot storage, and the demos.

> [!NOTE]
> This guide is verified against the code, not yet against a live Proxmox or
> bare-metal cluster. Expect rough edges. Report problems with your
> distribution, Kubernetes version, and the failing step.

## The big picture

Substrate never talks to Proxmox, bare metal, or any hypervisor. It talks to
the Kubernetes API. Your machines only need to run Kubernetes. Substrate
cannot tell what is underneath:

```
Proxmox host or bare-metal machines            (your layer — ends here)
  └── Linux VMs or machines, /dev/kvm if you want microVMs
        └── Kubernetes (kubeadm, k3s, ...)      (your layer — standard install)
              └── Substrate                     (the installer's layer)
                    ├── control plane pods: ate-api-server, ate-controller,
                    │   atenet-router
                    ├── rustfs (snapshot store) + PostgreSQL, in-cluster
                    └── atelet DaemonSet on every node
                          └── your actors, sandboxed in runsc or microVMs
```

## Worked example: Proxmox host to running actor

### Step 1: Create the VMs

Create one VM per Kubernetes node in Proxmox. Set the CPU type to `host` if
you want microVMs later. Bare metal: skip this step. The machines already
exist.

### Step 2: Install Kubernetes

Install Kubernetes on the VMs or machines. Run the k3s install script or
kubeadm per its documentation. Copy the kubeconfig to the machine that runs
the installer. Verify: `kubectl get nodes` shows every node `Ready`.

### Step 3: Start a registry

Run a `registry:2` container on any machine the nodes can reach. Note its
address.

### Step 4: Install Substrate

On the machine with the kubeconfig, clone this repository. Create
`.ate-dev-env.sh` with the four settings from the [Install](#install)
section. Run `./hack/install-ate.sh --deploy-ate-system`. The installer
builds the images, pushes them to your registry, and deploys the control
plane through the Kubernetes API.

### Step 5: Verify

Run the checks in [Verify](#verify). A counter actor is created, suspended,
resumed, and keeps its state.

Steps 2 through 5 never mention Proxmox. The same commands work unchanged on
bare metal or on any cloud's VMs.

## How the install works

`hack/install-ate.sh` reads its settings from environment variables and
selects its manifests accordingly:

- The GKE-specific pieces (GCS snapshot backend, Workload Identity, GCP image
  pull auth, managed telemetry) are being extracted to
  [ai-on-gke/substrate-gke](https://github.com/ai-on-gke/substrate-gke),
  which installs Substrate on GKE behind an interactive wizard.
- The non-GCP machinery lives in the Kind profile today: in-cluster rustfs as
  an S3-compatible snapshot store, plain environment credentials, and an
  in-cluster telemetry collector. It is being factored into the default
  install. Until that lands, this guide enables it with
  `ATE_INSTALL_KIND=true` against your own cluster.

The extension mechanism is bring-your-own patches:

- Put cluster settings in `.ate-dev-env.sh` at the repository root. The
  installer sources the file.
- Set `KO_DOCKER_REPO` to the registry your nodes pull from.
- Patch or extend manifests with your own kustomize overlays.

## Requirements

Verify each requirement on your cluster before you install. Each step names a
command that fails when the requirement is unmet.

### Kubernetes APIs

Substrate needs the `podcertificaterequests` and `clustertrustbundles` beta
APIs in `certificates.k8s.io`. Run:

```sh
kubectl api-resources --api-group=certificates.k8s.io \
  | grep -E 'podcertificaterequests|clustertrustbundles'
```

Both names must appear. Kubernetes 1.37 or newer serves them by default. On
1.36, enable the beta APIs before you install. Some providers accept the
enablement only at cluster creation. GKE is one of them. Follow your
distribution's documentation. The install hangs at "Waiting for
podcertificate ClusterTrustBundles to be ready..." when the APIs are absent.

### Machine running the installer

Install Go, `kubectl`, and `docker`. Build the tools as in the
[Quickstart (Development)](../../README.md#quickstart-development). Install
the `kubectl-ate` plugin:

```sh
go install ./cmd/kubectl-ate
```

The installer builds images with `ko` and pushes them to `KO_DOCKER_REPO`.

### Nodes

1. Each worker node runs Linux with containerd.
2. For gVisor, do nothing: atelet downloads runsc itself from the assets named
   in the gVisor `SandboxConfig`.
3. For microVMs, expose `/dev/kvm` on the worker nodes. On Proxmox, set the VM
   CPU type to `host`. Or enable nested virtualization for the CPU type you
   use. Verify with `ls -la /dev/kvm` on the node. atelet advertises the
   device to the scheduler; worker pods request it automatically.

### Storage class

The snapshot store (rustfs) requests a `PersistentVolumeClaim`. Give the
cluster a default `StorageClass` before you install. Run:

```sh
kubectl get storageclass
```

One entry must carry `(default)`. k3s ships `local-path` as the default. On
kubeadm, deploy one first. Size it for your snapshot retention.

### Pod security

Leave the default Pod Security settings in place on `ate-system` and on every
atespace namespace. The worker pods run as root with hostPath mounts and
unconfined AppArmor and Seccomp. The atelet pod runs as root with hostPath
mounts. A cluster-wide restricted enforcement label breaks the install.

### Registry

Set `KO_DOCKER_REPO` to a registry that the installer can push to and every
node can pull from. A `registry:2` container on your network is enough. Give
the registry a DNS name or IP that resolves on the nodes. Do not use a
localhost address, because nodes cannot pull from it.

## Install

Create `.ate-dev-env.sh` at the repository root with your settings:

```sh
export KUBECTL_CONTEXT=<your-cluster-context>
export KO_DOCKER_REPO=<registry-nodes-can-pull>/substrate
export BUCKET_NAME=ate-snapshots
export ATE_INSTALL_KIND=true
```

The installer skips `gcloud` when `KUBECTL_CONTEXT` is set. Leave
`PROJECT_ID` unset as well. Then run:

```sh
./hack/install-ate.sh --deploy-ate-system
```

The installer labels every node with the build version, deploys the control
plane, and starts rustfs and PostgreSQL in-cluster. A node added after the
install hosts no workers. Copy the `ate.dev/substrate-version` label value
from an existing node and apply it to the new node:

```sh
kubectl get nodes --show-labels | grep substrate-version
kubectl label node <node> ate.dev/substrate-version=<value-from-an-existing-node>
```

For external volumes, add `--setup-csi=nfs`. This deploys an in-cluster NFS
server. The node that runs the NFS server pod needs the `nfsd` kernel module.
The installer's preflight checks for `nfsd` on the machine that runs the
installer. The hostpath driver is Kind-only and the installer refuses it
here.

To remove the install, run `./hack/install-ate.sh --delete-all`.

When the Kind machinery is factored into the default install, drop the
`ATE_INSTALL_KIND` line. Nothing else in this guide changes.

## Verify

Run:

```sh
kubectl get pods -n ate-system
```

All pods must reach `Running`, including `ate-api-server`, the atelet
DaemonSet, `atenet-router`, `postgres`, and `rustfs`. Then bring up the
counter demo. Complete a suspend/resume round-trip as in the
[Counter Demo](../../demos/counter/README.md):

```sh
./hack/install-ate.sh --deploy-demo-counter
kubectl ate create actor my-counter-1 -a ate-demo-counter --template counter
kubectl port-forward -n ate-system svc/atenet-router 8000:80
curl -X POST -H "ate-target-actor: ate-demo-counter/my-counter-1" -i http://localhost:8000/
```

## Snapshot storage options

The non-GCP path deploys rustfs in-cluster with dev-grade credentials. For a
shared or longer-lived cluster, point Substrate at an external S3-compatible
store such as MinIO or Ceph RGW instead. Set these variables on the atelet
DaemonSet and the ate-api-server Deployment, and skip the rustfs resources:

```yaml
ATE_STORAGE_BACKEND: s3
AWS_REGION: us-east-1
AWS_ENDPOINT_URL: https://<your-s3-endpoint>
AWS_S3_USE_PATH_STYLE: "true"
AWS_ACCESS_KEY_ID: <key>
AWS_SECRET_ACCESS_KEY: <secret>
```

`manifests/ate-install/kind/kustomization.yaml` shows the same values as a
working example. The GCS backend expects GCP Application Default Credentials;
use the S3 backend off GCP.

## Known limitations on this path

- This guide is verified against the code, not yet against a live cluster on
  Proxmox or bare metal.
- The Kind profile sets debug logging and a localhost registry rewrite.
  Both are transitional and disappear when the machinery is factored into the
  default install.
- The autoscaled-workerpool demo is Kind-only. It depends on a metrics
  pipeline that the Kind scripts set up.
- MicroVM asset staging is automated only for Kind and for GCS. The staging
  script runs `docker` on the installer machine and reaches rustfs through a
  Kind node's network namespace, so it cannot run against a cluster Kind did
  not create. On this path, stage the guest assets into your snapshot store
  yourself and apply the microvm `SandboxConfig`, or track the automation
  upstream.
- The telemetry collector and rustfs run in-cluster. Move both out for
  production use. Patch the component configuration accordingly.
- Air-gapped clusters need a mirror for the gVisor release assets and the
  component images. Plan the mirror before the install.

## Extending further

Treat this repository as the generic baseline. If you need an opinionated
install for your environment — Proxmox, OpenStack, your internal platform —
package it outside core, as
[ai-on-gke/substrate-gke](https://github.com/ai-on-gke/substrate-gke) does for
GKE. Discuss the split in an issue before you build.
