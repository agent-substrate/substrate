# Image Streaming Live Cluster E2E Probe

`e2e-stream-probe` is a live validation tool for Agent Substrate image streaming acceleration.
It exercises the `riptide` (Riptide Snapshotter) and `soci` (SOCI Snapshotter) drivers against live snapshotter daemons on a Kubernetes node.

## What it exercises
1. Connects to the snapshotter socket given by `--socket`, or to the provider's default:
   - Riptide Snapshotter: `/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock`
   - SOCI Snapshotter: `/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock`
2. Validates `CanStream` for the target container image.
3. Invokes `PrepareLayers` on the live daemon:
   - Queries image manifest, config, diffIDs, and layer digests.
   - For each layer, calls `Stat`, `Prepare`, and `View` over the `Snapshots.v1` API, following the snapshotter contract in Section 3.4 of the [design doc](../../docs/image-streaming-api-design.md).
   - Verifies layer directories, `fs` symlinks, and pre-materialized `finalized` markers.
   - Inspects live rootfs filesystem contents inside the streamed views without pulling full tarballs.
4. Invokes `ReleaseLayers`:
   - Calls `Remove` on each layer's view.
   - Cleans up temporary layer directory wrappers.

## How to run manually

### 1. Build the Linux static binary
```bash
CGO_ENABLED=0 GOOS=linux go build -o /tmp/e2e-stream-probe ./tools/e2e-stream-probe
```

### 2. Connect to the GKE cluster
```bash
gcloud container clusters get-credentials kuiyue-stream-test --zone=us-central1-a --project=kuiyue-gke-dev
```

---

### Scenario A: Testing the Riptide Snapshotter

1. Start a runner pod that mounts the node's Riptide Snapshotter socket directory, `/run/containerd-gcfs-grpc`:
```bash
kubectl run stream-e2e-runner \
  --image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/nginx:latest \
  --restart=Never \
  --overrides='{
    "spec": {
      "containers": [{
        "name": "runner",
        "image": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/nginx:latest",
        "command": ["sleep", "3600"],
        "volumeMounts": [{
          "name": "riptide-snapshotter",
          "mountPath": "/run/containerd-gcfs-grpc"
        }]
      }],
      "volumes": [{
        "name": "riptide-snapshotter",
        "hostPath": {
          "path": "/run/containerd-gcfs-grpc"
        }
      }]
    }
  }'
```

Each layer's `fs` symlink points at a mount that the snapshotter creates on the host. The probe prints each target. To list layer contents from the runner pod, also mount the host directory that holds those mounts, with `mountPropagation: HostToContainer`.

2. Copy the binary and execute:
```bash
kubectl cp /tmp/e2e-stream-probe default/stream-e2e-runner:/tmp/e2e-stream-probe

# Test image-b (cold streaming)
kubectl exec -n default stream-e2e-runner -- /tmp/e2e-stream-probe \
  --provider=riptide \
  --socket=/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock \
  --image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/image-b:latest

# Test image-a (warm streaming)
kubectl exec -n default stream-e2e-runner -- /tmp/e2e-stream-probe \
  --provider=riptide \
  --socket=/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock \
  --image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/image-a:latest
```

3. Clean up:
```bash
kubectl delete pod stream-e2e-runner -n default
```

---

### Scenario B: Testing AWS SOCI (Seekable OCI)

1. Deploy the SOCI runner/daemon pod:
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: soci-daemon
  namespace: default
spec:
  hostNetwork: true
  containers:
  - name: daemon
    image: us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/image-b:latest
    command: ["sleep", "86400"]
    securityContext:
      privileged: true
    volumeMounts:
    - name: run-soci
      mountPath: /run/soci-snapshotter-grpc
    - name: var-lib-soci
      mountPath: /var/lib/soci-snapshotter-grpc
  volumes:
  - name: run-soci
    hostPath:
      path: /run/soci-snapshotter-grpc
      type: DirectoryOrCreate
  - name: var-lib-soci
    hostPath:
      path: /var/lib/soci-snapshotter-grpc
      type: DirectoryOrCreate
```

2. Configure and run `soci-snapshotter-grpc`:
```bash
kubectl exec -n default soci-daemon -- mkdir -p /etc/soci-snapshotter-grpc
kubectl exec -n default soci-daemon -- sh -c 'cat <<EOF > /etc/soci-snapshotter-grpc/config.toml
[pull_modes.soci_v1]
enable = true

[pull_modes.soci_v2]
enable = true
EOF'

kubectl exec -n default soci-daemon -- sh -c "nohup /usr/local/bin/soci-snapshotter-grpc --config=/etc/soci-snapshotter-grpc/config.toml --address=/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock --root=/var/lib/soci-snapshotter-grpc --log-level=debug > /var/log/soci-run.log 2>&1 &"
```

3. Copy the probe binary and execute:
```bash
kubectl cp /tmp/e2e-stream-probe default/soci-daemon:/usr/local/bin/e2e-stream-probe

kubectl exec -n default soci-daemon -- /usr/local/bin/e2e-stream-probe \
  --provider=soci \
  --socket=/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock \
  --image=public.ecr.aws/soci-workshop-examples/ffmpeg:latest
```

4. Clean up:
```bash
kubectl delete pod soci-daemon -n default
```

---

## Performance Benchmarking Mode (`-bench`)

The `-bench` mode executes a rigorous end-to-end comparative performance evaluation comparing traditional container image pull & unpack (baseline) against lazy-loading image streaming:

1. **Traditional Baseline**: Downloads every compressed layer blob over the network, decompresses (gzip), and untars all files to local disk, measuring total time and throughput.
2. **Image Streaming (Cold & Warm)**: Measures the time to verify eligibility via `CanStream` and issue sequential mount requests to the daemon (`PrepareLayers`), plus re-attaching warm cached views.
3. **In-Container Demand Paging**: Samples working set files across the mounted FUSE filesystem layers to measure first-touch uncached read throughput (MB/s), mean and P50 read latency, and subsequent cached re-read throughput.
4. **State & Byte-Level Data Integrity**: Verifies SHA-256 hashes of all sampled files, confirming 100% data integrity without byte corruption.

### Running the Benchmarks

#### Workload 1: Medium Training Image (1.88 GB, 12 layers) via the Riptide Snapshotter
```bash
/usr/local/bin/e2e-stream-probe \
  -provider=riptide \
  -socket=/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock \
  -image=us-docker.pkg.dev/kuiyue-gke-dev/axlearn/tpu:kuiyue-enabled \
  -bench
```

#### Workload 2: AWS SOCI Equivalent (3.14 GB compressed, 19 layers) via AWS SOCI
```bash
/usr/local/bin/e2e-stream-probe \
  -provider=soci \
  -socket=/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock \
  -image=public.ecr.aws/soci-workshop-examples/tensorflow_gpu:latest \
  -bench
```
