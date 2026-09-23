# One-Pager: Extensible Image Streaming API for Agent Substrate

**Author:** Kui Yue & Antigravity  
**Status:** Approved / Implemented  
**Date:** September 15, 2026  
**Target Package:** `internal/imagestreaming`  
**Related Docs:** [Image Streaming Performance Report](image-streaming-performance-report.md), [PoC Comparative Analysis](image-streaming-poc-comparison.md), [Architecture](architecture.md)

---

## 1. Context & Motivation
Agent Substrate’s goal is sub-500ms agent startup. Profiling shows that container image downloading and unpacking (taking 1.5 to 4+ minutes for 2GB–50GB images) is the primary bottleneck.

Image streaming addresses this by replacing upfront layer downloads with on-demand demand paging over FUSE: because agent workloads typically touch only 5%–15% of their rootfs during startup, streaming reduces image ready time from **>100s down to <2.5s** (a 38x–94x speedup).

However, Substrate clusters operate across heterogeneous cloud environments, for example:
- **Google Cloud (GKE):** Riptide remote snapshotter (`/run/containerd-gcfs-grpc`).
- **AWS (EKS):** Seekable OCI snapshotter (`/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock`).
- **Bare Metal / Local Dev:** No streaming daemon available (traditional local cache required).

### 1.1. Dual-Adoption Strategic Vision: Google Internal & External Industry Streaming

A foundational architectural requirement for Agent Substrate is **hybrid and multi-cloud workload portability**. Substrate cannot be coupled exclusively to proprietary Google infrastructure, nor can it sacrifice the deep performance optimizations available within Google Cloud.

The new `internal/imagestreaming` API is intentionally architected to serve as a **dual-adoption bridge**:
1. **Google Internal Streaming Adoption:** First-class support for Google Cloud / internal GKE streaming infrastructure powered by **Google Riptide (`containerd-gcfs-grpc`)** and Google Cloud Artifact Registry streaming metadata, unlocking sub-second cold starts on GKE TPU/GPU and CPU worker fleets.
2. **External Streaming Product Adoption:** Native adoption of external, industry-standard streaming products, anchored in this design by **AWS Seekable OCI (SOCI)** (`soci-snapshotter-grpc`). The same interface readily accommodates broader open-source OCI streaming standards (such as eStargz, Nydus, or Dragonfly) without modifying Substrate's core scheduling or execution paths.

By standardizing both **internal Google Riptide** and **external AWS SOCI** on the open CNCF Remote Snapshotter standard, we prove that Agent Substrate delivers a vendor-agnostic streaming runtime: workloads achieve equivalent 38x–94x cold boot latency reductions whether deployed on Google Cloud, AWS, or multi-cloud infrastructures.

### 1.2. Problem Statement
Substrate requires a unified, provider-agnostic Go API that:
1. Decouples the actor lifecycle engine (`cmd/atelet`) from cloud-specific streaming implementations.
2. Composes streamed layers into Substrate’s capability-less OCI overlay bundle architecture.
3. Provides automatic socket discovery with seamless, zero-disruption fallback to traditional image download and untar.
4. Preserves workload isolation and emits end-to-end OpenTelemetry telemetry.

---

## 2. Goals & Non-Goals

### Goals
- **Dual-Adoption Portability:** Unify Google internal streaming (Riptide) and external cloud products (AWS SOCI) behind an identical contract, enabling seamless multi-cloud deployment without vendor lock-in.
- **Provider-Agnostic Abstraction:** A clean `ImageStreamer` Go interface in `internal/imagestreaming` supporting pluggable backends.
- **Sub-Second Ready Time:** Enable virtual layer mount paths in `<2.5s` cold, and `<5µs` warm.
- **Overlayfs Drop-In Compatibility:** Deliver layer paths directly consumable by `ateom`'s read-only lowerdir overlay composition (`layerN/fs:...:layer0/fs`).
- **Zero-Disruption Fallback:** Transparently fall back to standard `imagecache.Store` (full layer untar) on unsupported images or daemon faults.
- **Automatic Daemon Discovery:** Support `--image-streamer=auto` to auto-detect ambient node daemons.
- **Telemetry Integration:** Emit OpenTelemetry Weaver instruments tracking streaming operations, outcomes, and latency.

### Non-Goals
- **In-Process FUSE Implementation:** Substrate does not implement custom FUSE filesystems in Go; it interfaces with host-level snapshotter daemons via gRPC/UNIX sockets.
- **Replacing Local Cache:** Traditional image caching (`internal/imagecache`) remains the authoritative baseline for non-streamable images.

---

## 3. High-Level Architecture & Core Design Principles

### 3.1. Core Principle: Standardizing on CNCF Remote Snapshotters While Bypassing Containerd CRI

In standard Kubernetes environments, image streaming operates through a multi-tier chain:
```
Kubelet (Node Agent)
  └── containerd (CRI Runtime Engine: containerd.sock)
        └── Remote Snapshotter Plugin (riptide-snapshotter / soci-snapshotter)
              └── FUSE Daemon (gcfsd / soci engine)
                    └── FUSE mounts (/run/.../fs)
                          └── runsc / runc (Container Sandbox)
```

**Agent Substrate standardizes directly on the CNCF Remote Snapshotter gRPC standard (`containerd.services.snapshots.v1.Snapshots`) while bypassing `kubelet` and `containerd` CRI (`containerd.sock`):**
```
ateapi (Substrate Control Plane)
  └── atelet (Worker Node Daemon)
        └── internal/imagestreaming/drivers/remotesnapshotter
              └── Remote Snapshotter Daemon (/run/containerd-gcfs-grpc or /run/soci-snapshotter-grpc/...)
                    └── FUSE mounts
                          └── ateom (Substrate Sandbox Overlay Manager)
```

#### Why Standardize on CNCF Remote Snapshotters:
1. **Clean CloudProvider Extraction:** Substrate core avoids importing proprietary vendor client libraries or custom protocol buffers (such as GCFS RPCs). By speaking the standard CNCF `containerd.services.snapshots.v1.Snapshots` gRPC API, a single unified driver (`remotesnapshotter`) connects identically to Google Riptide (`containerd-gcfs-grpc`), AWS SOCI (`soci-snapshotter-grpc`), eStargz (`containerd-stargz-grpc`), or Nydus.
2. **Reusing Ecosystem Snapshotter Capabilities:** Rather than reimplementing layer mounting, deduplication, chunk caching, and view management inside Substrate, Substrate leverages the robust, production-hardened remote snapshotter plugins maintained by Google and AWS.
3. **Preserving the Actor Multiplexing Model:** Substrate continues to bypass the Kubernetes control plane and `containerd.sock` CRI engine. Worker Pods remain pre-warmed and long-running. Connecting directly to the local snapshotter UNIX socket avoids containerd CRI daemon lock contention, namespace metadata sweeps, and Pod lifecycle delays.
4. **Direct Overlay LowerDir Integration:** Snapshot mounts returned by `Prepare` / `View` are directly integrated into `ateom`'s sandbox lowerdir overlay spec (`layerN/fs:...:layer0/fs`), matching traditional unpacked layers.

### 3.2. Structural Alignment: Google Riptide & AWS SOCI

By standardizing on the CNCF Remote Snapshotter interface, Google Riptide and AWS SOCI share an identical integration contract:

| Architectural Dimension | Google Cloud Riptide (`riptide`) | AWS Seekable OCI (`soci`) |
| :--- | :--- | :--- |
| **Daemon Endpoint** | `/run/containerd-gcfs-grpc` | `/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock` |
| **Interface Protocol** | `containerd.services.snapshots.v1.Snapshots` | `containerd.services.snapshots.v1.Snapshots` |
| **Runtime Interaction** | **Bypasses containerd CRI:** speaks direct snapshotter gRPC | **Bypasses containerd CRI:** speaks direct snapshotter gRPC |
| **FUSE Mount Location** | `/run/containerd-gcfs/...` or `/run/gcfsd/mnt/views/...` | `/var/lib/soci-snapshotter-grpc/snapshots/...` |
| **Layer View RPC** | `Prepare` / `View` with remote labels | `Prepare` / `View` with remote labels |
| **Metadata Index** | Cloud Artifact Registry Streaming Manifests | OCI Artifact SOCI Index (`application/vnd.amazon.soci.index.v1+json`) |

### 3.3. Architecture Flow Diagram

```mermaid
flowchart TD
    subgraph K8sStandard["Standard Kubernetes CRI Path (Bypassed)"]
        Kubelet[Kubelet] -.->|CRI containerd.sock| Containerd[containerd daemon]
        Containerd -.->|Plugin gRPC| RemotePlugin[Remote Snapshotter Plugin]
    end

    subgraph SubstrateControl["Substrate Control Plane"]
        API[ateapi / Scheduler] -->|RunActor| Atelet[cmd/atelet]
    end

    subgraph SubstrateHost["Node Host (Image Streaming Subsystem)"]
        Atelet -->|1. Resolve Image| StreamerMux["imagestreaming.ImageStreamer\n(Registry / Auto-Discovery)"]
        
        StreamerMux -->|Snapshots.v1 gRPC| GCFS["Riptide Snapshotter\n/run/containerd-gcfs-grpc"]
        StreamerMux -->|Snapshots.v1 gRPC| SOCI["SOCI Snapshotter\n/run/soci-snapshotter-grpc/..."]
        
        StreamerMux -.->|Fallback on error| ImgCache["internal/imagecache\n(Full Download & Untar)"]
        
        GCFS -->|FUSE Mount| LayerView1["/run/containerd-gcfs/.../fs"]
        SOCI -->|FUSE Mount| LayerView2["/var/lib/soci-.../snapshots/<id>/fs"]
    end

    subgraph ActorSandbox["Actor Sandbox (gVisor / runsc)"]
        Atelet -->|2. Write Overlay Spec| Ateom[ateom runtime]
        LayerView1 & LayerView2 -->|lowerdir (ro)| OverlayFS["Merged rootfs"]
        Ateom -->|upper/work (rw)| OverlayFS
        OverlayFS --> Workload["Agent Workload\n(Lazy Demand Paged)"]
    end
```

---

## 4. API Specification

The core abstraction lives in `internal/imagestreaming` and consists of three components:

### 4.1. Core Interface (`ImageStreamer`)

```go
package imagestreaming

import (
    "context"
    v1 "github.com/google/go-containerregistry/pkg/v1"
)

// ImageStreamer is the generalized interface implemented by streaming backends.
type ImageStreamer interface {
    // Name returns the provider identifier ("riptide", "soci").
    Name() string

    // CanStream checks if the provider can accelerate this image.
    CanStream(ctx context.Context, req *StreamRequest) (bool, error)

    // PrepareLayers prepares and mounts the virtual layer directories on the node.
    PrepareLayers(ctx context.Context, req *StreamRequest) (*StreamResult, error)

    // ReleaseLayers decrements the reference count or releases mounted views.
    ReleaseLayers(ctx context.Context, req *StreamRequest) error

    // ReconcileLeases restores active lease tracking and reference counts for
    // surviving actor workloads on node or process startup.
    ReconcileLeases(ctx context.Context, active []*ActiveLease) error
}
```

### 4.2. Request, Result, and Lease Models

```go
// StreamRequest holds the image reference and optional credentials.
type StreamRequest struct {
    ImageRef   string       // Fully-qualified OCI reference (e.g. us-docker.pkg.dev/...:tag)
    AuthConfig *AuthConfig // Optional pull secrets for private registries
}

// ActiveLease describes an active image lease restored during startup reconciliation.
type ActiveLease struct {
    ImageRef    string   // Fully-qualified OCI reference
    ImageDigest string   // Manifest digest ("sha256:<hex>")
    LayerDirs   []string // Host directory paths of mounted layer trees
    RefCount    int      // Number of active actor containers referencing this image
}

// StreamResult contains the prepared layers consumable by overlayfs.
type StreamResult struct {
    ImageDigest string     // Canonical manifest digest (sha256:<hex>)
    Config      *v1.Config // Parsed OCI container configuration
    LayerDirs   []string   // Host directories of mounted layers, bottom-most first
}
```

### 4.3. Registry & Factory Pattern

```go
type Factory func(ctx context.Context, cfg Config) (ImageStreamer, error)

// Register enables driver packages (e.g. drivers/riptide, drivers/soci) to
// register themselves via blank import (_ "github.com/.../drivers/soci").
func Register(name string, f Factory)

// Get constructs an instance of a registered streaming provider.
func Get(ctx context.Context, name string, cfg Config) (ImageStreamer, error)

// Providers returns all registered driver names in lexicographical order.
func Providers() []string
```

---

## 5. Runtime Integration & Contract with `atelet`

### 5.1. The Layer Wrapper Contract
`cmd/atelet` runs without elevated privileges (no `CAP_SYS_ADMIN`), and `ateom` constructs the final overlay mount. To maintain this clean separation, `ImageStreamer` produces a standard layer wrapper directory for each layer in `LayerDirs`:

```
<work_dir>/<sanitized_image_key>/
├── layer-0/
│   ├── fs -> /run/containerd-gcfs/.../fs   # Symlink to the daemon's FUSE mount
│   └── finalized                             # Sentinel marker
├── layer-1/
│   ├── fs -> /run/containerd-gcfs/.../fs
│   └── finalized
└── ...
```

- **`fs` Symlink:** Exposes the virtual layer filesystem tree to `ateom`'s overlay lowerdir.
- **`finalized` Marker:** Notifies `ateom` that the layer is immutable, instructing it to bypass whiteout materialization loops and mount directly.

### 5.2. Pluggable Resolution with Automatic Fallback
In `cmd/atelet/oci.go`:
```go
func ensureContainerImage(ctx context.Context, imageCache *imagecache.Store, streamer imagestreaming.ImageStreamer, ...) (*imagecache.Image, error) {
    if streamer != nil {
        if canStream, err := streamer.CanStream(ctx, req); err == nil && canStream {
            if res, err := streamer.PrepareLayers(ctx, req); err == nil && len(res.LayerDirs) > 0 {
                instruments.RecordImageStreaming(ctx, streamer.Name(), "streamed", dur)
                return &imagecache.Image{Digest: res.ImageDigest, Config: res.Config, LayerDirs: res.LayerDirs}, nil
            }
        }
        instruments.RecordImageStreaming(ctx, streamer.Name(), "fallback", dur)
    }
    // Fallback path: standard local cache download & untar
    return imageCache.EnsureImage(ctx, ref)
}
```

### 5.3. Mount Lifecycle Management, Garbage Collection & Reboot Recovery

Mounts and open file descriptors consume host kernel resources (VFS dentries, mount table slots, file descriptors). In a high-density actor multiplexing environment where actors may remain idle/sleeping for extended periods, releasing mounts eagerly prevents resource exhaustion while preserving rapid wake-up latency.

#### 5.3.1. Active-Only Leases (Approach 1 - Implemented)
- **PrepareLayers on Run/Wake:** When an actor starts or resumes from sleep, `PrepareLayers` ensures layer mounts are active and increments the reference count.
- **Release on Checkpoint / Sleep:** When an actor transitions to Sleep/Paused state via `atelet.Checkpoint`, `atelet` releases its image lease (`ReleaseLayers`).
  - If no other running actor on the worker references the image (`refCount == 0`), the FUSE mounts are unmounted immediately.
  - Benchmarks confirm that warm layer re-attachment takes only **~1.8µs to 2ms** (the snapshotter daemon retains compressed chunks and metadata in local cache). Thus, waking actors incur negligible overhead while host mount tables remain clean.
- **Release on Terminate:** As a safety invariant, actor termination (`atelet.Terminate`) also triggers `ReleaseLayers` if not already released.

#### 5.3.2. Candidate Future Improvements (Evaluated Alternatives)
- **Approach 2: Idle TTL / LRU Grace Period:**
  Rather than unmounting immediately upon Checkpoint, hold the lease during a configurable grace window (e.g. 5 minutes). If the actor wakes within the window, layer reuse is instant (zero RPCs). If it stays asleep past TTL, background GC unmounts the views.
- **Approach 3: Watermark-Driven Mount GC:**
  Retain warm mounts indefinitely across sleeping actors until host pressure thresholds are reached (e.g., active mount count > 100 or memory pressure), triggering LRU eviction of idle mounts.

*Decision:* Approach 1 is adopted for the initial prototype for simplicity, determinism, and zero state-machine complexity, with Approaches 2 and 3 documented for future optimization as workload density demands.

#### 5.3.3. Persistence & Reboot Recovery: Reconciliation on Startup (Implemented)
To ensure ref counting cleanly survives `atelet` crashes and node reboots without fragile disk file syncing on every microsecond call, the runtime adopts an **Actor-Derived Reconciliation Model** (matching how Kubernetes `kubelet` recovers state and how Substrate's non-streaming image cache GC discovers roots):
1. **Reconciliation at Startup (Self-Healing):**
   - Implemented via `scanActiveStreamedLeases` and `reconcileStreamingLeases` in `cmd/atelet/streaming_reconcile.go`.
   - On `atelet` startup, `atelet` scans active on-node bundle overlay specs (`ateompath.ActorsDir/*/bundles/*/rootfs-overlay.json`).
   - It tallies all active image references (`OverlaySpec.ImageRef`) across resident/running actors, passing `[]*ActiveLease` into `streamer.ReconcileLeases(ctx, active)`.
   - Both `riptide` and `soci` drivers restore their in-memory `d.leases` with exact live reference counts (`refCount`) and layer paths, enabling subsequent container creations to reuse warm mounts immediately.
   - This is completely self-healing: even if an actor or `atelet` crashed midway, the recovered count reflects ground truth rather than potentially stale persisted counters.
2. **Orphan View Garbage Collection (Sweeper):**
   - A periodic or startup background sweep queries the remote snapshotter daemon for mounted views and unmounts any view that has no matching active actor directory on disk.

### 5.4. Cold-Start Reliability & Sandbox Warmup: Host-Side Enumeration Probing & Metadata Prefetch

When streaming container images over FUSE, workloads encounter two distinct cold-node initialization phenomena: an asynchronous directory-index loading race condition, and sandbox traversal latency inside gVisor (`runsc`). To eliminate both without stalling actor startup, the streaming driver integrates a two-stage operational warmup pattern:

```
[atelet / driver.PrepareLayers]
       │
       ├── 1. Synchronous Enumeration Probe (os.ReadDir) ──► Guarantees daemon index is listable (prevents ENOENT race)
       │
       ├── 2. Return StreamResult (Ready in <2.5s) ────────► ateom boots actor sandbox immediately
       │
       └── 3. Background Goroutine (warmRootfsMetadata) ───► Host-side WalkDir(lstat) prefetches inode metadata
                                                             (eliminates gVisor Sentry/Gofer latency stalls)
```

#### 1. Synchronous Enumeration Probe: Eliminating the Cold-Start `ENOENT` Race
* **The Failure Mode:** Immediately after the snapshotter creates a FUSE view for a newly pulled layer, lookups for specific known file paths (e.g., `/bin/sh`) succeed instantly, but its internal directory index loads asynchronously. During this brief sub-second window, directory enumerations (`getdents64` / `readdir`) through the FUSE mount may either return empty or fail with `ENOENT`. If an actor starts during this window and scans directories (such as Python inspecting `sys.path` or Node.js module resolution), the application crashes on boot.
* **The Probe Mechanism:** In the driver, immediately after creating the layer view, the driver performs a quick listability probe on the host:
  ```go
  probeStart := time.Now()
  for attempt := 1; ; attempt++ {
      entries, err := os.ReadDir(viewPath)
      if err == nil && len(entries) > 0 {
          break
      }
      if time.Since(probeStart) > listableTimeout {
          return nil, fmt.Errorf("layer view %q not listable within %s: %w", viewPath, listableTimeout, err)
      }
      time.Sleep(100 * time.Millisecond)
  }
  ```
* **Performance Impact:** On a warm node where the image was previously mounted, `os.ReadDir` returns in **<1ms** on the first try. On a completely cold layer, it absorbs the 200ms–400ms index initialization window on the host, guaranteeing that `ateom` never mounts a half-initialized rootfs.

#### 2. Background Host-Side Metadata Warmup: Eliminating gVisor Sentry/Gofer Stalls
* **The Sandbox Bottleneck:** Substrate runs actors inside gVisor sandboxes. Unlike standard containers where syscalls go directly to the host kernel, filesystem operations inside gVisor traverse the **Sentry (guest kernel) $\rightarrow$ Gofer (9P/virtiofs server) $\rightarrow$ Host VFS $\rightarrow$ FUSE $\rightarrow$ Registry**. If metadata is resolved lazily, every `stat(2)` or `access(2)` during application initialization pays heavy sandbox round-trip penalties while waiting on remote FUSE reads.
* **The Host-Side Solution:** Because the snapshotter's metadata cache operates at the **host node level**, walking the directory tree directly on the host pre-populates the in-memory index for all containers on that machine. Right after `PrepareLayers` succeeds, `atelet` triggers an asynchronous background walker:
  ```go
  go warmLayerMetadata(viewPath)
  ```
  ```go
  func warmLayerMetadata(root string) {
      _ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
          if err != nil {
              return nil
          }
          // lstat forces daemon to pull and cache the directory entry and inode record
          _, _ = d.Info()
          return nil
      })
  }
  ```
* **Why This Is Safe and Efficient:**
  1. **Zero Payload Contention:** The walker strictly invokes `d.Info()` (`lstat`), forcing the daemon to fetch directory indexes and inode headers. It **deliberately does not read file bytes**, avoiding network and cache contention with the workload's actual data reads.
  2. **Non-Blocking:** Running in a background goroutine ensures the actor sandbox starts immediately without adding any latency to the critical startup path.
  3. **Amortized Across Multiplexed Actors:** In Substrate, multiplexed actors share underlying container images. Waking the metadata once on the host benefits all subsequent actors scheduled on that node.

---

## 6. Provider Driver Implementations

| Provider | Host Socket | Protocol | Image Indexing | Credential Resolution |
| :--- | :--- | :--- | :--- | :--- |
| **`riptide`** (Google) | `/run/containerd-gcfs-grpc` | containerd `SnapshotService` gRPC | Google Cloud Artifact Registry Streaming Manifests | GCE Instance Metadata + Google ADC via `google.Keychain` |
| **`soci`** (AWS) | `/run/soci-snapshotter-grpc/...` | containerd `SnapshotService` gRPC | OCI Artifact SOCI Index (`application/vnd.amazon.soci.index.v1+json`) | containerd registry auth + `google.Keychain` multi-keychain |

---

## 7. Telemetry & Observability

Image streaming instrumentation is codified in the OpenTelemetry Weaver registry (`docs/metrics/registry/metrics.yaml`):

1. **`atelet.image_streaming.operations` (Counter):**
   - Tracks total streaming evaluations.
   - Labels: `streamer` (`riptide`, `soci`), `outcome` (`streamed`, `fallback`, `error`).
2. **`atelet.image_streaming.duration` (Histogram):**
   - Measures latency (seconds) of layer preparation and metadata checks.
   - Labels: `streamer`, `outcome`.

---

## 8. Verified Performance Impact

Empirically validated on live GKE cluster `kuiyue-stream-test`:

| Metric | Traditional Baseline | Riptide (1.88 GB AXLearn) | SOCI (3.14 GB TensorFlow) |
| :--- | :--- | :--- | :--- |
| **Total Image Ready Time** | 100.65s – 164.85s | **2.63s (38.2x speedup)** | **1.75s (94.3x speedup)** |
| **Warm View Attachment** | 100.65s – 164.85s | **1.86 µs** | **2.23 µs** |
| **Demand Paging Throughput** | Local Disk Speed | 1.69 MB/s (uncached) | 6.20 MB/s (uncached) |
| **Cached Re-Read Throughput** | Local Disk Speed | 22.93 MB/s (VFS cache) | 43.96 MB/s (VFS cache) |
| **Data Integrity** | 100% | 100% (0 errors) | 100% (0 errors) |
