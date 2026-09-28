# One-Pager: Extensible Image Streaming API for Agent Substrate

**Author:** Kui Yue & Antigravity  
**Status:** Approved / Implemented  
**Date:** September 15, 2026  
**Target Package:** `internal/imagestreaming`  
**Related Docs:** [Image Streaming Performance Report](image-streaming-performance-report.md), [PoC Comparative Analysis](image-streaming-poc-comparison.md), [Architecture](architecture.md)

---

## 1. Context & Motivation
Agent Substrate’s goal is sub-500ms agent startup. Profiling shows that container image downloading and unpacking (taking 1.5 to 4+ minutes for 2GB–50GB images) is the primary bottleneck.

Image streaming addresses this by replacing upfront layer downloads with lazy loading over FUSE: because agent workloads typically touch only 5%–15% of their rootfs during startup (and restored actors load their application memory pages from Golden Snapshots), streaming reduces cold-node actor restore (`AteomHerder/Restore`) from **28.5s down to 1.36s–3.70s** (a **7.7x–21.0x** end-to-end restore speedup, based on the benchmark using a 1.19 GB compressed / ~3.5 GB unpacked `demos/sandbox` workload image built on `gcr.io/cloud-builders/gcloud:latest`).

However, Substrate clusters operate across heterogeneous cloud environments, for example:
- **Google Cloud (GKE):** Riptide Snapshotter v2 (`/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock`).
- **AWS (EKS):** Seekable OCI snapshotter (`/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock`).
- **Bare Metal / Local Dev:** No streaming daemon available (traditional local cache required).

### 1.1. Phased Strategic Vision: Riptide v2 at GA, Extensible to External OSS Streaming

A foundational architectural requirement for Agent Substrate is **hybrid and multi-cloud workload portability** while meeting a tight GA timeline. Substrate cannot be coupled exclusively to proprietary Google client libraries, nor can it delay GA to accommodate every implementation quirk across third-party snapshotter daemons.

The `internal/imagestreaming` API is therefore architected around a **phased dual-adoption model**:
1. **GA Scope — Full Production Support for Google Riptide v2 (`--enable-v2`) on GKE:** First-class, production-hardened support for Google Cloud GKE streaming infrastructure powered by **Google Riptide Snapshotter v2** (`containerd-gcfs-grpc` with `--enable-v2`) and Google Cloud Artifact Registry streaming metadata, unlocking sub-second cold starts on GKE TPU/GPU and CPU worker fleets.
2. **Post-GA Extensibility — External & OSS Streaming Products (AWS SOCI, eStargz, Nydus):** Rather than coupling `atelet` to a Riptide-specific protocol, `internal/imagestreaming` standardizes on the open CNCF Remote Snapshotter gRPC interface (`containerd.services.snapshots.v1.Snapshots`). We have already validated an end-to-end **AWS Seekable OCI (SOCI)** driver preset (`soci-snapshotter-grpc`) against fully-indexed SOCI images (`--min-layer-size=0`), and documented the exact driver extensions required post-GA to support arbitrary OSS snapshotters (such as hybrid local unpack + `Commit` for partially-indexed SOCI images, and OCI layer annotation forwarding for Nydus — see [Section 3.2.1](#321-subtle-behavioral-differences-across-cncf-remote-snapshotter-implementations) and [Section 9](#9-future-work-full-oss-snapshotter-support--zero-code-provider-parameterization)).

### 1.2. Problem Statement
Substrate requires a unified, provider-agnostic Go API that:
1. Decouples the actor lifecycle engine (`cmd/atelet`) from cloud-specific streaming implementations.
2. Fully supports **Google Riptide Snapshotter v2** at GA while remaining cleanly extensible to OSS remote snapshotters (AWS SOCI, eStargz, Nydus).
3. Composes streamed layers into Substrate’s capability-less OCI overlay bundle architecture.
4. Provides automatic socket discovery with seamless, zero-disruption fallback to traditional image download and untar.
5. Preserves workload isolation and emits end-to-end OpenTelemetry telemetry.

---

## 2. Goals & Non-Goals

### Goals
- **Riptide v2 Production Readiness at GA:** Full production support for Google Riptide Snapshotter v2 (`--enable-v2`) on GKE.
- **Extensible Multi-Cloud Architecture:** Unify Google Riptide and external CNCF remote snapshotters (such as AWS SOCI, validated in our benchmark PoC) behind a clean `ImageStreamer` Go interface in `internal/imagestreaming`.
- **Sub-Second Ready Time:** Enable virtual layer mount paths in `<2.5s` cold, and `<5µs` warm.
- **Overlayfs Drop-In Compatibility:** Deliver layer paths directly consumable by `ateom`'s read-only lowerdir overlay composition (`layerN/fs:...:layer0/fs`).
- **Zero-Disruption Fallback:** Transparently fall back to standard `imagecache.Store` (full layer untar) on unsupported images or daemon faults.
- **Automatic Daemon Discovery:** Support `--image-streamer=auto` to auto-detect ambient node daemons (Section 5.7).
- **Telemetry Integration:** Emit OpenTelemetry Weaver instruments tracking streaming operations, outcomes, and latency.

### Non-Goals
- **In-Process FUSE Implementation:** Substrate does not implement custom FUSE filesystems in Go; it interfaces with host-level snapshotter daemons via gRPC/UNIX sockets.
- **Riptide v1 Legacy Quirks or Full OSS Edge-Case Parity at GA:** Supporting legacy Riptide v1 (which lacks `chainID`-keyed mounts and inline `Remove`) or hybrid partial-layer `Commit` for unindexed sub-layers in OSS snapshotters is outside GA scope and tracked as future work (Section 3.2.1, Section 9).
- **Replacing Local Cache:** Traditional image caching (`internal/imagecache`) remains the authoritative baseline for non-streamable images.

---

## 3. High-Level Architecture & Core Design Principles

### 3.1. Core Principle: Standardizing on CNCF Remote Snapshotters While Bypassing Containerd CRI

In standard Kubernetes environments, image streaming operates through a multi-tier chain:
```
Kubelet (Node Agent)
  └── containerd (CRI Runtime Engine: containerd.sock)
        └── Remote Snapshotter Plugin (Riptide Snapshotter / SOCI Snapshotter)
              └── FUSE mounts (/run/.../fs)
                    └── runsc / runc (Container Sandbox)
```

**Agent Substrate standardizes directly on the CNCF Remote Snapshotter gRPC standard (`containerd.services.snapshots.v1.Snapshots`) while bypassing `kubelet` and `containerd` CRI (`containerd.sock`):**
```
ateapi (Substrate Control Plane)
  └── atelet (Worker Node Daemon)
        └── internal/imagestreaming/drivers/remotesnapshotter
              └── Remote Snapshotter Daemon (Riptide Snapshotter v2; extensible to SOCI / OSS)
                    └── FUSE mounts
                          └── ateom (Substrate Sandbox Overlay Manager)
```

#### Why Standardize on CNCF Remote Snapshotters:
1. **Clean CloudProvider Extraction:** Substrate core avoids importing proprietary vendor client libraries or custom protocol buffers. By speaking the standard CNCF `containerd.services.snapshots.v1.Snapshots` gRPC API, a single driver (`remotesnapshotter`) supports the Google Riptide Snapshotter v2 (`containerd-gcfs-grpc`) at GA and serves as the foundation for AWS SOCI (`soci-snapshotter-grpc`), eStargz (`containerd-stargz-grpc`), and Nydus.
2. **Reusing Ecosystem Snapshotter Capabilities:** Rather than reimplementing layer mounting, deduplication, chunk caching, and view management inside Substrate, Substrate leverages the production-hardened remote snapshotter daemons maintained by cloud providers and the CNCF ecosystem.
3. **Preserving the Actor Multiplexing Model:** Substrate continues to bypass the Kubernetes control plane and `containerd.sock` CRI engine on the actor launch/restore path. Worker Pods remain pre-warmed and long-running. Connecting directly to the local snapshotter UNIX socket avoids containerd CRI daemon lock contention, namespace metadata sweeps, and Pod lifecycle delays.
4. **Direct Overlay LowerDir Integration:** Snapshot mounts returned by `Prepare` / `View` are directly integrated into `ateom`'s sandbox lowerdir overlay spec (`layerN/fs:...:layer0/fs`), matching traditional unpacked layers.

### 3.2. Structural Alignment & Subtle Implementation Differences Across Snapshotters

At the gRPC surface, Google Riptide v2 and AWS SOCI share the same CNCF Remote Snapshotter interface:

| Architectural Dimension | Google Cloud Riptide v2 (`riptide` — **GA Supported**) | AWS Seekable OCI (`soci` — **PoC / Future Production**) |
| :--- | :--- | :--- |
| **Daemon Endpoint** | `/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock` (`--enable-v2`) | `/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock` |
| **Interface Protocol** | `containerd.services.snapshots.v1.Snapshots` | `containerd.services.snapshots.v1.Snapshots` |
| **Runtime Interaction** | **Bypasses containerd CRI:** speaks direct snapshotter gRPC | **Bypasses containerd CRI:** speaks direct snapshotter gRPC |
| **FUSE Mount Location** | `/var/lib/containerd/io.containerd.snapshotter.v1.gcfs/snapshotter/snapshots/<id>/fs` $\to$ `/run/gcfsd/mnt/views/<chainID>/fs` | `/var/lib/soci-snapshotter-grpc/snapshotter/snapshots/<id>/fs` |
| **Layer View RPC** | `Prepare` / `View` with remote labels | `Prepare` / `View` with remote labels |
| **Metadata Index** | Google Cloud Artifact Registry (GAR) Streaming Manifests | OCI Artifact SOCI Index (`application/vnd.amazon.soci.index.v1+json`) |
| **Registry Scope** | **GAR / GCR exclusively** (external registries fall back to non-streaming) | Any OCI registry supporting SOCI index artifacts (ECR, GAR, etc.) |
| **Authentication Source** | Ambient Node Identity via GCE Metadata Service (`169.254.169.254`) | Ambient Node Identity via EC2 Instance Profile / link-local metadata |

#### Special Characteristics and Encapsulation Boundaries

1. **Riptide Snapshotter Encapsulation & CNCF gRPC Standard:**
   Substrate communicates directly with the **Riptide Snapshotter** (`containerd-gcfs-grpc`) over its local UNIX domain socket. Substrate does not manage, monitor, or communicate with `gcfsd` directly; all layer virtualization, chunk demand-paging, and mount lifecycles are encapsulated behind `containerd-gcfs-grpc` through the standard CNCF `Snapshots.v1` gRPC contract (Section 3.4).
2. **Registry Scope: Google Artifact Registry (GAR) Exclusivity:**
   Google Riptide exclusively streams container images hosted in Google Artifact Registry (GAR) or Google Container Registry (GCR). External registries (such as Docker Hub, Quay.io, or AWS ECR) do not contain Riptide streaming metadata. When an image from an external registry is targeted on GKE, the Riptide Snapshotter declines `Prepare` (Section 3.4), and Substrate falls back to traditional non-streaming local caching (`imagecache.EnsureImage`).

#### 3.2.1. Subtle Behavioral Differences Across CNCF Remote Snapshotter Implementations

Although Riptide v2, Riptide v1, AWS SOCI, eStargz, and Nydus all implement the CNCF `containerd.services.snapshots.v1.Snapshots` gRPC service and containerd's remote snapshotter label conventions, **the gRPC protobuf alone does not guarantee identical runtime behavior when called directly without containerd's unpacker**. Auditing the Riptide codebase (`snapshot/v2/snapshotter.go` vs. legacy `snapshot/snapshot.go`), AWS SOCI (`soci-snapshotter-grpc`), and Nydus reveals six critical behavioral differences that explain why **GA is scoped to Riptide v2 (`--enable-v2`)** and what extensions are needed for full OSS parity:

| Behavioral Dimension | Riptide v2 (`--enable-v2`, **GA Target**) | Riptide v1 (Legacy) | AWS SOCI (`soci-snapshotter-grpc`) | eStargz (`containerd-stargz-grpc`) | Nydus (`containerd-nydus-grpc`) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1. Sub-Layer Decline & `Commit` Requirement** | Streams **all** layers of an imported GAR image (mounts keyed by `chainID`). Never declines individual sub-layers of a streamable image. | Declines the 2nd copy of any duplicate `DiffID` (`walkParentChainToFindDuplicateLayers`) because `gcfsd` v1 keys views by `diffID` and Linux `overlayfs` rejects duplicate symlink targets in `lowerdir`. Requires local unpack + `Commit` for the duplicate layer. | By default, `soci create` skips zTOCs for layers $<$ `--min-layer-size` (default **10 MiB**). `Prepare` declines small layers and expects caller to unpack + `Commit(chainID, key)` locally. All-or-nothing streaming works only when indexed with `--min-layer-size=0`. | Streams any layer formatted as eStargz; declines non-eStargz layers. | Streams all layers via Nydus bootstrap metadata. |
| **2. `cri.image-layers` & Early-Abort Behavior** | Expects shrinking suffix (`layers[i:]`) so top layer has `len == 1`. Uses a time-based backoff in `gcfsdClient.createSnapshot` on import errors; **safe to abort on first declined layer**. | Expects shrinking suffix (`layers[i:]`). On `ErrImageNotAvailable` at layer 0, adds `imageRef` to `o.nonImportedImages` and **only clears it when `Prepare` is called on the top layer (`len == 1`)**. Aborting on layer 0 permanently blocks the image until daemon restart. | Ignores `cri.image-layers`; safe to abort on first declined layer. | Ignores `cri.image-layers`; safe to abort on first declined layer. | Ignores `cri.image-layers`. |
| **3. `cri.image-ref` Tag vs. Digest Handling** | `filesystem.Mount` does **not** read `cri.manifest-digest`; passes `cri.image-ref` directly to `gcfsd`. Caller should pass a digest-pinned `cri.image-ref` (`repo@sha256:...`). | `convertTagToDigest` uses `cri.manifest-digest` to rewrite a tagged `cri.image-ref` into `repo@sha256:<manifest-digest>`. | Uses `cri.image-ref` and `cri.manifest-digest` to locate the OCI Referrers SOCI index. | Uses `cri.image-ref` and `cri.layer-digest`. | Uses `cri.image-ref` plus OCI layer annotations. |
| **4. Required `Prepare` Labels** | `containerd.io/snapshot.ref` + `containerd.io/snapshot/cri.*` (`image-ref`, `manifest-digest`, `layer-digest`, `image-layers`). | Same as Riptide v2. | Same as Riptide v2. | Same as Riptide v2. | **Also requires OCI layer descriptor annotations** (`containerd.io/snapshot/nydus-bootstrap`, `nydus-blob`, etc.) forwarded from the manifest. |
| **5. `View(key, chainID)` Return Value** | Layer 0: `bind` mount (`ro,rbind`). Layers $i \ge 1$: cumulative `overlay` mount (`lowerdir=<layer_i>:...:<layer_0>`). Driver extracts `parts[0]` (`<layer_i>`) for per-layer wrapper symlinks. | Same as Riptide v2 (`snapshot.go:L934-L957`). | Same as Riptide v2. | Same as Riptide v2. | Intermediate layers are dummy metadata entries; **only a `View` on the topmost (bootstrap) layer** yields a valid rootfs mount. |
| **6. Snapshot Deletion (`Remove` vs. `Cleanup`)** | `Remove(key)` cleans up BoltDB and filesystem directories synchronously inline (`AsynchronousRemove` and `Cleanup` are no-ops). | Unconditionally enables `snbase.AsynchronousRemove` (`main.go:L218`). `Remove(key)` only deletes the BoltDB record; on-disk directories leak unless `Snapshots.Cleanup` RPC is called. | `Remove(key)` cleans up inline. | `Remove(key)` cleans up inline. | `Remove(key)` cleans up inline. |

### 3.3. Architecture Flow Diagram

```mermaid
flowchart TD

    subgraph SubstrateControl["Substrate Control Plane"]
        API[ateapi / Scheduler] -->|RunActor| Atelet[cmd/atelet]
    end

    subgraph SubstrateHost["Node Host (Image Streaming Subsystem)"]
        Atelet -->|1. Resolve Image| StreamerMux["imagestreaming.ImageStreamer<br/>(Registry / Auto-Discovery)"]
        
        StreamerMux -->|Snapshots.v1 gRPC| GCFS["Riptide Snapshotter v2 (GA)<br/>/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock"]
        StreamerMux -.->|Snapshots.v1 gRPC (Extensible)| SOCI["SOCI / OSS Snapshotter<br/>/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock"]
        
        StreamerMux -.->|Fallback on error| ImgCache["internal/imagecache<br/>(Full Download & Untar)"]
        
        GCFS -->|FUSE Mount| LayerView1["/var/lib/containerd/.../snapshots/<id>/fs<br/>-> /run/gcfsd/mnt/views/<chainID>/fs"]
        SOCI -.->|FUSE Mount| LayerView2["/var/lib/soci-.../snapshots/<id>/fs"]
    end

    subgraph ActorSandbox["Actor Sandbox"]
        Atelet -->|2. Write Overlay Spec| Ateom[ateom runtime]
        %% Parentheses in labels are wrapped in string quotes below
        LayerView1 & LayerView2 -- "lowerdir (ro)" --> OverlayFS["Merged rootfs"]
        Ateom -- "upper/work (rw)" --> OverlayFS
        OverlayFS --> Workload["Agent Workload<br/>(Lazy Demand Paged)"]
    end
```

### 3.4. The Snapshotter Contract (Riptide v2 GA Baseline)

At GA, the generic driver (`internal/imagestreaming/drivers/remotesnapshotter`) implements the strict, all-or-nothing remote snapshotter contract tailored for **Riptide v2 (`--enable-v2`)** (and compatible with SOCI images where all layers are indexed):

1. **The `containerd.services.snapshots.v1.Snapshots` gRPC API.** The driver calls `Stat`, `Prepare`, `View`, and `Remove`. It never calls `Commit` at GA (see [Section 9.1](#91-driver-protocol-extensions-for-full-production-oss-snapshotter-support) for post-GA hybrid `Commit`).
2. **containerd's [remote snapshotter label protocol](https://github.com/containerd/containerd/blob/main/docs/snapshotters/remote-snapshotter.md).** For each layer $i$ ($0 \le i < N$), the driver labels `Prepare` with:
   - `containerd.io/snapshot.ref`: the layer's `chainID_i`.
   - `containerd.io/snapshot/cri.image-ref`: the canonical digest-pinned image reference (`<repo>@<manifest-digest>`), ensuring Riptide v2's `filesystem.Mount` (which does not rewrite tags via `cri.manifest-digest`) always passes a digest-pinned reference to `gcfsd`.
   - `containerd.io/snapshot/cri.manifest-digest`: the resolved OCI manifest digest (`sha256:<hex>`).
   - `containerd.io/snapshot/cri.layer-digest`: the compressed layer blob digest (`layerDigests[i]`).
   - `containerd.io/snapshot/cri.image-layers`: the **shrinking suffix** of comma-separated layer digests from the current layer to the top layer (`strings.Join(layerDigests[i:], ",")`), so Riptide detects the topmost layer when `len == 1`.

The driver handles each layer $i$, bottom to top, while holding a **per-`chainID` mutex (`d.chainLock(chainID_i)`)** around `Stat` + `Prepare`:

| Call | Result | Driver Action |
| :--- | :--- | :--- |
| `Stat(chainID_i)` | Found (`OK`) | The layer is already committed in the snapshotter (and in Riptide, `Stat` has verified/self-healed its mount). Unlock `chainID_i` and create a read-only `View` with `chainID_i` as its parent. |
| `Stat(chainID_i)` | `NotFound` | While still holding the `chainID_i` lock, call `Prepare` with a unique `prepKey`, `parent = chainID_{i-1}`, and the labels above. |
| `Prepare` | `AlreadyExists` | The snapshotter mounted the layer and committed `prepKey` under `chainID_i`. Confirm with `Stat(chainID_i)`, unlock `chainID_i`, and create a `View`. The snapshotter consumed `prepKey`, so the driver does not remove it. |
| `Prepare` | Mounts with a `nil` error | **Declined.** The snapshotter cannot stream the layer and returned writable mounts expecting the caller to unpack and `Commit` it. The driver removes `prepKey` without committing it, unlocks `chainID_i`, removes any views created for earlier layers, and returns `imagestreaming.ErrNotStreamable`. |
| Any call | Any other error | The driver removes `prepKey` (if created) and any views created for earlier layers, and returns the error. |

#### Why `Stat(chainID)` + Per-`chainID` Locking Before `Prepare` Is Mandatory:
Unlike containerd v2's `Unpacker` (which checks containerd's own local `meta.db` before calling `Prepare` on the remote snapshotter), `atelet` talks directly to the snapshotter socket without containerd. Calling `Stat(chainID)` under a per-`chainID` lock before `Prepare` is required for three reasons:
1. **Preventing Riptide's `storage.CommitActive` Active-Snapshot Leak on Duplicate/Concurrent `Prepare`:**
   In Riptide (both v1 `snapshot/snapshot.go:L439-L447` and v2 `snapshot/v2/snapshotter.go:L383-L388`), if `chainID` is already committed in BoltDB and `Prepare(prepKey, parent, labels)` is called anyway, Riptide mounts the layer and calls `o.Commit(ctx, chainID, prepKey)`. Inside `storage.CommitActive`, `CreateBucket([]byte(chainID))` fails with `ErrBucketExists`, rolling back the BoltDB transaction **before deleting the active snapshot `prepKey`**, yet Riptide treats `ErrAlreadyExists` as success and returns `codes.AlreadyExists` without calling `Remove(prepKey)`. Without a prior `Stat(chainID)` check **and** a per-`chainID` lock (to serialize concurrent pulls of different images that share base layers), the losing `Prepare` leaks an unconsumed Active snapshot in `metadata.db` and on disk, which also permanently pins its `parent` snapshot.
2. **Mount Self-Healing (`fs.Check`):**
   In Riptide (v1 `snapshot.go:L265-L288` and v2 `snapshotter.go:L231-L255`), `Stat(chainID)` is not passive: it invokes `fs.Check` on the layer and automatically remounts the view if `gcfsd` restarted.
3. **Reusing Layers Committed by Earlier Pulls:**
   If an earlier actor image (or another image sharing the same base layers) already committed `chainID` in the snapshotter, `Stat(chainID)` finds it in one local gRPC call and skips `Prepare`. After `Prepare` returns `AlreadyExists`, the driver calls `Stat(chainID)` a second time to verify that `chainID` itself exists (rather than `prepKey` colliding).

#### How `View(viewKey, chainID_i)` Mounts Are Extracted and Pinned:
- **Cumulative Overlay vs. Single-Layer Extraction:** In the CNCF Snapshotter API (including Riptide v2 `mounts.go:L110-L125`, Riptide v1 `snapshot.go:L934-L957`, and SOCI), `View(viewKey, chainID_0)` on the bottom layer returns a read-only `bind` mount (`ro,rbind`) pointing to `<root>/snapshots/<id_0>/fs`. For every upper layer ($i \ge 1$), `View(viewKey, chainID_i)` returns a cumulative `overlay` mount with `lowerdir=<layer_i>:<layer_{i-1}>:...:<layer_0>`. Because `ateom` composes its own final sandbox overlay across all layers, `extractMountDir` in `driver.go` extracts `parts[0]` (`<layer_i>`, the topmost entry of `lowerdir=`) for each layer's `layer-i/fs` symlink.
- **BoltDB Parent Pinning:** In BoltDB (`storage.Remove`), every active `View` (`viewKey`) is recorded as a child of its `chainID_i` parent. As long as an image lease holds `viewKey`, any attempt to `Remove(chainID_i)` fails with `ErrFailedPrecondition` (`cannot remove snapshot with child`), guaranteeing that underlying layer snapshots cannot be garbage-collected out from under running actors.

#### Operational Considerations When Running `containerd-gcfs-grpc` Without Containerd CRI:
- **`--enable-v2` Required:** GKE nodes running Substrate must run `containerd-gcfs-grpc` with `--enable-v2` (default on modern GKE COS nodes) so views are mounted by `chainID`, transient import misses use time-based backoff, and `Remove` cleans up snapshot directories inline without needing `Snapshots.Cleanup`.
- **`--enable-image-proxy-keychain-client=false`:** Must remain `false` (its default in `cmd/containerd-gcfs-grpc/main.go:L76`), as enabling it starts the CRI `ImageService` proxy which requires containerd CRI and kubeconfig.
- **Background `containerd.sock` Client & `imageStreamingStatusMap`:** Even when only the `Snapshots` gRPC service is used, `containerd-gcfs-grpc` (`main.go:L130`) initializes a `containerdclient.Client` against `/run/containerd/containerd.sock` (which is present on GKE nodes to run system/worker pods) and records one small status string per unique streamed image in `imageStreamingStatusMap` (`AddImageStreamingStatus`) that is only cleared on containerd `ContainerCreate` events. Because the number of distinct actor images per node is small, this in-memory map entry (~100 bytes per unique image) is benign.

The contract has two additional boundaries:
- **`AlreadyExists` means the layer is on the node, not that it is streamed.** A snapshotter may also provide a layer from local content, such as a GKE secondary boot disk. The driver treats both the same because the content is valid.
- **The contract carries no credentials.** See Section 5.5.3.

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

    // CanStream is a cheap check that the provider is available. It need not
    // inspect the image: PrepareLayers returns ErrNotStreamable if the
    // provider declines the image.
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
    AuthConfig *AuthConfig // Optional credentials for resolving the manifest; never sent to the snapshotter
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
`cmd/atelet` runs without elevated privileges (no `CAP_SYS_ADMIN`), and `ateom` constructs the final overlay mount. To maintain this clean separation, `ImageStreamer` produces a standard layer wrapper directory under `/var/lib/ateom-gvisor/streaming/<driver>` (shared with `ateom` via the `run-ateom` hostPath volume) for each layer in `LayerDirs`:

```
/var/lib/ateom-gvisor/streaming/<driver>/<sanitized_image_key>/
├── lease.json                                # Persisted config, digest, and snapshot view keys for restart recovery
├── layer-0/
│   ├── fs -> /var/lib/containerd/io.containerd.snapshotter.v1.gcfs/snapshotter/snapshots/<id>/fs
│   │         (which for Riptide symlinks to /run/gcfsd/mnt/views/<diffID>/fs)
│   └── finalized                             # Sentinel marker
├── layer-1/
│   ├── fs -> /var/lib/containerd/io.containerd.snapshotter.v1.gcfs/snapshotter/snapshots/<id>/fs
│   └── finalized
└── ...
```

- **`fs` Symlink:** Exposes the virtual layer filesystem tree to `ateom`'s overlay lowerdir. Both `/var/lib/containerd/io.containerd.snapshotter.v1.gcfs` (`RiptideSnapshotterRoot`), `/run/gcfsd` (`RiptideFUSERoot`), and `/var/lib/soci-snapshotter-grpc` (`SOCISnapshotterRoot`) are mounted into `atelet` and `ateom` with `HostToContainer` mount propagation so both pods can resolve the full symlink chain.
- **`finalized` Marker:** Notifies `ateom` that the layer is immutable, instructing it to bypass whiteout materialization loops and mount directly.
- **`lease.json` Metadata:** Records the resolved `v1.Config`, `digest`, `layerDirs`, and snapshotter `-view` keys so `ReconcileLeases` can reconstruct complete lease state across `atelet` restarts and sweep orphaned views.

### 5.2. Pluggable Resolution with Local Cache Fast Path & Automatic Fallback
Both container rootfs images (`containers[*].image`) and mounted OCI image volumes (`volumes[*].image` in `resolveImageVolumes`) resolve through `ensureContainerImage` in `cmd/atelet/oci.go`. The internal sandbox `pause` container (`ocispec.PauseContainer`) passes a `nil` streamer in `prepareOCIBundles` and always uses `imageCache`, avoiding a pointless streaming round-trip for the tiny (~300 KB) digest-pinned infrastructure image.

In `cmd/atelet/oci.go`:
```go
func ensureContainerImage(ctx context.Context, imageCache *imagecache.Store, streamer imagestreaming.ImageStreamer, ...) (*imagecache.Image, error) {
    // 1. Local cache fast path: if a digest-pinned image is already unpacked in
    // the local imageCache, return it immediately with zero network or gRPC I/O.
    if imageCache != nil && streamer != nil {
        if img, err := imageCache.CachedImage(ctx, ref); err == nil && img != nil {
            return img, nil
        }
    }
    // 2. Remote streaming path:
    if streamer != nil {
        if canStream, err := streamer.CanStream(ctx, req); err == nil && canStream {
            if res, err := streamer.PrepareLayers(ctx, req); err == nil && len(res.LayerDirs) > 0 {
                instruments.RecordImageStreaming(ctx, streamer.Name(), "success", dur)
                return &imagecache.Image{Digest: res.ImageDigest, Config: *res.Config, LayerDirs: res.LayerDirs}, nil
            }
        }
        instruments.RecordImageStreaming(ctx, streamer.Name(), outcome, dur)
    }
    // 3. Fallback path: standard local cache download & untar
    return imageCache.EnsureImage(ctx, ref)
}
```

### 5.3. Mount Lifecycle Management, Garbage Collection & Reboot Recovery

Mounts and open file descriptors consume host kernel resources (VFS dentries, mount table slots, file descriptors). In a high-density actor multiplexing environment where actors may remain idle/sleeping for extended periods, releasing mounts eagerly prevents resource exhaustion while preserving rapid wake-up latency.

#### 5.3.1. Reference Counting & Garbage Collection
The generic driver implements reference counting across actors sharing the same base images:
- **Active-Only Leases in Prototype:** Our prototype's `leases` map tracks `refCount` for each image:
  - **PrepareLayers on Run/Wake:** When an actor starts or resumes from sleep, `PrepareLayers` ensures layer mounts are active and increments the reference count (`refCount++`) for both container rootfs images and mounted OCI image volumes.
  - **Concurrent Cold-Start Deduplication:** Concurrent `PrepareLayers` calls for the same cold image reference are coalesced via an in-flight map (`d.inflight`). A single leader goroutine resolves the manifest and prepares the snapshot views, while concurrent callers wait for completion and increment `refCount++` on the shared `imageLease`.
  - **Negative Decline Cache (`DefaultDeclineTTL`):** When the snapshotter declines a layer (`imagestreaming.ErrNotStreamable`), the driver caches the decline in memory for `DefaultDeclineTTL` (10 minutes) so subsequent starts of the same unstreamable image reference immediately skip registry resolution and snapshotter RPCs.
  - **Release on Checkpoint / Sleep / Terminate:** When an actor transitions to Sleep/Paused state via `atelet.Checkpoint`, or when an actor terminates (`atelet.Terminate`), `atelet` releases its image leases (`ReleaseLayers`), decrementing `refCount` for each container image and mounted OCI image volume.
  - **Unmount on Zero RefCount:** Only when `refCount == 0` (no other running actor on the worker references the image) does the driver issue `RemoveSnapshot` on the `-view` keys and unmount the virtual directories from the host.
  - **Self-Healing Lease Verification Against Host `containerd` Proxy-Plugin GC (`layersExist`):** On GKE nodes, host `containerd` is configured with `[proxy_plugins.gcfs]` pointing to `/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock`, and `containerd-gcfs-grpc`'s BoltDB (`containerd/snapshots/storage`, bucket `v1/snapshots`) is shared without `containerd-namespace` partitioning. Whenever host `containerd` runs its snapshot garbage collector (`core/metadata/snapshot.go:garbageCollect`, triggered e.g. after a Kubernetes Pod terminates on the node), `containerd` walks `/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock` and calls `Snapshots.Remove` on snapshot keys not tracked in `containerd`'s own `meta.db`. Before returning a warm or reconciled `imageLease`, `Driver.PrepareLayers` calls `layersExist(lease.layers)` to verify via `os.Lstat` and `os.Stat` that every `layer-i/fs` symlink and its target snapshotter directory still exist on disk. If any layer's symlink target was removed by external GC, `PrepareLayers` logs `"Cached streaming lease layers missing on disk; re-preparing layers"` and transparently re-runs `prepareLayersCold` while preserving the existing lease's `refCount`.
  - **Microsecond Warm Re-attachment:** Benchmarks confirm that warm layer re-attachment takes only **~1.8µs to 2ms** (the snapshotter daemon retains compressed chunks and metadata in local cache). Thus, waking actors incur negligible overhead while host mount tables remain clean.

#### 5.3.2. Parallel Layer Preparation vs. OCI Parent Dependency
- **Can layers be prepared concurrently?**
- **OCI Parent Dependency:** In containerd snapshotters, layer $N$ requires committed layer $N-1$ as its parent in overlayfs (ChainID dependency: $\text{ChainID}_N = \text{SHA256}(\text{ChainID}_{N-1} + \text{" "} + \text{DiffID}_N)$). Therefore, initial snapshot preparation across the layer stack must proceed sequentially from bottom to top.
- **Concurrency Opportunity:** However, tag/manifest resolution, container config fetching, and snapshot `Stat` lookups for pre-existing layers can run concurrently across layers. Furthermore, once layers are prepared and mounted, chunk downloads happen completely concurrently and on-demand across all layers during actor startup as the sandbox accesses files.

#### 5.3.3. Candidate Future Improvements (Deferred Post-GA)
- **Approach 2: Idle TTL / LRU Grace Period:** Rather than unmounting `-view` snapshots immediately upon Checkpoint, hold the lease during a configurable grace window (e.g. 5 minutes). If the actor wakes within the window, layer reuse is instant (zero RPCs). If it stays asleep past TTL, background GC unmounts the views.
- **Approach 3: Watermark-Driven Mount GC:** Retain warm mounts indefinitely across sleeping actors until host pressure thresholds are reached (e.g., active mount count > 100 or memory pressure), triggering LRU eviction of idle mounts.
- **Committed ChainID Snapshot Eviction:** While active `-view` snapshots are removed immediately when `refCount == 0`, the underlying immutable layer snapshots committed under their `ChainID` remain in the snapshotter daemon's metadata store so subsequent actors (or images sharing base layers) hit `Stat(chainID) == OK` in microseconds. Because remote snapshotter `ChainID` snapshots store only lightweight streaming index metadata rather than unpacked layer tarballs, keeping them warm on the node is desirable at GA, with LRU/TTL eviction of committed `ChainID` snapshots deferred to future work.

*Decision:* Approach 1 (Active-Only Leases with strict reference counting and immediate `-view` cleanup) is adopted for the initial implementation for simplicity, determinism, and zero state-machine complexity, with Approaches 2, 3, and committed `ChainID` eviction documented for future optimization as workload density demands.

#### 5.3.4. Node Reboot Resiliency & Startup Recovery
- **Substrate Lifecycle Reality:** Substrate intentionally drains/crashes active actor workloads upon a node reboot (it does not attempt live in-memory VM migration).
- **Startup Recovery in the Driver:** When `atelet` starts up after a reboot or crash, it executes an **Actor-Derived Reconciliation Model** (matching how Kubernetes `kubelet` recovers state and how Substrate's non-streaming image cache GC discovers roots):
  1. `credentialprovider.New(...)` reloads the credential config immediately from the host (`/var/lib/kubelet/credential-provider-config.yaml`).
  2. The streaming daemon (Riptide Snapshotter `/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock` / SOCI Snapshotter `/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock`) is already running as a host service with its fresh metadata access.
  3. `reconcileStreamingLeases(streamer, actorsDir)` (in `cmd/atelet/streaming_reconcile.go`) scans surviving on-node bundle overlay specs (`ateompath.ActorsDir/*/bundles/*/rootfs-overlay.json`).
  4. It parses each bundle's `OverlaySpec.ImageRef`/`Layers` and `ImageVolumes[*].ImageRef`/`Layers` across all resident/running actors, aggregating them into `[]*ActiveLease` entries with exact live reference counts, and calls `streamer.ReconcileLeases(ctx, active)`.
  5. `Driver.ReconcileLeases` reads `lease.json` from each active image's wrapper directory (`/var/lib/ateom-gvisor/streaming/<driver>/<sanitized_image_key>/lease.json`) to restore the full `imageLease` (`refCount`, `digest`, `config`, `layers`, `workDir`, and `snapshotKeys`).
  6. Subsequent container creations or wakeups can immediately reuse warm mounts without redundant snapshotter or registry RPCs, and when all surviving actors for an image later release their leases, `ReleaseLayers` has the full `snapshotKeys` and `workDir` needed for clean teardown.
- **Orphan View Garbage Collection (Sweeper):** During `ReconcileLeases` (which runs on startup even when zero active leases are found), the driver scans `/var/lib/ateom-gvisor/streaming/<driver>/` for any wrapper directory not referenced by an active actor on disk, removes its `-view` snapshot keys from the remote snapshotter daemon (using its `lease.json`), and deletes the wrapper directory.

### 5.4. Cold-Start Reliability & Sandbox Warmup: Host-Side Enumeration Probing & Metadata Prefetch

When streaming container images over FUSE, workloads encounter two distinct cold-node initialization phenomena: an asynchronous directory-index loading race condition, and sandbox traversal latency inside gVisor (`runsc`). To eliminate both without stalling actor startup, the streaming driver integrates a two-stage operational warmup pattern:

```
[atelet / driver.PrepareLayers]
       │
       ├── 1. Synchronous Enumeration Probe (os.ReadDir) ──► Guarantees daemon index is listable (prevents ENOENT race)
       │
       ├── 2. Return StreamResult (Ready in <2.5s) ────────► ateom boots actor sandbox immediately
       │
       └── 3. Background Goroutine (warmLayerMetadata) ────► Host-side WalkDir(lstat) prefetches inode metadata
                                                             (eliminates gVisor Sentry/Gofer latency stalls)
```

#### 1. Synchronous Enumeration Probe: Eliminating the Cold-Start `ENOENT` Race
* **The Failure Mode:** Immediately after the snapshotter creates a FUSE view for a newly pulled layer, lookups for specific known file paths (e.g., `/bin/sh`) succeed instantly, but its internal directory index loads asynchronously. During this brief sub-second window, directory enumerations (`getdents64` / `readdir`) through the FUSE mount may either return empty or fail with `ENOENT`. If an actor starts during this window and scans directories (such as Python inspecting `sys.path` or Node.js module resolution), the application crashes on boot.
* **The Probe Mechanism:** In the driver, immediately after creating the layer view, `probeListable` performs a quick listability probe on the host (`DefaultListableTimeout = 10s`, `DefaultListableInterval = 50ms`):
  ```go
  deadline := time.Now().Add(d.listableTimeout)
  for {
      entries, err := os.ReadDir(mountDir)
      if err == nil && len(entries) > 0 {
          return nil
      }
      if time.Now().After(deadline) {
          return fmt.Errorf("mount directory %s did not become listable within %s (last err: %v)", mountDir, d.listableTimeout, lastErr)
      }
      time.Sleep(d.listableInterval)
  }
  ```
* **Performance Impact:** On a warm node where the image was previously mounted, `os.ReadDir` returns in **<1ms** on the first try. On a completely cold layer, it absorbs the index initialization window on the host, guaranteeing that `ateom` never mounts a half-initialized rootfs.

#### 2. Background Host-Side Metadata Warmup: Eliminating gVisor Sentry/Gofer Stalls
* **The Sandbox Bottleneck:** Substrate runs actors inside gVisor sandboxes. Unlike standard containers where syscalls go directly to the host kernel, filesystem operations inside gVisor traverse the **Sentry (guest kernel) $\rightarrow$ Gofer (9P/virtiofs server) $\rightarrow$ Host VFS $\rightarrow$ FUSE $\rightarrow$ Registry**. If metadata is resolved lazily, every `stat(2)` or `access(2)` during application initialization pays heavy sandbox round-trip penalties while waiting on remote FUSE reads.
* **The Host-Side Solution:** Because the snapshotter's metadata cache operates at the **host node level**, walking the directory tree directly on the host pre-populates the in-memory index for all containers on that machine. Right after `PrepareLayers` verifies each layer mount, the driver triggers an asynchronous background walker:
  ```go
  go warmLayerMetadata(mountDir)
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

### 5.5. Credential Management & Auth Strategy: Two-Layer Decoupled Auth Model

Authentication in the Substrate image streaming runtime is strictly separated into two independent, cloud-agnostic layers: the **Control Plane (Metadata Resolution)** and the **Data Plane (Chunk Streaming)**.

```mermaid
flowchart TD
    subgraph ControlPlane["Control Plane: Metadata Resolution (atelet)"]
        direction TB
        KubeletConfig["/var/lib/kubelet/credential-provider-config.yaml"] -->|Exec Plugin Protocol| CredProvider["cmd/atelet/internal/credentialprovider<br/>(In-Memory Token Cache, Min 1m TTL Floor)"]
        CredProvider -->|authn.Keychain| AteletResolver["cmd/atelet & Driver ImageResolver<br/>(remote.Image / fetchImageConfig)"]
        AteletResolver -->|Fetch Manifest, Config JSON, DiffIDs| Registry[("Container Registry<br/>(GAR / ECR / Docker Hub)")]
    end

    subgraph DataPlane["Data Plane: On-Demand Chunk Streaming (Node Daemon)"]
        direction TB
        NodeIAM["Node Cloud Metadata Service<br/>http://169.254.169.254 (Instance Identity)"] -->|Rotate / Refresh Tokens| StreamingDaemon["Remote Snapshotter Daemon<br/>(Riptide Snapshotter / SOCI Snapshotter)"]
        StreamingDaemon -->|FUSE Chunk HTTP Range Requests| Registry
        StreamingDaemon -->|Direct FUSE Mounts| Ateom["ateom Overlay Manager<br/>(runsc Sandboxes)"]
    end
```

#### 5.5.1. Control Plane: Node Keychain Integration (`cmd/atelet/internal/credentialprovider`)
- `cmd/atelet/internal/credentialprovider` implements `authn.Keychain` by reading `/var/lib/kubelet/credential-provider-config.yaml` and executing the node's local credential binaries over stdio (with an in-memory token cache).
- The `remotesnapshotter` driver accepts `WithKeychain(keychain authn.Keychain)` (also propagated via `WithKeychainContext` and `KeychainFromContext` during `imagestreaming.Get`).
- `atelet` passes its initialized `authn.Keychain` directly into both `remotesnapshotter` and `fetchImageConfig` (as well as `imagecache.Store`).
- **Result:** Resolving manifests, configs, and diffIDs for Artifact Registry, AWS ECR, and Azure ACR uses the exact same node-level credential mechanism without compiling cloud-specific SDKs into `atelet`.

#### 5.5.2. Data Plane: Token Refresh & Expiration
- **Question:** How does credential refreshing and timeout handling work when a workload runs for a long time or survives a restart?
- **How It Actually Works Under the Hood:**
  - The streaming daemon (Riptide Snapshotter `/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock` on GKE, SOCI Snapshotter `/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock` on EKS) runs as a node-level daemon with direct access to the VM instance metadata service.
  - **On GKE (Riptide Snapshotter):** The Riptide Snapshotter fetches Google OAuth2 access tokens directly from `http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token` (or `http://169.254.169.254`). The metadata server automatically rotates tokens before they expire (1-hour validity). The snapshotter handles refreshing internally whenever it performs HTTP Range requests against Artifact Registry.
  - **On AWS (SOCI Snapshotter):** The SOCI snapshotter uses the EC2 instance profile or link-local metadata service, which automatically negotiates and refreshes AWS authorization tokens.
- **Why We Do Not Pass Tokens via Snapshot Labels:** Containerd snapshotters explicitly avoid passing bearer tokens inside `PrepareSnapshotRequest.Labels` because labels are stored persistently in SQLite/bbolt and exposed via `Stat()`/`List()` RPCs, which would leak credentials.

#### 5.5.3. Architectural Decision on `imagePullSecrets` and Fallback Behavior

A fundamental architectural question is how Agent Substrate handles Kubernetes `imagePullSecrets` for private registries in the image streaming path.

##### The Core Architectural Rule:
**Under the snapshotter contract (Section 3.4), workloads that require explicit `imagePullSecrets` never stream, on either Google Riptide or external OSS streaming solutions like AWS SOCI. They always fall back to traditional non-streaming mode (`imagecache.EnsureImage`).**

Image streaming in Substrate is strictly scoped to **ambient Node Identity** (GCE VM Service Account on GKE, EC2 Instance Profile on EKS). This holds at GA and after the next step below; only the future work in Section 5.5.4 would change it.

##### Why `imagePullSecrets` Cannot Be Used with Image Streaming:

1. **CNCF Snapshotter Protocol Invariant:**
   Substrate communicates with remote snapshotters strictly via the standard CNCF `containerd.services.snapshots.v1.Snapshots` gRPC API (`Prepare`, `View`, `Stat`, `Remove`). This protocol contains **no RPC field or metadata header for client authentication credentials**.
2. **Strict Prohibition of Token Injection via Snapshot Labels:**
   Attempting to pass bearer tokens, registry passwords, or authorization headers inside `PrepareSnapshotRequest.Labels` is strictly forbidden:
   - Snapshotter daemons (both Riptide Snapshotter and SOCI Snapshotter) persist snapshot labels unencrypted to disk in their internal metadata stores (SQLite or bbolt).
   - Labels are visible to any node process querying the snapshotter via `Stat()` and `List()` RPCs, creating a severe credential leak and cross-tenant privilege escalation risk.
3. **How Streaming Snapshotters Handle Pull Secrets in Standard Kubernetes vs. Substrate:**
   - *In Standard Kubernetes (CRI Proxy Architecture):* In standard Kubernetes, Kubelet communicates with containerd through the CRI runtime interface (`CRI.PullImage`). When a pod specifies `imagePullSecrets`, Kubelet supplies those credentials in the CRI call. Remote snapshotters (the Riptide Snapshotter, AWS SOCI, eStargz, and Nydus) can deploy an opt-in **CRI proxy service** that sits between Kubelet and containerd. The CRI proxy intercepts `PullImage`, captures the credentials, caches them in memory keyed by image reference, and uses them for on-demand chunk fetches.
   - *In Agent Substrate (Bypassing Kubelet & CRI):* Substrate’s entire performance thesis relies on bypassing Kubelet and containerd CRI (`containerd.sock`) to achieve sub-500ms startup without CRI lock contention or pod lifecycle overhead. Because Substrate talks directly to the Remote Snapshotter over `Snapshots.v1` gRPC, no `PullImage` call passes through the proxy, so it never sees workload credentials. Driving the proxy from Substrate is future work (Section 5.5.4).
4. **Registry Exclusivity for Google Riptide:**
   As noted in Section 3.2, Google Riptide exclusively supports Google Artifact Registry (GAR/GCR). GAR access is authenticated ambiently via the node's GCE VM Service Account through the link-local metadata server (`http://169.254.169.254`), making `imagePullSecrets` unnecessary for GAR images that the node's service account can read.

##### Fallback Behavior for Workloads with `imagePullSecrets`:
When the node's identity can't read an image:
1. The snapshotter can't fetch the image's layers, so it declines each layer's `Prepare` (Section 3.4), and the driver returns `imagestreaming.ErrNotStreamable`.
2. `cmd/atelet` falls back to Substrate's traditional image caching pipeline: `imagecache.EnsureImage`.
3. `imagecache.EnsureImage` authenticates with atelet's node-level keychain (the kubelet credential provider, Section 5.5.1), downloads and verifies the complete layers, untars them into the node's local cache directory, and composes the overlay lowerdir as usual.

Substrate doesn't support per-workload pull secrets yet. Until it does, a private image works only if the node-level keychain can read it.

##### Roadmap and Alternatives:

| Approach | Status | Description |
| :--- | :--- | :--- |
| **Fall back to non-streaming** | GA | Images that the node identity can't read use `imagecache.EnsureImage`, as described above. |
| **Authorize with workload credentials, stream with node identity** | Next step after GA | Add per-workload pull secrets to Substrate. atelet resolves the manifest with the workload's credentials, which proves the workload may read the image, and the snapshotter streams with node identity. If the node identity can't read the image, the snapshotter declines and atelet falls back to `imagecache.EnsureImage` with the workload's credentials. Streamed layers are shared node-wide, so atelet must authorize every `PrepareLayers` call, including calls that reuse an existing lease. Needs nothing beyond the snapshotter contract. |
| **Register credentials through the snapshotter's CRI proxy** | Future work | Would stream images that only the workload can read, but depends on conventions outside the contract. See Section 5.5.4. |
| **Add a credential RPC to each snapshotter** | Rejected | No standard defines one. Every provider would have to add and maintain a Substrate-specific API. |
| **Call a provider's internal credential interface directly** | Rejected | Reaches below the snapshotter's public API, breaks the encapsulation boundary in Section 3.2, and works with only one provider. |
| **Pass credentials in snapshot labels** | Rejected | Leaks credentials (item 2 above). |

#### 5.5.4. Future Work: Per-Workload Pull Secrets via the Snapshotter CRI Credential Proxy

Streaming images that only a workload's `imagePullSecrets` can read is future work. The step after GA (Section 5.5.3) authorizes with workload credentials but still streams with node identity. The candidate mechanism for going further is the CRI credential proxy that several streaming snapshotters already ship. This section records how the proxy works, where it stands relative to the standards Substrate depends on, and what adopting it would require.

##### How the Proxy Works in Standard Kubernetes:
1. Kubelet's `--image-service-endpoint` points at the snapshotter's socket instead of containerd's.
2. The snapshotter serves the CRI `runtime.v1.ImageService` on that socket. On `PullImage`, it caches the request's `AuthConfig` (the Pod's resolved `imagePullSecrets`) in memory, keyed by image reference, and forwards the call to containerd.
3. When containerd then calls `Prepare` for each layer, the snapshotter finds the cached credentials through the `containerd.io/snapshot/cri.image-ref` label and uses them for on-demand chunk fetches.

##### Current State:
- **Not part of the CNCF snapshotter API.** None of the `containerd.services.snapshots.v1.Snapshots` RPCs carries credentials. Labels can't carry them either, because snapshotters store labels on disk and return them from `Stat` and `List`.
- **The wire format is standard; the credential behavior isn't.** The proxy uses Kubernetes CRI `ImageService.PullImage`. Using that call to capture credentials is a convention that started in stargz (`cri_keychain`) and was copied by SOCI, Nydus, and the Riptide Snapshotter.
- **No specification defines** whether the proxy exists, how credentials are keyed, how long they live, or when they're dropped. Each implementation makes the proxy opt-in.
- **The implementations already behave differently.** The Riptide Snapshotter drops credentials on `RemoveImage`. SOCI checks the node's Docker config first and stops at the first non-empty credentials ([SOCI registry authentication](https://github.com/awslabs/soci-snapshotter/blob/main/docs/registry-authentication.md)), so it ignores captured credentials for registries the node already has credentials for.
- **It is the CRI path that Substrate deliberately bypasses** (Section 3.1).

##### Open Questions Before Adoption:
With the proxy, atelet would call `PullImage` with the workload's credentials on the snapshotter socket before preparing layers. Adoption depends on resolving the following:

- **containerd re-enters the critical path.** The proxy forwards `PullImage` to containerd, which records the image in its `k8s.io` namespace and creates its own snapshots alongside the driver's.
- **Kubelet image garbage collection can drop credentials.** Actors are not Pods, so kubelet sees the image as unused and may remove it under disk pressure. On the Riptide Snapshotter, that `RemoveImage` also drops the credentials while actors are still reading from the image.
- **SOCI's forwarded pull stays lazy only if containerd's CRI snapshotter is also `soci`.** Otherwise it becomes a full download. Switching it also changes how Kubernetes Pods on the node pull images.
- **Credentials must stay valid for the actor's lifetime.** Lazy loading fetches data long after startup, for example when an actor wakes after hours idle. Short-lived tokens would need to be registered again on wake and restore.
- **The credential cache is node-wide.** Once one workload registers credentials for an image, any workload on the node that streams that image is served. atelet must authorize each actor with its own credentials before attaching layers.
- **Restart recovery.** The Riptide Snapshotter rebuilds its cache from Pods' `imagePullSecrets`, and SOCI keeps captured credentials only in memory; neither covers actors. atelet would need to register credentials again when it reconciles leases, which means re-obtaining them from the control plane.
- **Image references must match.** The reference passed to `PullImage` must normalize to the same string the driver sends in `containerd.io/snapshot/cri.image-ref`.
- **Registry scope is unchanged.** Riptide still streams only from Google Artifact Registry, so on GKE the proxy helps only with GAR repositories the node's service account can't read, such as cross-project repositories.
- **Node configuration.** We need to verify whether GKE enables the Riptide Snapshotter's proxy on nodes. On EKS, SOCI's CRI credentials must be enabled in the snapshotter config.

Until these are resolved, workloads that need `imagePullSecrets` use the non-streaming path in Section 5.5.3.

### 5.6. Fallback Contract & Error Codes

A clear contract specifies when `atelet` falls back to traditional download mode. Image streaming is an acceleration optimization; it must never become a single point of failure that prevents an actor from booting. When image streaming is enabled (`--image-streamer`), `atelet` adheres to the following contract:

| Error Category | Triggering Condition | Behavior |
| :--- | :--- | :--- |
| **Daemon Unavailable / Hung** | Socket connection refused, ENOENT, `codes.Unavailable`, or `CanStream` liveness probe (`DefaultProbeTimeout = 500ms`) fails | **Fallback:** Daemon is not running, crashed, or unresponsive; `CanStream` returns `false` and `atelet` falls back to `imagecache.EnsureImage`. |
| **Unsupported Image** | The snapshotter declines a layer: `Prepare` returns mounts with a nil error instead of `AlreadyExists` (e.g. the image has no SOCI index or Riptide streaming metadata) | **Fallback:** The driver removes the prepared snapshot without committing it, records the image in the negative decline cache (`DefaultDeclineTTL = 10m`) so subsequent starts skip `CanStream`, and returns `imagestreaming.ErrNotStreamable`; fall back to standard download. |
| **External Registry (Riptide)** | Image is hosted outside Google Artifact Registry (e.g. Docker Hub, Quay) on GKE | **Fallback:** The Riptide Snapshotter declines the layers, as for an unsupported image; fall back to standard download. |
| **`imagePullSecrets` Required** | The image is readable only with a workload's pull secret, not with the node identity | **Fallback:** `Snapshots.v1` can't carry credentials, so the snapshotter declines the layers; fall back to `imagecache.EnsureImage` (Section 5.5.3). |
| **Listable Timeout** | Daemon mounts FUSE, but directory listing fails or times out (`DefaultListableTimeout`) | **Fallback:** Daemon hung or unhealthy; unmount and fall back to standard download. |
| **Control Plane Auth (`atelet`)** | Credential provider returns 401 Unauthorized for metadata resolution | **Terminal Error:** `atelet` tries the fallback, but `imagecache.EnsureImage` uses the same node-level keychain and also fails with 401. |
| **Data Plane Auth (daemon)** | The snapshotter's node identity can't read the image, but `atelet`'s keychain can | **Fallback:** The snapshotter declines the layers; `imagecache.EnsureImage` pulls with `atelet`'s keychain. |
| **Snapshotter Error** | `Stat`, `Prepare`, or `View` fails with any other error (e.g. `codes.PermissionDenied`, or a failed internal commit) | **Fallback:** The driver removes the prepared snapshot, if any, and the image's views; fall back to standard download. |

### 5.7. Provider Selection

Two atelet flags select the provider:

| `--image-streamer` | `--image-streamer-socket` | Behavior |
| :--- | :--- | :--- |
| `none` (default) | Ignored, with a warning if set | Streaming is off. Every image uses `imagecache.EnsureImage`. |
| `auto` (set by `manifests/ate-install/atelet.yaml`) | Ignored, with a warning if set | atelet uses the first socket below that exists as a Unix socket. If none does, streaming is off. |
| A registered provider: `riptide`, `soci`, or `remotesnapshotter` | Required | atelet fails to start unless the path exists and is a Unix socket. |

A named provider is an explicit choice, so a missing socket is a configuration error. `auto` is the zero-configuration path.

`auto` checks these sockets, in order:

1. `/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock`: the Riptide Snapshotter (`riptide`).
2. `/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock`: the SOCI Snapshotter (`soci`).

`auto` matches socket files, not their directories. The atelet DaemonSet mounts both socket directories from the host, so the directories exist on every node, including nodes where no snapshotter runs.

atelet logs one line at startup:

- `Image streaming enabled`, with `mode`, `provider`, `socket`, and `reachable`.
- `Image streaming disabled`, with `mode` and `reason`.

A socket file doesn't prove that the snapshotter is running, because the file can outlive the daemon. To set `reachable`, atelet calls `Stat` once on a key that doesn't exist and expects `NotFound`. The check is informational. If the snapshotter doesn't answer, atelet logs the line at Warn and keeps streaming enabled, and images fall back to `imagecache.EnsureImage` (Section 5.6) until the snapshotter answers.

---

## 6. Provider Driver Implementations

| Provider | Support Status | Host Socket | Protocol | Image Indexing & Registry Scope | Control Plane Auth (atelet) | Data Plane Auth (Streaming Daemon) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`riptide`** (Google Riptide v2) | **GA Supported (`--enable-v2`)** | `/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock` | containerd `SnapshotService` gRPC | Google Cloud Artifact Registry Streaming Manifests (**GAR/GCR only**) | Kubelet Credential Provider plugin (`authn.Keychain`) | VM link-local metadata service (`http://169.254.169.254`) / Node IAM |
| **`soci`** (AWS SOCI) | **Benchmark PoC / Post-GA Production** (currently requires all layers indexed via `--min-layer-size=0`; see Section 9.1) | `/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock` | containerd `SnapshotService` gRPC | OCI Artifact SOCI Index (`application/vnd.amazon.soci.index.v1+json`) | Kubelet Credential Provider plugin (`authn.Keychain`) | VM link-local metadata service (`http://169.254.169.254`) / Node IAM |

---

## 7. Telemetry & Observability

Image streaming instrumentation is codified in the OpenTelemetry Weaver registry (`docs/metrics/registry/metrics.yaml`):

1. **`ate.imagestreaming.requests` (Counter):**
   - Counts the images atelet tries to stream.
   - Labels: `ate.imagestreaming.provider` (`riptide`, `soci`), `ate.imagestreaming.outcome` (`success`, `fallback`, `error`).
2. **`ate.imagestreaming.duration` (Histogram):**
   - Measures how long each attempt takes, in seconds.
   - Labels: `ate.imagestreaming.provider`, `ate.imagestreaming.outcome`.

atelet also logs the outcome for each image, at levels that keep busy nodes quiet:

| Log Message | Level | When |
| :--- | :--- | :--- |
| `Image streamed` | Debug | The image was streamed. |
| `Image streaming unavailable; falling back to cache` | Debug | `CanStream` returned false, for example because the snapshotter doesn't answer. |
| `Image not streamable; falling back to cache` | Info | The snapshotter declined a layer (`imagestreaming.ErrNotStreamable`). |
| Other messages ending in `falling back to cache` | Warn | Any other error, for example a failed `PrepareLayers`. |

Use the `ate.imagestreaming.requests` counter to measure streaming across a node, and `--log-level=debug` to trace individual images. The startup log lines are described in Section 5.7.

---

## 8. Verified Performance Impact

Empirically validated end-to-end on live GKE cluster `substrate-stream-test` against the **exact same 1.19 GB compressed (~3.5 GB unpacked, 9 layers) workload image** — `gcr.io/cloud-builders/gcloud:latest` (Google's official Cloud SDK image, 8 layers) with Substrate's `demos/sandbox` binary added as a tiny (~2 MB) 9th entrypoint layer via `ko` and pushed with a SOCI index (`--min-layer-size=0`) to `us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e` — with `pause:3.10.2` pre-pulled in `image-cache`:

| Operation / Phase | Mode 1: Without Streaming (`none`) | Mode 2: Google Riptide (`riptide`) | Mode 3: AWS SOCI (`soci`) | Speedup (`riptide` vs `none`) | Speedup (`soci` vs `none`) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Cold-Node `resume actor` (`restore.duration.oci_unpack`)** | `26,955.96 ms` | **`1,135.43 ms`** | **`3,468.99 ms`** | **23.7x faster** | **7.8x faster** |
| **Cold-Node `resume actor` (`AteomHerder/Restore` total)** | `28,463.45 ms` | **`1,357.44 ms`** | **`3,696.12 ms`** | **21.0x faster** | **7.7x faster** |
| **Cold-Node `kubectl-ate resume actor` Wall-Clock** | `29,561 ms` | **`2,317 ms`** | **`4,656 ms`** | **12.8x faster** | **6.3x faster** |
| **Warm-Node `resume actor` (`AteomHerder/Restore` total)** | `288.69 ms` (`oci_unpack`: `1.41 ms`) | **`265.25 ms`** (`oci_unpack`: `2.96 ms`) | **`244.53 ms`** (`oci_unpack`: `1.14 ms`) | **No FUSE penalty** | **No FUSE penalty** |
| **Cold `create actor-template` (`AteomHerder/Run` RPC)** | `27,840.00 ms` | **`1,147.23 ms`** | **`4,600.23 ms`** | **24.3x faster** | **6.1x faster** |

---

## 9. Future Work: Full OSS Snapshotter Support & Zero-Code Provider Parameterization

While GA is intentionally scoped to **Google Riptide Snapshotter v2 (`--enable-v2`)**, the `ImageStreamer` abstraction and `remotesnapshotter` driver are designed to evolve post-GA in two dimensions: **(1) protocol extensions to accommodate the behavioral differences of OSS remote snapshotters** (documented in [Section 3.2.1](#321-subtle-behavioral-differences-across-cncf-remote-snapshotter-implementations)), and **(2) declarative configuration parameterization** so new providers require zero Go code changes.

### 9.1. Driver Protocol Extensions for Full Production OSS Snapshotter Support

To graduate OSS snapshotters (AWS SOCI, eStargz, Nydus) from the GA all-or-nothing contract (Section 3.4) to full production parity across arbitrary community images:

1. **Hybrid Local Unpack + `Snapshots.Commit` for Declined Sub-Layers (Required for Default AWS SOCI Images):**
   - **Why needed:** By default, `soci create` only generates zTOCs for layers $\ge$ `--min-layer-size` (default 10 MiB). When `soci-snapshotter-grpc` receives `Prepare` for a smaller unindexed layer (such as a small entrypoint or config layer), it declines remote mounting by returning writable local directory mounts (`Mounts`, `err == nil`) and expects the caller to unpack the layer blob into that directory and call `Snapshots.Commit(Name: chainID, Key: prepKey)` so subsequent upper streamed layers have `parent = chainID` in `metadata.db`.
   - **Post-GA extension:** When `Prepare` returns `codes.OK` (mounts) on layer $i$ of an image that has at least one streamable layer (or a valid SOCI index), instead of immediately aborting the entire image with `ErrNotStreamable`, `remotesnapshotter.Driver` can download and unpack that single small layer blob into the returned mount path (reusing `imagecache`'s layer unpacker with whiteout conversion) and call `client.Commit(ctx, &CommitSnapshotRequest{Snapshotter: snapshotter, Name: c.ChainID, Key: prepKey})`.
2. **Forwarding OCI Layer Descriptor Annotations (Required for Nydus):**
   - **Why needed:** `containerd-nydus-grpc` requires OCI manifest layer annotations (`containerd.io/snapshot/nydus-bootstrap`, `containerd.io/snapshot/nydus-blob`, etc.) in `PrepareSnapshotRequest.Labels` (matching containerd's `snapshots.FilterInheritedLabels`).
   - **Post-GA extension:** Extend `ImageResolverFunc` to return `manifest.Layers[i].Annotations` and copy any annotation prefixed with `containerd.io/snapshot/` into `PrepareSnapshotRequest.Labels`.
3. **Top-Layer-Only `View` Mode (Required for Nydus):**
   - **Why needed:** Unlike Riptide, SOCI, and eStargz (where every layer $i$ materializes a FUSE directory on the host), Nydus treats intermediate layers ($0 \le i < N-1$) as metadata-only records and mounts the complete merged rootfs only when `View` is called on the topmost bootstrap layer (`chainID_{N-1}`).
   - **Post-GA extension:** Add a `TopLayerViewOnly` driver option for Nydus that calls `Prepare` across all layers $0 \dots N-1$ to register layer metadata, but calls `View` and `probeListable` only on `chainID_{N-1}`, returning a single-element `LayerDirs` slice to `ateom`.

### 9.2. Driver Preset & Auto-Discovery (`remotesnapshotter` & `cmd/atelet`)
Currently, `drivers/riptide` and `drivers/soci` register thin constructor presets and `cmd/atelet` maintains `autoStreamerCandidates`. These touchpoints can be collapsed into a single declarative provider table plus flag-driven fallback:
- **Unified Built-in Registry:** Define a single table of well-known presets (`{Name, DefaultSocket, SnapshotterName}`) used by both `--image-streamer=<name>` lookup and `--image-streamer=auto` socket probing.
- **Arbitrary Provider Names via CLI Flags:** Instead of rejecting unknown `--image-streamer=<name>` values, treat any non-built-in name (when paired with `--image-streamer-socket=<path>` and an optional `--image-streamer-snapshotter=<name>` flag that defaults to `<name>`) as a `remotesnapshotter.New(WithName(name), WithSocketPath(socket), WithSnapshotterName(snap))` instance.
- **Configurable Auto-Discovery Candidates:** Allow a comma-separated `--image-streamer-candidates=stargz=/run/containerd-stargz-grpc/address,...` flag to extend `autoStreamerCandidates` at runtime without recompiling `atelet`.

### 9.3. Pod Mount Propagation (`manifests/ate-install/atelet.yaml` & `workerpool.go`)
Currently, `atelet` and `ateom` pods mount provider-specific host directories (`/var/lib/containerd/io.containerd.snapshotter.v1.gcfs`, `/run/gcfsd`, and `/var/lib/soci-snapshotter-grpc`) because `layer-N/fs` is a symlink pointing to the snapshotter daemon's host mount path (`mount.Source`), which both `atelet` and `ateom` must resolve inside their respective mount namespaces. This per-provider manifest wiring can be eliminated in one of three ways:
1. **Option A — Shared Host Snapshotter Parent Directory (Recommended):** Pre-mount a single canonical parent directory on both `atelet` and `ateom` with `HostToContainer` mount propagation (e.g., `/var/lib/ateom-gvisor/snapshotters` for FUSE roots and `/run/ate-snapshotters` for UNIX sockets). Any new OSS snapshotter daemon installed on the node simply sets its `--root` and `--address` under those pre-mounted parent directories (e.g., `--root=/var/lib/ateom-gvisor/snapshotters/stargz`), making its socket and FUSE mounts automatically visible to both `atelet` and `ateom` with zero manifest or `workerpool.go` changes.
2. **Option B — Parameterized `atecontroller` Host Mounts Flag:** Add a `--worker-extra-host-mounts=/var/lib/containerd-stargz-grpc` flag to `atecontroller` so `workerpool.go` dynamically injects `HostToContainer` volume mounts into worker pods at deployment time rather than hardcoding paths in Go.
3. **Option C — Bind-Mounting `mount.Source` onto `layer-N/fs`:** Have `atelet` bind-mount `mount.Source` (with `MS_BIND | MS_REC`) directly onto `/var/lib/ateom-gvisor/streaming/<driver>/<imgKey>/layer-N/fs` (which is already shared with `ateom` via `/var/lib/ateom-gvisor` with `Bidirectional` / `HostToContainer` propagation) instead of creating a symlink. Note the security trade-off: `os.Symlink` works in an unprivileged `atelet` container, whereas `mount(MS_BIND)` requires `CAP_SYS_ADMIN` in `atelet` plus host-root visibility of the daemon's FUSE mount point.

### 9.4. Metric Registry Enum (`docs/metrics/registry/metrics.yaml`)
Currently, `ate.imagestreaming.provider` in `docs/metrics/registry/metrics.yaml` uses a strict Weaver enum (`riptide`, `soci`, `remotesnapshotter`) enforced by `hack/verify/metrics.sh`. To support arbitrary provider names without editing `metrics.yaml`:
- **Option A — Keep Enum & Normalize Custom Providers:** Emit `ate.imagestreaming.provider="remotesnapshotter"` for any custom flag-configured snapshotter not in the built-in preset list, preserving strict compile-time enum validation in OpenTelemetry Weaver.
- **Option B — Relax Type to `string` with Bounded Cardinality Rule:** Change `ate.imagestreaming.provider` in `docs/metrics/registry/metrics.yaml` from an enum to `type: string`, and record in `docs/metrics/substrate.yaml` that its cardinality is $O(1)$ per node (bounded by the operator-configured `--image-streamer` startup flag, never by workload or user input).
