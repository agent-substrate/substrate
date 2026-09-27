# Agent Substrate Image Streaming Performance Evaluation Report

**Authors:** Kui Yue & Antigravity  
**Date:** September 27, 2026  
**Cluster:** `substrate-stream-test` (`us-central1-a`, project `kuiyue-gke-dev`)  
**Node:** `gke-substrate-stream-substrate-node-p-01c8a8ea-sae4` (Container-Optimized OS, containerd v2.1.7)  
**Architecture:** Unified CNCF Remote Snapshotter (`containerd.services.snapshots.v1.Snapshots`)  
**Related Docs:** [Image Streaming API Design One-Pager](image-streaming-api-design.md), [PoC Comparative Analysis](image-streaming-poc-comparison.md), [Architecture](architecture.md)

---

## 1. Executive Summary

This report documents the live-cluster end-to-end performance benchmark of Agent Substrate's image streaming pipeline across all three image delivery modes (`--image-streamer=none`, `--image-streamer=riptide`, and `--image-streamer=soci`) against the **exact same 1.19 GB compressed (~3.5 GB unpacked, 9 layers) workload image** (`us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e`, the `demos/sandbox` agent workload built on `gcr.io/cloud-builders/gcloud:latest`) with `registry.k8s.io/pause:3.10.2` pre-pulled in the node `image-cache`:

- **Cold-Node `resume actor` (`AteomHerder/Restore` RPC — Primary Metric):**
  - **Without Streaming (`none`):** **`28,463.45 ms`** (`oci_unpack`: `26,955.96 ms`; CLI wall-clock: `29,561 ms`)
  - **Google Riptide (`riptide`):** **`1,357.44 ms`** (`oci_unpack`: `1,135.43 ms`; CLI wall-clock: `2,317 ms`) — **21.0x faster** (`-95.2%` restore RPC latency)
  - **AWS SOCI (`soci`):** **`3,696.12 ms`** (`oci_unpack`: `3,468.99 ms`; CLI wall-clock: `4,656 ms`) — **7.7x faster** (`-87.0%` restore RPC latency)
- **Warm-Node `resume actor` (`AteomHerder/Restore` RPC — 2nd Actor on Same Node):**
  - **Without Streaming (`none`):** **`288.69 ms`** (`oci_unpack`: `1.41 ms`; CLI wall-clock: `1,285 ms`)
  - **Google Riptide (`riptide`):** **`265.25 ms`** (`oci_unpack`: `2.96 ms`, active lease hit in `1.03 ms`; CLI wall-clock: `1,352 ms`)
  - **AWS SOCI (`soci`):** **`244.53 ms`** (`oci_unpack`: `1.14 ms`, active lease hit in `0.61 ms`; CLI wall-clock: `1,159 ms`)
  - Because gVisor's application memory pages are restored from the Golden Snapshot (`download` + `ateom_restore`), warm-node restore incurs **zero FUSE demand-paging penalty** while eliminating the 27-second cold-node image unpack.
- **Cold `create actor-template` (`AteomHerder/Run` RPC — Golden Actor Boot):**
  - **Without Streaming (`none`):** **`27,840.00 ms`** (`Image pulled into layer cache`: `27,475.39 ms`)
  - **Google Riptide (`riptide`):** **`1,147.23 ms`** (`Image streamed`: `972.25 ms`) — **24.3x faster**
  - **AWS SOCI (`soci`):** **`4,600.23 ms`** (`Image streamed`: `4,091.70 ms`) — **6.1x faster**

---

## 2. Live-Cluster End-to-End Actor Lifecycle Benchmark

### 2.1 Benchmark Setup & Methodology

- **Cluster:** GKE `substrate-stream-test` (`us-central1-a`), nodes `gke-substrate-stream-substrate-node-p-01c8a8ea-sae4` and `gke-substrate-stream-substrate-node-p-01c8a8ea-65vz`.
- **Workload Image (identical across all 3 modes):**
  - **What It Is:** `gcr.io/cloud-builders/gcloud:latest` (Google's official Cloud SDK image — 8 layers, `1.19 GB` compressed / `~3.5 GB` unpacked) with Substrate's `demos/sandbox` agent binary added as a tiny (`~2 MB`) 9th entrypoint layer via `ko` (which is why `ko` generated the `sandbox-04176181c57f0bf23e61506b0ddcf1fd` repository name).
  - **Why Hosted in GAR with a SOCI Index:** Testing `none`, `riptide`, and `soci` against the exact same image through Substrate's full `actor-template` $\rightarrow$ `resume actor` lifecycle requires (1) hosting in Google Artifact Registry so Riptide's server-side streaming metadata is generated, (2) write access to the repository to push the OCI Referrers SOCI Index (`application/vnd.amazon.soci.index.v1+json`) and 9 layer zTOCs alongside the image manifest, and (3) a long-running server entrypoint (`demos/sandbox`) so the golden actor stays running for checkpointing (`AteomHerder/Checkpoint`) and restoration (`AteomHerder/Restore`).
  - **URI:** `us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e`
  - **SOCI Index Artifact (pushed to Google Artifact Registry):** `sha256:b1fd22e44bb07ee1623c42e8321414ffd460221e97ac7a5d6a5dc75df951bb8a` (with 9 layer zTOCs)
- **Pause Image:** `registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4` — pre-pulled in `/var/lib/ateom-gvisor/image-cache` on all nodes (`"Image cache hit"` verified on every run; streaming is skipped for `pause` and local cache is checked prior to calling the remote snapshotter).
- **Worker Placement:** Across all 3 modes, all benchmarked operations ran on the **exact same GKE node** (`gke-substrate-stream-substrate-node-p-01c8a8ea-sae4`) and the **exact same worker pods**:
  - Cold-Node `resume actor` (`bench-<mode>-1`): `sandbox-workerpool-66b96975f8-gs577`
  - Warm-Node `resume actor` (`bench-<mode>-2`): `sandbox-workerpool-66b96975f8-25qvw`
- **Cache Reset Between Cold Runs:** Before both `create actor-template` and Cold-Node `resume actor`, `/var/lib/ateom-gvisor/image-cache` (excluding `pause:3.10.2`), `/var/lib/ateom-gvisor/streaming/*`, and stale actor bundles were removed on all nodes, and for SOCI mode `soci-snapshotter-grpc` (`v0.15.0`) was restarted with an empty `/var/lib/soci-snapshotter-grpc` directory.

---

### 2.2 Primary Metric: Cold-Node `resume actor` (`AteomHerder/Restore`)

When an actor is resumed onto a node that does not yet have the workload image cached, `AteomHerder/Restore` runs `oci_unpack` (image pull/unpack or streaming mount) **in parallel** with `download` (fetching the Golden Snapshot memory/state from GCS), followed by `ateom_restore` (`runsc restore`).

| Metric / Restore Phase | Mode 1: Without Streaming (`none`) | Mode 2: Google Riptide (`riptide`) | Mode 3: AWS SOCI (`soci`) | Speedup (`riptide` vs `none`) | Speedup (`soci` vs `none`) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Image Delivery (`Image pulled` / `Image streamed`)** | `26,955.40 ms` | **`1,134.54 ms`** | **`3,468.60 ms`** | **23.8x faster** | **7.8x faster** |
| `restore.duration.oci_unpack` | `26,955.96 ms` | **`1,135.43 ms`** | **`3,468.99 ms`** | **23.7x faster** | **7.8x faster** |
| `restore.duration.download` *(parallel GCS fetch)* | `53.93 ms` | `42.00 ms` | `56.17 ms` | — | — |
| `restore.duration.manifest_fetch` | `38.16 ms` | `48.26 ms` | `33.38 ms` | — | — |
| `restore.duration.ateom_restore` *(gVisor restore)* | `381.42 ms` | `149.29 ms` | `170.55 ms` | **2.6x faster** | **2.2x faster** |
| **`restore.duration.total` (`AteomHerder/Restore` RPC)** | **`28,463.45 ms`** | **`1,357.44 ms`** | **`3,696.12 ms`** | **21.0x faster** | **7.7x faster** |
| **End-to-End `kubectl-ate resume actor` Wall-Clock** | **`29,561 ms`** | **`2,317 ms`** | **`4,656 ms`** | **12.8x faster** | **6.3x faster** |

---

### 2.3 Secondary Metric: Warm-Node `resume actor` (2nd Actor on Same Node)

Once the first actor is running on the node, the second actor (`bench-<mode>-2` on worker pod `sandbox-workerpool-66b96975f8-25qvw`) reuses the node's unpacked layer cache (`none`) or active streaming lease (`riptide` / `soci`).

| Metric / Restore Phase | Mode 1: Without Streaming (`none`) | Mode 2: Google Riptide (`riptide`) | Mode 3: AWS SOCI (`soci`) |
| :--- | ---: | ---: | ---: |
| **Image Delivery (Cache Hit / Lease Hit)** | `Image cache hit` | `1.03 ms` (`Image streamed`) | `0.61 ms` (`Image streamed`) |
| `restore.duration.oci_unpack` | `1.41 ms` | `2.96 ms` | `1.14 ms` |
| `restore.duration.download` *(GCS snapshot fetch)* | `112.72 ms` | `45.92 ms` | `39.19 ms` |
| `restore.duration.manifest_fetch` | `47.14 ms` | `26.26 ms` | `33.78 ms` |
| `restore.duration.ateom_restore` *(gVisor restore)* | `125.02 ms` | `168.96 ms` | `148.05 ms` |
| **`restore.duration.total` (`AteomHerder/Restore` RPC)** | **`288.69 ms`** | **`265.25 ms`** | **`244.53 ms`** |
| **End-to-End `kubectl-ate resume actor` Wall-Clock** | **`1,285 ms`** | **`1,352 ms`** | **`1,159 ms`** |

---

### 2.4 Setup Metric: Cold `create actor-template` (Golden Snapshot Creation)

During `create actor-template`, Substrate boots a temporary golden actor (`AteomHerder/Run`) on a cold node, waits for it to initialize, checkpoints its memory/state to GCS (`AteomHerder/Checkpoint`), and records the `goldenSnapshot` URI on the template.

| Metric / Phase | Mode 1: Without Streaming (`none`) | Mode 2: Google Riptide (`riptide`) | Mode 3: AWS SOCI (`soci`) | Speedup (`riptide` vs `none`) | Speedup (`soci` vs `none`) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`pause:3.10.2` Lookup** | `Image cache hit` | `Image cache hit` | `Image cache hit` | — | — |
| **Image Delivery (`Image pulled` / `Image streamed`)** | `27,475.39 ms` | **`972.25 ms`** | **`4,091.70 ms`** | **28.3x faster** | **6.7x faster** |
| **`AteomHerder/Run` RPC (Golden Actor Boot)** | **`27,840.00 ms`** | **`1,147.23 ms`** | **`4,600.23 ms`** | **24.3x faster** | **6.1x faster** |
| **`AteomHerder/Checkpoint` RPC** | `425.54 ms` | `628.09 ms` | `487.32 ms` | — | — |
| **End-to-End Golden Snapshot Ready Wall-Clock** | `51,500 ms` | **`28,393 ms`** | **`40,633 ms`** | **1.8x faster** | **1.3x faster** |

---

## 3. Concrete Logs, CLI Outputs, and Filesystem Evidence

### 3.1 Mode 1: Without Streaming (`--image-streamer=none`)

#### CLI Execution Output
```text
=== Step 3: Cold create actor-template (sandbox-1gb-none) + wait for Golden Snapshot ===
ATESPACE           NAME               SANDBOX CLASS          GOLDEN SNAPSHOT   ERROR   AGE
ate-demo-sandbox   sandbox-1gb-none   SANDBOX_CLASS_GVISOR                             0s
>>> Golden Snapshot Ready: gs://snapshot-substrate-test-kuiyue-gke-dev/ate-demo-sandbox-1gb/atespaces/ate-golden/actors/1ce51017-5df6-4141-8dfc-30e2afb745e8/snapshots/88c19d6d-dad6-4428-a8da-0436e21832c3
>>> Cold create actor-template (end-to-end golden snapshot) wall-clock: 51500 ms

=== Step 5: Resuming 2 actors sequentially (Actor 1 = Cold Node, Actor 2 = Warm Node) ===
--- Creating and resuming bench-none-1 ---
ATESPACE           NAME           TEMPLATE                            STATE                   WORKER POD   WORKER IP   VERSION   AGE
ate-demo-sandbox   bench-none-1   ate-demo-sandbox/sandbox-1gb-none   ACTOR_STATE_SUSPENDED   <none>                   1         0s
ATESPACE           NAME           TEMPLATE                            STATE                 WORKER POD                                             WORKER IP    VERSION   AGE
ate-demo-sandbox   bench-none-1   ate-demo-sandbox/sandbox-1gb-none   ACTOR_STATE_RUNNING   ate-demo-sandbox/sandbox-workerpool-66b96975f8-gs577   10.96.2.22   3         29s
>>> bench-none-1 resume wall-clock: 29561 ms
--- Creating and resuming bench-none-2 ---
ATESPACE           NAME           TEMPLATE                            STATE                   WORKER POD   WORKER IP   VERSION   AGE
ate-demo-sandbox   bench-none-2   ate-demo-sandbox/sandbox-1gb-none   ACTOR_STATE_SUSPENDED   <none>                   1         0s
ATESPACE           NAME           TEMPLATE                            STATE                 WORKER POD                                             WORKER IP    VERSION   AGE
ate-demo-sandbox   bench-none-2   ate-demo-sandbox/sandbox-1gb-none   ACTOR_STATE_RUNNING   ate-demo-sandbox/sandbox-workerpool-66b96975f8-25qvw   10.96.2.21   3         1s
>>> bench-none-2 resume wall-clock: 1285 ms
```

#### `atelet` Structured Logs (`pod/atelet-v612b1bb0bb-dllzk` on `gke-substrate-stream-substrate-node-p-01c8a8ea-sae4`)
```json
{"time":"2026-09-27T01:08:39.235294736Z","level":"INFO","msg":"Image streaming disabled","mode":"none","reason":"disabled by flag"}
{"time":"2026-09-27T01:09:00.858964564Z","level":"INFO","msg":"Image cache hit","ref":"registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","digest":"sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","trace_id":"06e6cdb4ec7a7d6c06e268fadf96dc2e","span_id":"de0fbab598289741","trace_flags":"01"}
{"time":"2026-09-27T01:09:28.334505776Z","level":"INFO","msg":"Image pulled into layer cache","digest":"sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","layers":9,"took":27475390414,"trace_id":"06e6cdb4ec7a7d6c06e268fadf96dc2e","span_id":"d19352c3bda3d8f2","trace_flags":"01"}
{"time":"2026-09-27T01:09:28.675869809Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Run","req":{"target_ateom_uid":"b4c8279d-b247-4dad-a82a-238f7d467e97","atespace":"ate-golden","actor_name":"584bbbbb-94cd-46c9-b930-7400e62aa992","actor_uid":"1ce51017-5df6-4141-8dfc-30e2afb745e8","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-none","spec":{"containers":[{"name":"sandbox","image":"us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e"}]}},"resp":{},"err":null,"elapsed-time":"27.840004027s","trace_id":"06e6cdb4ec7a7d6c06e268fadf96dc2e","span_id":"c7ecd11e3e349fc3","trace_flags":"01"}
{"time":"2026-09-27T01:09:49.11167833Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Checkpoint","req":{"target_ateom_uid":"b4c8279d-b247-4dad-a82a-238f7d467e97","atespace":"ate-golden","actor_name":"584bbbbb-94cd-46c9-b930-7400e62aa992","actor_uid":"1ce51017-5df6-4141-8dfc-30e2afb745e8","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-none"},"resp":{},"err":null,"elapsed-time":"425.536574ms","trace_id":"224c9b256e2df47cdab28d7a2669e7ef","span_id":"283ccaad01e2b0cd","trace_flags":"00"}
{"time":"2026-09-27T01:10:00.932571302Z","level":"INFO","msg":"Image cache hit","ref":"registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","digest":"sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","trace_id":"3ae355ae7b8f77c1efa43e699997f1ee","span_id":"156bfdd070b810e1","trace_flags":"00"}
{"time":"2026-09-27T01:10:27.887773277Z","level":"INFO","msg":"Image pulled into layer cache","digest":"sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","layers":9,"took":26955404062,"trace_id":"3ae355ae7b8f77c1efa43e699997f1ee","span_id":"86d30b5cc8e95183","trace_flags":"00"}
{"time":"2026-09-27T01:10:29.357111804Z","level":"INFO","msg":"Restore timing breakdown","ate.atespace":"ate-demo-sandbox","ate.actor.name":"bench-none-1","ate.actor.uid":"2c97a01c-1613-43e9-8efb-2f426a51863b","ate.template.atespace":"ate-demo-sandbox","ate.template.name":"sandbox-1gb-none","ate.snapshot.scope":"full","ate.snapshot.kind":"golden","ate.sandbox.class":"gvisor","ate.actor.restore.duration.volume_mount":4.42e-7,"ate.actor.restore.duration.manifest_fetch":0.038161706,"ate.actor.restore.duration.sandbox_assets":0.000080687,"ate.actor.restore.duration.download":0.053925363,"ate.actor.restore.duration.oci_unpack":26.955961467,"ate.actor.restore.duration.ateom_restore":0.381418644,"ate.actor.restore.duration.total":28.463454846,"trace_id":"3ae355ae7b8f77c1efa43e699997f1ee","span_id":"18fd680682bc6d12","trace_flags":"00"}
{"time":"2026-09-27T01:10:29.357239636Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Restore","req":{"target_ateom_uid":"b4c8279d-b247-4dad-a82a-238f7d467e97","atespace":"ate-demo-sandbox","actor_name":"bench-none-1","actor_uid":"2c97a01c-1613-43e9-8efb-2f426a51863b","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-none"},"resp":{},"err":null,"elapsed-time":"28.463696061s","trace_id":"3ae355ae7b8f77c1efa43e699997f1ee","span_id":"18fd680682bc6d12","trace_flags":"00"}
{"time":"2026-09-27T01:10:31.552781516Z","level":"INFO","msg":"Image cache hit","ref":"us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","digest":"sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","trace_id":"f7697b9bd00ebf3265e06644ed7f2d40","span_id":"5a52aea1b4e04d5f","trace_flags":"00"}
{"time":"2026-09-27T01:10:31.552875836Z","level":"INFO","msg":"Image cache hit","ref":"registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","digest":"sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","trace_id":"f7697b9bd00ebf3265e06644ed7f2d40","span_id":"38fbce2ac3a2375c","trace_flags":"00"}
{"time":"2026-09-27T01:10:31.792515847Z","level":"INFO","msg":"Restore timing breakdown","ate.atespace":"ate-demo-sandbox","ate.actor.name":"bench-none-2","ate.actor.uid":"590f22a2-30c4-43da-a963-3bdd4ba70d06","ate.template.atespace":"ate-demo-sandbox","ate.template.name":"sandbox-1gb-none","ate.snapshot.scope":"full","ate.snapshot.kind":"golden","ate.sandbox.class":"gvisor","ate.actor.restore.duration.volume_mount":5.48e-7,"ate.actor.restore.duration.manifest_fetch":0.047139784,"ate.actor.restore.duration.sandbox_assets":0.000086706,"ate.actor.restore.duration.download":0.112718288,"ate.actor.restore.duration.oci_unpack":0.001410794,"ate.actor.restore.duration.ateom_restore":0.125020842,"ate.actor.restore.duration.total":0.288689081,"trace_id":"f7697b9bd00ebf3265e06644ed7f2d40","span_id":"f9239b9d68b690d5","trace_flags":"00"}
{"time":"2026-09-27T01:10:31.792603809Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Restore","req":{"target_ateom_uid":"62e4b806-23be-4df5-8044-15be6f169ff2","atespace":"ate-demo-sandbox","actor_name":"bench-none-2","actor_uid":"590f22a2-30c4-43da-a963-3bdd4ba70d06","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-none"},"resp":{},"err":null,"elapsed-time":"288.869258ms","trace_id":"f7697b9bd00ebf3265e06644ed7f2d40","span_id":"f9239b9d68b690d5","trace_flags":"00"}
```

#### Filesystem Evidence (`none`)
- **Streaming lease (`lease.json`):** None (`(no lease.json)`).
- **`rootfs-overlay.json` (`/var/lib/ateom-gvisor/actors/2c97a01c-1613-43e9-8efb-2f426a51863b/bundles/sandbox/rootfs-overlay.json`):** Points to unpacked layer directories in `/var/lib/ateom-gvisor/image-cache/layers/sha256/`:
```json
{
  "version": 1,
  "imageDigest": "sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e",
  "imageRef": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e",
  "layers": [
    "/var/lib/ateom-gvisor/image-cache/layers/sha256/ea16cace89338c84eb6bcb91a7efdfcae6838fff359efe951858227436486c34",
    "/var/lib/ateom-gvisor/image-cache/layers/sha256/fd14681b3f78357c6ffbcef2d304d2323c29797bde46a32f3037701f7a431fd6",
    "/var/lib/ateom-gvisor/image-cache/layers/sha256/17e3008119c76df2e3a50da6fe465488993955150901315b1f09d9fded346507",
    "/var/lib/ateom-gvisor/image-cache/layers/sha256/8512b779ac15768e46d1c70d0c7d0031eae14a47cd0c3ce7588d843419cee4d1",
    "/var/lib/ateom-gvisor/image-cache/layers/sha256/f5f3409de2bcb398e84055464ed1a8b701f7e964f019e9c5d679002e6a0bea93",
    "/var/lib/ateom-gvisor/image-cache/layers/sha256/fc77ab78e128550fbf89806ef258df94a58ff8a9cdcb5c66241c7815bf630de0",
    "/var/lib/ateom-gvisor/image-cache/layers/sha256/584c997258045503a652a2773234cd3f9c48e320ebbeb037b7a703d8a684d2b8",
    "/var/lib/ateom-gvisor/image-cache/layers/sha256/ffe56a1c5f3878e9b5f803842adb9e2ce81584b6bd027e8599582aefe14a975b",
    "/var/lib/ateom-gvisor/image-cache/layers/sha256/b80be4baa9e9d46a68c44421143a8241fc7f7e03178118805f002b931332caa1"
  ]
}
```

---

### 3.2 Mode 2: Google Riptide (`--image-streamer=riptide`)

#### CLI Execution Output
```text
=== Step 3: Cold create actor-template (sandbox-1gb-riptide) + wait for Golden Snapshot ===
ATESPACE           NAME                  SANDBOX CLASS          GOLDEN SNAPSHOT   ERROR   AGE
ate-demo-sandbox   sandbox-1gb-riptide   SANDBOX_CLASS_GVISOR                             0s
>>> Golden Snapshot Ready: gs://snapshot-substrate-test-kuiyue-gke-dev/ate-demo-sandbox-1gb/atespaces/ate-golden/actors/97de4e6e-8ce1-4287-bf4a-d93b60112a50/snapshots/e99fb725-bf7f-4bb5-be38-5d1a38ed895a
>>> Cold create actor-template (end-to-end golden snapshot) wall-clock: 28393 ms

=== Step 5: Resuming 2 actors sequentially (Actor 1 = Cold Node, Actor 2 = Warm Node) ===
--- Creating and resuming bench-riptide-1 ---
ATESPACE           NAME              TEMPLATE                               STATE                   WORKER POD   WORKER IP   VERSION   AGE
ate-demo-sandbox   bench-riptide-1   ate-demo-sandbox/sandbox-1gb-riptide   ACTOR_STATE_SUSPENDED   <none>                   1         0s
ATESPACE           NAME              TEMPLATE                               STATE                 WORKER POD                                             WORKER IP    VERSION   AGE
ate-demo-sandbox   bench-riptide-1   ate-demo-sandbox/sandbox-1gb-riptide   ACTOR_STATE_RUNNING   ate-demo-sandbox/sandbox-workerpool-66b96975f8-gs577   10.96.2.22   3         2s
>>> bench-riptide-1 resume wall-clock: 2317 ms
--- Creating and resuming bench-riptide-2 ---
ATESPACE           NAME              TEMPLATE                               STATE                   WORKER POD   WORKER IP   VERSION   AGE
ate-demo-sandbox   bench-riptide-2   ate-demo-sandbox/sandbox-1gb-riptide   ACTOR_STATE_SUSPENDED   <none>                   1         0s
ATESPACE           NAME              TEMPLATE                               STATE                 WORKER POD                                             WORKER IP    VERSION   AGE
ate-demo-sandbox   bench-riptide-2   ate-demo-sandbox/sandbox-1gb-riptide   ACTOR_STATE_RUNNING   ate-demo-sandbox/sandbox-workerpool-66b96975f8-25qvw   10.96.2.21   3         1s
>>> bench-riptide-2 resume wall-clock: 1352 ms
```

#### `atelet` Structured Logs (`pod/atelet-v612b1bb0bb-q67s9` on `gke-substrate-stream-substrate-node-p-01c8a8ea-sae4`)
```json
{"time":"2026-09-27T01:01:04.265460328Z","level":"INFO","msg":"Image streaming enabled","streamer":"riptide","mode":"riptide","socket":"/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock"}
{"time":"2026-09-27T01:01:36.224497704Z","level":"INFO","msg":"Image cache hit","ref":"registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","digest":"sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","trace_id":"681534a54bf2bc11d8ff930552ed0108","span_id":"775f7848e8cf0fb5","trace_flags":"00"}
{"time":"2026-09-27T01:01:37.196759712Z","level":"INFO","msg":"Image streamed","streamer":"riptide","ref":"us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","digest":"sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","layers":9,"duration":972246059,"trace_id":"681534a54bf2bc11d8ff930552ed0108","span_id":"b3248e582fcfa650","trace_flags":"00"}
{"time":"2026-09-27T01:01:37.371284162Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Run","req":{"target_ateom_uid":"b4c8279d-b247-4dad-a82a-238f7d467e97","atespace":"ate-golden","actor_name":"62b4bbdf-4583-45ef-a10d-6bd85823b813","actor_uid":"97de4e6e-8ce1-4287-bf4a-d93b60112a50","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-riptide"},"resp":{},"err":null,"elapsed-time":"1.147226764s","trace_id":"681534a54bf2bc11d8ff930552ed0108","span_id":"333716f3335a6868","trace_flags":"00"}
{"time":"2026-09-27T01:01:57.991731538Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Checkpoint","req":{"target_ateom_uid":"b4c8279d-b247-4dad-a82a-238f7d467e97","atespace":"ate-golden","actor_name":"62b4bbdf-4583-45ef-a10d-6bd85823b813","actor_uid":"97de4e6e-8ce1-4287-bf4a-d93b60112a50","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-riptide"},"resp":{},"err":null,"elapsed-time":"628.08993ms","trace_id":"4e77416f85c4f1d311bf1ab27d7d9685","span_id":"b880227494f2d144","trace_flags":"00"}
{"time":"2026-09-27T01:02:11.615292588Z","level":"INFO","msg":"Image cache hit","ref":"registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","digest":"sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","trace_id":"848ff622253c7cf41173d4c490fd8d28","span_id":"183df2213eb9e4de","trace_flags":"00"}
{"time":"2026-09-27T01:02:12.749852002Z","level":"INFO","msg":"Image streamed","streamer":"riptide","ref":"us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","digest":"sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","layers":9,"duration":1134543467,"trace_id":"848ff622253c7cf41173d4c490fd8d28","span_id":"1cb5fb217f789d4e","trace_flags":"00"}
{"time":"2026-09-27T01:02:12.971808239Z","level":"INFO","msg":"Restore timing breakdown","ate.atespace":"ate-demo-sandbox","ate.actor.name":"bench-riptide-1","ate.actor.uid":"ef9e7552-df2a-4c36-9f69-db721c6a58e1","ate.template.atespace":"ate-demo-sandbox","ate.template.name":"sandbox-1gb-riptide","ate.snapshot.scope":"full","ate.snapshot.kind":"golden","ate.sandbox.class":"gvisor","ate.actor.restore.duration.volume_mount":2.6e-7,"ate.actor.restore.duration.manifest_fetch":0.048256004,"ate.actor.restore.duration.sandbox_assets":0.000105396,"ate.actor.restore.duration.download":0.041998572,"ate.actor.restore.duration.oci_unpack":1.135426412,"ate.actor.restore.duration.ateom_restore":0.149287855,"ate.actor.restore.duration.total":1.357443492,"trace_id":"848ff622253c7cf41173d4c490fd8d28","span_id":"6ee80f7d76731f92","trace_flags":"00"}
{"time":"2026-09-27T01:02:12.971934696Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Restore","req":{"target_ateom_uid":"b4c8279d-b247-4dad-a82a-238f7d467e97","atespace":"ate-demo-sandbox","actor_name":"bench-riptide-1","actor_uid":"ef9e7552-df2a-4c36-9f69-db721c6a58e1","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-riptide"},"resp":{},"err":null,"elapsed-time":"1.357690005s","trace_id":"848ff622253c7cf41173d4c490fd8d28","span_id":"6ee80f7d76731f92","trace_flags":"00"}
{"time":"2026-09-27T01:02:14.234538589Z","level":"INFO","msg":"Image streamed","streamer":"riptide","ref":"us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","digest":"sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","layers":9,"duration":1032467,"trace_id":"f6d52616cd993da60bd8b0cf3532ff40","span_id":"ec4e9fc1528b92bd","trace_flags":"00"}
{"time":"2026-09-27T01:02:14.235610873Z","level":"INFO","msg":"Image cache hit","ref":"registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","digest":"sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","trace_id":"f6d52616cd993da60bd8b0cf3532ff40","span_id":"f9a44eb54e2bd35a","trace_flags":"00"}
{"time":"2026-09-27T01:02:14.477394115Z","level":"INFO","msg":"Restore timing breakdown","ate.atespace":"ate-demo-sandbox","ate.actor.name":"bench-riptide-2","ate.actor.uid":"55cdff25-075d-4ac8-a896-a4d2c7e0976f","ate.template.atespace":"ate-demo-sandbox","ate.template.name":"sandbox-1gb-riptide","ate.snapshot.scope":"full","ate.snapshot.kind":"golden","ate.sandbox.class":"gvisor","ate.actor.restore.duration.volume_mount":3.9e-7,"ate.actor.restore.duration.manifest_fetch":0.026262316,"ate.actor.restore.duration.sandbox_assets":0.000105396,"ate.actor.restore.duration.download":0.045920229,"ate.actor.restore.duration.oci_unpack":0.002957582,"ate.actor.restore.duration.ateom_restore":0.168961788,"ate.actor.restore.duration.total":0.26525237,"trace_id":"f6d52616cd993da60bd8b0cf3532ff40","span_id":"4ec4fbbf988565d9","trace_flags":"00"}
{"time":"2026-09-27T01:02:14.477514864Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Restore","req":{"target_ateom_uid":"62e4b806-23be-4df5-8044-15be6f169ff2","atespace":"ate-demo-sandbox","actor_name":"bench-riptide-2","actor_uid":"55cdff25-075d-4ac8-a896-a4d2c7e0976f","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-riptide"},"resp":{},"err":null,"elapsed-time":"265.489242ms","trace_id":"f6d52616cd993da60bd8b0cf3532ff40","span_id":"4ec4fbbf988565d9","trace_flags":"00"}
```

#### Filesystem Evidence (`riptide`)
- **Streaming lease (`/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/lease.json`):**
```json
{
  "streamer": "riptide",
  "imageRef": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e",
  "imageDigest": "sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e",
  "layerDirs": [
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-0",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-1",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-2",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-3",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-4",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-5",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-6",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-7",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-8"
  ],
  "snapshotKeys": [
    "ate-view-ea16cace8933-e9fd18be3c946134",
    "ate-view-fd14681b3f78-e9fd18be3c946134",
    "ate-view-17e3008119c7-e9fd18be3c946134",
    "ate-view-8512b779ac15-e9fd18be3c946134",
    "ate-view-f5f3409de2bc-e9fd18be3c946134",
    "ate-view-fc77ab78e128-e9fd18be3c946134",
    "ate-view-584c99725804-e9fd18be3c946134",
    "ate-view-ffe56a1c5f38-e9fd18be3c946134",
    "ate-view-b80be4baa9e9-e9fd18be3c946134"
  ]
}
```
- **`rootfs-overlay.json` (`/var/lib/ateom-gvisor/actors/ef9e7552-df2a-4c36-9f69-db721c6a58e1/bundles/sandbox/rootfs-overlay.json`):**
```json
{
  "version": 1,
  "imageDigest": "sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e",
  "imageRef": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e",
  "layers": [
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-0",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-1",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-2",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-3",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-4",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-5",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-6",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-7",
    "/var/lib/ateom-gvisor/streaming/riptide/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-8"
  ]
}
```

---

### 3.3 Mode 3: AWS SOCI (`--image-streamer=soci`)

#### CLI Execution Output
```text
=== Step 3: Cold create actor-template (sandbox-1gb-soci) + wait for Golden Snapshot ===
ATESPACE           NAME               SANDBOX CLASS          GOLDEN SNAPSHOT   ERROR   AGE
ate-demo-sandbox   sandbox-1gb-soci   SANDBOX_CLASS_GVISOR                             0s
>>> Golden Snapshot Ready: gs://snapshot-substrate-test-kuiyue-gke-dev/ate-demo-sandbox-1gb/atespaces/ate-golden/actors/72909359-160a-4bd5-acdb-ff0cf0de7e38/snapshots/e1c7ef2c-e359-4da8-b956-fa3f8024e91a
>>> Cold create actor-template (end-to-end golden snapshot) wall-clock: 40633 ms

=== Step 5: Resuming 2 actors sequentially (Actor 1 = Cold Node, Actor 2 = Warm Node) ===
--- Creating and resuming bench-soci-1 ---
ATESPACE           NAME           TEMPLATE                            STATE                   WORKER POD   WORKER IP   VERSION   AGE
ate-demo-sandbox   bench-soci-1   ate-demo-sandbox/sandbox-1gb-soci   ACTOR_STATE_SUSPENDED   <none>                   1         0s
ATESPACE           NAME           TEMPLATE                            STATE                 WORKER POD                                             WORKER IP    VERSION   AGE
ate-demo-sandbox   bench-soci-1   ate-demo-sandbox/sandbox-1gb-soci   ACTOR_STATE_RUNNING   ate-demo-sandbox/sandbox-workerpool-66b96975f8-gs577   10.96.2.22   3         4s
>>> bench-soci-1 resume wall-clock: 4656 ms
--- Creating and resuming bench-soci-2 ---
ATESPACE           NAME           TEMPLATE                            STATE                   WORKER POD   WORKER IP   VERSION   AGE
ate-demo-sandbox   bench-soci-2   ate-demo-sandbox/sandbox-1gb-soci   ACTOR_STATE_SUSPENDED   <none>                   1         0s
ATESPACE           NAME           TEMPLATE                            STATE                 WORKER POD                                             WORKER IP    VERSION   AGE
ate-demo-sandbox   bench-soci-2   ate-demo-sandbox/sandbox-1gb-soci   ACTOR_STATE_RUNNING   ate-demo-sandbox/sandbox-workerpool-66b96975f8-25qvw   10.96.2.21   3         1s
>>> bench-soci-2 resume wall-clock: 1159 ms
```

#### `atelet` Structured Logs (`pod/atelet-v612b1bb0bb-59mvs` on `gke-substrate-stream-substrate-node-p-01c8a8ea-sae4`)
```json
{"time":"2026-09-27T01:05:31.59445249Z","level":"INFO","msg":"Image streaming enabled","streamer":"soci","mode":"soci","socket":"/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock"}
{"time":"2026-09-27T01:05:49.09283784Z","level":"INFO","msg":"Image cache hit","ref":"registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","digest":"sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","trace_id":"6fb911245b9d50dc34bb32bc5fa5e9e7","span_id":"3365e79893d79d23","trace_flags":"01"}
{"time":"2026-09-27T01:05:53.184560807Z","level":"INFO","msg":"Image streamed","streamer":"soci","ref":"us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","digest":"sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","layers":9,"duration":4091703256,"trace_id":"6fb911245b9d50dc34bb32bc5fa5e9e7","span_id":"78cb6435e1885046","trace_flags":"01"}
{"time":"2026-09-27T01:05:53.692440587Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Run","req":{"target_ateom_uid":"b4c8279d-b247-4dad-a82a-238f7d467e97","atespace":"ate-golden","actor_name":"d64e7a5a-03df-4f76-b11a-76054d12eb9b","actor_uid":"72909359-160a-4bd5-acdb-ff0cf0de7e38","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-soci"},"resp":{},"err":null,"elapsed-time":"4.600229283s","trace_id":"6fb911245b9d50dc34bb32bc5fa5e9e7","span_id":"5c4902ab1f229495","trace_flags":"01"}
{"time":"2026-09-27T01:06:13.840472185Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Checkpoint","req":{"target_ateom_uid":"b4c8279d-b247-4dad-a82a-238f7d467e97","atespace":"ate-golden","actor_name":"d64e7a5a-03df-4f76-b11a-76054d12eb9b","actor_uid":"72909359-160a-4bd5-acdb-ff0cf0de7e38","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-soci"},"resp":{},"err":null,"elapsed-time":"487.31683ms","trace_id":"de8eb2a29d7a47964fc88d068fd66957","span_id":"616fd5fc8471525c","trace_flags":"00"}
{"time":"2026-09-27T01:06:45.650241078Z","level":"INFO","msg":"Image cache hit","ref":"registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","digest":"sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","trace_id":"41ed0e03d40abfe8faeb25729f276f83","span_id":"b325c6ac1f63d512","trace_flags":"00"}
{"time":"2026-09-27T01:06:49.118852758Z","level":"INFO","msg":"Image streamed","streamer":"soci","ref":"us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","digest":"sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","layers":9,"duration":3468596816,"trace_id":"41ed0e03d40abfe8faeb25729f276f83","span_id":"953953d10d928f2e","trace_flags":"00"}
{"time":"2026-09-27T01:06:49.345655144Z","level":"INFO","msg":"Restore timing breakdown","ate.atespace":"ate-demo-sandbox","ate.actor.name":"bench-soci-1","ate.actor.uid":"e8269925-07a0-480b-9fd4-dfdbad0b216b","ate.template.atespace":"ate-demo-sandbox","ate.template.name":"sandbox-1gb-soci","ate.snapshot.scope":"full","ate.snapshot.kind":"golden","ate.sandbox.class":"gvisor","ate.actor.restore.duration.volume_mount":5.41e-7,"ate.actor.restore.duration.manifest_fetch":0.033379901,"ate.actor.restore.duration.sandbox_assets":0.000095615,"ate.actor.restore.duration.download":0.056167765,"ate.actor.restore.duration.oci_unpack":3.468993389,"ate.actor.restore.duration.ateom_restore":0.170551835,"ate.actor.restore.duration.total":3.696117533,"trace_id":"41ed0e03d40abfe8faeb25729f276f83","span_id":"7eb9c2939c7399ac","trace_flags":"00"}
{"time":"2026-09-27T01:06:49.345764428Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Restore","req":{"target_ateom_uid":"b4c8279d-b247-4dad-a82a-238f7d467e97","atespace":"ate-demo-sandbox","actor_name":"bench-soci-1","actor_uid":"e8269925-07a0-480b-9fd4-dfdbad0b216b","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-soci"},"resp":{},"err":null,"elapsed-time":"3.696342073s","trace_id":"41ed0e03d40abfe8faeb25729f276f83","span_id":"7eb9c2939c7399ac","trace_flags":"00"}
{"time":"2026-09-27T01:06:50.340146587Z","level":"INFO","msg":"Image streamed","streamer":"soci","ref":"us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","digest":"sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e","layers":9,"duration":606845,"trace_id":"6ba220780747b00da5a12321e88fc632","span_id":"33658a4f44199018","trace_flags":"00"}
{"time":"2026-09-27T01:06:50.340928179Z","level":"INFO","msg":"Image cache hit","ref":"registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","digest":"sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4","trace_id":"6ba220780747b00da5a12321e88fc632","span_id":"953953d10d928f2e","trace_flags":"00"}
{"time":"2026-09-27T01:06:50.562135982Z","level":"INFO","msg":"Restore timing breakdown","ate.atespace":"ate-demo-sandbox","ate.actor.name":"bench-soci-2","ate.actor.uid":"75a94885-351d-4d00-9ff2-2a5389e9ebfb","ate.template.atespace":"ate-demo-sandbox","ate.template.name":"sandbox-1gb-soci","ate.snapshot.scope":"full","ate.snapshot.kind":"golden","ate.sandbox.class":"gvisor","ate.actor.restore.duration.volume_mount":3.9e-7,"ate.actor.restore.duration.manifest_fetch":0.03377768,"ate.actor.restore.duration.sandbox_assets":0.000090837,"ate.actor.restore.duration.download":0.039193746,"ate.actor.restore.duration.oci_unpack":0.00114474,"ate.actor.restore.duration.ateom_restore":0.148045564,"ate.actor.restore.duration.total":0.244532073,"trace_id":"6ba220780747b00da5a12321e88fc632","span_id":"3f4448d88e882a10","trace_flags":"00"}
{"time":"2026-09-27T01:06:50.562247396Z","level":"INFO","msg":"Handle RPC","method":"/atelet.AteomHerder/Restore","req":{"target_ateom_uid":"62e4b806-23be-4df5-8044-15be6f169ff2","atespace":"ate-demo-sandbox","actor_name":"bench-soci-2","actor_uid":"75a94885-351d-4d00-9ff2-2a5389e9ebfb","actor_template_atespace":"ate-demo-sandbox","actor_template_name":"sandbox-1gb-soci"},"resp":{},"err":null,"elapsed-time":"244.756556ms","trace_id":"6ba220780747b00da5a12321e88fc632","span_id":"3f4448d88e882a10","trace_flags":"00"}
```

#### Filesystem Evidence (`soci`)
- **Streaming lease (`/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/lease.json`):**
```json
{
  "streamer": "soci",
  "imageRef": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e",
  "imageDigest": "sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e",
  "layerDirs": [
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-0",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-1",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-2",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-3",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-4",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-5",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-6",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-7",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-8"
  ],
  "snapshotKeys": [
    "ate-view-ea16cace8933-e4986d6aaecfc407",
    "ate-view-fd14681b3f78-e4986d6aaecfc407",
    "ate-view-17e3008119c7-e4986d6aaecfc407",
    "ate-view-8512b779ac15-e4986d6aaecfc407",
    "ate-view-f5f3409de2bc-e4986d6aaecfc407",
    "ate-view-fc77ab78e128-e4986d6aaecfc407",
    "ate-view-584c99725804-e4986d6aaecfc407",
    "ate-view-ffe56a1c5f38-e4986d6aaecfc407",
    "ate-view-b80be4baa9e9-e4986d6aaecfc407"
  ]
}
```
- **`rootfs-overlay.json` (`/var/lib/ateom-gvisor/actors/e8269925-07a0-480b-9fd4-dfdbad0b216b/bundles/sandbox/rootfs-overlay.json`):**
```json
{
  "version": 1,
  "imageDigest": "sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e",
  "imageRef": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e",
  "layers": [
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-0",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-1",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-2",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-3",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-4",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-5",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-6",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-7",
    "/var/lib/ateom-gvisor/streaming/soci/fa2f27bfa131cd35d2b81074ffd2e4ff6fe2bc78b92f0f9dfc494318f70e819e/layer-8"
  ]
}
```

---

## 4. Bottleneck Analysis & Next Optimization Targets

### 4.1 What the Measured Data Shows (Phase-Level Bottlenecks)

#### A. Cold-Node `resume actor` (`AteomHerder/Restore` RPC)

| Phase | Mode 1: `none` (`28,463 ms`) | Mode 2: `riptide` (`1,357 ms`) | Mode 3: `soci` (`3,696 ms`) |
| :--- | ---: | ---: | ---: |
| **1. `oci_unpack` (Image Pull / Stream Mount)** | **`26,956 ms` (94.7%)** | **`1,135 ms` (83.6%)** | **`3,469 ms` (93.9%)** |
| **2. `manifest_fetch` + `download` *(parallel GCS fetch)*** | `92 ms` *(hidden by unpack)* | `90 ms` *(hidden by stream)* | `90 ms` *(hidden by stream)* |
| **3. `ateom_restore` *(gVisor `runsc restore`)*** | `381 ms` (1.3%) | **`149 ms` (11.0%)** | `171 ms` (4.6%) |
| **4. Other `Restore` overhead *(bundle setup, etc.)*** | `1,126 ms` (4.0%) | `73 ms` (5.4%) | `56 ms` (1.5%) |
| **5. Control-Plane + CLI Poll (`CLI wall-clock - RPC`)** | `1,098 ms` | **`960 ms`** | **`960 ms`** |

1. **Without Streaming (`none`):**
   - **Bottleneck:** `oci_unpack` (`26.96 s`, **94.7%** of the `Restore` RPC) — downloading 1.19 GB compressed and extracting 3.5 GB across 9 layers onto local disk.
2. **With Streaming (`riptide` / `soci`) — Cold Node:**
   - **Bottleneck #1 (Inside `atelet`): Cold `PrepareLayers` (`1,135 ms` on Riptide, `3,469 ms` on SOCI — 84% to 94% of the `Restore` RPC).**
     - Even with streaming, `oci_unpack` remains the largest phase of a **cold-node** restore (`~126 ms/layer` on Riptide, `~385 ms/layer` on SOCI across 9 layers).
     - `Driver.PrepareLayers` in `internal/imagestreaming/drivers/remotesnapshotter/driver.go` first resolves the image manifest and config JSON over HTTPS from Artifact Registry (`defaultImageResolver`), and then iterates over the 9 layers **strictly sequentially** (making `Stat` $\rightarrow$ `Prepare` $\rightarrow$ `Stat` $\rightarrow$ `View` gRPC calls + `probeListable` for each layer $i$ before starting layer $i+1$).
   - **Bottleneck #2 (End-to-End CLI): Control-plane scheduling + CLI polling (`~960 ms`).**
     - On Riptide, `AteomHerder/Restore` finishes in `1,357 ms`, while `kubectl-ate resume actor` takes `2,317 ms` (`960 ms` spent across `ateapi` $\rightarrow$ `atescheduler` worker assignment $\rightarrow$ `atelet` dispatch and CLI status polling).

---

#### B. Warm-Node `resume actor` (Once Image Lease is Active on Node)

| Phase | Mode 1: `none` | Mode 2: `riptide` | Mode 3: `soci` | Share of Warm Restore |
| :--- | ---: | ---: | ---: | :--- |
| **`oci_unpack` (Cache / Lease Hit)** | `1.4 ms` | `3.0 ms` | `1.1 ms` | **< 1% (Eliminated)** |
| **`manifest_fetch` + `download` (GCS Snapshot)** | `160 ms` | `72 ms` | `73 ms` | **~28–55% of RPC** |
| **`ateom_restore` (`ateom` / `runsc restore`)** | `125 ms` | `169 ms` | `148 ms` | **~43–64% of RPC** |
| **Total `AteomHerder/Restore` RPC** | **`289 ms`** | **`265 ms`** | **`245 ms`** | **100% of RPC** |
| **Control-Plane + CLI Poll (`CLI - RPC`)** | **`996 ms`** | **`1,087 ms`** | **`914 ms`** | **~79% of CLI wall-clock** |

Once the node has an active lease (or warm layer cache), image delivery (`oci_unpack`) drops to **`1–3 ms`** and is no longer a bottleneck. The next optimization targets for warm-node actor boot are:
1. **Control-plane / CLI round-trip (`~900–1,080 ms`):** Dominates user-perceived `kubectl-ate resume actor` latency (`~1.2 s` total vs `~250 ms` actual restore on the node).
2. **`ateom_restore` (`~125–169 ms`):** Inside the `~250 ms` node restore, `ateom` / `runsc restore` is the largest remaining step.
3. **GCS Golden Snapshot fetch (`manifest_fetch` + `download` = `~72–160 ms`):** Fetching checkpoint metadata and memory pages from GCS on every restore.

---

### 4.2 Where Finer-Grained Sub-Phase Data Is Not Yet Collected

To further decompose the remaining bottlenecks above, additional sub-span instrumentation would be needed in three places:

1. **Inside cold `PrepareLayers` (`1.13 s` Riptide / `3.47 s` SOCI):**
   - `"Image streamed"` currently logs the total `PrepareLayers` duration as a single number. It does not yet separate:
     - `defaultImageResolver` (remote registry manifest + config fetch inside `atelet`) vs.
     - `client.Prepare` (snapshotter daemon fetching TOC/zTOC and mounting FUSE) vs.
     - `probeListable` polling vs. `client.Stat` / `client.View` gRPC round-trips.
2. **Inside `ateom_restore` (`125–170 ms`):**
   - `Restore timing breakdown` records the outer `Ateom.Restore` gRPC call from `atelet` to `ateom`, without breaking down overlayfs mount setup, `atenet` network configuration, and `runsc restore` inside `ateom`.
3. **Inside the `~960 ms` control-plane / CLI gap:**
   - Current logs do not separate per-hop latency across `kubectl-ate` poll interval, `ateapi`, and `atescheduler` worker allocation.

---

## 5. Appendix: Standalone Snapshotter & FUSE Demand-Paging Microbenchmark

In addition to the full end-to-end actor lifecycle benchmark above, standalone snapshotter microbenchmarks (`tools/e2e-stream-probe`) were executed against two large ML images to measure raw `PrepareLayers` latency and FUSE demand-paging throughput:

| Benchmark Profile | Workload 1: JAX/AXLearn TPU (`riptide`) | Workload 2: TensorFlow GPU (`soci`) |
| :--- | :--- | :--- |
| **Image Reference** | `us-docker.pkg.dev/kuiyue-gke-dev/axlearn/tpu:kuiyue-enabled` | `public.ecr.aws/soci-workshop-examples/tensorflow_gpu:latest` |
| **Streaming Protocol** | `containerd.services.snapshots.v1.Snapshots` | `containerd.services.snapshots.v1.Snapshots` |
| **Daemon Socket** | `/run/containerd-gcfs-grpc/containerd-gcfs-grpc.sock` | `/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock` |
| **Compressed Size** | 1,800.65 MB (1.88 GB) | 3,220.46 MB (3.14 GB) |
| **Uncompressed Size** | 5,417.64 MB (5.42 GB) | 6,480.55 MB (6.48 GB) |
| **Layer Count** | 12 layers | 19 layers |
| **Traditional Pull & Unpack (Baseline)** | **89.99s** (20.01 MB/s) | **140.08s** (22.99 MB/s) |
| **Image Streaming Ready Time (Cold)** | **0.47s** (470.35ms) | **4.85s** |
| **Image Streaming Speedup Factor** | **191.3x FASTER (-99.5%)** | **28.9x FASTER (-96.5%)** |
| **Warm View Re-attachment Latency** | **3.38 µs** (view reuse) | **2.92 µs** (view reuse) |
| **In-Container Demand Paging (Uncached)** | **4.96 MB/s** (mean latency: 1.37ms, p50: 1.18ms) | **6.17 MB/s** (mean latency: 2.86ms, p50: 459µs) |
| **In-Container Cached Re-Read** | **20.90 MB/s** (VFS page cache) | **40.70 MB/s** (VFS page cache) |
| **Data & State Integrity** | **100% PASSED** (100/100 files verified) | **100% PASSED** (100/100 files verified) |
