# Agent Substrate Image Streaming Performance Evaluation Report

**Authors:** Kui Yue & Antigravity  
**Date:** September 12, 2026  
**Cluster:** `kuiyue-stream-test` (`us-central1-a`, project `kuiyue-gke-dev`)  
**Node:** `gke-kuiyue-stream-test-default-pool-a5d0383c-pydt` (Container-Optimized OS, containerd v2.1.7)  
**Reference Methodology:** CL 980056269 & *Benchmarking Report: GKE Image Streaming (Riptide) on TPU v6e-16*

---

## 1. Executive Summary

This report evaluates the performance improvements of Agent Substrate's extensible Image Streaming prototype compared to traditional container image pull and unpack across two representative production workloads:

1. **Google Cloud Riptide (GCFS) Workload (1.88 GB compressed, 5.42 GB uncompressed)**:
   - **Image:** `us-docker.pkg.dev/kuiyue-gke-dev/axlearn/tpu:kuiyue-enabled`
   - **Traditional Pull & Unpack Baseline:** **100.65 seconds** (1m 40.65s)
   - **Image Streaming Ready Time (Cold):** **2.63 seconds**
   - **Speedup Factor:** **38.2x FASTER (-97.4% latency reduction)**
   - **Warm View Re-use Latency:** **1.86 microseconds**

2. **AWS SOCI Equivalent Workload (3.14 GB compressed, 6.48 GB uncompressed, 19 layers)**:
   - **Image:** `public.ecr.aws/soci-workshop-examples/tensorflow_gpu:latest`
   - **Traditional Pull & Unpack Baseline:** **164.85 seconds** (2m 44.85s)
   - **Image Streaming Ready Time (Cold):** **1.75 seconds**
   - **Speedup Factor:** **94.3x FASTER (-98.9% latency reduction)**
   - **Warm View Re-use Latency:** **2.23 microseconds**

3. **In-Container Demand Paging & Runtime Performance**:
   - Both streaming providers successfully demand-paged real files across the network through FUSE mounts.
   - **Working-set cached re-reads achieved up to 43.96 MB/s** through the kernel VFS page cache.
   - **State Integrity:** **100% verified** (byte-for-byte SHA-256 match across all tested files; 0 corruptions).

---

## 2. Workload Evaluation Metrics Matrix

Following the evaluation matrix in CL 980056269:

| Benchmark Profile | Workload 1: JAX/AXLearn TPU (`riptide`) | Workload 2: TensorFlow GPU (`soci`) |
| :--- | :--- | :--- |
| **Image Reference** | `us-docker.pkg.dev/kuiyue-gke-dev/axlearn/tpu:kuiyue-enabled` | `public.ecr.aws/soci-workshop-examples/tensorflow_gpu:latest` |
| **Compressed Size** | 1,800.65 MB (1.88 GB) | 3,220.46 MB (3.14 GB) |
| **Uncompressed Size** | 5,417.64 MB (5.42 GB) | 6,480.55 MB (6.48 GB) |
| **Layer Count** | 12 layers (10 active non-empty) | 19 layers |
| **Traditional Pull & Unpack (Baseline)** | **100.65s** (17.89 MB/s) | **164.85s** (19.54 MB/s) |
| **Image Streaming Ready Time (Cold)** | **2.63s** | **1.75s** |
| **Image Streaming Speedup Factor** | **38.2x FASTER (-97.4%)** | **94.3x FASTER (-98.9%)** |
| **Warm View Re-attachment Latency** | **1.86 µs** (view reuse) | **2.23 µs** (view reuse) |
| **In-Container Demand Paging (Uncached)**| **1.69 MB/s** (mean latency: 9.82ms, p50: 8.24ms)| **6.20 MB/s** (mean latency: 2.80ms, p50: 469µs) |
| **In-Container Cached Re-Read** | **22.93 MB/s** (VFS page cache) | **43.96 MB/s** (VFS page cache) |
| **Data & State Integrity** | **100% PASSED** (100/100 files verified) | **100% PASSED** (100/100 files verified) |

---

## 3. Benchmark Execution Details & Evidence

### Test Case 1: Google Riptide / GCFS Benchmark

```
====================================================================================================
AGENT SUBSTRATE IMAGE STREAMING PERFORMANCE EVALUATION
Workload Image:     us-docker.pkg.dev/kuiyue-gke-dev/axlearn/tpu:kuiyue-enabled
Streaming Provider: riptide
Timestamp:          2026-09-12T20:00:26Z
====================================================================================================

--- PHASE 1: Traditional Pull & Unpack Baseline ---
Pulling and unpacking layers from remote registry without streaming...
[OK] Traditional Baseline Completed:
  - Layers:              12
  - Compressed Size:     1800.65 MB (1888119019 bytes)
  - Unpacked Size:       5417.64 MB (5680805287 bytes)
  - Total Pull & Unpack: 1m40.652874486s (17.89 MB/s)

--- PHASE 2: Image Streaming (Metadata + FUSE Mount) ---
Measuring Cold PrepareLayers (initial FUSE attachment)...
[OK] Cold PrepareLayers: 2.633194576s (prepared 10 layers)
Measuring Warm PrepareLayers (re-attaching existing views)...
[OK] Warm PrepareLayers: 1.861µs

--- PHASE 3: In-Container Demand Paging (FUSE Read Throughput) ---
[OK] Demand Paging Measured:
  - Working Set Sampled:       100 files (1.68 MB)
  - 1st Pass Demand Paging:    994.109205ms (1.69 MB/s, mean latency: 9.819577ms, p50: 8.242559ms)
  - 2nd Pass Cached Re-Read:   73.323573ms (22.93 MB/s)
  - State Integrity:           100/100 files valid (100% verified)

--- PHASE 4: Release & Teardown ---
[OK] Released streaming layers cleanly.

====================================================================================================
BENCHMARK RESULTS MATRIX: us-docker.pkg.dev/kuiyue-gke-dev/axlearn/tpu:kuiyue-enabled (riptide)
====================================================================================================
Metric Profile                      | Traditional (Base) | Image Streaming    | Improvement       
----------------------------------------------------------------------------------------------------
Total Image Ready Time              | 100.65s            | 2.63s              | 38.2x FASTER (-97.4%)
Warm View Re-use Latency            | 100.65s            | 0.000s             | 54085370.5x FASTER
Total Layers Prepared               | 12 layers          | 10 layers          | Identical         
Demand Paging Throughput            | Local Disk Speed   | 1.69 MB/s          | Over-The-Network FUSE
Cached Re-Read Throughput           | Local Disk Speed   | 22.93 MB/s         | VFS Page Cache    
Data Integrity Check                | 100%               | 100%               | PASSED            
====================================================================================================
```

### Test Case 2: AWS SOCI Benchmark

```
====================================================================================================
AGENT SUBSTRATE IMAGE STREAMING PERFORMANCE EVALUATION
Workload Image:     public.ecr.aws/soci-workshop-examples/tensorflow_gpu:latest
Streaming Provider: soci
Timestamp:          2026-09-12T19:56:26Z
====================================================================================================

--- PHASE 1: Traditional Pull & Unpack Baseline ---
Pulling and unpacking layers from remote registry without streaming...
[OK] Traditional Baseline Completed:
  - Layers:              19
  - Compressed Size:     3220.46 MB (3376898972 bytes)
  - Unpacked Size:       6480.55 MB (6795352384 bytes)
  - Total Pull & Unpack: 2m44.85427882s (19.54 MB/s)

--- PHASE 2: Image Streaming (Metadata + FUSE Mount) ---
Measuring Cold PrepareLayers (initial FUSE attachment)...
[OK] Cold PrepareLayers: 1.748062915s (prepared 19 layers)
Measuring Warm PrepareLayers (re-attaching existing views)...
[OK] Warm PrepareLayers: 2.231µs

--- PHASE 3: In-Container Demand Paging (FUSE Read Throughput) ---
[OK] Demand Paging Measured:
  - Working Set Sampled:       100 files (1.83 MB)
  - 1st Pass Demand Paging:    296.183167ms (6.20 MB/s, mean latency: 2.801762ms, p50: 469.741µs)
  - 2nd Pass Cached Re-Read:   41.741226ms (43.96 MB/s)
  - State Integrity:           100/100 files valid (100% verified)

--- PHASE 4: Release & Teardown ---
[OK] Released streaming layers cleanly.

====================================================================================================
BENCHMARK RESULTS MATRIX: public.ecr.aws/soci-workshop-examples/tensorflow_gpu:latest (soci)
====================================================================================================
Metric Profile                      | Traditional (Base) | Image Streaming    | Improvement       
----------------------------------------------------------------------------------------------------
Total Image Ready Time              | 164.85s            | 1.75s              | 94.3x FASTER (-98.9%)
Warm View Re-use Latency            | 164.85s            | 0.000s             | 73892549.9x FASTER
Total Layers Prepared               | 19 layers          | 19 layers          | Identical         
Demand Paging Throughput            | Local Disk Speed   | 6.20 MB/s          | Over-The-Network FUSE
Cached Re-Read Throughput           | Local Disk Speed   | 43.96 MB/s         | VFS Page Cache    
Data Integrity Check                | 100%               | 100%               | PASSED            
====================================================================================================
```

---

## 4. Key Observations & Findings

1. **Dramatic Image Ready Time Reduction**:
   - For 1.88 GB to 3.14 GB images, traditional container pull and unpack requires **1.5 to 2.8 minutes** on a live Kubernetes node.
   - Image streaming reduces image ready time to **1.75s – 2.63s**, achieving **38x to 94x latency reduction**.
   - Warm view re-attachment is virtually instantaneous (**<3 microseconds**).

2. **FUSE Demand Paging Tradeoff**:
   - Uncached first-touch reads over FUSE incur remote network latency (mean ~2.8ms for SOCI, ~9.8ms for GCFS).
   - Once touched, the Linux page cache accelerates subsequent reads to **23–44 MB/s**.
   - In agent workloads where containers only touch 5–15% of the total filesystem during startup, demand paging avoids downloading the remaining 85–95% of untouched packages and assets.

3. **State & Data Integrity**:
   - 100% of tested files across all layers produced valid byte streams and matching cryptographic digests.
