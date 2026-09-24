# Agent Substrate Image Streaming Benchmark: Architecture & Usage Guide

This document outlines the setup, architecture, and telemetry strategy for profiling Agent Substrate's actor lifecycle events. The goal of the suite is to benchmark the latency bottlenecks of actor cold boot, suspension, and resumption, and to measure the performance benefits of the node-local image cache (`internal/imagecache`) and GKE Image Streaming (`--image-streaming-poc`).

---

## 1. Environment & Pre-Run Setup Requirements (GKE + Artifact Registry)

Because Substrate's image streaming PoC ([`internal/imagecache/streaming_linux.go`](../internal/imagecache/streaming_linux.go#L53-L98)) relies on the GKE Image Streaming (`gcfs` / Riptide) containerd snapshotter and GCE metadata server authentication, all benchmarking (both the node-local image cache baseline and `--image-streaming-poc=true`) runs against **Google Artifact Registry** on a **GKE cluster**.

1. **Install Prerequisites**: Ensure Go, Docker, `gcloud`, and `kubectl` are installed and authenticated (`gcloud auth configure-docker us-west1-docker.pkg.dev`).
2. **Deploy Agent Substrate on GKE**:
   ```bash
   ./hack/install-ate.sh --deploy-ate-system
   ```
3. **Install the CLI**:
   ```bash
   go install ./cmd/kubectl-ate
   ```
4. **Enable GKE Image Streaming on the Worker Node Pool** (see [`demos/streaming/README.md`](../demos/streaming/README.md)):
   ```bash
   gcloud container clusters update "$CLUSTER_NAME" --location "$CLUSTER_LOCATION" \
     --enable-image-streaming
   ```
5. **Grant Artifact Registry Read Access to the Worker Pod Identity**:
   Streaming pulls execute inside `ateom-gvisor` using the worker pod's Workload Identity (`<atespace>/default` KSA):
   ```bash
   PROJECT_ID="amywxu-gke-dev"
   PN=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
   MEMBER="principal://iam.googleapis.com/projects/$PN/locations/global/workloadIdentityPools/$PROJECT_ID.svc.id.goog/subject/ns/$ATESPACE/sa/default"
   gcloud projects add-iam-policy-binding "$PROJECT_ID" --condition=None \
     --role=roles/artifactregistry.reader --member="$MEMBER"
   gcloud projects add-iam-policy-binding "$PROJECT_ID" --condition=None \
     --role=roles/storage.objectViewer --member="$MEMBER"
   ```
6. **ActorTemplate Requirements for Image Streaming**:
   * **Sandbox Class**: Must be `gvisor` (`ate-controller` only injects `applyStreamingPoCMounts` hostPath mounts for `containerd.sock` and `gcfs` into `gvisor` worker pods).
   * **Explicit `command` Required**: Because `atelet` skips pulling the image config when `--image-streaming-poc=true` ([`cmd/atelet/oci.go:L116-L120`](../cmd/atelet/oci.go#L116-L120)), every application container in `ActorTemplate` **must** explicitly set `command` (entrypoint), or `atelet` will fail with `streaming PoC requires ActorTemplate command for "<container>"`.

---

## 2. Image Mirroring Process (SWE-Bench Verified in Artifact Registry)

SWE-bench Verified images are mirrored into Google Artifact Registry under:
* **Project**: `amywxu-gke-dev`
* **Location**: `us-west1`
* **Repository**: `swebench-mirror`
* **Image Package**: `us-west1-docker.pkg.dev/amywxu-gke-dev/swebench-mirror/swebench-verified`

> [!NOTE]
> The first pull of a newly pushed image in Artifact Registry triggers a one-time, server-side streaming index preparation. Subsequent cold-node pulls stream immediately through `gcfs`.

### Mirroring Script (`mirror-swebench.sh`)
This script pulls upstream `swebench/sweb.eval.x86_64.<instance>:latest` images from Docker Hub and pushes them to `us-west1-docker.pkg.dev/amywxu-gke-dev/swebench-mirror/swebench-verified:<sweb.eval.x86_64.<instance>>`:

```bash
#!/bin/bash
# mirror-swebench.sh

set -euo pipefail

AR_IMAGE_REPO="us-west1-docker.pkg.dev/amywxu-gke-dev/swebench-mirror/swebench-verified"
IMAGE_LIST="${IMAGE_LIST:-swebench_images.txt}"

while IFS= read -r remote_image; do
    # Skip empty lines and comments
    [[ -z "$remote_image" || "$remote_image" =~ ^# ]] && continue

    echo "[*] Processing $remote_image..."

    # 1. Pull the image from Docker Hub (e.g. swebench/sweb.eval.x86_64.django__django-16560:latest)
    docker pull "$remote_image"

    # 2. Extract the SWE-bench instance name as the tag
    #    swebench/sweb.eval.x86_64.django__django-16560:latest -> sweb.eval.x86_64.django__django-16560
    base_name="${remote_image##*/}"
    instance_tag="${base_name%%:*}"
    ar_image="${AR_IMAGE_REPO}:${instance_tag}"

    # 3. Tag and push to Google Artifact Registry
    docker tag "$remote_image" "$ar_image"
    docker push "$ar_image"

    # 4. Remove local tags to save disk space
    docker rmi "$remote_image" "$ar_image"
done < "$IMAGE_LIST"

echo "[*] All images mirrored to $AR_IMAGE_REPO successfully!"
```

### Mirrored SWE-Bench Verified Images Reference (`swebench-mirror`)
The following 20 SWE-bench Verified images are currently mirrored in `us-west1-docker.pkg.dev/amywxu-gke-dev/swebench-mirror/swebench-verified` (~12.2 GB total):

| # | Tag (`swebench-verified:<tag>`) | Digest (`sha256`) |
| :--- | :--- | :--- |
| 1 | `sweb.eval.x86_64.astropy__astropy-13977` | `sha256:bd003a6a247d33fc29c4df16c9288f6d2b75ff4ed94e1fdc6ff161f1db94496f` |
| 2 | `sweb.eval.x86_64.django__django-11239` | `sha256:d0e1e22a263cf2ae9b54884de5ad1b0089cfde31aa16fbd7546550dd947954dc` |
| 3 | `sweb.eval.x86_64.django__django-11400` | `sha256:d8321ebabb7ba24e7db20275bba97dfd611fb36d078746ff08524b10225ba366` |
| 4 | `sweb.eval.x86_64.django__django-11740` | `sha256:cd95eb83c40d5738a534d6210a485345fffc9cf653c2c5995b4395d202ff0eb4` |
| 5 | `sweb.eval.x86_64.django__django-12304` | `sha256:86ac7281087965cb576c24e92ee6a71fecaf952948e388583949dac9935c5f7a` |
| 6 | `sweb.eval.x86_64.django__django-12708` | `sha256:5f920ccf13410e9fbc43b9e3a83e2b876f5a28d6b0db5369b428552320567b7d` |
| 7 | `sweb.eval.x86_64.django__django-12713` | `sha256:7a238335a0f05c3fbc1d9d193efb32d83cbba7c7fc72a89e37cc0496901e1993` |
| 8 | `sweb.eval.x86_64.django__django-13569` | `sha256:941f05dde54551c184816c4510eb5dde3085bef76087834294436bce215c10d5` |
| 9 | `sweb.eval.x86_64.django__django-14007` | `sha256:a7f15cde39353cf6a45f4108104182951506a55eb1067030b792d5a502ce4110` |
| 10 | `sweb.eval.x86_64.django__django-14351` | `sha256:cdbd1bfcd9546406425b6dc517312e5fe55d2c6b13ab4acb53a71d1b1ae4f624` |
| 11 | `sweb.eval.x86_64.django__django-15851` | `sha256:25255217ee5f2ba10801b5ec77738959d8167898e119bc483eafe93b1fa05c5f` |
| 12 | `sweb.eval.x86_64.django__django-16560` | `sha256:0bafff953ce186aa261162d4091549fb4ad49df938900474b5f070d511bb1604` |
| 13 | `sweb.eval.x86_64.django__django-16662` | `sha256:88f4e9078d8d403ca4f431b35a9c4dcfeb83b8f59dad2a69d988cca6a82a7bbc` |
| 14 | `sweb.eval.x86_64.matplotlib__matplotlib-23476` | `sha256:64bb043b17a25dbeeba766c82d1d5f66ca446ca32da12c88418ded9e0d79cfdc` |
| 15 | `sweb.eval.x86_64.pytest-dev__pytest-7205` | `sha256:db1e248257a048e498295866ef35a1f7dee7d4c7f9bf638cda6bcf99e1fb623c` |
| 16 | `sweb.eval.x86_64.scikit-learn__scikit-learn-10908` | `sha256:f3903e9da732f6d69540bd41a61e13b856d97dd54da060fee9fafab3a3c84116` |
| 17 | `sweb.eval.x86_64.scikit-learn__scikit-learn-26194` | `sha256:fca1ded18212c7cf74a6b741c4a42cc58b7c0f7e48f5885b625780045b1c2163` |
| 18 | `sweb.eval.x86_64.sphinx-doc__sphinx-10449` | `sha256:f6c9a3a3ecb31d4e4fcd7e4cc384d003dc63594a32a9ba91161626d110fcbbb6` |
| 19 | `sweb.eval.x86_64.sympy__sympy-13974` | `sha256:babcbe096ff1c628334260144452b20e35d13d4be44a226810bd5e7d84a647a0` |
| 20 | `sweb.eval.x86_64.sympy__sympy-23262` | `sha256:7c8f88bc121da29758c49f4877c3b34d5503e30d81b53b7f7e6bc802ad3a7170` |

**`ActorTemplate` Configuration Example:**
```yaml
containers:
  - name: swebench-eval
    image: us-west1-docker.pkg.dev/amywxu-gke-dev/swebench-mirror/swebench-verified:sweb.eval.x86_64.django__django-16560
    command: ["/bin/bash", "-c", "python3 -m http.server 80"] # Explicit command required by --image-streaming-poc
```

---

## 3. Lifecycle Mechanics & Telemetry Strategy

### Crucial Substrate Lifecycle Mechanics
1. **`kubectl ate create actor` Does NOT Boot the Actor**:
   `CreateActor` ([`cmd/ateapi/internal/controlapi/actor.go:L127-L144`](../cmd/ateapi/internal/controlapi/actor.go#L127-L144)) only writes a metadata record in `ateapi` with `state = ACTOR_STATE_SUSPENDED`. No worker pod is assigned and no containers are started during `create actor`.
2. **When Cold Boot (`Run`) vs. Restore (`Restore`) Happens**:
   * **Cold Boot (`atelet.Run` $\rightarrow$ `ateom.RunWorkload`)** occurs in two places:
     1. Automatically when an `ActorTemplate` is created (the golden snapshot controller boots a golden actor from scratch via `Run` and checkpoints it to create the golden tag), **OR**
     2. On the **first `kubectl ate resume actor`** when the `ActorTemplate` has **no golden snapshot** (hitting `"Actor has no snapshot; Booting from ActorTemplate spec"` in [`cmd/ateapi/internal/controlapi/workflow_resume.go:L759`](../cmd/ateapi/internal/controlapi/workflow_resume.go#L759)).
   * If the `ActorTemplate` already has a golden snapshot, the first `kubectl ate resume actor` executes a **Golden Snapshot Restore** (`ate.snapshot.kind="golden"`).
   * Therefore, the complete benchmark sequence is:
     `create actor` (born `SUSPENDED`) $\rightarrow$ `resume actor` (Initial Boot / Golden Restore) $\rightarrow$ `suspend actor` (Checkpoints to object storage) $\rightarrow$ `resume actor` (Restores from `latest` snapshot).

### Log Formatting & Selector Rules
* **Worker Pod Labels**: Worker pods are pre-warmed by a `WorkerPool` Deployment and are labeled **`ate.dev/worker-pool=<pool-name>`** in the pool's namespace (they are **never** labeled `ate.dev/actor=<actor-name>`).
* **Two JSON Log Envelopes in `ateom`**:
  1. `ActorLogger.EmitLifecycleLog` ([`internal/actorlog/logger.go:L98-L106`](../internal/actorlog/logger.go#L98-L106)): Emits `"message"` (e.g. `"Actor starting"`, `"Actor started"`, `"Actor checkpointing"`, `"Actor checkpointed"`) and nests actor identity under `"labels": {"ate.actor.name": "...", "ate.actor.uid": "..."}` (or `"logging.googleapis.com/labels"` on GCE).
  2. Standard Go `slog.InfoContext` (`slog.NewJSONHandler`): Emits `"msg"` (e.g. `"Actor boot phases"`, `"ateom restore breakdown"`, `"About to run runsc create"`). **Importantly, `slog.JSONHandler` serializes `slog.Duration` fields as `int64` nanoseconds (e.g. `310000000` = `0.31s`), NOT strings like `"310ms"`!**
* **`atelet` `"Restore timing breakdown"`**: Uses `snapshotLogAttrs` ([`cmd/atelet/metrics.go:L251-L296`](../cmd/atelet/metrics.go#L251-L296), called from [`cmd/atelet/main.go:L1006-L1007`](../cmd/atelet/main.go#L1006-L1007)), which explicitly converts phase durations to **`float64` seconds** (e.g. `0.310`) and places `ate.actor.name` and `ate.actor.uid` at the top level of the JSON object.

### Telemetry & Log Scraping Matrix

| Lifecycle Phase | Component & Selector | Log Message (`msg` or `message`) | Code References (Where Emitted & Triggered) | Tracked Fields / Boundaries | Value Format |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Cold Boot (`atelet` Unpack)** | `ateapi` (`app=ate-api-server`) $\rightarrow$ `ateom` (`ate.dev/worker-pool`) | `"Actor has no snapshot; Booting from ActorTemplate spec"` $\rightarrow$ `"Actor starting"` | • **Start log emitted at:** [workflow_resume.go:L759](../cmd/ateapi/internal/controlapi/workflow_resume.go#L759) (`ensureAteletRestored`), which triggers `client.Run(ctx, req)` ([workflow_resume.go:L784](../cmd/ateapi/internal/controlapi/workflow_resume.go#L784))<br>• **Unpack executed in:** `atelet`'s `(*AteomHerder).Run` ([cmd/atelet/main.go:L516-L518](../cmd/atelet/main.go#L516-L518) $\rightarrow$ `prepareOCIDirectory` in [cmd/atelet/oci.go:L75-L176](../cmd/atelet/oci.go#L75-L176)), which then triggers `client.RunWorkload` ([cmd/atelet/main.go:L534](../cmd/atelet/main.go#L534))<br>• **End log emitted at:** [internal/actorlog/logger.go:L98-L106](../internal/actorlog/logger.go#L98-L106) (`EmitLifecycleLog`), triggered in [cmd/ateom-gvisor/main.go:L673](../cmd/ateom-gvisor/main.go#L673) / [cmd/ateom-microvm/run.go:L300](../cmd/ateom-microvm/run.go#L300) | Timestamp delta joined by `trace_id` (captures `atelet.prepareOCIBundles` image pull & unpack before `ateom.RunWorkload`) | RFC3339Nano `time` delta |
| **Cold Boot (microVM `ateom`)** | Worker Pod (`ate.dev/worker-pool=<pool>`) | `msg: "Actor boot phases"` (filtered by `id == actor_uid`) | • **Triggered by:** `atelet` calling `client.RunWorkload` ([cmd/atelet/main.go:L534](../cmd/atelet/main.go#L534)) $\rightarrow$ `(*AteomService).RunWorkload` ([cmd/ateom-microvm/run.go:L282](../cmd/ateom-microvm/run.go#L282)) $\rightarrow$ `coldBootActor` ([cmd/ateom-microvm/run.go:L414](../cmd/ateom-microvm/run.go#L414))<br>• **Emitted at:** [cmd/ateom-microvm/run.go:L603-L608](../cmd/ateom-microvm/run.go#L603-L608) (measuring steps from `client.BootVM` at [run.go:L559](../cmd/ateom-microvm/run.go#L559) through `readyz.WaitAll` at [run.go:L597](../cmd/ateom-microvm/run.go#L597)) | `vsock_wait`, `agent_dial`, `containers`, `readyz`, `since_boot` | `int64` Nanoseconds (`slog.Duration`) |
| **Cold Boot (gVisor `ateom` Granular)** | Worker Pod (`ate.dev/worker-pool=<pool>`) | Sequential logs inside `RunWorkload` (joined by `trace_id` or window `[Actor starting, Actor started]`):<br>1. `message: "Actor starting"` ($T_0$)<br>2. `msg: "Interior NetNS Link Routes"` ($T_1$)<br>3. `msg: "About to run runsc create"` (`container="pause"`, $T_2$)<br>4. `msg: "About to run runsc start"` (`container="pause"`, $T_3$)<br>5. `msg: "About to run runsc create"` (`container="<app>"`, $T_4$)<br>6. `msg: "About to run runsc start"` (`container="<app>"`, $T_5$)<br>7. `msg: "Readyz reached 200"` ($T_6$, plus `elapsed` field)<br>8. `message: "Actor started"` ($T_7$) | All triggered inside `(*AteomService).RunWorkload` ([cmd/ateom-gvisor/main.go:L656-L778](../cmd/ateom-gvisor/main.go#L656-L778)):<br>1. **$T_0$:** Emitted by `EmitLifecycleLog` ([internal/actorlog/logger.go:L98](../internal/actorlog/logger.go#L98)), triggered at [cmd/ateom-gvisor/main.go:L673](../cmd/ateom-gvisor/main.go#L673)<br>2. **$T_1$:** Emitted by `DumpNetInfo` ([internal/ateomnet/net.go:L460](../internal/ateomnet/net.go#L460)) at end of `SetupActorNetwork` ([net.go:L587](../internal/ateomnet/net.go#L587)), triggered at [cmd/ateom-gvisor/main.go:L689](../cmd/ateom-gvisor/main.go#L689) right before `composeBundleRootfsAll` ([main.go:L736](../cmd/ateom-gvisor/main.go#L736))<br>3. **$T_2$:** Emitted in `(*runsc).cmdCreate` ([cmd/ateom-gvisor/runsc.go:L75](../cmd/ateom-gvisor/runsc.go#L75)), triggered for `pause` at [cmd/ateom-gvisor/main.go:L742](../cmd/ateom-gvisor/main.go#L742)<br>4. **$T_3$:** Emitted in `(*runsc).cmdStart` ([cmd/ateom-gvisor/runsc.go:L120](../cmd/ateom-gvisor/runsc.go#L120)), triggered for `pause` at [cmd/ateom-gvisor/main.go:L745](../cmd/ateom-gvisor/main.go#L745)<br>5. **$T_4$:** Emitted in `(*runsc).cmdCreate` ([cmd/ateom-gvisor/runsc.go:L75](../cmd/ateom-gvisor/runsc.go#L75)), triggered for `<app>` at [cmd/ateom-gvisor/main.go:L758](../cmd/ateom-gvisor/main.go#L758)<br>6. **$T_5$:** Emitted in `(*runsc).cmdStart` ([cmd/ateom-gvisor/runsc.go:L120](../cmd/ateom-gvisor/runsc.go#L120)), triggered for `<app>` at [cmd/ateom-gvisor/main.go:L761](../cmd/ateom-gvisor/main.go#L761)<br>7. **$T_6$:** Emitted in `readyz.Wait` ([internal/readyz/readyz.go:L127-L131](../internal/readyz/readyz.go#L127-L131)), triggered via `readyz.WaitAll` at [cmd/ateom-gvisor/main.go:L767](../cmd/ateom-gvisor/main.go#L767)<br>8. **$T_7$:** Emitted by `EmitLifecycleLog`, triggered at [cmd/ateom-gvisor/main.go:L774](../cmd/ateom-gvisor/main.go#L774) | • `net_setup`: $T_1 - T_0$<br>• `rootfs_compose` (overlay or GCFS mount): $T_2 - T_1$<br>• `runsc_create_pause`: $T_3 - T_2$<br>• `runsc_start_pause`: $T_4 - T_3$<br>• `runsc_create_app`: $T_5 - T_4$<br>• `runsc_start_app`: $(T_6 - T_5) - \text{readyz.elapsed}$<br>• `readyz`: `elapsed` field on `"Readyz reached 200"`<br>• `ateom_total`: $T_7 - T_0$ | RFC3339Nano `time` deltas + `int64` Nanoseconds (`elapsed`) |
| **Suspend (microVM)** | Worker Pod (`ate.dev/worker-pool=<pool>`) | `msg: "Actor checkpointed"` (filtered by `id == actor_uid`) | • **Triggered by:** `ateapi` `ensureAteletSuspended` ([workflow_suspend.go:L262](../cmd/ateapi/internal/controlapi/workflow_suspend.go#L262)) $\rightarrow$ `atelet` `(*AteomHerder).Checkpoint` ([cmd/atelet/main.go:L652](../cmd/atelet/main.go#L652)) $\rightarrow$ `(*AteomService).CheckpointWorkload` ([cmd/ateom-microvm/checkpoint.go:L64](../cmd/ateom-microvm/checkpoint.go#L64))<br>• **Emitted at:** [cmd/ateom-microvm/checkpoint.go:L201-L208](../cmd/ateom-microvm/checkpoint.go#L201-L208) | `pause`, `snapshot`, `durable_dir`, `rootfs_upper`, `teardown` | `int64` Nanoseconds (`slog.Duration`) |
| **Suspend (gVisor)** | Worker Pod (`ate.dev/worker-pool=<pool>`) & `ateapi` (`app=ate-api-server`) | `message: "Actor checkpointing"` ($T_0$) $\rightarrow$ `msg: "About to run runsc checkpoint"` ($T_1$) $\rightarrow$ `message: "Actor checkpointed"` ($T_2$) $\rightarrow$ `ateapi` `msg: "Actor state changed"` (`ate.actor.state="suspended"`, $T_3$) | • **$T_0$:** Emitted by `EmitLifecycleLog`, triggered in `(*AteomService).CheckpointWorkload` at [cmd/ateom-gvisor/main.go:L796](../cmd/ateom-gvisor/main.go#L796)<br>• **$T_1$:** Emitted in `(*runsc).cmdCheckpoint` ([cmd/ateom-gvisor/runsc.go:L147](../cmd/ateom-gvisor/runsc.go#L147)), triggered at [cmd/ateom-gvisor/main.go:L839](../cmd/ateom-gvisor/main.go#L839)<br>• **$T_2$:** Emitted by `EmitLifecycleLog`, triggered at [cmd/ateom-gvisor/main.go:L880](../cmd/ateom-gvisor/main.go#L880) (after `terminateWorkload` at [main.go:L866](../cmd/ateom-gvisor/main.go#L866))<br>• **$T_3$:** Emitted in `logActorStateChanged` ([cmd/ateapi/internal/controlapi/workflow.go:L98](../cmd/ateapi/internal/controlapi/workflow.go#L98)), triggered in `finalizeSuspended` at [workflow_suspend.go:L430](../cmd/ateapi/internal/controlapi/workflow_suspend.go#L430) after `atelet` uploads snapshot via `uploadExternalCheckpoint` ([cmd/atelet/main.go:L701](../cmd/atelet/main.go#L701)) | • `ateom_checkpoint_total`: $T_2 - T_0$<br>• `runsc_checkpoint_and_cleanup`: $T_2 - T_1$<br>• `persist_and_commit`: $T_3 - T_2$ | RFC3339Nano `time` deltas |
| **Resume (`atelet` Breakdown)** | `atelet` DaemonSet (`app=atelet`, `-n ate-system`) | `msg: "Restore timing breakdown"` (filtered by `ate.actor.uid` or `ate.actor.name`) | • **Triggered by:** `ateapi` `ensureAteletRestored` calling `client.Restore` ([workflow_resume.go:L713,756](../cmd/ateapi/internal/controlapi/workflow_resume.go#L713)) $\rightarrow$ `(*AteomHerder).Restore` ([cmd/atelet/main.go:L961](../cmd/atelet/main.go#L961))<br>• **Emitted at:** Deferred `slog.LogAttrs` in [cmd/atelet/main.go:L1006-L1007](../cmd/atelet/main.go#L1006-L1007) (attributes built by `snapshotLogAttrs` in [cmd/atelet/metrics.go:L251-L296](../cmd/atelet/metrics.go#L251-L296)) | `ate.actor.restore.duration.{volume_mount, manifest_fetch, sandbox_assets, download, oci_unpack, ateom_restore, total}` | `float64` Seconds (e.g. `0.310`) |
| **Resume (gVisor `ateom` Sub-Breakdown)** | Worker Pod (`ate.dev/worker-pool=<pool>`) | `msg: "ateom restore breakdown"` (filtered by `actor == actor_name`) | • **Triggered by:** `atelet` `(*AteomHerder).Restore` calling `client.RestoreWorkload` ([cmd/atelet/main.go:L1175](../cmd/atelet/main.go#L1175)) $\rightarrow$ `(*AteomService).RestoreWorkload` ([cmd/ateom-gvisor/main.go:L938](../cmd/ateom-gvisor/main.go#L938))<br>• **Emitted at:** [cmd/ateom-gvisor/main.go:L1115-L1122](../cmd/ateom-gvisor/main.go#L1115-L1122) (`rootfs_compose` times `composeBundleRootfsAll` at [main.go:L1026-L1030](../cmd/ateom-gvisor/main.go#L1026-L1030) $\rightarrow$ `setupStreamingRootfs` in [streaming_linux.go:L70](../internal/imagecache/streaming_linux.go#L70)) | `net`, **`rootfs_compose`** (GCFS streaming mount!), `runsc_create`, `runsc_restore` (page-in + restore), `readyz`, `total` | `int64` Nanoseconds (`slog.Duration`) |
| **Resume (microVM `ateom` Sub-Breakdown)** | Worker Pod (`ate.dev/worker-pool=<pool>`) | `msg: "Actor restore phases"` (filtered by `id == actor_uid`) | • **Triggered by:** `atelet` `(*AteomHerder).Restore` calling `client.RestoreWorkload` ([cmd/atelet/main.go:L1175](../cmd/atelet/main.go#L1175)) $\rightarrow$ `(*AteomService).RestoreWorkload` ([cmd/ateom-microvm/restore.go:L81](../cmd/ateom-microvm/restore.go#L81)) $\rightarrow$ `restoreFullScope` ([restore.go:L177](../cmd/ateom-microvm/restore.go#L177))<br>• **Emitted at:** [cmd/ateom-microvm/restore.go:L365-L376](../cmd/ateom-microvm/restore.go#L365-L376) | `prep`, `bundles`, `upper_join`, `lowers`, `durable`, `tap`, `vmm_launch`, `vm_restore`, `resume`, `readyz`, `total` | `int64` Nanoseconds (`slog.Duration`) |

### Key Caveat: Concurrent Resume Phases & Where Image Streaming Shows Up
1. **Overlapping Phases in `atelet`**: In `atelet`'s `"Restore timing breakdown"`, `download` (snapshot fetch from object storage) runs concurrently with `sandbox_assets` and `oci_unpack` (via `errgroup` in [cmd/atelet/main.go:L1108-L1153](../cmd/atelet/main.go#L1108-L1153)). The end-to-end time (`total`) is roughly $\max(\text{download}, \text{sandbox\_assets} + \text{oci\_unpack}) + \text{ateom\_restore}$.
2. **Where Image Streaming (`--image-streaming-poc=true`) Shifts Latency**:
   * When `--image-streaming-poc=false` (baseline), `atelet` pulls and unpacks the image into the layer cache during `oci_unpack` ([cmd/atelet/oci.go:L152](../cmd/atelet/oci.go#L152)), and `ateom`'s `rootfs_compose` only performs a sub-millisecond overlay mount ([internal/imagecache/bundle_linux.go:L47](../internal/imagecache/bundle_linux.go#L47)).
   * When `--image-streaming-poc=true`, `atelet` skips pulling the image and only writes a `rootfs-overlay.json` pointer ([cmd/atelet/oci.go:L116-L133](../cmd/atelet/oci.go#L116-L133); `oci_unpack` drops to $\approx 0\text{s}$). The `gcfs` metadata pull and snapshot mount happen inside `ateom-gvisor` during `ateom_restore` ([internal/imagecache/streaming_linux.go:L70-L181](../internal/imagecache/streaming_linux.go#L70-L181)), specifically captured by **`rootfs_compose`** (and on-demand file page-in during **`runsc_restore`**) in `ateom-gvisor`'s `"ateom restore breakdown"` log ([cmd/ateom-gvisor/main.go:L1115-L1122](../cmd/ateom-gvisor/main.go#L1115-L1122))!

---

## 4. The Benchmarking Script (`benchmark_substrate.py`)

```python
#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import time
from datetime import datetime

# Globals set by argparse in main()
SANDBOX_TYPE = "gvisor"
ATESPACE = "ate-demo"
WORKER_POOL = "default-pool"
WORKER_NAMESPACE = "ate-demo"


def run_cmd(cmd, check=False):
    """Executes a shell command and returns stdout."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed ({cmd}): {result.stderr.strip()}")
    return result.stdout.strip()


def extract_json_logs(selector, namespace, grep_filter=""):
    """Scrapes structured JSON logs from pods matching selector in namespace."""
    cmd = f"kubectl logs -l {selector} -n {namespace} --tail=2000"
    if grep_filter:
        cmd += f" | grep -F '{grep_filter}'"
    output = run_cmd(cmd)

    parsed_logs = []
    for line in output.splitlines():
        try:
            log_data = json.loads(line)
            msg = log_data.get("msg") or log_data.get("message") or ""
            if not grep_filter or grep_filter in msg or grep_filter in line:
                parsed_logs.append(log_data)
        except json.JSONDecodeError:
            continue
    return parsed_logs


def get_actor_labels(log_entry):
    """Returns the actor identity label dict from either plain or GCE label keys."""
    return (
        log_entry.get("labels")
        or log_entry.get("logging.googleapis.com/labels")
        or {}
    )


def parse_slog_duration(val):
    """
    Converts a Go slog.Duration JSON value to float seconds.
    Note: Go's slog.JSONHandler serializes time.Duration as int64 nanoseconds!
    """
    if val is None:
        return 0.0
    if isinstance(val, int):
        return float(val) / 1e9
    if isinstance(val, float):
        # Heuristic: values > 1e6 from slog JSON are nanoseconds
        return val / 1e9 if val > 1e6 else val

    match = re.match(r"([\d\.]+)(ms|s|µs|us|ns)", str(val))
    if not match:
        return 0.0
    num, unit = float(match.group(1)), match.group(2)
    if unit == "ms":
        return num / 1e3
    elif unit in ("µs", "us"):
        return num / 1e6
    elif unit == "ns":
        return num / 1e9
    return num


def parse_k8s_time(ts_str):
    """Parses RFC3339 / RFC3339Nano timestamps into UNIX float seconds."""
    if not ts_str:
        return 0.0
    try:
        ts_str = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ts_str).timestamp()
    except Exception:
        return 0.0


def get_actor_uid(actor_name, atespace):
    """Fetches the server-assigned UID of the actor via kubectl-ate."""
    out = run_cmd(f"kubectl ate get actor {actor_name} -a {atespace} -o json")
    try:
        data = json.loads(out)
        return data.get("metadata", {}).get("uid", "")
    except json.JSONDecodeError:
        return ""


def toggle_image_streaming(enable: bool):
    """Toggles --image-streaming-poc on the atelet DaemonSet and waits for rollout."""
    flag_val = "true" if enable else "false"
    state_str = "ENABLED" if enable else "DISABLED"
    print(f"[*] Configuring atelet DaemonSet: Image Streaming {state_str}...")
    patch = json.dumps([
        {
            "op": "replace",
            "path": "/spec/template/spec/containers/0/args",
            "value": [
                "--gcp-auth-for-image-pulls=true",
                f"--image-streaming-poc={flag_val}",
            ],
        }
    ])
    run_cmd(
        f"kubectl patch ds atelet -n ate-system --type=json -p='{patch}'",
        check=True,
    )
    run_cmd("kubectl rollout status ds/atelet -n ate-system --timeout=120s", check=True)


def parse_gvisor_cold_boot_granular(worker_logs, actor_name, actor_uid):
    """
    Extracts granular cold boot phases from ateom-gvisor logs using the
    sequential logs emitted inside RunWorkload.
    """
    result = {}
    start_idx = None
    end_idx = None
    trace_id = None

    for i, entry in enumerate(worker_logs):
        labels = get_actor_labels(entry)
        msg = entry.get("message") or entry.get("msg") or ""
        if (
            labels.get("ate.actor.name") == actor_name
            or labels.get("ate.actor.uid") == actor_uid
        ):
            if msg == "Actor starting":
                start_idx = i
                trace_id = entry.get("trace_id")
            elif msg == "Actor started" and start_idx is not None:
                end_idx = i

    if start_idx is None or end_idx is None:
        return {"ateom_gvisor_boot": "Logs incomplete"}

    window = worker_logs[start_idx : end_idx + 1]
    if trace_id:
        window = [
            e for e in window if not e.get("trace_id") or e.get("trace_id") == trace_id
        ]

    t_start = parse_k8s_time(window[0].get("time"))
    t_end = parse_k8s_time(window[-1].get("time"))
    t_net_done = None
    t_create_pause = None
    t_start_pause = None
    t_create_app = None
    t_start_app = None
    t_readyz_done = None
    readyz_elapsed = None

    for e in window:
        msg = e.get("msg") or e.get("message") or ""
        ts = parse_k8s_time(e.get("time"))
        ctr = e.get("container", "")
        if "Interior NetNS Link Routes" in msg:
            t_net_done = ts
        elif msg == "About to run runsc create" and ctr == "pause":
            t_create_pause = ts
        elif msg == "About to run runsc start" and ctr == "pause":
            t_start_pause = ts
        elif msg == "About to run runsc create" and ctr != "pause":
            t_create_app = ts
        elif msg == "About to run runsc start" and ctr != "pause":
            t_start_app = ts
        elif msg == "Readyz reached 200":
            t_readyz_done = ts
            readyz_elapsed = parse_slog_duration(e.get("elapsed", 0))

    if t_net_done and t_start:
        result["1_net_setup"] = t_net_done - t_start
    if t_create_pause and t_net_done:
        result["2_rootfs_compose"] = t_create_pause - t_net_done
    if t_start_pause and t_create_pause:
        result["3_runsc_create_pause"] = t_start_pause - t_create_pause
    if t_create_app and t_start_pause:
        result["4_runsc_start_pause"] = t_create_app - t_start_pause
    if t_start_app and t_create_app:
        result["5_runsc_create_app"] = t_start_app - t_create_app
    if t_readyz_done and t_start_app and readyz_elapsed is not None:
        result["6_runsc_start_app"] = max(0.0, (t_readyz_done - t_start_app) - readyz_elapsed)
        result["7_readyz_wait"] = readyz_elapsed
    result["ateom_gvisor_total"] = t_end - t_start

    return result


def profile_lifecycle(actor_name, template_name):
    metrics = {
        "initial_activation": {},
        "suspend": {},
        "resume_atelet": {},
        "resume_ateom": {},
        "e2e": {},
    }

    # 0. CREATE ACTOR (Metadata-only: born SUSPENDED)
    print(f"[*] Creating actor {actor_name} in {ATESPACE} (born SUSPENDED)...")
    run_cmd(
        f"kubectl ate create actor {actor_name} -a {ATESPACE} --template {template_name}",
        check=True,
    )
    actor_uid = get_actor_uid(actor_name, ATESPACE)
    print(f"    Actor UID: {actor_uid}")

    # 1. INITIAL ACTIVATION (Cold Boot if no golden snapshot, else Golden Restore)
    print(f"[*] Triggering Initial Activation (resume #1) for {actor_name} ({SANDBOX_TYPE})...")
    t0 = time.time()
    run_cmd(f"kubectl ate resume actor {actor_name} -a {ATESPACE}", check=True)
    metrics["e2e"]["1. Initial Activation (Client)"] = time.time() - t0

    worker_selector = f"ate.dev/worker-pool={WORKER_POOL}"
    worker_logs = extract_json_logs(worker_selector, WORKER_NAMESPACE)

    if SANDBOX_TYPE == "microvm":
        boot_logs = [
            l for l in worker_logs
            if l.get("msg") == "Actor boot phases" and l.get("id") == actor_uid
        ]
        if boot_logs:
            latest = boot_logs[-1]
            for k in ("vsock_wait", "agent_dial", "containers", "readyz", "since_boot"):
                metrics["initial_activation"][k] = parse_slog_duration(latest.get(k, 0))
    elif SANDBOX_TYPE == "gvisor":
        metrics["initial_activation"] = parse_gvisor_cold_boot_granular(
            worker_logs, actor_name, actor_uid
        )

    # 2. LIVE SUSPEND (Checkpoint + Upload to Object Store)
    print(f"[*] Triggering Live Suspend for {actor_name}...")
    t0 = time.time()
    run_cmd(f"kubectl ate suspend actor {actor_name} -a {ATESPACE}", check=True)
    metrics["e2e"]["2. Live Suspend (Client)"] = time.time() - t0

    worker_logs = extract_json_logs(worker_selector, WORKER_NAMESPACE)
    if SANDBOX_TYPE == "microvm":
        # ateom-microvm logs "Actor checkpointed" with slog.Duration fields (int64 ns)
        ckp_logs = [
            l for l in worker_logs
            if l.get("msg") == "Actor checkpointed" and l.get("id") == actor_uid
        ]
        if ckp_logs:
            latest = ckp_logs[-1]
            for k in ("pause", "snapshot", "durable_dir", "rootfs_upper", "teardown"):
                metrics["suspend"][k] = parse_slog_duration(latest.get(k, 0))
    elif SANDBOX_TYPE == "gvisor":
        # ateom-gvisor emits EmitLifecycleLog ("Actor checkpointing" -> "Actor checkpointed")
        t_ckp_start, t_ckp_end = None, None
        for l in worker_logs:
            labels = get_actor_labels(l)
            if labels.get("ate.actor.uid") == actor_uid or labels.get("ate.actor.name") == actor_name:
                msg = l.get("message") or ""
                if msg == "Actor checkpointing":
                    t_ckp_start = parse_k8s_time(l.get("time"))
                elif msg == "Actor checkpointed":
                    t_ckp_end = parse_k8s_time(l.get("time"))
        if t_ckp_start and t_ckp_end:
            metrics["suspend"]["ateom_checkpoint_total"] = t_ckp_end - t_ckp_start

    # 3. RESUME FROM SNAPSHOT (Restore)
    print(f"[*] Triggering Snapshot Resume (resume #2) for {actor_name}...")
    t0 = time.time()
    run_cmd(f"kubectl ate resume actor {actor_name} -a {ATESPACE}", check=True)
    metrics["e2e"]["3. Snapshot Resume (Client)"] = time.time() - t0

    # 3a. Scrape atelet's "Restore timing breakdown" (float64 seconds)
    restore_logs = extract_json_logs("app=atelet", "ate-system", "Restore timing breakdown")
    my_restores = [
        l for l in restore_logs
        if l.get("ate.actor.uid") == actor_uid or l.get("ate.actor.name") == actor_name
    ]
    if my_restores:
        latest = my_restores[-1]
        for phase in (
            "volume_mount",
            "manifest_fetch",
            "sandbox_assets",
            "download",
            "oci_unpack",
            "ateom_restore",
            "total",
        ):
            key = f"ate.actor.restore.duration.{phase}"
            if key in latest:
                metrics["resume_atelet"][phase] = float(latest[key])
        if "ate.actor.restore.duration.total" in latest:
            metrics["e2e"]["3. Snapshot Resume (atelet Server Total)"] = float(
                latest["ate.actor.restore.duration.total"]
            )

    # 3b. Scrape ateom's internal restore breakdown (int64 nanoseconds -> seconds)
    worker_logs = extract_json_logs(worker_selector, WORKER_NAMESPACE)
    if SANDBOX_TYPE == "gvisor":
        ateom_restores = [
            l for l in worker_logs
            if l.get("msg") == "ateom restore breakdown" and l.get("actor") == actor_name
        ]
        if ateom_restores:
            latest = ateom_restores[-1]
            for k in ("net", "rootfs_compose", "runsc_create", "runsc_restore", "readyz", "total"):
                metrics["resume_ateom"][k] = parse_slog_duration(latest.get(k, 0))
    elif SANDBOX_TYPE == "microvm":
        ateom_restores = [
            l for l in worker_logs
            if l.get("msg") == "Actor restore phases" and l.get("id") == actor_uid
        ]
        if ateom_restores:
            latest = ateom_restores[-1]
            for k in (
                "prep",
                "bundles",
                "upper_join",
                "lowers",
                "durable",
                "tap",
                "vmm_launch",
                "vm_restore",
                "resume",
                "readyz",
                "total",
            ):
                metrics["resume_ateom"][k] = parse_slog_duration(latest.get(k, 0))

    return metrics


def print_table(title, columns, data_dict):
    print(f"\n=== {title} ===")
    row_format = "{:<35} | {:<15}"
    print(row_format.format(columns[0], columns[1]))
    print("-" * 54)
    for key, val in data_dict.items():
        if isinstance(val, (float, int)):
            print(row_format.format(key, f"{val:.4f}s"))
        else:
            print(row_format.format(key, str(val)))


def print_comparison_table(title, columns, data_disabled, data_enabled):
    print(f"\n=== {title} ===")
    row_format = "{:<30} | {:<12} | {:<12} | {:<12}"
    print(row_format.format(columns[0], "Disabled", "Enabled", "Delta"))
    print("-" * 74)

    all_keys = list(data_disabled.keys())
    for k in data_enabled.keys():
        if k not in all_keys:
            all_keys.append(k)

    for key in all_keys:
        val_dis = data_disabled.get(key, 0.0)
        val_en = data_enabled.get(key, 0.0)
        if isinstance(val_dis, (float, int)) and isinstance(val_en, (float, int)):
            diff = val_en - val_dis
            print(row_format.format(key, f"{val_dis:.4f}s", f"{val_en:.4f}s", f"{diff:+.4f}s"))
        else:
            print(row_format.format(key, str(val_dis), str(val_en), "-"))


def main():
    global SANDBOX_TYPE, ATESPACE, WORKER_POOL, WORKER_NAMESPACE

    parser = argparse.ArgumentParser(description="Substrate Image Streaming & Lifecycle Benchmark")
    parser.add_argument(
        "--compare-image-streaming",
        action="store_true",
        help="Run profiling twice (streaming disabled vs. enabled on atelet) and compare.",
    )
    parser.add_argument("--sandbox-type", type=str, default="gvisor", choices=["gvisor", "microvm"])
    parser.add_argument("--atespace", type=str, default="ate-demo")
    parser.add_argument("--template", type=str, default="test-template")
    parser.add_argument("--worker-pool", type=str, default="default-pool")
    parser.add_argument("--worker-namespace", type=str, default="ate-demo")
    args = parser.parse_args()

    SANDBOX_TYPE = args.sandbox_type.lower()
    ATESPACE = args.atespace
    WORKER_POOL = args.worker_pool
    WORKER_NAMESPACE = args.worker_namespace
    base_timestamp = int(time.time())

    if args.compare_image_streaming:
        print("==================================================")
        print(" RUN 1: BASELINE (Image Streaming DISABLED)")
        print("==================================================")
        toggle_image_streaming(False)
        actor_disabled = f"bench-base-{base_timestamp}"
        metrics_disabled = profile_lifecycle(actor_disabled, args.template)

        print("\n==================================================")
        print(" RUN 2: COMPARISON (Image Streaming ENABLED)")
        print("==================================================")
        toggle_image_streaming(True)
        actor_enabled = f"bench-stream-{base_timestamp}"
        metrics_enabled = profile_lifecycle(actor_enabled, args.template)

        print("\n" + "=" * 74)
        print(" IMAGE STREAMING COMPARISON RESULTS")
        print("=" * 74)
        print_comparison_table(
            "1. INITIAL ACTIVATION PHASES",
            ["Phase"],
            metrics_disabled["initial_activation"],
            metrics_enabled["initial_activation"],
        )
        print_comparison_table(
            "2. SUSPEND PHASES",
            ["Phase"],
            metrics_disabled["suspend"],
            metrics_enabled["suspend"],
        )
        print_comparison_table(
            "3A. RESTORE PHASES (atelet)",
            ["Phase"],
            metrics_disabled["resume_atelet"],
            metrics_enabled["resume_atelet"],
        )
        print_comparison_table(
            "3B. RESTORE SUB-PHASES (ateom)",
            ["Phase"],
            metrics_disabled["resume_ateom"],
            metrics_enabled["resume_ateom"],
        )
        print_comparison_table(
            "4. END-TO-END (E2E) TIMES",
            ["Operation"],
            metrics_disabled["e2e"],
            metrics_enabled["e2e"],
        )

        print("\n[*] Cleaning up actors...")
        run_cmd(f"kubectl ate delete actor {actor_disabled} -a {ATESPACE}")
        run_cmd(f"kubectl ate delete actor {actor_enabled} -a {ATESPACE}")
    else:
        print("==================================================")
        print(f" Substrate Single Benchmark (Sandbox: {SANDBOX_TYPE.upper()})")
        print("==================================================")
        actor_name = f"bench-actor-{base_timestamp}"
        metrics = profile_lifecycle(actor_name, args.template)

        print_table("1. INITIAL ACTIVATION PHASES", ["Phase", "Duration"], metrics["initial_activation"])
        print_table("2. SUSPEND PHASES", ["Phase", "Duration"], metrics["suspend"])
        print_table("3A. RESTORE PHASES (atelet)", ["Phase", "Duration"], metrics["resume_atelet"])
        print_table("3B. RESTORE SUB-PHASES (ateom)", ["Phase", "Duration"], metrics["resume_ateom"])
        print_table("4. END-TO-END (E2E) TIMES", ["Operation", "Duration"], metrics["e2e"])

        print(f"\n[*] Cleaning up {actor_name}...")
        run_cmd(f"kubectl ate delete actor {actor_name} -a {ATESPACE}")

    print("[*] Done.")


if __name__ == "__main__":
    main()
```