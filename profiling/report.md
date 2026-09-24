# Event timeline — scikit-learn\_\_scikit-learn-10908

Runs of 2026-09-22. Image f3903e9da732, **1 473 765 596 bytes** (1 473.77 MB / 1 405 MiB), 10 layers.&nbsp;

Row tags:

- **M** measured directly by the script  
- **R** residual — the parent minus its measured children, attributed by reading the code  
- **C** a constant, not a measurement  
- \* **script / harness overhead** — cost the profiler adds, not Substrate server work

> \[\!NOTE\] The cold boot pulled onto **smpv**; lifecycle step 2 landed on **2lb5** and paid a *second* full pull of the same image; step 4 migrated back to **smpv** and hit the cache. That gives two independent pulls of one identical image, minutes apart.

---

## A. Cold boot — the template build

**54 617.4 ms** end to end.

| \# | event | ms |  | what happens | code |
| :---- | :---- | :---- | :---- | :---- | :---- |
| **1** | **harness prologue** \* | **4 491.8** | **M** | Shell only, before Substrate is asked for anything: crane digest, create the atespace, check for an existing template. Measured as createTime − t0. | [swebench-template.sh:52-70](https://github.com/agent-substrate/substrate/blob/main/profiling/swebench-template.sh#L52-L70) · [profile\_coldboot.py:114](https://github.com/agent-substrate/substrate/blob/main/profiling/profile_coldboot.py#L114) |
| **2** | **template created → reconciler noticed** | **7 437.4** | **M** | Dead wait. Creating a template enqueues nothing; the only queue.Add is in resync, which ticks every 20 s. A uniform draw over \[0, 20 s\] — this sample landed at 7.4 s. | [template\_reconciler.go:104](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/template_reconciler.go#L104) · [:123-124](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/template_reconciler.go#L123-L124) · [ateapi/main.go:89](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/main.go#L89) |
| **3** | **reconciler noticed → snapshot deadline** | **39 381.3** | **M** | The reconciler takes the lease, creates the golden actor, resumes it, and stamps the warmup deadline. | [template\_reconciler.go:168-294](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/template_reconciler.go#L168-L294) |
| 3.1 | lease, GetActorTemplate, GetTag, CreateActor, scheduling, atelet pre/post-pull work | **24.4** | R | Everything the control plane does around the pull. **24 ms** — negligible next to the pull and the warmup. Includes AcquireLease, ensureActorExists, worker selection, and atelet's bundle/network/dial work. | [:168](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/template_reconciler.go#L168) · [:404-431](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/template_reconciler.go#L404-L431) · [workflow\_resume.go:503](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_resume.go#L503) |
| 3.2 | **registry pull** — 1 473.77 MB, 10 layers, **76.6 MB/s**, node smpv | **19 233.5** | **M** | Store.pull: manifest \+ config, then all 10 layers unpacked 4-at-a-time. took covers network *and* gunzip *and* untar to disk. | [imagecache.go:537](https://github.com/agent-substrate/substrate/blob/main/internal/imagecache/imagecache.go#L537) · [:591](https://github.com/agent-substrate/substrate/blob/main/internal/imagecache/imagecache.go#L591) · [:643-646](https://github.com/agent-substrate/substrate/blob/main/internal/imagecache/imagecache.go#L643-L646) |
| 3.3 | ateom boot from spec | 123.4 | M | RunActor. No snapshot exists, so this is the one true boot in the whole dataset. | [ateom-gvisor/main.go:661-760](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L661-L760) |
| 3.3.1 | Actor starting → first runsc | 25.1 | M | SetupBundleRootfs composes the pause bundle's overlayfs — cached layers as lowerdirs, private upper. A mount, not a copy. | [main.go:661](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L661) → [:721](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L721) |
| 3.3.2 | runsc create\[\_pause\] | 31.7 | M | Creates the gVisor **sandbox**. The Sentry starts here; the netns is claimed. | [runsc.go:77](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L77) |
| 3.3.3 | runsc start\[\_pause\] | 36.8 | M | Starts the pause process. The sandbox now outlives individual containers. | [runsc.go:122](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L122) |
| 3.3.4 | runsc create\[testbed\] | 7.5 | M | Workload container *inside* the existing sandbox — cheap because 3.3.2 paid for the Sentry. | [runsc.go:77](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L77) |
| 3.3.5 | runsc start\[testbed\] → Actor started | 22.3 | M | Starts /bin/sleep infinity, then ateom emits Actor started. | [runsc.go:122](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L122) → [main.go:760](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L760) |
| 3.4 | golden-snapshot warmup | 20 000.0 | C | Not measured — the deadline is stamped now \+ warmup after ResumeActor returns. 20 s because no container declares readyz; it would be 0 if every one did. | [template\_reconciler.go:292](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/template_reconciler.go#L292) · [:390](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/template_reconciler.go#L390) |
| **4** | **snapshot deadline → golden tag** | **3 306.9** | **M** | Checkpoint the golden actor, upload, tag, delete the actor. | [profile\_coldboot.py:118](https://github.com/agent-substrate/substrate/blob/main/profiling/profile_coldboot.py#L118) |
| 4.1 | ateom checkpoint | 91.2 | M | CheckpointActor. | [main.go:782-866](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L782-L866) |
| 4.1.1 | Actor checkpointing → first runsc | 0.087 | M | Argument marshalling. The smallest value in this run. | [main.go:782](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L782) |
| 4.1.2 | runsc checkpoint\[\_pause\] | 27.0 | M | Serializes the whole sandbox: Sentry state, memory, FDs. Only \_pause is checkpointed because one checkpoint covers the sandbox. | [runsc.go:149](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L149) |
| 4.1.3 | runsc kill\[testbed\] | 7.9 | M | Signals the workload once its state is captured. | [runsc.go:376](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L376) |
| 4.1.4 | runsc wait\[testbed\] | 6.1 | M | Reaps it. | [runsc.go:406](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L406) |
| 4.1.5 | runsc kill\[\_pause\] | 6.1 | M | Signals the sandbox. | [runsc.go:376](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L376) |
| 4.1.6 | runsc wait\[\_pause\] → Actor checkpointed | 44.0 | M | Sandbox teardown — the largest checkpoint leaf, and it is teardown rather than serialization. | [runsc.go:406](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L406) → [main.go:866](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L866) |
| 4.2 | snapshot upload, CreateTag, DeleteActor, status write, script poll \* | 3 215.7 | R | atelet uploads the checkpoint, ateapi copies it under the golden tag, deletes the golden actor, writes status. The script then polls for the tag on a 5 s interval — the whole segment is only 3.3 s, so the poll cost **less than one full interval** here. | [template\_reconciler.go:325-346](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/template_reconciler.go#L325-L346) · [swebench-template.sh:108-142](https://github.com/agent-substrate/substrate/blob/main/profiling/swebench-template.sh#L108-L142) |

&nbsp;

---

## B. Lifecycle — four separate client invocations

**Not a continuous timeline.** Each step is its own kubectl-ate process with a 3 s settle sleep between, so the steps do not sum to anything.

### Step 1 · create actor record — client\_wall 444.3 ms

| event | ms |  | what happens | code |
| :---- | :---- | :---- | :---- | :---- |
| ateapi\_rpc\_total | 2.8 | M | One CreateActor. Writes an actor record and freezes the template's golden tag as the actor's source — the actor is **born holding the golden snapshot**, borrowed under the tag's prefix. | [ateinterceptors.go:52](https://github.com/agent-substrate/substrate/blob/main/internal/ateinterceptors/ateinterceptors.go#L52) · [actor.go:92-95](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/actor.go#L92-L95) · [:132-138](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/actor.go#L132-L138) |
| kubectl-ate startup \+ mTLS \* | 441.5 | R | client\_wall − ateapi\_rpc\_total. Go process start, kubeconfig load, TLS handshake. **159× the server work.** | [profile\_lifecycle.py:166-168](https://github.com/agent-substrate/substrate/blob/main/profiling/profile_lifecycle.py#L166-L168) |

No worker, no images. The actor is born suspended.

### Step 2 · initial activation — client\_wall 21 557.7 ms — node 2lb5, cache **MISS**

| event | ms |  | what happens | code |
| :---- | :---- | :---- | :---- | :---- |
| client startup \+ mTLS \* | 420.6 | R | Per-invocation CLI cost. | [profile\_lifecycle.py:166-168](https://github.com/agent-substrate/substrate/blob/main/profiling/profile_lifecycle.py#L166-L168) |
| **ateapi\_rpc\_total** | **21 137.1** | **M** | One ResumeActor. The scheduler picked 2lb5, which had never seen this image — so the golden restore paid a second full pull. | [ateinterceptors.go:52](https://github.com/agent-substrate/substrate/blob/main/internal/ateinterceptors/ateinterceptors.go#L52) |
| scheduling, store writes, ateapi bookkeeping | 12.3 | R | Everything in resumeActorLocked bar the atelet call: GetActor, lease, loadActorForResume, volumes, ensureWorkerAssigned, finalizeRunning. A subtraction, not a logged value. | [workflow\_resume.go:91-135](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_resume.go#L91-L135) |
| **atelet restore total** | **21 124.8** | **M** | RestoreActor on a cold node. | [ateattr.go:357-362](https://github.com/agent-substrate/substrate/blob/main/internal/ateattr/ateattr.go#L357-L362) |
| volume\_mount | 0.0006 | M | No volumes declared — an empty loop, 0.61 µs. | [ateattr.go:357](https://github.com/agent-substrate/substrate/blob/main/internal/ateattr/ateattr.go#L357) |
| manifest\_fetch | **121.0** | M | One GCS GET of the snapshot's **sandbox manifest** — a small file naming the runsc binaries that made the checkpoint and the SnapshotFiles the download needs. Not a registry call. **3.1× step 4's figure.** | [atelet/main.go:1029-1097](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/main.go#L1029-L1097) |
| ─ parallel ─ download | 73.2 | M | First errgroup leg: the golden snapshot from GCS. Not the critical path here. | [atelet/main.go:1135-1181](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/main.go#L1135-L1181) |
| ─ parallel ─ sandbox\_assets | 0.026 | M | Second leg, part one: runsc binary \+ pause image, already node-local. | [atelet/main.go:1186-1191](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/main.go#L1186-L1191) |
| ─ parallel ─ **oci\_unpack** ← **critical path**, contains the 20 800.2 ms pull (**70.9 MB/s**) | **20 804.3** | **M** | prepareOCIBundles → EnsureImage found an empty cache. **98.5% of the restore.** | [atelet/main.go:1196](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/main.go#L1196) → [oci.go:113](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/oci.go#L113) |
| ateom\_restore | 175.1 | M | The RPC to ateom after the join. | [ateattr.go:362](https://github.com/agent-substrate/substrate/blob/main/internal/ateattr/ateattr.go#L362) |
| Actor restoring → first runsc | 24.6 | M | Overlay compose against layers written seconds earlier — 3× step 4's 8.1 ms, the page cache is cold. | [main.go:961](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L961) → [:1021](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L1021) |
| runsc create\[\_pause\] | 54.7 | M | Sandbox shell. Also inflated vs step 4's 31.1 ms. | [runsc.go:77](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L77) |
| runsc restore\[\_pause\] | 14.9 | M | Rehydrates Sentry state from the checkpoint. | [runsc.go:265](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L265) |
| runsc create\[testbed\] | 7.8 | M | Workload container in the restored sandbox. | [runsc.go:77](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L77) |
| runsc restore\[testbed\] → Actor restored | 72.0 | M | Restores the workload's process state. | [runsc.go:265](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L265) → [main.go:1089](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L1089) |
| dialAteom, build workload spec | 24.3 | R | First contact with this node's ateom: fresh gRPC dial plus buildAteomWorkloadSpec. | [atelet/main.go:521](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/main.go#L521) · [:526](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/main.go#L526) |

### Step 3 · live suspend — client\_wall 760.9 ms

| event | ms |  | what happens | code |
| :---- | :---- | :---- | :---- | :---- |
| client startup \+ mTLS \* | 466.8 | R | Per-invocation CLI cost. | [profile\_lifecycle.py:166-168](https://github.com/agent-substrate/substrate/blob/main/profiling/profile_lifecycle.py#L166-L168) |
| **ateapi\_rpc\_total** | **294.1** | **M** | One SuspendActor. Checkpoints, uploads, releases the worker. | [ateinterceptors.go:52](https://github.com/agent-substrate/substrate/blob/main/internal/ateinterceptors/ateinterceptors.go#L52) |
| snapshot upload to object storage | 194.7 | R | The dominant real cost. Scales with *dirty* sandbox state, not rootfs size. Scope is FULL because the template sets onCommit: FULL. | [workflow\_suspend.go:209-255](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_suspend.go#L209-L255) |
| **ateom checkpoint** | **95.6** | **M** | Same shape as cold-boot 4.1, on the live actor. | [main.go:782-866](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L782-L866) |
| Actor checkpointing → first runsc | 0.056 | M | Marshalling only. **The finest value in this run — 56 µs.** | [main.go:782](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L782) |
| runsc checkpoint\[\_pause\] | 28.1 | M | Serializes the sandbox. | [runsc.go:149](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L149) |
| runsc kill\[testbed\] | 7.1 | M | Signals the workload. | [runsc.go:376](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L376) |
| runsc wait\[testbed\] | 6.8 | M | Reaps it. | [runsc.go:406](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L406) |
| runsc kill\[\_pause\] | 7.0 | M | Signals the sandbox. | [runsc.go:376](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L376) |
| runsc wait\[\_pause\] → Actor checkpointed | 46.4 | M | Sandbox teardown. | [runsc.go:406](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L406) → [main.go:866](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L866) |
| **finalize\_suspended** | **3.8** | **M** | ateapi's post-checkpoint store transaction, one log record with five duration fields. | [workflow\_suspend.go:345](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_suspend.go#L345) |
| get\_actor | 0.202 | M | Re-read the actor record. | [workflow\_suspend.go:348](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_suspend.go#L348) |
| release\_worker | 2.197 | M | Return the worker to the pool — slowest leaf, and the only one touching shared state. | [workflow\_suspend.go:349](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_suspend.go#L349) |
| refetch\_actor | 0.190 | M | Re-read after the release, to write against current state. | [workflow\_suspend.go:345](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_suspend.go#L345) |
| release\_snapshot | 0.041 | M | Drop the reference to the borrowed golden snapshot — **this is where the actor stops borrowing and takes over its own prefix.** | [workflow\_suspend.go:345](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_suspend.go#L345) |
| update\_actor | 1.141 | M | Commit the suspended status. | [workflow\_suspend.go:345](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_suspend.go#L345) |

### Step 4 · resume from snapshot — client\_wall 703.4 ms — migration 2lb5 → smpv, cache **HIT**

| event | ms |  | what happens | code |
| :---- | :---- | :---- | :---- | :---- |
| client startup \+ mTLS \* | 478.9 | R | Per-invocation CLI cost — **2.1× the server work below.** | [profile\_lifecycle.py:166-168](https://github.com/agent-substrate/substrate/blob/main/profiling/profile_lifecycle.py#L166-L168) |
| **ateapi\_rpc\_total** | **224.6** | **M** | One ResumeActor, reading the actor's **own** snapshot written by step 3\. Migrated back to smpv, where the cold boot had already pulled the image. **The fastest resume recorded to date.** | [ateinterceptors.go:52](https://github.com/agent-substrate/substrate/blob/main/internal/ateinterceptors/ateinterceptors.go#L52) |
| scheduling, store writes | 9.5 | R | Same resumeActorLocked steps. The node came from the Picked worker log; the duration is a subtraction. | [workflow\_resume.go:91-135](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_resume.go#L91-L135) · [:503](https://github.com/agent-substrate/substrate/blob/main/cmd/ateapi/internal/controlapi/workflow_resume.go#L503) |
| **atelet restore total** | **215.0** | **M** | RestoreActor with a warm layer cache. | [ateattr.go:362](https://github.com/agent-substrate/substrate/blob/main/internal/ateattr/ateattr.go#L362) |
| volume\_mount | 0.0007 | M | Empty loop. | [ateattr.go:357](https://github.com/agent-substrate/substrate/blob/main/internal/ateattr/ateattr.go#L357) |
| manifest\_fetch | 38.6 | M | Same GCS sandbox-manifest GET as step 2 — but **82 ms faster**, on a different node. | [atelet/main.go:1029-1097](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/main.go#L1029-L1097) |
| ─ parallel ─ download ← **critical path** | 45.6 | M | The actor's own snapshot from GCS. With the image cached, this leg wins. | [atelet/main.go:1135-1181](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/main.go#L1135-L1181) |
| ─ parallel ─ sandbox\_assets | 0.025 | M | Node-local. | [atelet/main.go:1186-1191](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/main.go#L1186-L1191) |
| ─ parallel ─ oci\_unpack (image HIT) | 0.55 | M | Bundle composition only. **37 800× cheaper than step 2's.** | [oci.go:113](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/oci.go#L113) |
| ateom\_restore | 127.6 | M | RPC to ateom. 47.5 ms faster than step 2 on a node whose page cache is warm. | [ateattr.go:362](https://github.com/agent-substrate/substrate/blob/main/internal/ateattr/ateattr.go#L362) |
| Actor restoring → first runsc | 8.1 | M | Overlay compose, warm. | [main.go:1021](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L1021) |
| runsc create\[\_pause\] | 31.1 | M | Sandbox shell. | [runsc.go:77](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L77) |
| runsc restore\[\_pause\] | 15.5 | M | Sentry rehydrate. | [runsc.go:265](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L265) |
| runsc create\[testbed\] | 7.0 | M | Workload container. | [runsc.go:77](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L77) |
| runsc restore\[testbed\] → Actor restored | 65.1 | M | Workload process restore — largest leaf, 29% of the whole resume. | [runsc.go:265](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/runsc.go#L265) → [main.go:1089](https://github.com/agent-substrate/substrate/blob/main/cmd/ateom-gvisor/main.go#L1089) |
| dialAteom, spec build | 3.3 | R | smpv's ateom was already dialled during the cold boot. | [atelet/main.go:521](https://github.com/agent-substrate/substrate/blob/main/cmd/atelet/main.go#L521) |

**Two independent clocks agree.** atelet's ateom\_restore times the RPC; ateom's markers time its own work. Step 2: 175.1 vs 174.0. Step 4: 127.6 vs 126.8. The 0.8–1.2 ms gap is the round trip. Unlike the segment sums above, these had every opportunity to disagree.

---

## Everything marked \* — the harness's own cost

| where | ms | what it is |
| :---- | :---- | :---- |
| A.1 harness prologue | 4 491.8 | crane digest \+ atespace create \+ template existence check. Entirely the script. |
| A.4.2 tag poll | ≤ 3 306.9 of 3 306.9 | 5 s poll interval, but the whole segment finished in 3.3 s, so under one interval was consumed. |
| B.1 client residual | 441.5 | kubectl-ate process start \+ mTLS. |
| B.2 client residual | 420.6 | ditto |
| B.3 client residual | 466.8 | ditto |
| B.4 client residual | 478.9 | ditto |
| between every lifecycle step | 3 000 × 3 | \--settle sleeps. **In no table** — they fall in the gaps, which is why section B does not sum. |

Not overhead despite appearances: **A.3.4's 20 000 ms warmup** and **A.2's 7 437.4 ms resync wait** are both paid with no profiler attached.

---

&nbsp;