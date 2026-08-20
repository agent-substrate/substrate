# Substrate Benchmarking

This is the nascent suite for benchmarking Substrate's performance at scale.

The suite also measures the telemetry volume and the capacity of the OTel
collector: how much trace data and metric data substrate and its actors send,
and if the collector can accept it. To make a measurement, read
[telemetry/README.md](telemetry/README.md). For the prerequisites and the
scenario ladder, read [observability.md](observability.md).

## Deploy benchmarks

> [!IMPORTANT]
> Source the environment configuration file (e.g., `source .ate-dev-env.sh`)
> first so `PROJECT_ID`, `BUCKET_NAME`, etc. are set.
>
> **For Local Kind Clusters**: Set `KO_DOCKER_REPO` to the local registry so images are pulled without remote cloud credentials:
> ```bash
> export KO_DOCKER_REPO="localhost:5001"
> export BUCKET_NAME="ate-snapshots"
> ```

Note that deploying the benchmarks does not run them. You must visit Locust's
web UI to start a test.

A single wrapper deploys the scale workloads, builds and pushes the Locust
image, then deploys the Locust workers:

```bash
./benchmarking/deploy_locust.sh --deploy
```

Useful flags:

* `--worker-count N` — number of `WorkerPool` replicas (default 1).
* `--skip-build` — reuse the existing `:latest` locust image (skip the
  `docker build && docker push` step).

To tear everything down (locust then workloads, in reverse order):

```bash
./benchmarking/deploy_locust.sh --delete
```

The same operations are also reachable from the top-level installer for
convenience:

```bash
./hack/install-ate.sh --deploy-benchmarks
./hack/install-ate.sh --delete-benchmarks
```

The installer accepts `--benchmark-worker-count N` (default `1`).
`--skip-build` is only available when invoking
`benchmarking/deploy_locust.sh` directly.

## Running Tests

### Locust Web UI
* Run `kubectl port-forward svc/locust -n benchmarking 8089:8089`
* Visit `http://localhost:8089` in your browser to configure and start the load test.

The different user classes you can select are different types of load behaviors
you can throw at the system. Note that the "CounterUser" load type requires
that the counter demo be installed.

You can also configure things like the number of users, how quickly those users
are spawned, the frequency with which requests are made and whether or not tracing is
enabled.

User classes implemented in boomer rather than Python are selected at deploy
time — the stack runs one per deployment:

```bash
./benchmarking/locust/deploy.sh --deploy --user-class durdir
```

### Headless (automation only)

`runner.py` runs a test without the web UI, writing CSVs, logs and traces to
`--dest`. The nightly automation submits it as a Job on the test cluster; it is
not a local entry point. See [automation/README.md](automation/README.md).

```bash
python3 runner.py -f tests/<user-class>.py -t 1m -u 1 --name <run-name> --dest /tmp/bench
```

One flag controls the optional post-run measurements described in
[Benchmark output files](#benchmark-output-files):

* `--cluster-facts` / `--no-cluster-facts`: read node capacity and worker pod
  count from the Kubernetes API once the run ends, to derive density frontiers.
  On by default. Pass `--no-cluster-facts` to skip Kubernetes API discovery.

Test-specific flags are appended to the same command; see the sections below.

### DurDir Benchmark

The DurDir benchmark evaluates actor suspend/resume performance, disk persistence overhead,
and state restoration latency when a durable directory is attached to the actor.

#### DurDir Configuration Knobs

* `--durdir-file-size-bytes`: Size in bytes of the data file (default `8388608` = 8 MiB).
* `--resume-mode`: Resume trigger mode:
  * `explicit` (default): Client invokes the `ResumeActor` RPC before sending traffic.
  * `implicit`: Client sends traffic through the router without an explicit wake RPC, testing traffic-triggered resume.
* `--durdir-read-mode`: Verification read mode:
  * `data` (default): Server returns full payload bytes for client-side SHA-256 verification.
  * `digest`: Server hashes the file and returns size and digest, reducing network transfer.
* `--durdir-template`: ActorTemplate name:
  * `glutton-durdir-data` (default): Attaches a durable data directory without memory snapshot restore.
  * `glutton-durdir-full`: Attaches a durable data directory and performs a full memory snapshot restore.

#### DurDir Reported Metrics

* `DurDirWrite`: Initial truncate-write creating the data file.
* `DurDirServeInitial`: First read immediately following file creation.
* `SuspendActor`: Actor suspend latency (snapshot creation + persistence upload).
* `ResumeActor`: Actor resume latency.
* `DurDirServeAfterResume`: First read after resume (measures page faults / lazy load overhead on restored volume).
* `DurDirServeWarm`: Subsequent read within the same active cycle (cached state baseline).
* `DurDirOverwrite`: In-place file overwrite with checksum verification.

### Sweperf Benchmark

The sweperf benchmark replays a recorded SWE-Perf task inside an actor, suspending and
resuming between cycles to measure the cost of actor state transitions under a realistic
agent workload. One task is four cycles by default.

#### Sweperf Reported Metrics

All rows are in milliseconds. CEL (command execution latency) is the time the trace commands
ran inside the sandbox, as reported by `replay.py`.

* `ResumeToFirstExec`: Resume RPC start until the sandbox accepts the cycle's `/execute`.
* `CycleCEL`: CEL for one cycle.
* `TaskCEL`: CEL summed over one task.
* `TaskWallClock`: Client wall clock for one task, excluding inter-cycle think time.
* `CreateAtespace`, `CreateActor`, `ResumeActor`, `SuspendActor`, `DeleteActor`: Server-side
  elapsed time for each control-plane RPC from the response trailer, or client time without one.
* `<rpc>_rtt`: Client round trip for the RPC of the same name, recorded only when the trailer is
  present, so network and queueing overhead stays visible separately.
* `Workload_Cycle_<n>`: Client time for cycle `n`: `POST /execute` plus `/status` polling until
  the job finishes. Polled every `--sweperf-poll-interval-ms` (default 100), so this row sits
  up to one interval above the job's actual end.

The liveness check at session start already has the actor running, so the first cycle's
resume is a no-op. Its successful `ResumeActor`, `ResumeActor_rtt` and `ResumeToFirstExec`
samples are not recorded; failures still are.

### Agent-Session Benchmark

The agent-session benchmark (`--user-class agentsession`) emulates a fleet of
coding agents on Substrate. Each locust user is one session: an actor driven
through a scripted 20-step coding task ("clone a repo, build it, fix a test,
refactor, package it"), where every step costs the sandbox the CPU, memory,
disk, and network a real coding agent's action would. Between steps the agent
is "waiting for the LLM to think": the driver **suspends the actor** for the
step's think time, and the next step's first request **wakes it through the
atenet router** (request parking). Mostly-idle sessions plus fast wake is
exactly the oversubscription story this measures.

The entire workload is the declarative script in
[`internal/benchmarking/boomer/agentsession/script.go`](../internal/benchmarking/boomer/agentsession/script.go)
— one table entry per step, naming what the agent is doing and the resource
ops that act it out. To change the workload, edit the table. Each session is
an actor from the stock `glutton` template; steps are sequences of glutton
RPCs:

| Step | The agent is… | Sandbox effect |
|---|---|---|
| 01_read_task | reading the task prompt | fill 32Mi RAM (agent context) |
| 02_clone_repo | `git clone` | 16Mi arrives over the network → disk; 0.5s CPU |
| 03_explore_tree | listing/grepping the tree | disk read (digest); 0.2s CPU |
| 04_read_key_files | opening files into context | disk read shipped back out; 8Mi RAM churn |
| 05_install_deps | `pip install` / `go mod download` | 32Mi over the network → disk; 1.5s CPU ×2 |
| 06_first_build | first full build | fill 64Mi RAM; 3s CPU ×2; 24Mi disk write |
| 07_run_unit_tests | running the test suite (one fails) | disk read; 2.5s CPU ×2 |
| 08_reason_about_failure | tracing the bug (long LLM turn, 8s think) | full RAM page-walk after the wake |
| 09_edit_source | applying the fix | 64Ki patch over the network; 4Mi disk write |
| 10_incremental_build | rebuilding changed packages | 1.2s CPU ×2; 8Mi disk write |
| 11_rerun_failed_test | re-running the failing test | 0.8s CPU |
| 12_write_new_tests | authoring regression tests (6s think) | 128Ki over the network; 2Mi disk write |
| 13_run_new_tests | running the new tests | disk read; 1s CPU |
| 14_full_test_suite | full-suite regression run | 4s CPU ×2; disk read; 8Mi RAM churn |
| 15_lint_format | lint + format pass | disk read; 0.9s CPU |
| 16_refactor | multi-file refactor (8s think) | 512Ki over the network; 6Mi disk write; 0.6s CPU |
| 17_rebuild | full rebuild | 32Mi RAM churn; 2s CPU ×2; 16Mi disk write |
| 18_final_test_suite | final full-suite run | 3.5s CPU ×2 |
| 19_package_artifact | building the release package | disk read; 1s CPU; 24Mi disk write |
| 20_commit_and_summarize | committing + summarizing | 256Ki disk write; 24Mi shipped back out; RAM walk |

The script needs bigger actors than the 256Mi default: it holds ~96Mi of
RAM arrays + ~110Mi of tmpfs files, and the observed guest peak with
allocator transients is ~320Mi (512Mi OOMs). Deploy the workloads with
`--actor-memory 1Gi`. `TestSessionBudgets` bounds the script-declared bytes
only, so script edits that grow the working set fail the test and force this
guidance to be revisited.

```sh
./benchmarking/deploy_locust.sh --deploy --sandbox-class gvisor --actor-memory 1Gi
./benchmarking/locust/deploy.sh --deploy --user-class agentsession
```

#### Agent-Session Configuration Knobs

* `--agentsession-think-scale` — multiplier on every think gap; 0.5 makes the
  fleet twice as chatty, 4.0 models slow reasoning models (default 1.0). Each
  gap gets ±20% jitter so sessions don't move in lockstep.
* `--resume-mode implicit|explicit` — implicit (default) lets the parked
  first request wake the actor; explicit issues ResumeActor before traffic.
* `--lifecycle-mode suspend|pause` — durable suspend (default) or node-local
  pause between steps.

#### Agent-Session Reported Metrics

* `WakeFirstTouch`: latency of a dedicated ping sent before each step's ops —
  in implicit mode that ping is what triggers the parked wake, so this row
  **is** the user-visible wake latency, unpolluted by the step's own work.
* `Step_<name>` (e.g. `Step_06_first_build`): wall time of that step's ops,
  think gap excluded.
* `SuspendActor` / `ResumeActor` / `CreateActor` / `DeleteActor`: control-plane
  lifecycle latencies.

### External Volume Storage Benchmark (`glutton_storage`)

The `glutton_storage` benchmark evaluates Substrate External Volume lifecycle performance (CSI volume dynamic provisioning, worker node attachment, sandbox mounting, and direct write I/O durability) under concurrent user load and oversubscription.

#### Storage Configuration Knobs

* `--storage-class-name`: Underlying CSI StorageClass name (default `csi-nfs-sc`, or e.g. `csi-hostpath-sc`, `standard-rwo`, `premium-rwo`). Can also be passed via `STORAGE_CLASS_NAME` env var to `deploy.sh` or `storageClassName` in `tests.yaml`.

#### Storage Reported Metrics

* `CreateAtespace`: Latency to ensure the benchmark atespace exists.
* `CreateActor`: Latency to create actor CRDs with external volume templates.
* `ResumeActorColdStart`: First resume latency, including direct CSI volume dynamic provisioning (`CreateVolume`), worker node attachment (`ControllerPublishVolume`), and sandbox mounting.
* `ResumeActor`: Warm volume re-attachment and sandbox mount latency.
* `GluttonWriteDisk`: HTTP/1.1 write I/O latency to `/mnt/storage` with `f.Sync()` disk durability flush.
* `SuspendActor`: Actor suspend latency including sandbox filesystem unmount and CSI volume detachment (`ControllerUnpublishVolume`).
* `DeleteActor`: Actor deletion and CSI volume destruction (`DeleteVolume`).

### Viewing Traces
You must have enabled otel tracing for your cluster to view traces.

You can find trace IDs by viewing the `logs` tab in the Locust UI

## Benchmark output files

A run writes the following to `--dest`. Each run produces them fresh; none of
them are checked into the repository.

* `status.json`: `locust_exit_code` and `stats_generated`. Deliberately just
  those two keys, because it is what CI orchestration reads to decide whether a
  trial ran at all.
* `stats.csv`, `stats_history.csv`, `failures.csv`, `exceptions.csv`: Locust's
  own CSV output.
* `logs.txt`, `traces.txt`: the runner log, and the trace IDs seen during the run.
* `stats.jsonl`: one JSON object per line, one per metric. Every row carries
  the same five keys: `timestamp`, `tag`, `test_name`, `metric`, and a flat
  `measurements` map holding that metric's numbers.

### Density frontiers

With cluster discovery enabled, `stats.jsonl` gains a `trial_summary` row
describing how densely actors are packed onto the hardware. Its `measurements`
map holds the raw facts and the derived numbers side by side.

* `machine_type`, `node_count`, `allocatable_cores`, `allocatable_ram_gb`
  (GiB), `worker_pod_count`: the measured facts, before any arithmetic.
  Capacity covers the nodes the worker pods are running on rather than the
  whole cluster, so a separate infrastructure pool is not counted. They are
  recorded so the ratios below can be re-derived later, or recomputed against
  a different denominator.
* `actors_per_node`, `actors_per_vcpu`, `actors_per_gb_ram`: the most actors
  Locust reported running, over the matching capacity. The `-u` flag only
  stands in when no sample was read.
* `actors_per_pod_p50`, `actors_per_pod_p90`, `actors_per_pod_p99`: actors per
  worker pod across the run. Reported as a distribution rather than one
  average, and it spans ramp-up too, because a custom load shape has no
  single user count to call steady.
* `aggregate_failure_ratio`: failures over requests for the run.
* `<operation>_failure_ratio`: the same ratio for every operation Locust
  reported, so each test carries its own names through. The operation name is
  lowercased with underscores, so `DurDirWrite` becomes
  `dur_dir_write_failure_ratio`. A key is absent when the test has no such
  row, and null when the row ran no requests.

The six `actors_per_*` ratios rest on three assumptions. Read them before
comparing numbers across runs:

* **Actors are derived, not counted.** Locust only sees virtual users, so the
  numerator is the peak user count times `--actors-per-user`. No server-side
  gauge counts resident actors: `ate.actor.stats.sampled_actors` drops any
  actor without a live resource measurement, so suspended ones fall out.
* **The denominators are read once, after the run.** A cluster that autoscaled
  mid-run is measured at its final size, so the ratio pairs a peak from one
  moment with a capacity from another.
* **The peak assumes every actor is alive at once.** A workload that creates
  and deletes actors as it goes never holds them all at the same time, so its
  real density is lower than reported.

The Kubernetes API is not required. If it is unreachable, or discovery was
skipped, the affected fields are written as `null` and the run still
succeeds. A `null` means the value was not measured. It never means zero.

## Optional: Prometheus + Grafana

Locust provides graphs, statistics, etc. via the UI. However, you
can install Prometheus/Grafana if you want richer details or
the ability to perform deeper analysis. Skip this section if
you're only using the Locust web UI.

```bash
kubectl apply -f benchmarking/monitoring.yaml
```

Once installed:

* Run `kubectl port-forward svc/grafana -n benchmarking 3000:3000`
* Visit `http://localhost:3000` in your browser.

## Development

### Generating gRPC Python clients

The clients are not checked in. The locust and nighthawk-ingress images
generate them at build time, and `hack/verify/python-protos.sh` compiles them
on every PR, so a proto change needs no extra step. For local use, such as
editor completion, run `benchmarking/locust/codegen/generate.sh`. It manages
its own virtual environment under `locust/codegen/venv`.

### Unit tests

`locust/unit_tests` covers the runner's helpers and needs no cluster. From the
repository root:

```bash
python3 -m unittest discover -s benchmarking/locust/unit_tests
```
