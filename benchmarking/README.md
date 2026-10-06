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
* `--worker-memory SIZE` — memory request and limit for each `WorkerPool` pod
  (default unset, the pod is unsized). Set it above half a node's allocatable
  memory to get one worker per node.
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
`--skip-build` and `--worker-memory` are only available when invoking
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

Three flags control the optional post-run measurements described in
[Benchmark output files](#benchmark-output-files):

* `--cluster-facts` / `--no-cluster-facts`: read node capacity and worker pod
  count from the Kubernetes API once the run ends, to derive density frontiers.
  On by default. Pass `--no-cluster-facts` to skip Kubernetes API discovery.
* `--prometheus-url`: the Prometheus to harvest server-side telemetry from.
  Defaults to the in-cluster service installed by
  [Optional: Prometheus + Grafana](#optional-prometheus--grafana).
* `--atelet-lag-s`: how long to wait after the run before reading the
  atelet's snapshot metrics. Defaults to 70.

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

The entire workload is a YAML script. The default,
[`internal/benchmarking/boomer/agentsession/scripts/coding-session.yaml`](../internal/benchmarking/boomer/agentsession/scripts/coding-session.yaml),
has one entry per step naming what the agent is doing and the resource ops
that act it out. To change the workload, edit it or add a sibling file and
select it with `--agentsession-script` (see [Writing a
script](#writing-an-agent-session-script)). Each session is an actor from
the stock `glutton` template; steps are sequences of glutton RPCs:

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
`--actor-memory 1Gi`. Every script declares that floor as
`min_actor_memory`; the worker reads the `glutton` template's memory limit
at start and refuses to run a script against a smaller actor, so a too-small
deployment fails loudly instead of showing up as OOM-flaky steps.

```sh
./benchmarking/deploy_locust.sh --deploy --sandbox-class gvisor --actor-memory 1Gi
./benchmarking/locust/deploy.sh --deploy --user-class agentsession
```

#### Agent-Session Configuration Knobs

* `--agentsession-script` — built-in script variant to run, by file name
  under `internal/benchmarking/boomer/agentsession/scripts/` (default
  `coding-session`). Resolved when a session starts, so a change takes
  effect for sessions started after the next swarm; sessions already
  running finish on the script they started with.
* `--agentsession-script-file` — path, on the boomer worker, of a script
  YAML to run instead of a built-in variant; wins over
  `--agentsession-script`. Normally set for you by
  `locust/deploy.sh --agentsession-script FILE` (below).
* `--agentsession-think-scale` — multiplier on every think gap; 0.5 makes the
  fleet twice as chatty, 4.0 models slow reasoning models (default 1.0). Each
  gap gets ±20% jitter so sessions don't move in lockstep.
* `--resume-mode implicit|explicit` — implicit (default) lets the parked
  first request wake the actor; explicit issues ResumeActor before traffic.
* `--lifecycle-mode suspend|pause` — durable suspend (default) or node-local
  pause between steps.

#### Writing an agent-session script

A script is a named step list with a memory floor:

```yaml
name: coding-session
min_actor_memory: 1Gi
steps:
  - name: 01_read_task
    agent: Boots, reads the task prompt, loads its context window
    think: 2s
    ops:
      - fill_ram: {key: agent_context, size: 32Mi}
      - ping: {}
  - name: 02_clone_repo
    agent: git clone of the target repository
    think: 3s
    ops:
      - ingest: {key: repo_tarball, size: 16Mi}
      - burn_cpu: {millis: 500, parallel: 1}
```

`think` is the suspended gap before the step (a Go duration, scaled by
`--agentsession-think-scale`); `name` keys the `Step_<name>` stats row.
Sizes are Kubernetes quantities. Each op is one glutton RPC:

| Op | Arguments | Sandbox effect |
|---|---|---|
| `ingest` | `key`, `size` | bytes cross the network from the driver and land in a file: a download |
| `burn_cpu` | `millis`, `parallel` (default 1) | spins `parallel` goroutines for `millis` of wall clock |
| `write_disk` | `key`, `size` | writes locally generated bytes to a file |
| `read_disk_digest` | `key` | reads and hashes a file; nothing ships back |
| `read_disk_data` | `key` | reads a file and returns its bytes: disk read plus egress |
| `fill_ram` | `key`, `size` | allocates a resident RAM array |
| `churn_ram` | `key`, `size` | re-randomizes part of an array in place, dirtying pages |
| `walk_ram` | `key` | touches one byte per page: demand-paging cost after a resume |
| `ping` | none | a minimal round trip through the router |
| `dwell` | `millis` | no request: the actor stays resident and idle, as a gateway does during a model round trip or a typing gap |

Loading is strict: unknown op kinds or fields, an argument a kind does not
take, a read of a file nothing wrote, a walk of an array nothing filled, a
duplicate step name, or a `min_actor_memory` below the declared RAM plus
disk all fail before any actor is created. Built-in variants are checked by
`TestEmbeddedScriptsAreValid`, so a broken file cannot merge.

#### Built-in script variants

`coding-session` (default) is a coding agent working one task: 20 steps of
build, test and edit work, each preceded by a few seconds of LLM thinking
spent suspended; about 40 CPU-seconds of sandbox work per lap.

`personal-assistant` is an always-on assistant of the OpenClaw or Hermes
Agent kind: one lap is one day in real time, with about 30 user messages
in 7 clusters, a heartbeat turn every 30 minutes while the user is awake,
hourly system-event crons, a morning briefing, model-catalog refreshes,
one context compaction and a nightly memory sweep (61 wakes, about 65
CPU-seconds and 150 MiB written per lap, a 640Mi resident heap that grows
to about 960Mi). Each turn reads its session store, ships a 50 to 110 KiB
context out through the router, dwells for the model round trip, and
appends to its store, sized from an strace profile of OpenClaw; the
script's header comment lists the figures. Its think gaps are the real
idle gaps, so run it with a fractional think scale:
`--agentsession-script personal-assistant --agentsession-think-scale 0.02`
plays a day in about 30 minutes. Because every heartbeat and cron is a
wake, the script also shows what an always-on agent costs a system that
suspends it between events: an internal timer cannot fire in a suspended
sandbox, so each of those turns needs an external wake.

To run a script of your own without rebuilding anything, hand it to the
locust deploy:

```sh
./benchmarking/locust/deploy.sh --deploy --user-class agentsession --agentsession-script ./my-session.yaml
```

The script validates the file locally first (the same check the worker
runs, via `boomer-worker --check-agentsession-script`), uploads it as the
`agentsession-script` ConfigMap, mounts it into the boomer workers at
`/etc/agentsession/script.yaml`, and points the master's
`--agentsession-script-file` default at that path. Workers log the loaded
script's name, step count, and declared budgets on their first iteration.
To go back to a built-in variant, redeploy without the flag.

#### Agent-Session Reported Metrics

* `WakeFirstTouch`: latency of a dedicated ping sent before each step's ops —
  in implicit mode that ping is what triggers the parked wake, so this row
  **is** the user-visible wake latency, unpolluted by the step's own work.
* `Step_<name>` (e.g. `Step_06_first_build`): wall time of that step's ops,
  think gap excluded.
* `SuspendActor` / `ResumeActor` / `CreateActor` / `DeleteActor`: control-plane
  lifecycle latencies.

### Spawn Benchmark

The Spawn benchmark creates a batch of actors once and measures how long each
actor takes from creation to its first answered ping, and how long the whole
batch takes. `tests.yaml` runs it as `spawn_smoke_10_actors` with
`shapes/spawn_shape.py`, which holds one user and ends the run once
`TimeToAllReady` is recorded.

Each boomer process creates one batch; extra users in the same process do
nothing. Actors are named `spawn-<run-id>-<n>` and deleted when boomer exits.

#### Spawn Configuration Knobs

* `--total-actors`: Actors in the batch (default `100`).
* `--spawn-concurrency`: Actors created concurrently (default `1`).
* `--actor-deadline`: Per-actor timeout in seconds, covering create, resume and
  first ping (default `120`).

The web UI shows the same fields; `0` keeps the value boomer-worker started with.

#### Spawn Reported Metrics

* `CreateActor`, `ResumeActor`, `GluttonPing`: Latency of each call.
* `ActorTimeToReady`: Per actor, from its first `CreateActor` attempt to its
  first successful ping.
* `TimeToReady_<k>pct` (`k` = 10, 20, … 100): From batch start until `k`% of
  the batch was ready.
* `TimeToAllReady`: From batch start until the last ready actor answered.
  Actors that failed show up as `ActorTimeToReady` failures instead.
* `CrashCount`: Actors that crashed during resume.

The `actors_per_*` ratios in `trial_summary` are wrong for this test: they
count users × `--actors-per-user`, not `--total-actors`.

### External Volume Storage Benchmark (`glutton_storage`)

The `glutton_storage` benchmark evaluates Substrate External Volume lifecycle performance (CSI volume dynamic provisioning, worker node attachment, sandbox mounting, and direct write I/O durability) under concurrent user load and oversubscription.

#### Storage Configuration Knobs

The `glutton-storage` template is deployed only when a StorageClass is set,
because it needs a CSI driver that not every cluster has.

* `--storage-class-name`: StorageClass for the external volume, e.g.
  `csi-nfs-sc` on kind. Can also be set with the `STORAGE_CLASS_NAME` env var
  or `storageClassName` in `tests.yaml`.

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
* `server_summary.json`: server-side telemetry harvested from Prometheus,
  including the per-sample bin-packing timeseries.

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

### Server ground truth

With a reachable Prometheus, `server_summary.json` records what the server
actually did, independent of what the load generator reported.

* `cluster_packing`: workers holding at least one actor (`partial` or
  `at_capacity`) over the pool size at each sample (the sum of all
  `ate_workerpool_workers` states), as a percentile `summary` plus the
  per-sample `timeseries` it was computed from.
* `node_psi.cpu_stall_pct`, `mem_stall_pct`, `io_stall_pct`: kernel pressure
  stall percentages on the nodes hosting a worker pod, with every node's
  samples pooled. `node_psi.nodes` is how many nodes were seen.
* `pod_psi.cpu_stall_pct`, `mem_stall_pct`, `io_stall_pct`: the same for each
  worker pod's cgroup slice, which also holds its gVisor sandbox, with every
  pod's samples pooled. `pod_psi.pods` is how many pods were seen.
* `snapshots.size_p50_mb` through `size_p99_mb`: actor memory image sizes.
* `snapshots.size_avg_mb`: mean memory image size.
* `snapshots.restore_p50_s` through `restore_p99_s`, `restore_mean_s`, and the
  same for `checkpoint_*`: atelet restore and checkpoint latency.
* `snapshots.checkpoints_in_window`, `checkpoints_cumulative`: checkpoint
  volume over the steady-state window, and since the atelet started.
* `snapshots.checkpoint_mb_s`: bytes written per second spent checkpointing,
  not per second of wall clock.

Every distribution reports p50, p90, p95 and p99 over the steady-state window.
The steady-state window runs from the first to the last Locust sample at 90% or
more of the run's peak user count, so ramp-up and teardown are left out. Under
a step-ladder load shape it covers only the top step.
Counts, means and `checkpoint_mb_s` come from the growth of the histogram's sum
and count across the window, and the size, restore and checkpoint percentiles
are interpolated inside its buckets. PSI is a one-minute rolling average read
every 10s.

The atelet exports before Prometheus scrapes it, so the harvest waits
`--atelet-lag-s` seconds (default 70, enough for the OTel SDK's 60s default
export and a 10s scrape)
and reads the snapshot window half that late. The window ends at the last
full-load sample, so teardown suspends are left out.

`metadata.start_ts` and `end_ts` bound the whole run, which the packing
`timeseries` covers. `steady_start_ts` and `steady_end_ts` bound the
steady-state window. A flat subset of the numbers also goes into a
`server_summary` row in `stats.jsonl`.

Neither the Kubernetes API nor Prometheus is required. If either is unreachable,
or discovery was skipped, the affected fields are written as `null` and the run
still succeeds. A `null` means the value was not measured. It never means zero.

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
