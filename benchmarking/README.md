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

> [!IMPORTANT]
> The ate-api-server checks every call against
> [authorization](../docs/authorization.md), and Locust calls it as the
> `benchmarking/default` ServiceAccount. Install Substrate with that identity
> as a bootstrap owner, alongside the default `ate-client` owner, or every
> benchmark call is denied:
> ```bash
> ATE_API_AUTHZ_BOOTSTRAP_OWNERS=system:serviceaccount:ate-system:ate-client,spiffe://cluster.local/ns/benchmarking/sa/default \
>   ./hack/install-ate.sh --deploy-ate-system
> ```
> The nightly automation sets this itself; see
> [automation/README.md](automation/README.md).

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

`--skip-build` and `--worker-memory` are only available when invoking
`benchmarking/deploy_locust.sh` directly.

### Settings

`ate-setup` reads these settings in addition to those in
[docs/operator-install.md](../docs/operator-install.md#settings), through the same
flag, environment variable, and configuration document channels.
`hack/install-ate.sh` forwards them as `--benchmark-worker-count`,
`--benchmark-sandbox-class`, and `--benchmark-actor-memory`.

| Key                      | Environment variable          | Flag                       | Default  | Applies to                               |
|--------------------------|-------------------------------|----------------------------|----------|------------------------------------------|
| `benchmark.actorMemory`  | `BENCHMARK_ACTOR_MEMORY`      | `--benchmark-actor-memory` | —        | (global)                                 |
| `benchmark.sandboxClass` | `ATE_BENCHMARK_SANDBOX_CLASS` | `--sandbox-class`          | `gvisor` | `deploy benchmarks`, `delete benchmarks` |
| `benchmark.workerCount`  | `ATE_BENCHMARK_WORKER_COUNT`  | `--worker-count`           | `1`      | `deploy benchmarks`, `delete benchmarks` |

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

Four flags control the optional post-run measurements described in
[Benchmark output files](#benchmark-output-files):

* `--cluster-facts` / `--no-cluster-facts`: read node capacity and worker pod
  count from the Kubernetes API once the run ends, to derive density frontiers.
  On by default. Pass `--no-cluster-facts` to skip Kubernetes API discovery.
* `--prometheus-url`: the Prometheus to harvest server-side telemetry from.
  Defaults to the in-cluster service installed by
  [Optional: Prometheus + Grafana](#optional-prometheus--grafana).
* `--atelet-lag-s`: how long to wait after the run before reading the
  atelet's snapshot and active actor metrics. Defaults to 70.
* `--phase-breakdown` / `--no-phase-breakdown`: read the atelet and worker pod
  logs once the run ends and add the suspend/resume phase percentiles to
  `stats.jsonl`. On by default; needs `pods/log` access in `ate-system` and
  `benchmark-workloads` (granted in `automation/manifests/runner-job.yaml.tmpl`).
  See [analysis/README.md](analysis/README.md#automated-runs).

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
  * `glutton-storage`: Attaches an external CSI volume instead of a durable
    directory. `deploy_locust.sh` and `workloads/deploy.sh` deploy it only
    when `--storage-class-name` names an existing StorageClass. To run it on
    NFS, install with `hack/install-ate.sh --setup-csi=nfs` and pass
    `--storage-class-name csi-nfs-sc`, the class the e2e tests also use.

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
agent workload. One task is four cycles by default. Each task runs on a fresh actor: once
the last cycle finishes, the actor is deleted and the user creates a new one for the next
task.

#### Sweperf Tasks

Each task is one entry (name, digest-pinned image, step count) in
`workloads/manifests/sweperf/sweperf-images.json`. Its ActorTemplate, `sweperf-<name>`,
is generated into `workloads/manifests/sweperf/generated/` and checked in. To add or change
a task, edit the catalog and regenerate:

```bash
SWEPERF=benchmarking/workloads/manifests/sweperf
python3 -m venv "${SWEPERF}/venv" && "${SWEPERF}/venv/bin/pip" install -r "${SWEPERF}/requirements.txt"
"${SWEPERF}/venv/bin/python" "${SWEPERF}/generate_sweperf_templates.py" '*'
"${SWEPERF}/venv/bin/python" "${SWEPERF}/generate_sweperf_templates.py" --check   # fail if generated/ is missing or stale
hack/verify/benchmarking-python.sh   # unit tests, as CI runs them
```

Deploy a task and the sweperf user class:

```bash
WORKLOAD_TEMPLATES=sweperf-django-11099 ./benchmarking/workloads/deploy.sh --deploy
./benchmarking/locust/deploy.sh --deploy --user-class sweperf
```

Then start the run with `--sweperf-template sweperf-django-11099` and
`--sweperf-total-steps 15`, set in the Locust web UI or through `LOCUST_SWEPERF_TEMPLATE`
and `LOCUST_SWEPERF_TOTAL_STEPS`. The client takes the step count from that option, not
from the template, so it must match the catalog.

#### Sweperf Reported Metrics

All rows are in milliseconds. CEL (command execution latency) is the time the trace commands
ran inside the sandbox, as reported by `replay.py`.

* `ResumeToFirstExec`: Resume RPC start until the sandbox accepts the cycle's `/execute`.
* `CycleCEL`: CEL for one cycle.
* `TaskCEL`: CEL summed over one task.
* `TaskWallClock`: Client wall clock for one task, excluding inter-cycle think time and actor
  startup.
* `CreateAtespace`, `CreateActor`, `ResumeActor`, `SuspendActor`, `DeleteActor`: Server-side
  elapsed time for each control-plane RPC from the response trailer, or client time without one.
* `<rpc>_rtt`: Client round trip for the RPC of the same name, recorded only when the trailer is
  present, so network and queueing overhead stays visible separately.
* `Workload_Cycle_<n>`: Client time for cycle `n`: `POST /execute` plus `/status` polling until
  the job finishes. Polled every `--sweperf-poll-interval-ms` (default 100), so this row sits
  up to one interval above the job's actual end.

The liveness check on a new actor already has it running, so the first cycle of every task
runs on a freshly started actor and its resume is a no-op. Its successful `ResumeActor`,
`ResumeActor_rtt` and `ResumeToFirstExec` samples are not recorded; failures still are.

The last cycle of a task is not suspended: the actor is deleted straight from running. A
task of `n` cycles therefore records `n - 1` `SuspendActor` samples, and `TaskWallClock`
does not include a final suspend. `DeleteActor` therefore times deleting a running actor,
including its sandbox teardown, so it is not comparable with older runs that suspended the
actor before deleting it.

`locust_users` counts live actors rather than virtual users: it rises when a user starts an
actor and falls when that actor is deleted, so it dips between tasks while a user waits to
start its next actor.

`CreateAtespace` is sent once per worker, not per task, and again only if `CreateActor`
reports the atespace missing.

When the run ends, shutdown cancels cycles still in flight. Calls cut short this way record
nothing, and the interrupted task records no `TaskCEL` or `TaskWallClock` row, so the end of
a run does not show up as a burst of failures.

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

`personal-assistant-light` is the same day with the RAM arrays scaled
down to 28Mi and everything else unchanged (files, reads, CPU, dwell and
the 61 wakes), for runs that want the always-on wake pattern with a small
memory image: about 40 to 95 MB per pause under gVisor, and under micro-VM
about 100 MB of guest before the script writes anything, bounded by the
actor's memory limit. It fits a 256Mi actor. Run it with the same
fractional think scale.

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

### ResumePaused Benchmark

The ResumePaused benchmark drives ate-api-server's cold resume path at a set
rate. A cold resume here is a ResumeActor of a paused actor: ateapi takes the
actor lease, binds a worker, and restores the actor from its snapshot. A
ResumeActor of a running actor does none of that. Each user owns one actor
from the `glutton` template. On its first iteration the user creates the actor
and makes it cold with a resume and a pause; every later iteration resumes the
actor and pauses it again, so every measured resume is cold.

The warm-up runs in the step that added the user, unpaced, and is recorded
under its own names.

It sends no requests to the actors themselves, so it runs on the fake data
plane, where it measures what a cold resume costs ate-api-server and Postgres
without the router or the restore in the way. The pauses run at the resume
rate, so a capacity it finds is one of resume and pause cycles, not of resumes
alone.

Actors are named `rp-<uuid>` and deleted when boomer exits. If the atespace
cannot be ensured, boomer exits at startup. An actor that fails to warm is
deleted, and its user creates another on its next iteration. When a step lowers
the user count, the actors of the stopped users stay paused, unused, until
boomer exits.

#### ResumePaused Configuration Knobs

* `--target-rps`: Cold resumes per second the whole boomer process paces to
  (default `0`, unpaced). A `--ladder` step ending in `@target_rps`, as in
  `200:3m@1000`, changes it as the step starts. Keep the users at or above the
  rate times the p99 of one resume plus one pause in seconds, or the step falls
  short of its rate.

The web UI shows the same field.

Trim the start of each ladder step. A rate change reaches boomer at its next
poll of the master, within 10 s, and a step that adds users carries their
warm-up until they have all joined at the spawn rate.

#### ResumePaused Reported Metrics

* `ResumeActorCold`: Latency of each measured resume. A resume that finds its
  actor already running fails, as no cold resume happened.
* `PauseActor`: Latency of the pause that makes the actor cold again.
* `PauseActorRecovery`: After a failed cycle, the pause that makes the actor
  cold for the next one. It is load on the server, so it is recorded, but
  apart from `PauseActor`. A recovery pause that finds the actor already
  paused counts as a success.
* `CreateActor`, `ResumeActorWarmup`, `PauseActorWarmup`: A user's warm-up of
  its actor, in the step that added the user.

### Isolate Mode

A real cold resume is limited by the node's restore capacity, which hides the
limits of ate-api-server and Postgres. Isolate mode takes the nodes out by
installing substrate with a fake data plane:

```bash
hack/install-ate.sh --deploy-ate-system --fake-data-plane
./benchmarking/workloads/deploy.sh --deploy --worker-count 200 \
  --fake-atelet-env FAKE_ATELET_DELAY=500ms
```

`deploy.sh` deploys the usual WorkerPool, but the install serves it with two
fakes from `cmd/benchmarking/isolate/` instead of worker pods:

* `fake-workersync` stands in for ate-controller's WorkerPool controller and
  worker syncer. It honors each pool's `spec.replicas` with fake Workers, none
  backed by a pod, round robin on the nodes that run fake-atelet, and scales them
  up and down with the pool, draining a Worker before deleting it. Each Worker
  reports what a real worker would: cpu and memory from the pool's
  `template.resources.limits`, or the node's allocatable without one, and 1000
  actors, ateworker's default, unless the pool's `ate.dev/fake-max-actors`
  annotation says otherwise. It writes the pool's status, so
  `kubectl get workerpool` and `kubectl scale workerpool` work. A node keeps
  its Workers for `--node-grace`, five minutes by default, after its
  fake-atelet pod goes, so a DaemonSet rollout does not move them; past that
  they move to a node that still runs fake-atelet.
* `fake-atelet` takes atelet's place in its DaemonSet, on every node atelet
  would run on, and answers every
  lifecycle call with success after its `--delay`, without running or
  saving any workload. It writes one placeholder object per snapshot, because
  ate-api-server refuses to tag an empty one. Like ateworker, it mints the actor's
  certificate from ate-api-server on every run and restore that names an
  egress gateway, which the default install does; the mint is ate-api-server's
  time, so it adds to `--delay`, the data plane's share of the call.
  It also relays fake-workersync's capacity reports, as atelet relays ateworker's:
  ate-api-server accepts a report only from the atelet on the Worker's node.
  The relay admits only fake-workersync's pod identity.

The install also scales ate-controller to zero, whose worker syncer would
delete the fake Workers. Its other controllers stop with it: no NetworkPolicy
is made for a WorkerPool, and the egress MITM trust bundle is not derived.
Fake actors need neither. fake-atelet takes the storage settings of atelet's
objectstore-plugin sidecar, whose writes it takes over, so its snapshot
placeholders go to the storage the install points atelet at. The install
refuses a cluster whose WorkerPools still have worker pods, which would
outlive ate-controller. A normal `hack/install-ate.sh --deploy-ate-system`
puts the real data plane back, and `--delete-ate-system` removes the fakes
either way. Never use the mode on a
cluster that serves real actors: actors placed on fake Workers do not exist.

The installer only chooses the fake data plane; the benchmark tunes it.
fake-atelet reads each tuning flag from a `FAKE_ATELET_<FLAG>` variable in the
optional `fake-atelet-tuning` ConfigMap, which no install creates or resets:
`FAKE_ATELET_DELAY` sets `--delay`, `FAKE_ATELET_DELAY_RESTORE` overrides it
for Restore alone, and `FAKE_ATELET_MINT_ACTOR_CERTIFICATE=false` skips the
certificate mint. `deploy.sh --fake-atelet-env NAME=VALUE`, repeatable, writes
that ConfigMap to exactly the given entries and restarts fake-atelet when it
changes, before the WorkerPool is applied.

Nothing answers behind the router, so the mode suits only a user class that
sends the actors no requests, such as `ate_api.py`, which creates, gets,
resumes and suspends actors. Every boomer user class pings its actors through
the router. In `tests.yaml`, an `isolate` block selects the mode, and its
keys become `deploy.sh --fake-atelet-env` entries: `delay` sets
`FAKE_ATELET_DELAY`, and so on.

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
  own CSV output. `stats_history.csv` has a row per operation every second as
  well as the `Aggregated` row, so each operation's latency can be charted
  over the run; filter to `Aggregated` for run-wide numbers.
* `logs.txt`, `traces.txt`: the runner log, and the trace IDs seen during the run.
* `stats.jsonl`: one JSON object per line, one per metric. Every row carries
  the same five keys: `timestamp`, `tag`, `test_name`, `metric`, and a flat
  `measurements` map holding that metric's numbers. The `phase_*` rows and
  the `phase_breakdown_summary` row are the suspend/resume phase percentiles
  read from the node logs (see
  [analysis/README.md](analysis/README.md#automated-runs)).
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
* `kubernetes_version`: the worker nodes' Kubernetes version (kubelet version).
* `worker_actor_capacity`, `worker_memory_limit_gb` (GiB): the capacity each
  worker pod declares, its ateworker `--max-actors` (1000 when unset) and its
  container memory limit (null when unset, as the node bounds it instead).
  `sandbox_class`: `gvisor` or `microvm`, from the worker image. A mixed pool
  gives a sorted comma-joined list for each. An unreadable value is skipped
  without affecting the others, and logged once.
* `multi_actor_worker`: `true` when `actors_per_pod_p50` is above 1.
* `run_start`, `run_end`: Unix seconds bracketing the run, for finding its
  server-side metrics after the cluster is deleted.
* `metadata_<key>`: one per key of the free-form `--metadata '<json>'` object,
  for facts only the caller knows. Non-string values are stored as JSON, and
  null values are skipped. The orchestrator passes `cluster_name`,
  `cluster_location` and `project_id` from the target cluster, plus any keys in
  the `BENCHMARK_METADATA` environment variable (a JSON object) and in the
  test's own `--metadata` flag, whose keys win on conflict.
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
  actor without a live resource measurement, so suspended ones fall out. It
  is recorded as `active_actors` in `server_summary.json`, a running-actor
  count rather than a resident one.
* **The denominators are read once, after the run.** A cluster that autoscaled
  mid-run is measured at its final size, so the ratio pairs a peak from one
  moment with a capacity from another. The same holds for every worker and
  node fact above, including `kubernetes_version` and the worker capacity, and
  so for `multi_actor_worker`, which is derived from `actors_per_pod`.
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
  same for `checkpoint_*`: how long the atelet took to resume or suspend an
  actor (`ate_actor_{restore,checkpoint}_duration`, `total` phase). Every
  snapshot kind counts, as in the sizes: the actor's own snapshot, a
  template's first start from its golden snapshot, and a pause. The buckets
  reach 60s, so a slower operation reads as 60s.
* `snapshots.checkpoints_in_window`, `checkpoints_cumulative`: checkpoint
  volume over the steady-state window, and since the atelet started.
* `snapshots.checkpoint_mb_s`: bytes written per second spent checkpointing,
  not per second of wall clock.
* `active_actors`: running actors (`ate_actor_stats_sampled_actors`), as a
  percentile `summary` of the cluster-wide count, `atelets` seen, and a
  `timeseries` every 10s over the whole run (ramp-up included). Each point has
  the cluster-wide count and `active_atelets` hosting an actor. As the
  assumptions above note, the gauge drops actors without a live measurement,
  so suspended ones fall out and the count dips while actors are suspended.
  The atelet stops exporting when a node has no running actor, so at a step
  where at least one atelet reported, a missing one counts as 0. A step where
  none reported is skipped, not 0, since a failed scrape looks the same; this
  mostly hits 1-user runs. If the metric never appeared during the run, every
  field is `null`. `atelets` counts atelet processes seen in the run, not
  nodes: the collector keeps an exited atelet's last value for about 5
  minutes, so a restart can briefly count twice, and an atelet scraped both
  directly and through a collector counts twice.

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
export and a 10s scrape) and reads the snapshot and `active_actors` windows
half that late, stamping `active_actors` points back by the same amount. The
window ends at the last full-load sample, so teardown suspends are left out.

`metadata.start_ts` and `end_ts` bound the whole run, which the packing and
`active_actors` timeseries cover. `steady_start_ts` and `steady_end_ts` bound
the steady-state window. A flat subset of the numbers also goes into a
`server_summary` row in `stats.jsonl`.

Neither the Kubernetes API nor Prometheus is required. If either is unreachable,
or discovery was skipped, the affected fields are written as `null` and the run
still succeeds. A `null` means the value was not measured. It never means zero.

## Suspend/resume phase breakdown

`SuspendActor` and `ResumeActor` latency can be attributed to the phases
inside them (object-storage transfer, hypervisor snapshot/restore, rootfs
assembly, ...) from the structured log records atelet and ateworker-microvm
emit. `analysis/collect_logs.sh` dumps the node logs of a run and
`analysis/phase_report.py` aggregates them into per-phase percentiles and
waterfalls of the slowest operations. See
[analysis/README.md](analysis/README.md).

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

`analysis/test_phase_report.py` covers the phase-log report's parser and
aggregation:

```bash
python3 -m unittest discover -s benchmarking/analysis
```
