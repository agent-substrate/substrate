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
