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

Two flags control the optional post-run measurements described in
[Benchmark output files](#benchmark-output-files):

* `--cluster-facts` / `--no-cluster-facts`: read node capacity and worker pod
  count from the Kubernetes API once the run ends, to derive density frontiers.
  On by default. Pass `--no-cluster-facts` on a large cluster, where listing
  every node and pod is expensive.
* `--prometheus-url`: the Prometheus to harvest server-side telemetry from.
  Defaults to the in-cluster service installed by
  [Optional: Prometheus + Grafana](#optional-prometheus--grafana).

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
  `timestamp`, `tag`, `test_name` and `metric`.
* `server_summary.json`: server-side telemetry harvested from Prometheus,
  including the per-sample bin-packing timeseries.

### Density frontiers

With cluster discovery enabled, `stats.jsonl` gains a `trial_summary` row
describing how densely actors packed onto the hardware.

* `raw_configuration`: the measured facts, before any arithmetic:
  `machine_type`, `node_count`, `allocatable_cores`, `allocatable_ram_gb`
  (GiB), `worker_pod_count`. They are recorded so the ratios below can be
  re-derived later, or recomputed against a different denominator.
* `frontiers.actors_per_node`, `frontiers.actors_per_vcpu`,
  `frontiers.actors_per_gb_ram`: active users over the matching capacity.
* `frontiers.ap_ratio_p50`, `ap_ratio_p90`, `ap_ratio_p99`: the
  actor-to-pod ratio across the steady-state part of the run. Reported as a
  distribution rather than one average, because the ratio moves a lot while
  users are still ramping up.
* `frontiers.aggregate_failure_ratio`: failures over requests for the run.

### Server ground truth

With a reachable Prometheus, `server_summary.json` records what the server
actually did, independent of what the load generator reported.

* `cluster_packing`: assigned workers over total workers, as a percentile
  `summary` plus the per-sample `timeseries` it was computed from.
* `node_psi.cpu_stall_pct`, `mem_stall_pct`, `io_stall_pct`: kernel pressure
  stall percentages on the nodes under test.
* `node_psi.cfs_throttled_rate`: CFS quota throttling rate.
* `snapshots.size_p50_mb`, `size_p90_mb`, `size_p95_mb`: actor snapshot sizes.
* `snapshots.size_avg_mb`: mean snapshot size, taken from the histogram's
  own sum and count, so it is exact rather than bucket-interpolated.
* `snapshots.checkpoint_p50_s`, `checkpoint_p95_s`, `restore_p50_s`,
  `restore_p95_s`: checkpoint and restore latency.
* `snapshots.checkpoints_in_window`, `checkpoints_cumulative`,
  `throughput_mb_s`: checkpoint volume over the steady-state window.

A flattened subset of the same numbers is appended to `stats.jsonl` as a
`server_summary` row, so both metrics can be read from the one file.

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

### Rebuilding gRPC Python clients

`hack/update/codegen.sh` regenerates them along with the rest of the generated
code; it manages its own virtual environment under `locust/codegen/venv`.
`hack/verify/codegen.sh` fails if the checked-in clients have drifted from the
protos.

### Unit tests

`locust/unit_tests` covers the runner's helpers and needs no cluster. From the
repository root:

```bash
python3 -m unittest discover -s benchmarking/locust/unit_tests
```

Tests that need the Kubernetes client are skipped when it is not installed.
