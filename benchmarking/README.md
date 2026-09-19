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

* `--worker-count N` — total number of `WorkerPool` replicas (default 1).
* `--worker-pools LIST` — comma-separated `name:weight[:nodeSelectorKey=value]`
  entries. See [Multiple worker pools](#multiple-worker-pools).
* `--skip-build` — reuse the existing `:latest` locust image (skip the
  `docker build && docker push` step).

### Multiple worker pools

By default the stack creates one `WorkerPool`, and the scheduler may place an
actor on any of its workers. `--worker-pools` creates one pool per entry
instead, splits `--worker-count` between them by weight, and pins each actor to
a single pool for its whole life:

```bash
./benchmarking/deploy_locust.sh --deploy --worker-count 100 \
  --worker-pools 'n4d:1:cloud.google.com/machine-family=n4d,c4:1:cloud.google.com/machine-family=c4'
```

That run puts 50 workers on `n4d` nodes and 50 on `c4`, and sends half the
actors to each.

Pinning is a correctness requirement once the pools differ in machine type, not
a tuning knob. A suspended actor's memory snapshot records the CPU features the
guest saw, and nothing masks them to a common baseline on resume, so an actor
that moves between CPU models fails to restore. Pinning is also how a run
measures one machine type against another in the same test.

A pool name is a class of interchangeable workers, not one `WorkerPool`: it
reaches the scheduler as a `pool=<name>` label that every worker in the pool
inherits, so several pools may share a value when an actor can freely move
between them. What a value must never span is workers a snapshot cannot move
between. The key is `pool` and not `cpu-class` because CPU compatibility is
only today's reason to separate workers.

The pool list reaches the actors through the boomer workers, which set it as
each actor's `worker_selector`; `deploy_locust.sh` forwards the same list to
both halves so they cannot drift. Passing `--worker-pools` to
`benchmarking/workloads/deploy.sh` alone creates the pools but leaves the
actors unpinned.

### Teardown

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

The installer accepts `--benchmark-worker-count N` (default `1`) and
`--benchmark-worker-pools LIST`, which it forwards to
`benchmarking/deploy_locust.sh`. `--skip-build` is only available when invoking
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
