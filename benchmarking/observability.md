# Observability benchmarking

This page shows how much telemetry substrate sends, and what that telemetry
costs the OTel collector.

- To make a measurement, read [telemetry/README.md](telemetry/README.md).
- For the volume model and the collector size guidance, read
  [OTel Collector](../docs/dev/best-practices/otel-collector.md).
- This page contains the scenario ladder.

This page holds no results. The automation writes the values of each run to
GCS. A result in this repository becomes incorrect after a change to the
sample rates or to the instruments, and no reader can then see which copy is
correct.

---

## 1. Scenario ladder

The ladder is in [automation/tests.yaml](automation/tests.yaml), as four tests
whose names start with `observability_`. Each one is one run of one step or
more:

| # | Test | Duration | Steps |
|---|---|---|---|
| **S0** | `observability_s0_idle_floor` | 5m | No load |
| **S1** | `observability_s1_user_sweep` | 3m for each step | Users 5 → 10 → 15, at `--trace-probability 0.1` |
| **S2** | `observability_s2_sample_rate_sweep` | 3m for each step | 10 users. The sample rate goes 0 → 0.1 → 1.0 |
| **S3** | `observability_s3_soak` | 10m | 12 users, 80% of the maximum of S1 |

Each test uses `GluttonUser`, thus the load comes from the boomer worker.
Do not put a ladder on a Python user class. Python and gRPC hold each other at
a high number of users: the latency then is the latency of the load generator,
and the run measures the generator and not substrate.

The steps of one run are `--ladder users:duration[:trace_probability]`, which
[`locust/shapes/ladder_shape.py`](locust/shapes/ladder_shape.py) reads. The
shape holds each step for its duration and then goes to the next one, thus one
run gives the whole sweep, with one deploy and one baseline. A step that names
a probability changes the sample rate as it starts: the value goes to the
parsed options of the master, and boomer reads it from `/boomer-config` at the
spawn message that the step change makes.

Do not make a step shorter than 3 minutes. The metrics push interval is 60
seconds. A shorter step gives less than three datapoints for each series. You
cannot then tell the difference between a trend and noise.

The number of users of S1 stops at 15, and not at a higher value, because the
pool of the run is 10 workers. A step above that measures the queue of the
scheduler and not the telemetry of a working system. Raise `workerCount` with
the steps to go higher.

The locust web UI is for manual examination only, and it holds no ladder. Two
conditions apply there. The `boomer-glutton` sidecar makes its own load for
each user class that you select in the form. Also, the form changes the sample
rate of the boomer worker but not of the Python workers: `locust.yaml` gives
boomer `--master-web-port`, thus boomer reads `/boomer-config` from the master
at each spawn message.

**Pass criteria.** `otelcol_receiver_refused_*` must be zero.
`otelcol_exporter_enqueue_failed_*` must be zero. `queue_full` on the client
must be zero. The number of collector replicas must stay below `maxReplicas`.
In S3, the slope of the collector working set and the slope of
`otelcol_exporter_queue_size` must be equivalent to zero.

Examine the positive control also. `otelcol_receiver_accepted_*` must be more
than zero for each service that must send data. If you do not examine it, an
exporter that sends nothing looks the same as a good run.

---

## 2. Open items

- Write the volume for each service as an artifact of each run. `runner.py`
  writes the locust statistics, and a reader must still query Prometheus for
  the `substrate_*` counts of the run.
- Measure the collector CPU and memory from the working set. Do not use
  `kubectl top`, which calculates an average and hides peaks.
- Record `otelcol_receiver_accepted_*` with each volume table as a positive
  control.
- Record the number of collector replicas with the CPU and the memory. Without
  the number of replicas, a collector with no capacity and a collector with
  much capacity give the same table.
- Measure the actor volume. The `glutton` template now sets an OTLP endpoint,
  thus actor telemetry arrives for the first time.
- Measure the DaemonSet configuration in the collector size guidance.
