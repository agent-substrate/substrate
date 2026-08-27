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

`SleepUser` ([`locust/tests/sleep.py`](locust/tests/sleep.py)) does one suspend
operation and one resume operation for each task. Thus
`cycles/sec ≈ users / (1.0s + avg_wait_time)`. Change the number of users. Do
not change the wait time.

| # | Scenario | Duration | Setup |
|---|---|---|---|
| **S0** | Idle floor | 5m | No load |
| **S1** | Suspend and resume sweep | 3m for each step | `SleepUser`, `--trace-probability 0.1`, users 5 → 15 → 30 |
| **S2** | Sample-rate sweep | 3m for each step | The number of users stays the same. `--trace-probability` 0 → 0.1 → 1.0 |
| **S3** | Soak | 15m | 80% of the maximum load above |

Do not make a step shorter than 3 minutes. The metrics push interval is 60
seconds. A shorter step gives less than three datapoints for each series. You
cannot then tell the difference between a trend and noise.

Do these runs with the headless runner in
[automation/tests.yaml](automation/tests.yaml). In headless mode, `runner.py`
sends `--trace-probability` to the boomer worker at start, and writes the value
in the run config header. Thus each run records the rate that it used.

The locust web UI is for manual examination only. Two conditions apply there.
The `boomer-glutton` sidecar makes its own load for each user class that you
select in the form. Also, the form changes the sample rate of the boomer worker
but not of the Python workers: `locust.yaml` gives boomer `--master-web-port`,
thus boomer reads `/boomer-config` from the master at each spawn message.

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

- Add the ladder to [automation/tests.yaml](automation/tests.yaml). Write the
  results as artifacts of each run.
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
