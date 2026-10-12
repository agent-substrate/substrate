# Suspend/resume phase breakdown: a runbook

This guide walks through getting a per-phase breakdown of `SuspendActor` and
`ResumeActor` latency from a cluster you run yourself, from deploying the
workloads to reading the report. It strings together tools documented
elsewhere; follow the links when a step needs more than the command shown.

The breakdown comes from two developer-facing log records, `Checkpoint timing
breakdown` and `Restore timing breakdown`, written once per operation by
atelet (the outer phases: `download`, `persist`, `ateom_*`, …) and by
ateom-microvm (the phases inside `ateom_*`: `pause`, `snapshot`, `vm_restore`,
`wakeup_probe`, …). `benchmarking/analysis/` collects the records from the pod
logs and aggregates them. See
[docs/observability.md](../observability.md#actor-attributed-component-logs)
for the record shape and
[benchmarking/analysis/README.md](../../benchmarking/analysis/README.md) for
the report.

## What you need

* A cluster with Agent Substrate installed from a tree that has the records
  and the analysis tools: the
  [Quickstart (Development)](../../README.md#quickstart-development) for kind,
  or the [GKE Quickstart](../../README.md#gke-quickstart-development). For GKE
  the scripts below read `.ate-dev-env.sh` from the repository root
  (`collect_logs.sh` sources it itself; set `NO_DEV_ENV` to opt out).
* The **microVM** sandbox class, if you want the inner (ateom) phases: see
  [microvm-local.md](microvm-local.md). ateom-gvisor writes no breakdown
  record, so on a gVisor pool the report shows the atelet layer only.
* `kubectl`, `kubectl-ate` (`go install ./cmd/kubectl-ate`), and `python3`
  (standard library only).

## 1. Deploy the benchmark workloads

```sh
./benchmarking/deploy_locust.sh --deploy \
  --sandbox-class microvm --worker-count 3 --actor-memory 1536Mi
```

This deploys the benchmark `WorkerPool` and `ActorTemplate`s into the
`benchmark-workloads` atespace, builds and pushes the locust image, and deploys
the locust master and workers (`benchmarking/README.md`,
[Deploy benchmarks](../../benchmarking/README.md#deploy-benchmarks)). On a kind
cluster with limited memory use smaller values, e.g. `--worker-count 1
--actor-memory 512Mi`, and scale the load flags in step 2 down to match.

Wait until every template has a golden snapshot — the column is a UUID once
the golden build (a full boot plus checkpoint) has finished:

```sh
kubectl ate get actor-templates -a benchmark-workloads
```

Those golden builds are themselves checkpoints, so the worker pods already
carry `Checkpoint timing breakdown` records at this point:

```sh
kubectl logs -n benchmark-workloads -l ate.dev/worker-pool --since=15m \
  | grep -c "timing breakdown"
```

A zero here means the running image predates the records; redeploy ate-system
from the current tree before going on.

## 2. Run suspend/resume load

The `glutton` user class creates an actor per user and cycles it through
suspend and resume. Give it a realistically sized working set so the memory
phases have something to measure:

```sh
kubectl port-forward svc/locust -n benchmarking 8089:8089
```

Open <http://localhost:8089>, pick `GluttonUser`, and set:

| field | value | why |
|---|---|---|
| users / spawn rate | 2 / 1 | two concurrent cycles; keep it small so transfers do not contend for one NIC |
| `--mem-target` | `1Gi` | resident working set before the first suspend (below `--actor-memory`) |
| `--mem-churn` | `64Mi` | re-dirty part of it each cycle, so successive snapshots differ |
| `--mem-read` | `all` | walk the whole set after each resume, so a demand-paged restore pays for its pages |
| `--min-wait-time` / `--max-wait-time` | `1.0` / `1.0` | one cycle per second per user |

The form fields are the flags documented in
`benchmarking/locust/common/memload_config.py`; the boomer workers fetch the
values from the master on each spawn. Let it run for about 90 seconds (≈10
cycles per user), then stop the test. For a single operation without locust,
`kubectl ate suspend actor <name> -a benchmark-workloads` and
`kubectl ate resume actor <name> -a benchmark-workloads` on any actor in the
atespace produce one record pair each.

## 3. Collect the node logs

Collect **while the pods still exist**. `benchmarking/automation/
orchestrator.py` deletes the workload and ate-system pods right after each
test and does not run this step yet, so on a cluster you manage yourself this
is the moment to do it:

```sh
./benchmarking/analysis/collect_logs.sh --dest /tmp/run1 --since-time 2026-09-24T18:00:00Z
```

`--since-time` (or a relative `--since 30m`) should cover the run and nothing
before it. The script writes one file per atelet pod (`ate-system`) and per
worker pod (`benchmark-workloads`) under `--dest`; a pod it cannot read is
skipped with a warning. Use either these dumps or the Cloud Logging export
below for a run, not both. `kubectl logs` returns only a container's current log
file, so collect soon after the run, before kubelet rotates it.

On GKE the same records are also in Cloud Logging, where they outlive both
rotation and teardown; an export is accepted by the report as-is:

```sh
gcloud logging read '(jsonPayload.msg="Checkpoint timing breakdown" OR jsonPayload.msg="Restore timing breakdown") AND resource.labels.cluster_name="<cluster>" AND timestamp>="2026-09-24T18:00:00Z"' \
  --project <project> --format json > /tmp/run1/export.json
```

## 4. Produce the report

```sh
python3 benchmarking/analysis/phase_report.py /tmp/run1/*.log --csv /tmp/run1/report
```

The first line says how many records it found; expect two per suspend and two
per resume (one per layer). If it is zero, the window in step 3 missed the run
or the images predate the records.

## 5. Read it

* **Phase percentiles** rank the phases. For a suspend, compare atelet's
  `persist` against `ateom_checkpoint`, then the ateom rows (`snapshot`,
  `teardown`, `pause`, …) against each other; for a resume, `download` against
  `ateom_restore`, then `vm_restore` and `wakeup_probe`. The `unattributed`
  row is the part of a checkpoint no logged phase names.
* **Waterfalls** show the slowest operations with the ateom record nested
  under the atelet phase it decomposes; a tail that is always the same phase
  is systematic, one that moves between phases is environmental.
* Two sanity checks on a healthy run: the atelet and ateom rows for one
  operation have the same `n`, and atelet's `ateom_checkpoint` p50 is a few
  tens of milliseconds above ateom's `total` p50 (the `(gap)` line in a
  waterfall is that RPC overhead).

`/tmp/run1/report/phase_percentiles.csv` holds the same table for comparing
runs, for example one at `--mem-target 1Gi` against one at `2Gi` to see which
phases scale with dirty memory.

## 6. Tear down

```sh
./benchmarking/deploy_locust.sh --delete
```

## Troubleshooting

* **Only atelet rows, no ateom rows.** The pool is gVisor (ateom-gvisor emits
  no record), or the worker image predates the records. Check
  `kubectl get workerpools -A` and step 1's grep.
* **Counts differ between layers.** The collection window cut through a
  cycle, or a worker pod restarted and took its log with it (the script dumps
  a restarted container's previous log as `<pod>.previous.log`). On GKE the
  Cloud Logging export has everything.
* **A record has `error.type`.** That operation failed; the report leaves it
  out of the percentiles and the waterfalls. To see where it died, grep the
  record itself: a `DeadlineExceeded` whose time sits in `download` or
  `persist` with a normal-looking `ateom_*` phase points at object storage,
  not the runtime.
