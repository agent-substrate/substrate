# Suspend/resume phase analysis

This directory turns the node side's developer-facing timing logs into the
percentiles a benchmark run needs, without making the phases metric API. The
`ate.actor.{restore,checkpoint}.duration` histograms stop at
`ateom_restore` / `ateom_checkpoint` / `persist` / `download` on purpose:
phases inside those are implementation details of a runtime, and metrics are
a contract. The finer breakdown is emitted as structured log records instead,
and this is the consumer that aggregates them.

Two joinable JSON log records feed it, each written by both layers:

| record (`msg`) | emitter | keys |
|---|---|---|
| `Restore timing breakdown` | atelet and ateom-microvm | `ate.actor.restore.duration.<phase>` / `ateom.actor.restore.duration.<phase>` |
| `Checkpoint timing breakdown` | atelet and ateom-microvm | `ate.actor.checkpoint.duration.<phase>` / `ateom.actor.checkpoint.duration.<phase>` |

Every record carries the full actor identity (`ate.actor.uid`, name,
atespace, template) and the snapshot scope, which the histograms are barred
from, so records join per actor and per operation. Durations are float
seconds, the histograms' unit. A phase that never ran is absent, not zero.
A failed atelet operation still writes its record, marked with `error.type`
(the gRPC code); the report excludes those from the percentiles.

## Making a measurement

1. Run a suspend/resume-heavy load (e.g. the `glutton` or `sweperf` user
   class; see `../README.md`).
2. Dump the node logs for the run's window while the pods still exist.
   `automation/orchestrator.py` deletes the workload and ate-system pods right
   after each test, and does not call this script yet, so run it before that
   teardown (or against a cluster you manage yourself):

   ```bash
   ./collect_logs.sh --dest /tmp/run1 --since-time 2026-09-24T18:00:00Z
   ```

   The script sources `.ate-dev-env.sh` from the repository root when present
   (set `NO_DEV_ENV` to skip) and honors `KUBECTL_CONTEXT`, like the `hack/`
   scripts. `--since-time` (or a relative `--since 30m`) should cover the run
   and nothing before it. `--namespace` (default `ate-system`) selects the
   atelet pods and `--worker-namespace` (default `benchmark-workloads`) the
   worker pods; the ateom records land in the worker pod's stdout. `kubectl logs`
   returns only a container's current log file, so on a long or busy run
   kubelet's rotation can drop earlier records — collect soon after the run.
   A container that restarted mid-run is dumped twice (`<pod>.previous.log`).

   On GKE the same records are in Cloud Logging, which keeps them past
   rotation and teardown; an export is accepted as input directly, instead of
   the kubectl dumps:

   ```bash
   gcloud logging read '(jsonPayload.msg="Checkpoint timing breakdown" OR jsonPayload.msg="Restore timing breakdown") AND resource.labels.cluster_name="<cluster>" AND timestamp>="2026-09-24T18:00:00Z"' \
     --project <project> --format json > /tmp/run1/export.json
   ```

3. Aggregate, from the kubectl dumps:

   ```bash
   python3 phase_report.py /tmp/run1/*.log --csv /tmp/run1/report
   ```

   or from the Cloud Logging export:

   ```bash
   python3 phase_report.py /tmp/run1/export.json --csv /tmp/run1/report
   ```

   Use one source per run: the dumps and an export of the same pods hold the
   same records, and passing both would count each twice.

## Reading the report

**Phase percentiles.** Per layer, operation, sandbox class, snapshot kind,
scope and phase: count, p50, p90, p95 and max. A `golden` restore downloads
the golden image and a `latest` one the actor's own, so they are separate
rows, as are gVisor and micro-VM checkpoints. The atelet rows split a checkpoint between
`sandbox_assets`, `ateom_checkpoint` and `persist`, and a restore between
`volume_mount`, `manifest_fetch`, `sandbox_assets`, `download`, `oci_unpack`
and `ateom_restore`. The ateom rows split the `ateom_*` phase further:
`prep` / `pause` / `snapshot` / `durable_dir` / `rootfs_upper` / `teardown`
for a checkpoint, `prep` / `bundles` / `upper_join` / `lowers` / `tap` /
`vmm_launch` / `vm_restore` / `resume` / `wakeup_probe` for a restore.

Concurrency matters when reading them: the atelet restore phases overlap
(the download runs alongside the asset fetch and OCI unpack), the three
ateom checkpoint captures run concurrently on the paused guest (the paused
window costs their max), and the ateom restore phases are sequential. For
the two checkpoint layers the report derives an `unattributed` row: the total
minus what the logged phases account for, counting the concurrent captures
once. It is the time the instrumentation does not yet name. The ateom
records carry no snapshot kind of their own; a paired one takes its kind from
the atelet record, so both layers split into the same rows.

**Waterfalls.** The slowest operations, with the ateom record of the same
actor nested under the atelet `ateom_*` phase and that phase's gap to the
ateom total (RPC and queueing between the layers). The ateom record is the
one written inside that phase's window (for a checkpoint, before `persist`
began), and each ateom record pairs with at most one operation, so an
operation whose own record is missing prints without one rather than with
another cycle's. Failed operations (`error.type` set) are left out of both
the percentiles and the waterfalls. Tail outliers that blow up in one phase
every time are systematic; different phases each time are environmental.

`--csv` also writes `phase_percentiles.csv` for run-over-run comparison.

The parser is deliberately tolerant: it scans any line for a JSON object and
matches on `msg`, so raw `kubectl logs` dumps (even with prefixes) work.

## Tests

```bash
python3 -m unittest discover -s benchmarking/analysis
```
