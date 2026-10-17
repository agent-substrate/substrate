# Disk contention benchmark

Measures how much actor (guest) disk I/O slows Substrate's suspend/resume
critical path when both share one node disk.

Each run has 3 **probe** Glutton actors cycling resume → touch 512 MiB RAM →
suspend → think 20–30 s. Probe rows are reported as `ProbeResumeActor` /
`ProbeSuspendActor`, separate from noise. The probes are deliberately light
(about 7% disk utilization on their own): one suspend/resume cycle of a
1 GiB-touched gVisor actor writes about 1.8 GB to the boot disk, so the
`glutton_mem_1gi_*` config (4 actors, 1 s think) saturates a 290 MB/s disk by
itself and leaves no clean baseline. The arms differ only in what runs next to
the probes:

| Arm | Noise next to the probes |
| --- | --- |
| `A` | none (baseline) |
| `B25` / `B50` / `B100` | 8 `glutton-durdir-data` actors writing fsync'd 64 MiB chunks to their durable dir (host `/var/lib/ate`), capped in total at 25 / 50 / 100 % of the disk's provisioned MB/s |
| `Bmax` | same, uncapped |
| `B<x>nf` | as `B<x>` without fsync (page cache + writeback only), for attribution |
| `C` | 8 sweperf actors (SWE-bench replay, `sweperf-astropy-7336`) |

The verdict is the probe p95 ratio of each arm vs `A` for the same runtime,
with a bootstrap 95% CI, plus node evidence (disk MB/s, util, await, PSI io,
GCE `performance_status`) and per-phase server timings
(`ate_actor_{checkpoint,restore}_duration_seconds` by `ate_snapshot_phase`).

## Files

| File | What |
| --- | --- |
| `run_arm.sh` | Runs one arm: renders `benchmarking/automation/manifests/runner-job.yaml.tmpl`, waits, downloads results, runs `collect.py`, deletes leftover actors |
| `run_matrix.sh` | All arms for one runtime, interleaved per repetition |
| `collect.py` | Prometheus + Cloud Monitoring evidence over the measurement window → `collect.json` |
| `report.py` | Markdown report + quality checks over a results directory |
| `recollect.sh` | Re-runs `collect.py` across an existing results directory using each run's `meta.json` window |
| `node-exporter.yaml` | node-exporter DaemonSet (disk + PSI collectors) scraped by `benchmarking/monitoring.yaml` Prometheus |
| `test_collect.py`, `test_report.py` | Unit tests, run by `hack/verify/benchmarking-python.sh` |

Load generator: user class `diskcontention`
(`internal/benchmarking/boomer/glutton/diskcontention.go`), flags `--dc-*`
(`benchmarking/locust/common/diskcontention_config.py`).

## Reproduce

```sh
source .ate-dev-env.sh
export WORKLOAD_TEMPLATES="glutton glutton-durdir-data sweperf-astropy-7336"

# Step 1: Substrate + workloads, all on ONE node: probes and noise must share
# its disk, and collect.py reads that node's metrics (it fails if a node query
# returns more than one series). 14 workers: 3 probes + 8 noise + 3 spare, so
# probes never wait for a worker (that would confound the measurement). The
# runner Job calls ate-api-server as benchmarking/benchmark-runner, which must
# be a bootstrap owner, as in benchmarking/automation's installs.
ATE_API_AUTHZ_BOOTSTRAP_OWNERS="system:serviceaccount:ate-system:ate-client,spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner" \
  ./hack/install-ate.sh --deploy-ate-system
./hack/install-microvm-deps.sh --install                       # microVM only
./benchmarking/workloads/deploy.sh --deploy --worker-count 14 \
  --sandbox-class gvisor --actor-memory 1536Mi                 # or microvm

# Step 2: Prometheus and node-exporter (the sed is GNU sed).
sed '0,/^\( *\)- benchmarking$/s//\1- benchmarking\n\1- ate-system/' \
  benchmarking/monitoring.yaml | kubectl apply -f -
kubectl apply -f benchmarking/diskcontention/node-exporter.yaml

# Step 3: runner image + results bucket.
export RUNNER_IMAGE=${KO_DOCKER_REPO}/locust-test:$(git rev-parse --short HEAD)
docker build --platform linux/amd64 -t "${RUNNER_IMAGE}" -f benchmarking/locust/Dockerfile .
docker push "${RUNNER_IMAGE}"

# Step 4: run. Port-forward Prometheus for collect.py.
kubectl -n benchmarking port-forward svc/prometheus 19090:9090 &
export DEST=gs://<results-bucket>/diskcontention OUT_DIR=$PWD/dc-results
export PROJECT_ID GCE_INSTANCE_ID=$(gcloud compute instances describe <node> --zone <zone> --format='value(id)')
export DISK_MBPS=290            # provisioned MB/s of the node disk
benchmarking/diskcontention/run_matrix.sh gvisor
python3 benchmarking/diskcontention/report.py "${OUT_DIR}" --md report.md
```

A subset: `REPS=1 EVERY_REP="A C" SWEEP_ONCE="" benchmarking/diskcontention/run_matrix.sh gvisor`.
With `LIFECYCLE_MODE=pause`, the probes pause (local snapshots) instead of
suspending, and the report puts those runs in a section of their own.

## Reading the report

- Latencies come from `traces.txt` (every request is traced at
  `--trace-probability 1.0`), restricted to `[start + WARMUP_SECS, end - 20 s]`
  (default warmup 180 s), so first-resume, spawn and shutdown effects are
  excluded.
- `Noise app MB/s` is what the noise actors wrote (bytes acknowledged by
  WriteDisk, fsynced except in the `B<x>nf` arms); `Disk write MB/s` is what
  reached the block device (node-exporter) holding `/var/lib/ate/actors`.
- When `/var/lib/ate/snapshots` is a disk of its own, a separate table shows
  that disk, where snapshot staging I/O goes.
- Quality checks flag: < 30 samples per op, > 1 % failures, noise more than
  20 % off its target rate, leftover actors at start, baseline p95 CV > 25 %
  across repetitions.

## Known limitations

- Arms on the same runtime run back to back without a full reset (interleaved
  per repetition instead). After each arm, `run_arm.sh` deletes the actors
  still in the benchmark atespace (it needs `kubectl ate`). An actor wedged
  mid-transition can survive that (agent-substrate/substrate#2011); the next
  run's quality checks flag it.
- On a 1-node cluster the Substrate control plane (including postgres's
  separate PVC) shares the node with the actors.
- A `C<n>` arm needs a worker for each of its n + 3 actors, plus spares. With
  the control plane and monitoring on the same node, GKE's default limit of
  110 pods per node leaves room for about 70 workers.
