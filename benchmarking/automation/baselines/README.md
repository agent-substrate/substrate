# Dataplane baselines

A dataplane baseline is a short suite of tests that measures the actor data
plane on one or two nodes, with the control plane nearly idle. Run it against a
cluster with and without a change, for example a microVM change, and attach both
reports to the PR.

Each test drives one actor on one microVM worker, with a one-second wait between
suspend/resume cycles. That keeps ate-api-server and Postgres well under one
request a second, so control plane queueing does not show up in the numbers.

## Versions

Each `dataplane-v<N>.yaml` is one version. Results from the same version are
comparable with each other, whichever commit or cluster they came from. To
change a workload, a size or the load, copy the latest file to the next version
and edit the copy. Leave the old file in place, so old results can still be
reproduced. Every entry names its version in its test name and in its
`baseline_version` metadata, and `test_locust.py` checks that.

| Version | Workloads |
|---|---|
| v1 | Go (glutton), Node.js (glutton-node), Python (sweperf astropy-7336) |

Java, glibc against musl, and Rust/jemalloc are not covered yet.

## Running

You need a cluster that meets the
[test cluster prerequisites](../README.md#test-cluster-prerequisites), with KVM
nodes for microVM workers and
[`benchmarking/monitoring.yaml`](../../monitoring.yaml) applied. Without
Prometheus the system stats and snapshot numbers are `null`. One node is enough.
With two, the runner Job and the system components can stay off the worker's
node: add `--cordon-system-infra-components` to each test's `ateArgs` and give
the second node the taints that flag expects.

Every test deletes and reinstalls Substrate on the cluster, so do not use a
shared cluster. See [Running from a workstation](../README.md#running-from-a-workstation)
for the tools and docker auth the orchestrator needs. From the root of the
checkout to measure:

```bash
benchmarking/automation/venv/bin/python -u benchmarking/automation/orchestrator.py \
  --substrate-dir . --env-file .ate-dev-env.sh \
  --dest gs://<DEST_BUCKET>/<PREFIX> \
  --tests benchmarking/automation/baselines/dataplane-v1.yaml
```

Add `--test-name <NAME>` to run one test. Results go to
`<dest>/runs/<test name>/run_date=<date>/run_ts=<unix ts>/run_tag=<commit>/stats.jsonl`.
A checkout with uncommitted changes is tagged `<commit>-dirty-<random>`.

To compare a change, run the suite from a checkout of the base commit and again
from the branch, against the same cluster, then compare the two `stats.jsonl`
files row by row.

## Report

`stats.jsonl` holds one row per metric. The rows that make up the report:

| What | Row (`metric`) | Fields |
|---|---|---|
| Nodes | `trial_summary` | `node_count`, `machine_type`, `capacity_cores`, `capacity_ram_gb`, `allocatable_cores`, `allocatable_ram_gb`, `zone`, `metadata_cpu_platform` |
| Software | `trial_summary` | `kubernetes_version`, `os_image`, `kernel_version`, `container_runtime_version`, `node_architecture`, `worker_image` |
| microVM | `trial_summary` | `metadata_kata_version`, `metadata_cloud_hypervisor_version`, `microvm_assets` |
| Baseline | `trial_summary` | `metadata_baseline_version`, `metadata_workload`, and the commit in `tag` |
| Golden snapshot | `golden_snapshot` | `warmup_s`, `golden_snapshot_s` |
| Cold start | `actor_ColdStart` | glutton: `CreateActor` plus the first `ResumeActor`. sweperf: `CreateActor` until the sandbox answers |
| First suspend | `grpc_SuspendActorFirstSuspend` | |
| First resume from the actor's own snapshot | `grpc_ResumeActorAfterFirstSuspend` | |
| Steady state | `grpc_SuspendActor`, `grpc_ResumeActor` | |
| Snapshot sizes | `server_summary` | `snapshot_size_p50_mb` … `snapshot_size_p99_mb`, `snapshot_size_avg_mb` |
| Atelet restore and checkpoint | `server_summary` | `restore_p50_s` …, `checkpoint_p50_s` …, `checkpoint_mb_s` |
| CPU | `server_summary` | `node_cpu_cores_*`, `psi_cpu_stall_*`, `pod_psi_cpu_stall_*` |
| Network | `server_summary` | `node_net_rx_mb_s_*`, `node_net_tx_mb_s_*` |
| Storage | `server_summary` | `node_disk_<device>_read_*`, `node_disk_<device>_write_*`, `psi_io_stall_*` |
| Memory | `server_summary` | `working_set_*`, `psi_mem_stall_*` |

Locust rows hold client latencies in milliseconds, with `median_response_time`,
`average_response_time` and percentiles `p50` … `p100`. glutton's
`ResumeActorFirstResume` row is the first resume from the golden snapshot. Each
field is described in the
[benchmarking guide](../../../docs/guides/benchmarking.md#benchmark-output-files).
