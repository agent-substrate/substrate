# Running the ate-api-server cold resume test locally

The `ateapi_cold_resume_pause_fake_*` tests in [tests.yaml](tests.yaml)
measure how many cold resumes per second ate-api-server and Postgres sustain.
They run the [ResumePaused benchmark](../README.md#resumepaused-benchmark) on
the [fake data plane](../README.md#isolate-mode) as a rate ladder.

Both tests run through `benchmarking/automation/orchestrator.py`. The
[automation README](README.md#how-it-works) covers what it does for each test
and how to run it from the CronJob. This doc covers a one-off run from your
workstation. Run the commands from the repository root.

## 1. Cluster

Create the test cluster as in
[Test cluster prerequisites](README.md#test-cluster-prerequisites).

Both tests pass `--cordon-system-infra-components`, which needs the two node
pools in [System Infra Isolation](../../docs/architecture.md#system-infra-isolation):
`ate-system-infra` and `ate-postgres`. If the `ate-postgres` pool is missing,
the install fails before it applies Postgres. If the `ate-system-infra` pool
is missing or too small, its pods stay Pending and the install fails when
the rollout wait times out. Size that pool for ate-api-server's
`replicas × cpu` plus the other system infra pods. Both tests run one
ate-api-server at 2 CPUs.

```bash
gcloud container node-pools create ate-system-infra \
  --cluster=$CLUSTER_NAME --location=$CLUSTER_LOCATION \
  --machine-type=<type> --num-nodes=1 \
  --node-labels=ate.dev/workloadType=ate-system-infra \
  --node-taints=ate.dev/workloadType=ate-system-infra:NoSchedule

gcloud container node-pools create ate-postgres \
  --cluster=$CLUSTER_NAME --location=$CLUSTER_LOCATION \
  --machine-type=<type> --num-nodes=1 \
  --node-labels=ate.dev/workloadType=ate-postgres \
  --node-taints=ate.dev/workloadType=ate-postgres:NoSchedule
```

The default pool runs the locust runner (8 CPUs in `_2cpu`) and fake-atelet.

## 2. Prometheus

The orchestrator does not deploy Prometheus. The runner needs it for
`server_summary.json` and the per-step rows in `stats.jsonl`, so install it
once as in [Prometheus + Grafana](../README.md#optional-prometheus--grafana).

## 3. Workstation

You need `gcloud`, `kubectl`, `docker`, Go, and Python 3 with PyYAML. ko
runs from `hack/tools/ko` through `go tool`, so it need not be installed.
The orchestrator polls `docker info` first and fails if Docker is not
running within 2 minutes.

```bash
python3 -m venv benchmarking/automation/venv
benchmarking/automation/venv/bin/pip install -r benchmarking/automation/requirements.txt
gcloud auth login
gcloud auth configure-docker gcr.io
```

Your account needs the orchestrator's roles, and the runner's KSA needs
write access to the results bucket. See
[IAM prerequisites](README.md#iam-prerequisites).

## 4. Pick the tests

The orchestrator runs every test in the file you pass to `--tests`. To run
only some of them, pass `--test-name NAME` once per entry, as in step 6.

| Test | Load | Duration |
|---|---|---|
| `ateapi_cold_resume_pause_fake_smoke` | 50 users, 20 then 50 cold resumes/s | 6m |
| `ateapi_cold_resume_pause_fake_2cpu` | 1000 users, 100 to 1500 cold resumes/s in 7 steps | 23m |

Run `_smoke` first to check the setup before the 23 minute ladder. The
header comments in `tests.yaml` describe the fields, such as
`controlPlane`, `isolate` and `--ladder`. If you change `replicas` or
`poolMaxConns`, keep within Postgres's connection slots as described in
[Running at scale](../README.md#running-at-scale).

## 5. Target cluster file

The orchestrator copies `<targetCluster>.sh` from the `--target-cluster-dir`
directory into the clone as `.ate-dev-env.sh`. Both tests use
`targetCluster: dev`, so step 6 copies your `.ate-dev-env.sh` there as
`dev.sh`. Create it from
[hack/ate-dev-env.sh.example](../../hack/ate-dev-env.sh.example)
if you don't have one. The install needs `PROJECT_ID`, `CLUSTER_NAME`,
`CLUSTER_LOCATION`, `KO_DOCKER_REPO`, `SNAPSHOT_LOCATION`,
`SNAPSHOT_TRANSFER_GSA_NAME` and `ATE_CREDENTIAL_PROVIDER`.

## 6. Run

The orchestrator clones `--branch` from `--repo`, so commit your changes
first. A `file://` URL clones your local checkout. Set `SUBSTRATE_DIR` to a
directory that does not exist yet, because the default is the pod path
`/workspace/substrate`. The orchestrator runs `gcloud container clusters
get-credentials`, so point `KUBECONFIG` at a separate file to keep your
current context:

```bash
S=$PWD                      # this checkout
D=$HOME/bench-runs/cold-$(date +%Y%m%d-%H%M)
mkdir -p $D/target-clusters
cp $S/.ate-dev-env.sh $D/target-clusters/dev.sh
export KUBECONFIG=$D/kubeconfig

cat > $D/run.py <<PY
import sys
sys.path.insert(0, "$S/benchmarking/automation")
import orchestrator
orchestrator.SUBSTRATE_DIR = "$D/substrate"
orchestrator.main()
PY

$S/benchmarking/automation/venv/bin/python -u $D/run.py \
  --repo file://$S --branch $(git branch --show-current) \
  --dest gs://<results-bucket>/<prefix> \
  --tests $S/benchmarking/automation/tests.yaml \
  --test-name ateapi_cold_resume_pause_fake_smoke \
  --target-cluster-dir $D/target-clusters \
  --manifests-dir $S/benchmarking/automation/manifests \
  --junit-output $D/junit.xml 2>&1 | tee $D/orchestrator.log
```

Keep your workstation awake for the whole run. If it suspends, the
orchestrator pauses too, and the teardown and the next test wait for it. For
long ladders, run the command under `setsid nohup` or in a VM.

## 7. Results

[Analyzing a ResumePaused Ladder Run](../README.md#analyzing-a-resumepaused-ladder-run)
explains the per-step rows and where the run path is in the log.
[Benchmark output files](../README.md#benchmark-output-files)
lists the files. To copy them all down:

```bash
st=$(grep -o 'gs://[^ ]*/status.json' $D/orchestrator.log | head -1)
mkdir -p $D/results && gcloud storage cp "${st%/status.json}/*" $D/results/
```

The capacity is the highest step whose `resume_rps` keeps up with its
`target_rps` and has no failures.
