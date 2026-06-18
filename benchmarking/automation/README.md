# Substrate benchmark automation

Scheduled, repeatable benchmark runs of a substrate branch. A CronJob on an
**orchestration cluster** drives the full build/deploy/run/teardown cycle
against a separate **test cluster**, once per entry in [tests.yaml](tests.yaml).
Each run uploads results to GCS via `benchmarking/locust/runner.py`.

## How it works

1. CronJob fires on the orchestration cluster; pod starts (orchestrator +
   DIND sidecar).
2. Orchestrator waits for DIND, copies the appropriate `ate-dev-env.sh`, runs
   `gcloud container clusters get-credentials` for the test cluster.
3. Shallow-clones `--repo` at `--branch`, captures the commit hash.
4. `docker build && docker push` builds the locust image tagged with the commit
   hash and pushes it to `${KO_DOCKER_REPO}/locust-test:<commit>`.
5. `hack/install-ate.sh --deploy-ate-system` + `benchmarking/workloads/deploy.sh
   --deploy` (these build & push substrate / workload images via `ko` as part
   of their deploy steps — there's no separate `make build-images` step).
6. For each test in `tests.yaml`:
   - Submits a Job using the just-built locust image; the Job runs
     `runner.py -f <file> -t <duration> -u <users> --tag <commit> --name <name>
     --dest <dest>`.
   - Polls until complete/failed/timeout; tails logs; deletes the Job.
   - Tears down substrate + workloads.
   - If not the last test, redeploys them so the next run starts clean.

## Setup

```bash
./benchmarking/automation/setup.sh
```

The wizard treats the repo's `.ate-dev-env.sh` as the source of truth for the
target cluster's environment and snapshots it to
`scratch/target-clusters/<name>.sh`. It only prompts for the target cluster
name (the routing key in `tests.yaml`) and the GCP project ID of the
orchestrator image registry. It then builds + pushes the orchestrator image
to `gcr.io/<ORCH_PROJECT_ID>/ate-images/substrate-benchmark-orchestrator:<short-commit>`
(with a `-dirty` suffix if `benchmarking/automation/` has uncommitted
changes) and renders `scratch/cronjob.yaml`, `scratch/test-list.yaml`, and
`scratch/target-clusters.yaml`. You can edit the `.ate-dev-env.sh` for your
workload cluster directly in the config map. 

Then edit the `--repo / --branch / --dest` args in `scratch/cronjob.yaml` and
apply:

```bash
kubectl --context=<orchestration-cluster> apply -f scratch/cronjob.yaml
```

To trigger immediately instead of waiting for the schedule:

```bash
kubectl --context=<orchestration-cluster> -n substrate-benchmark \
  create job --from=cronjob/substrate-benchmark manual-$(date +%s)
```

To change the schedule, edit `spec.schedule` in `scratch/cronjob.yaml` (the
default is `0 3 * * *`, 3am UTC).

## Test cluster prerequisites

Create the test cluster with the substrate-required beta APIs and Workload
Identity enabled. The control plane must be on Kubernetes 1.36+ so
`certificates.k8s.io/v1beta1` is available:

```bash
gcloud container clusters create <CLUSTER_NAME> \
  --location=<CLUSTER_LOCATION> \
  --num-nodes=5 \
  --workload-pool=<PROJECT_ID>.svc.id.goog \
  --enable-kubernetes-unstable-apis=certificates.k8s.io/v1beta1/podcertificaterequests,certificates.k8s.io/v1beta1/clustertrustbundles
```

The orchestration cluster needs Workload Identity but no special APIs. It only
ever runs one pod (the orchestrator + DIND sidecar), so a single zonal node
keeps costs to the minimum:

```bash
gcloud container clusters create <ORCH_CLUSTER_NAME> \
  --location=<ORCH_ZONE> \
  --workload-pool=<ORCH_PROJECT_ID>.svc.id.goog \
  --num-nodes=1
```

## IAM prerequisites

This setup assumes both clusters and the destination GCS bucket already exist.
Two Workload Identity bindings are needed.

Both ServiceAccounts are created by the manifests (`cronjob.yaml` for the
orchestrator KSA, `runner-job.yaml.tmpl` for the runner KSA — applied by
`orchestrator.py` at runtime), so no `kubectl create serviceaccount` steps are
needed. Grant IAM roles directly to each KSA's Workload Identity principal
(no GSA / annotation required). The principal format is:

```
principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/<NAMESPACE>/sa/<KSA>
```

**Orchestrator pod** (KSA `substrate-benchmark-orchestrator` in namespace
`substrate-benchmark` on the orchestration cluster's project) needs:

- `roles/container.admin` on the test cluster's project — required to manage
  cluster-scoped resources (CRDs, ClusterRoles, ClusterRoleBindings,
  Namespaces) that `hack/install-ate.sh --deploy-ate-system` creates.
  `container.developer` is not enough — it intentionally omits the
  `container.clusterRoles.*` and `container.customResourceDefinitions.*`
  permissions.
- `roles/artifactregistry.writer` on `KO_DOCKER_REPO` — for `ko` (substrate
  images) and `docker push` (locust image).

```bash
ORCH_PRINCIPAL="principal://iam.googleapis.com/projects/<ORCH_PROJECT_NUMBER>/locations/global/workloadIdentityPools/<ORCH_PROJECT_ID>.svc.id.goog/subject/ns/substrate-benchmark/sa/substrate-benchmark-orchestrator"
gcloud projects add-iam-policy-binding <TEST_PROJECT_ID> \
  --role=roles/container.admin --member="${ORCH_PRINCIPAL}"
gcloud projects add-iam-policy-binding <TEST_PROJECT_ID> \
  --role=roles/artifactregistry.writer --member="${ORCH_PRINCIPAL}"
```

**Runner Job pod** (KSA `benchmark-runner` in namespace `benchmarking` on the
test cluster's project) needs `roles/storage.objectUser` on the destination
bucket so `runner.py` can upload results:

```bash
RUNNER_PRINCIPAL="principal://iam.googleapis.com/projects/<TEST_PROJECT_NUMBER>/locations/global/workloadIdentityPools/<TEST_PROJECT_ID>.svc.id.goog/subject/ns/benchmarking/sa/benchmark-runner"
gcloud storage buckets add-iam-policy-binding gs://<DEST_BUCKET> \
  --role=roles/storage.objectCreator --member="${RUNNER_PRINCIPAL}"
```

## Updating tests

`tests.yaml` is delivered to the orchestrator via a ConfigMap mounted at
`/etc/orchestrator/tests.yaml`, so the image doesn't need to be rebuilt when
the test list changes. Just reapply the config map.
