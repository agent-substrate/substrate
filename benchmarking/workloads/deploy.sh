#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment variables if configured
if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

# Ensure BUCKET_NAME is set
if [[ -z "${BUCKET_NAME:-}" ]]; then
  echo "Error: BUCKET_NAME environment variable is not set." >&2
  exit 1
fi

MANIFEST_DIR="benchmarking/workloads/manifests"
POOL_MANIFEST="${MANIFEST_DIR}/workloads.yaml.tmpl"
# Generated sweperf ActorTemplates; see manifests/sweperf/generate_sweperf_templates.py.
SWEPERF_TEMPLATE_DIR="${MANIFEST_DIR}/sweperf/generated"
# The benchmark ActorTemplates: <name>-template.yaml.tmpl each, created
# through the ate API in the benchmark-workloads atespace. sweperf-* names are
# read from SWEPERF_TEMPLATE_DIR, the rest from MANIFEST_DIR. WORKLOAD_TEMPLATES
# overrides the default set — the usermem and kernelmem templates (for the
# matching locust tests) are not deployed by default. The glutton-storage
# template is added when a StorageClass is set, because it needs a CSI driver
# that not every cluster has.
read -r -a TEMPLATES <<<"${WORKLOAD_TEMPLATES:-sleep glutton glutton-durdir-data glutton-durdir-full}"

# template_manifest prints the manifest path for ActorTemplate NAME.
template_manifest() {
  case "$1" in
    sweperf-*) echo "${SWEPERF_TEMPLATE_DIR}/$1-template.yaml.tmpl" ;;
    *)         echo "${MANIFEST_DIR}/$1-template.yaml.tmpl" ;;
  esac
}

if [[ ! -f "${POOL_MANIFEST}" ]]; then
  echo "Error: ${POOL_MANIFEST} not found in $(pwd)" >&2
  exit 1
fi

WORKER_COUNT=1
SANDBOX_CLASS="gvisor"
# Actor memory limit (ActorTemplate resources.limits.memory). The default
# is the smallest size microvm admits (128Mi VMM reserve + 128Mi guest floor),
# since a bigger guest drags more page cache into every memory snapshot. Raise
# it for RAM-consuming suites.
ACTOR_MEMORY="256Mi"
# Actor CPU limit (ActorTemplate resources.limits.cpu). The scheduler books
# it against the worker's CPU capacity, so it bounds how many actors a worker
# hosts at once as much as the memory limit does.
ACTOR_CPU="1"
# Worker pod memory (WorkerPool spec.template.resources), set as both request
# and limit. Empty leaves the pod unsized. The limit is also the memory the
# worker reports as its actor capacity.
WORKER_MEMORY=""
# The StorageClass for the glutton-storage external volume. Empty means the
# template is not deployed.
STORAGE_CLASS_NAME="${STORAGE_CLASS_NAME:-}"
# The address to which an instrumented actor container sends its telemetry.
# --otlp-endpoint sets it. Without the flag, resolve_otlp_endpoint reads the
# address that the control plane uses.
OTLP_ENDPOINT=""
# The timeout, in whole seconds, for waiting for the worker pods to be
# ready.
WAIT_TIMEOUT_SECS=300
# fake-atelet's tuning on the fake data plane, as FAKE_ATELET_<FLAG>=VALUE
# entries for the fake-atelet-tuning ConfigMap; --fake-atelet-env adds one.
FAKE_ATELET_ENV=()

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy                    Substitute env vars and deploy workloads to the cluster using ko apply"
  echo "  --delete                    Substitute env vars and delete workloads from the cluster"
  echo "  --worker-count N            Number of WorkerPool replicas (default: 1)"
  echo "  --sandbox-class CLASS       Sandbox runtime for the WorkerPool: gvisor | microvm (default: gvisor)."
  echo "                              microvm requires hack/install-microvm-deps.sh --install to have run."
  echo "  --actor-memory SIZE         Memory limit for the benchmark ActorTemplates (default: 256Mi,"
  echo "                              the smallest size microvm admits)"
  echo "  --actor-cpu CPU             CPU limit for the benchmark ActorTemplates (default: 1)"
  echo "  --worker-memory SIZE        Memory request and limit for each WorkerPool pod"
  echo "                              (default: unset, the pod is unsized)"
  echo "  --storage-class-name NAME   StorageClass for the glutton-storage external volume. Setting it"
  echo "                              deploys glutton-storage (default: not deployed). Env: STORAGE_CLASS_NAME."
  echo "  --otlp-endpoint URL         The address to which an instrumented actor container"
  echo "                              sends telemetry (default: the endpoint in the"
  echo "                              ate-otel-config ConfigMap)"
  echo "  --wait-timeout SECONDS      The timeout in seconds for waiting for the workers to be ready (default: 300)"
  echo "  --fake-atelet-env NAME=VALUE"
  echo "                              On a substrate installed with --fake-data-plane, set a fake-atelet flag:"
  echo "                              FAKE_ATELET_DELAY=500ms sets --delay, FAKE_ATELET_DELAY_RESTORE=2s sets"
  echo "                              --delay-restore. Repeatable; unset flags keep fake-atelet's defaults"
  echo "  -h, --help                  Show this help message"
}

# Read the endpoint from the ate-otel-config ConfigMap, which every control
# plane component reads through envFrom. The value is correct for the cluster
# in use: the GKE collector, the kind collector in otel-system, or the
# telemetry meter while a measurement runs. Thus the actors follow the control
# plane, and this script needs no test for the type of cluster.
#
# ate-system must exist, because the workloads below need its CRDs. Thus an
# absent ConfigMap is an error and not a condition to work around: a default
# here would send the actor telemetry to the wrong collector with no message.
resolve_otlp_endpoint() {
  if [[ -n "${OTLP_ENDPOINT}" ]]; then
    return 0
  fi
  OTLP_ENDPOINT="$(kubectl get configmap ate-otel-config --namespace=ate-system \
    -o jsonpath='{.data.OTEL_EXPORTER_OTLP_ENDPOINT}' 2>/dev/null || true)"
  if [[ -z "${OTLP_ENDPOINT}" ]]; then
    echo "Error: cannot read OTEL_EXPORTER_OTLP_ENDPOINT from the ate-otel-config" >&2
    echo "ConfigMap in ate-system. Deploy ate-system first, or give --otlp-endpoint." >&2
    exit 1
  fi
}

# kubectl-ate runs once per poll while waiting for the golden snapshots;
# build_kubectl_ate builds it once up front instead of paying the `go run`
# toolchain startup on every call.
KUBECTL_ATE_BIN=""

build_kubectl_ate() {
  local dir
  dir="$(mktemp -d)"
  trap 'rm -rf '"${dir}" EXIT
  KUBECTL_ATE_BIN="${dir}/kubectl-ate"
  go build -o "${KUBECTL_ATE_BIN}" ./cmd/kubectl-ate
}

run_kubectl_ate() {
  "${KUBECTL_ATE_BIN}" "$@"
}

substitute() {
  local manifest="$1"
  local worker_resources=""
  # One flow-style line, so an unset WORKER_MEMORY leaves only a blank line.
  if [[ -n "${WORKER_MEMORY}" ]]; then
    worker_resources="resources: {requests: {memory: \"${WORKER_MEMORY}\"}, limits: {memory: \"${WORKER_MEMORY}\"}}"
  fi
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${WORKER_COUNT}|${WORKER_COUNT}|g" \
      -e "s|\${SANDBOX_CLASS}|${SANDBOX_CLASS}|g" \
      -e "s|\${OTLP_ENDPOINT}|${OTLP_ENDPOINT}|g" \
      -e "s|\${ACTOR_MEMORY}|${ACTOR_MEMORY}|g" \
      -e "s|\${ACTOR_CPU}|${ACTOR_CPU}|g" \
      -e "s|\${WORKER_RESOURCES}|${worker_resources}|g" \
      -e "s|\${STORAGE_CLASS_NAME}|${STORAGE_CLASS_NAME}|g" \
      "${manifest}"
}

# wait_actortemplate_ready polls a substrate ActorTemplate resource until its
# golden snapshot exists (the substrate counterpart of `kubectl wait
# --for=condition=Ready actortemplate/...`). Fails fast when the template
# reconciler reports an error.
wait_actortemplate_ready() {
  local atespace="$1"
  local template="$2"
  local timeout_secs="${3:-300}"
  local deadline=$((SECONDS + timeout_secs))
  local json snapshot error_message

  while ((SECONDS < deadline)); do
    if json=$(run_kubectl_ate get actor-template "${template}" -a "${atespace}" -o json 2>/dev/null); then
      snapshot=$(jq -r '.status.goldenSnapshotStatus.goldenTag.name // empty' <<<"${json}")
      if [[ -n "${snapshot}" ]]; then
        return 0
      fi
      error_message=$(jq -r '.status.goldenSnapshotStatus.errorMessage // empty' <<<"${json}")
      if [[ -n "${error_message}" ]]; then
        echo "actor template ${atespace}/${template} failed: ${error_message}" >&2
        return 1
      fi
    fi
    sleep 5
  done

  echo "timed out waiting for actor template ${atespace}/${template} golden snapshot" >&2
  return 1
}

# wait_templates_ready blocks until every benchmark template's golden
# snapshot exists (there is no kubectl wait for substrate resources), failing
# fast when the template reconciler reports an error.
wait_templates_ready() {
  if ! command -v jq &>/dev/null; then
    echo "jq is required to wait for the benchmark actor templates" >&2
    return 1
  fi
  local template
  for template in "${TEMPLATES[@]}"; do
    echo "Waiting for the benchmark-workloads/${template} golden snapshot..."
    wait_actortemplate_ready benchmark-workloads "${template}" "${WAIT_TIMEOUT_SECS}"
  done
}

# fake_data_plane reports whether substrate was installed with
# --fake-data-plane, which serves the WorkerPool with fake Workers instead of
# worker pods.
fake_data_plane() {
  [[ -n "$(kubectl get deployment/fake-workersync --namespace=ate-system -o name --ignore-not-found)" ]]
}

# tune_fake_atelet writes FAKE_ATELET_ENV, and nothing else, to the
# fake-atelet-tuning ConfigMap fake-atelet loads its flags from, and restarts
# fake-atelet when that changes the ConfigMap. No install creates or resets
# it: the installer chooses the fake data plane, and the benchmark tunes it.
tune_fake_atelet() {
  local before after
  before="$(kubectl get configmap fake-atelet-tuning --namespace=ate-system -o jsonpath='{.data}' --ignore-not-found)"
  local literals=()
  local entry
  for entry in "${FAKE_ATELET_ENV[@]}"; do
    literals+=("--from-literal=${entry}")
  done
  kubectl create configmap fake-atelet-tuning --namespace=ate-system "${literals[@]}" \
    --dry-run=client -o yaml | kubectl apply -f -
  after="$(kubectl get configmap fake-atelet-tuning --namespace=ate-system -o jsonpath='{.data}')"
  if [[ "${before}" == "${after}" ]]; then
    return 0
  fi
  echo "Restarting fake-atelet with its new tuning (${FAKE_ATELET_ENV[*]:-defaults})..."
  # The DaemonSet name carries the version, so it is addressed by label.
  local ds
  for ds in $(kubectl get daemonset --namespace=ate-system -l app=atelet -o name); do
    kubectl rollout restart --namespace=ate-system "${ds}"
    kubectl rollout status --namespace=ate-system "${ds}" --timeout="${WAIT_TIMEOUT_SECS}s"
  done
}

deploy() {
  # Check every requested template has a manifest before anything on the
  # cluster is deleted or created.
  local template manifest
  for template in "${TEMPLATES[@]}"; do
    manifest="$(template_manifest "${template}")"
    if [[ ! -f "${manifest}" ]]; then
      echo "Error: no manifest for template ${template}: ${manifest} not found" >&2
      if [[ "${template}" == sweperf-* ]]; then
        echo "sweperf templates come from ${MANIFEST_DIR}/sweperf/sweperf-images.json; add the task" >&2
        echo "there and run generate_sweperf_templates.py (see \"Sweperf Tasks\" in benchmarking/README.md)" >&2
      fi
      exit 1
    fi
  done

  resolve_otlp_endpoint
  # Before the pool, so its fake Workers are served by the tuned fake-atelet.
  if fake_data_plane; then
    tune_fake_atelet
  elif ((${#FAKE_ATELET_ENV[@]} > 0)); then
    echo "Error: --fake-atelet-env needs substrate installed with --fake-data-plane" >&2
    exit 1
  fi
  echo "Deploying workloads (worker_count=${WORKER_COUNT}, actor_memory=${ACTOR_MEMORY}, actor_cpu=${ACTOR_CPU}, worker_memory=${WORKER_MEMORY:-unset}, storage_class=${STORAGE_CLASS_NAME:-none}, otlp_endpoint=${OTLP_ENDPOINT})..."
  substitute "${POOL_MANIFEST}" | hack/run-tool.sh ko apply -f -
  echo "Waiting for worker pool to be ready (timeout: ${WAIT_TIMEOUT_SECS}s)..."
  if fake_data_plane; then
    # No Deployment is made for the pool; fake-workersync counts a fake Worker
    # ready once its capacity is reported, and writes that to the pool's
    # status.
    if ((WORKER_COUNT > 0)); then
      kubectl wait --for=jsonpath='{.status.readyReplicas}'="${WORKER_COUNT}" workerpool/benchmark-worker \
        --namespace=benchmark-workloads --timeout="${WAIT_TIMEOUT_SECS}s"
    fi
  else
    kubectl wait --for=create deployment/benchmark-worker \
      --namespace=benchmark-workloads --timeout="${WAIT_TIMEOUT_SECS}s"
    kubectl rollout status deployment/benchmark-worker \
      --namespace=benchmark-workloads --timeout="${WAIT_TIMEOUT_SECS}s"
  fi

  # The store enforces that a template's atespace exists at create time.
  run_kubectl_ate create atespace benchmark-workloads >/dev/null 2>&1 \
    || run_kubectl_ate get atespace benchmark-workloads >/dev/null

  # Actor templates are immutable (no update RPC). A value that changes for
  # each run — the OTLP endpoint or the sandbox class — needs the removal of
  # the old template first. This removal is safe, because the benchmark
  # automation deletes the actors between the tests; it also removes the old
  # golden actor and snapshot server-side.
  for template in "${TEMPLATES[@]}"; do
    run_kubectl_ate delete actor-template "${template}" -a benchmark-workloads \
      >/dev/null 2>&1 || true
    # ko resolve builds the ko:// image references and replaces them with
    # pushed digests before the manifest reaches kubectl-ate.
    substitute "$(template_manifest "${template}")" \
      | hack/run-tool.sh ko resolve -f - \
      | run_kubectl_ate create actor-template -f -
  done

  wait_templates_ready
}

delete() {
  echo "Deleting workloads..."
  local template
  for template in "${TEMPLATES[@]}"; do
    run_kubectl_ate delete actor-template "${template}" -a benchmark-workloads \
      >/dev/null 2>&1 || true
  done
  run_kubectl_ate delete atespace benchmark-workloads >/dev/null 2>&1 \
    || echo "atespace benchmark-workloads not deleted (may not exist or is not empty)"
  # The pool manifest contains ko:// image references; route through
  # `ko delete` so they get resolved before kubectl sees them.
  substitute "${POOL_MANIFEST}" | hack/run-tool.sh ko delete --ignore-not-found -f -
}

if [[ "$#" -eq 0 ]]; then
  usage
  exit 1
fi

action=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --deploy)
      action="deploy"
      ;;
    --delete)
      action="delete"
      ;;
    --worker-count)
      shift
      WORKER_COUNT="$1"
      ;;
    --worker-count=*)
      WORKER_COUNT="${1#*=}"
      ;;
    --sandbox-class)
      shift
      SANDBOX_CLASS="$1"
      ;;
    --sandbox-class=*)
      SANDBOX_CLASS="${1#*=}"
      ;;
    --otlp-endpoint)
      shift
      OTLP_ENDPOINT="$1"
      ;;
    --otlp-endpoint=*)
      OTLP_ENDPOINT="${1#*=}"
      ;;
    --actor-memory)
      shift
      ACTOR_MEMORY="$1"
      ;;
    --actor-memory=*)
      ACTOR_MEMORY="${1#*=}"
      ;;
    --actor-cpu)
      shift
      ACTOR_CPU="$1"
      ;;
    --actor-cpu=*)
      ACTOR_CPU="${1#*=}"
      ;;
    --worker-memory)
      shift
      WORKER_MEMORY="$1"
      ;;
    --worker-memory=*)
      WORKER_MEMORY="${1#*=}"
      ;;
    --storage-class-name)
      shift
      STORAGE_CLASS_NAME="$1"
      ;;
    --storage-class-name=*)
      STORAGE_CLASS_NAME="${1#*=}"
      ;;
    --wait-timeout)
      shift
      WAIT_TIMEOUT_SECS="$1"
      ;;
    --wait-timeout=*)
      WAIT_TIMEOUT_SECS="${1#*=}"
      ;;
    --fake-atelet-env)
      shift
      FAKE_ATELET_ENV+=("$1")
      ;;
    --fake-atelet-env=*)
      FAKE_ATELET_ENV+=("${1#*=}")
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

case "${SANDBOX_CLASS}" in
  gvisor|microvm) ;;
  *)
    echo "Error: --sandbox-class must be gvisor or microvm, got '${SANDBOX_CLASS}'" >&2
    exit 1
    ;;
esac

if ! [[ "${WAIT_TIMEOUT_SECS}" =~ ^[0-9]+$ ]]; then
  echo "Error: --wait-timeout must be a whole number of seconds like 300, got '${WAIT_TIMEOUT_SECS}'" >&2
  exit 1
fi

# glutton-storage joins the default set only when it has a StorageClass. Delete
# always includes it, because deleting a template that does not exist is a
# no-op.
if [[ -z "${WORKLOAD_TEMPLATES:-}" ]] \
  && { [[ -n "${STORAGE_CLASS_NAME}" ]] || [[ "${action}" == "delete" ]]; }; then
  TEMPLATES+=(glutton-storage)
fi

if [[ "${action}" == "deploy" && -z "${STORAGE_CLASS_NAME}" && " ${TEMPLATES[*]} " == *" glutton-storage "* ]]; then
  echo "Error: the glutton-storage template needs --storage-class-name" >&2
  exit 1
fi

for entry in "${FAKE_ATELET_ENV[@]}"; do
  if ! [[ "${entry}" =~ ^FAKE_ATELET_[A-Z_]+=.+$ ]]; then
    echo "Error: --fake-atelet-env must be FAKE_ATELET_<FLAG>=VALUE, such as FAKE_ATELET_DELAY=500ms, got '${entry}'" >&2
    exit 1
  fi
done

if [[ "${action}" == "deploy" ]]; then
  build_kubectl_ate
  deploy
elif [[ "${action}" == "delete" ]]; then
  build_kubectl_ate
  delete
fi
