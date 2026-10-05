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
# The benchmark ActorTemplates: <name>-template.yaml.tmpl each, created
# through the ate API in the benchmark-workloads atespace. WORKLOAD_TEMPLATES
# overrides the default set — the usermem and kernelmem templates (for the
# matching locust tests) are not deployed by default.
#
# A sweperf-<repo>-<id> name (e.g. sweperf-django-11099) has no file of its
# own: it is rendered from SWEPERF_TASK_TEMPLATE using that task's entry in
# SWEPERF_CATALOG, so adding a task only takes an images.json entry. The
# SWEPERF_CATALOG environment variable points at a different catalog.
read -r -a TEMPLATES <<<"${WORKLOAD_TEMPLATES:-sleep glutton glutton-durdir-data glutton-durdir-full}"
SWEPERF_CATALOG="${SWEPERF_CATALOG:-${MANIFEST_DIR}/images.json}"
SWEPERF_TASK_TEMPLATE="${MANIFEST_DIR}/sweperf-template.yaml.tmpl"

if [[ ! -f "${POOL_MANIFEST}" ]]; then
  echo "Error: ${POOL_MANIFEST} not found in $(pwd)" >&2
  exit 1
fi

WORKER_COUNT=1
SANDBOX_CLASS="gvisor"
# Actor memory limit (ActorTemplate resources.limits.memory). The default
# is the smallest size microvm admits (128Mi VMM reserve + 128Mi guest floor),
# so benchmark actors do not inherit the 2 GiB kata default and drag its page
# cache into every memory snapshot. Raise it for RAM-consuming suites.
ACTOR_MEMORY="256Mi"
# Worker pod memory (WorkerPool spec.template.resources), set as both request
# and limit. Empty leaves the pod unsized. The limit is also the memory the
# worker reports as its actor capacity.
WORKER_MEMORY=""
# The address to which an instrumented actor container sends its telemetry.
# --otlp-endpoint sets it. Without the flag, resolve_otlp_endpoint reads the
# address that the control plane uses.
OTLP_ENDPOINT=""
# The timeout, in whole seconds, for waiting for the ateom worker pods to be
# ready.
WAIT_TIMEOUT_SECS=300

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy                    Substitute env vars and deploy workloads to the cluster using ko apply"
  echo "  --delete                    Substitute env vars and delete workloads from the cluster"
  echo "  --render NAME               Print the ActorTemplate manifest NAME would be created from,"
  echo "                              without touching the cluster. Cluster-derived values, such as"
  echo "                              the OTLP endpoint, stay empty unless their flags are given"
  echo "  --worker-count N            Number of WorkerPool replicas (default: 1)"
  echo "  --sandbox-class CLASS       Sandbox runtime for the WorkerPool: gvisor | microvm (default: gvisor)."
  echo "                              microvm requires hack/install-microvm-deps.sh --install to have run."
  echo "  --actor-memory SIZE         Memory limit for the benchmark ActorTemplates (default: 256Mi,"
  echo "                              the smallest size microvm admits)"
  echo "  --worker-memory SIZE        Memory request and limit for each WorkerPool pod"
  echo "                              (default: unset, the pod is unsized)"
  echo "  --otlp-endpoint URL         The address to which an instrumented actor container"
  echo "                              sends telemetry (default: the endpoint in the"
  echo "                              ate-otel-config ConfigMap)"
  echo "  --wait-timeout SECONDS      The timeout in seconds for waiting for the ateom workers to be ready (default: 300)"
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
  # SandboxConfig names are pinned per class in the ActorTemplates (rather
  # than defaulted) so a stale config from a dirty teardown fails loudly
  # instead of silently binding these workloads. gvisor-default is applied by
  # hack/install-ate.sh; microvm is applied by hack/install-microvm-deps.sh.
  # The protojson templates take the sandbox class as its proto enum spelling.
  local manifest="$1"
  local sandbox_config_name sandbox_class_enum worker_template=""
  case "${SANDBOX_CLASS}" in
    gvisor)  sandbox_config_name="gvisor-default" sandbox_class_enum="SANDBOX_CLASS_GVISOR" ;;
    microvm) sandbox_config_name="microvm"        sandbox_class_enum="SANDBOX_CLASS_MICROVM" ;;
  esac
  # One flow-style line, so an unset WORKER_MEMORY leaves only a blank line.
  if [[ -n "${WORKER_MEMORY}" ]]; then
    worker_template="template: {resources: {requests: {memory: \"${WORKER_MEMORY}\"}, limits: {memory: \"${WORKER_MEMORY}\"}}}"
  fi
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${WORKER_COUNT}|${WORKER_COUNT}|g" \
      -e "s|\${SANDBOX_CLASS}|${SANDBOX_CLASS}|g" \
      -e "s|\${SANDBOX_CLASS_ENUM}|${sandbox_class_enum}|g" \
      -e "s|\${SANDBOX_CONFIG_NAME}|${sandbox_config_name}|g" \
      -e "s|\${OTLP_ENDPOINT}|${OTLP_ENDPOINT}|g" \
      -e "s|\${ACTOR_MEMORY}|${ACTOR_MEMORY}|g" \
      -e "s|\${WORKER_TEMPLATE}|${worker_template}|g" \
      "${manifest}"
}

# sweperf_entry prints the images.json entry whose template name is $1, as
# tab-separated tag, image, digest and steps. The template name of an entry
# is "sweperf-" plus its tag with underscores turned into hyphens, e.g.
# astropy_7336 -> sweperf-astropy-7336.
sweperf_entry() {
  jq -r --arg name "$1" '
    .images[]
    | select("sweperf-" + (.tag | gsub("_"; "-")) == $name)
    | [.tag, .image, .digest, (.steps | tostring)]
    | @tsv' "${SWEPERF_CATALOG}"
}

# check_templates fails before anything is deployed when a requested template
# has neither a <name>-template.yaml.tmpl file nor exactly one usable
# images.json entry.
check_templates() {
  local template entry steps
  for template in "${TEMPLATES[@]}"; do
    # SWEPERF_TASK_TEMPLATE matches the <name>-template.yaml.tmpl pattern for
    # the bare name "sweperf", but it only deploys once filled for a task.
    if [[ "${template}" == "sweperf" ]]; then
      echo "Error: 'sweperf' is the generic task template; name a task instead, e.g. sweperf-astropy-7336" >&2
      return 1
    fi
    if [[ -f "${MANIFEST_DIR}/${template}-template.yaml.tmpl" ]]; then
      continue
    fi
    if [[ "${template}" != sweperf-* ]]; then
      echo "Error: no ${MANIFEST_DIR}/${template}-template.yaml.tmpl for template '${template}'" >&2
      return 1
    fi
    if ! command -v jq &>/dev/null; then
      echo "Error: jq is required to render the ${template} template from ${SWEPERF_CATALOG}" >&2
      return 1
    fi
    entry="$(sweperf_entry "${template}")"
    if [[ -z "${entry}" ]]; then
      echo "Error: template '${template}' has no entry in ${SWEPERF_CATALOG}" >&2
      return 1
    fi
    if [[ "${entry}" == *$'\n'* ]]; then
      echo "Error: template '${template}' matches more than one entry in ${SWEPERF_CATALOG}" >&2
      return 1
    fi
    IFS=$'\t' read -r _ _ _ steps <<<"${entry}"
    if ! [[ "${steps}" =~ ^[1-9][0-9]*$ ]]; then
      echo "Error: template '${template}' has invalid steps '${steps}' in ${SWEPERF_CATALOG}" >&2
      return 1
    fi
  done
}

# render_template prints the substituted manifest for template $1: its own
# <name>-template.yaml.tmpl when there is one, else the generic SWE-perf task
# template filled from the task's images.json entry.
render_template() {
  local template="$1"
  local file="${MANIFEST_DIR}/${template}-template.yaml.tmpl"
  if [[ -f "${file}" ]]; then
    substitute "${file}"
    return
  fi
  local tag image digest steps
  IFS=$'\t' read -r tag image digest steps <<<"$(sweperf_entry "${template}")"
  sed -e "s|\${SWEPERF_NAME}|${template}|g" \
      -e "s|\${SWEPERF_CONTAINER}|${tag%%_*}|g" \
      -e "s|\${SWEPERF_IMAGE}|${image}@${digest}|g" \
      -e "s|\${SWEPERF_STEPS}|${steps}|g" \
      "${SWEPERF_TASK_TEMPLATE}" \
    | substitute /dev/stdin
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

deploy() {
  check_templates
  resolve_otlp_endpoint
  echo "Deploying workloads (worker_count=${WORKER_COUNT}, actor_memory=${ACTOR_MEMORY}, worker_memory=${WORKER_MEMORY:-unset}, otlp_endpoint=${OTLP_ENDPOINT})..."
  substitute "${POOL_MANIFEST}" | hack/run-tool.sh ko apply -f -
  echo "Waiting for worker pool to be ready (timeout: ${WAIT_TIMEOUT_SECS}s)..."
  kubectl wait --for=create deployment/benchmark-ateom \
    --namespace=benchmark-workloads --timeout="${WAIT_TIMEOUT_SECS}s"
  kubectl rollout status deployment/benchmark-ateom \
    --namespace=benchmark-workloads --timeout="${WAIT_TIMEOUT_SECS}s"

  # The store enforces that a template's atespace exists at create time.
  run_kubectl_ate create atespace benchmark-workloads >/dev/null 2>&1 \
    || run_kubectl_ate get atespace benchmark-workloads >/dev/null

  # Actor templates are immutable (no update RPC). A value that changes for
  # each run — the OTLP endpoint or the sandbox class — needs the removal of
  # the old template first. This removal is safe, because the benchmark
  # automation deletes the actors between the tests; it also removes the old
  # golden actor and snapshot server-side.
  local template
  for template in "${TEMPLATES[@]}"; do
    run_kubectl_ate delete actor-template "${template}" -a benchmark-workloads \
      >/dev/null 2>&1 || true
    # ko resolve builds the ko:// image references and replaces them with
    # pushed digests before the manifest reaches kubectl-ate.
    render_template "${template}" \
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
    --render)
      shift
      action="render"
      RENDER_TEMPLATE="$1"
      ;;
    --render=*)
      action="render"
      RENDER_TEMPLATE="${1#*=}"
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
    --worker-memory)
      shift
      WORKER_MEMORY="$1"
      ;;
    --worker-memory=*)
      WORKER_MEMORY="${1#*=}"
      ;;
    --wait-timeout)
      shift
      WAIT_TIMEOUT_SECS="$1"
      ;;
    --wait-timeout=*)
      WAIT_TIMEOUT_SECS="${1#*=}"
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

if [[ "${action}" == "deploy" ]]; then
  build_kubectl_ate
  deploy
elif [[ "${action}" == "delete" ]]; then
  build_kubectl_ate
  delete
elif [[ "${action}" == "render" ]]; then
  TEMPLATES=("${RENDER_TEMPLATE}")
  check_templates
  render_template "${RENDER_TEMPLATE}"
fi
