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
NAMESPACE_MANIFEST="${MANIFEST_DIR}/workloads.yaml.tmpl"
WORKER_POOL_MANIFEST="${MANIFEST_DIR}/workerpool.yaml.tmpl"
# The benchmark ActorTemplates: <name>-template.yaml.tmpl each, created
# through the ate API in the benchmark-workloads atespace. WORKLOAD_TEMPLATES
# overrides the default set — the usermem and kernelmem templates (for the
# matching locust tests) are not deployed by default.
read -r -a TEMPLATES <<<"${WORKLOAD_TEMPLATES:-sleep glutton glutton-durdir-data glutton-durdir-full}"

for manifest in "${NAMESPACE_MANIFEST}" "${WORKER_POOL_MANIFEST}"; do
  if [[ ! -f "${manifest}" ]]; then
    echo "Error: ${manifest} not found in $(pwd)" >&2
    exit 1
  fi
done

WORKER_COUNT=1
# Worker pools to create, as name:count[:nodeSelectorKey=value] entries. Empty
# means the single pool named benchmark-ateom that this script has always
# created. Pass the same name:count list to the boomer workers so each pool
# gets the actors its workers can hold.
WORKER_POOLS=""
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
  echo "  --worker-count N            Number of WorkerPool replicas when --worker-pools is not set (default: 1)"
  echo "  --worker-pools LIST         Comma-separated name:count[:nodeSelectorKey=value] entries."
  echo "                              One WorkerPool per entry, labeled pool=<name>, with <count>"
  echo "                              replicas. Pass the same name:count list to the boomer workers"
  echo "                              (--worker-pools). Default: a single pool named benchmark-ateom"
  echo "                              with --worker-count replicas."
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
  local sandbox_config_name sandbox_class_enum
  case "${SANDBOX_CLASS}" in
    gvisor)  sandbox_config_name="gvisor-default" sandbox_class_enum="SANDBOX_CLASS_GVISOR" ;;
    microvm) sandbox_config_name="microvm"        sandbox_class_enum="SANDBOX_CLASS_MICROVM" ;;
  esac
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${WORKER_COUNT}|${WORKER_COUNT}|g" \
      -e "s|\${POOL_NAME}|${POOL_NAME:-}|g" \
      -e "s|\${POOL_WORKERPOOL_NAME}|${POOL_WORKERPOOL_NAME:-}|g" \
      -e "s|\${POOL_WORKER_COUNT}|${POOL_WORKER_COUNT:-}|g" \
      -e "s|\${SANDBOX_CLASS}|${SANDBOX_CLASS}|g" \
      -e "s|\${SANDBOX_CLASS_ENUM}|${sandbox_class_enum}|g" \
      -e "s|\${SANDBOX_CONFIG_NAME}|${sandbox_config_name}|g" \
      -e "s|\${OTLP_ENDPOINT}|${OTLP_ENDPOINT}|g" \
      -e "s|\${ACTOR_MEMORY}|${ACTOR_MEMORY}|g" \
      -e "s|\${SWEPERF_IMAGE}|${SWEPERF_IMAGE:-}|g" \
      "${manifest}"
}

# Parsed form of WORKER_POOLS, filled by parse_worker_pools. Index i of each
# array describes the same pool.
POOL_NAMES=()
POOL_NODE_SELECTORS=()
POOL_WORKER_COUNTS=()

# parse_worker_pools reads WORKER_POOLS into the POOL_* arrays, using each
# entry's absolute worker count directly so no rounding alters the ratios
# relative to what the load generator uses. An empty WORKER_POOLS yields the
# historical single pool with WORKER_COUNT replicas.
parse_worker_pools() {
  POOL_NAMES=()
  POOL_NODE_SELECTORS=()
  POOL_WORKER_COUNTS=()

  if [[ -z "${WORKER_POOLS}" ]]; then
    POOL_NAMES=("benchmark-ateom")
    POOL_NODE_SELECTORS=("")
    POOL_WORKER_COUNTS=("${WORKER_COUNT}")
    return 0
  fi

  local entry name count selector rest total=0
  local IFS=,
  for entry in ${WORKER_POOLS}; do
    [[ -z "${entry}" ]] && continue
    name="${entry%%:*}"
    rest="${entry#*:}"
    if [[ "${rest}" == "${entry}" ]]; then
      echo "Error: worker pool '${entry}': want name:count[:nodeSelectorKey=value]" >&2
      exit 1
    fi
    count="${rest%%:*}"
    # A third field is optional; without it rest still holds just the count.
    if [[ "${rest}" == *:* ]]; then
      selector="${rest#*:}"
    else
      selector=""
    fi
    if [[ -z "${name}" ]]; then
      echo "Error: worker pool '${entry}': name must not be empty" >&2
      exit 1
    fi
    if (( ${#name} > 47 )); then
      echo "Error: worker pool '${entry}': name is longer than the 47-character limit" >&2
      exit 1
    fi
    if ! [[ "${name}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
      echo "Error: worker pool '${entry}': name must be a lowercase DNS-1123 label" >&2
      exit 1
    fi
    local existing
    for existing in "${POOL_NAMES[@]}"; do
      if [[ "${existing}" == "${name}" ]]; then
        echo "Error: worker pool '${entry}': duplicate pool name '${name}'" >&2
        exit 1
      fi
    done
    if ! [[ "${count}" =~ ^[0-9]+$ ]] || (( count == 0 )); then
      echo "Error: worker pool '${entry}': count must be a positive integer" >&2
      exit 1
    fi
    if [[ -n "${selector}" ]] && { [[ "${selector}" != *=* ]] || [[ -z "${selector%%=*}" ]] || [[ -z "${selector#*=}" ]]; }; then
      echo "Error: worker pool '${entry}': node selector must be key=value" >&2
      exit 1
    fi
    POOL_NAMES+=("${name}")
    POOL_WORKER_COUNTS+=("${count}")
    POOL_NODE_SELECTORS+=("${selector}")
    total=$((total + count))
  done

  if (( ${#POOL_NAMES[@]} == 0 )); then
    echo "Error: --worker-pools is set but names no pool" >&2
    exit 1
  fi

  WORKER_COUNT="${total}"
}

# render_worker_pool writes the WorkerPool manifest for pool index $1. The pod
# template (worker memory and node selector) is appended rather than templated:
# it is an optional nested block, which a line-oriented placeholder cannot
# express without leaving a dangling key behind when both are unset.
render_worker_pool() {
  local idx="$1"
  local POOL_NAME="${POOL_NAMES[idx]}"
  local POOL_WORKER_COUNT="${POOL_WORKER_COUNTS[idx]}"
  local POOL_WORKERPOOL_NAME
  POOL_WORKERPOOL_NAME="$(worker_pool_deployment_name "${idx}")"

  substitute "${WORKER_POOL_MANIFEST}"

  local selector="${POOL_NODE_SELECTORS[idx]}"
  if [[ -n "${WORKER_MEMORY}" || -n "${selector}" ]]; then
    printf '  template:\n'
    if [[ -n "${WORKER_MEMORY}" ]]; then
      printf '    resources:\n      requests:\n        memory: %s\n      limits:\n        memory: %s\n' \
        "$(yaml_quote "${WORKER_MEMORY}")" "$(yaml_quote "${WORKER_MEMORY}")"
    fi
    if [[ -n "${selector}" ]]; then
      # Split on the first = only, so a value may contain one. Both sides are
      # quoted: nodeSelector values must be strings, and a bare `true` or `8`
      # would parse as a bool or an int.
      printf '    nodeSelector:\n      %s: %s\n' \
        "$(yaml_quote "${selector%%=*}")" "$(yaml_quote "${selector#*=}")"
    fi
  fi
}

# yaml_quote prints $1 as a double-quoted YAML scalar.
yaml_quote() {
  local s="${1//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '"%s"' "${s}"
}

# worker_pool_deployment_name is both the WorkerPool name and the name of the
# Deployment ate-controller derives from it, which is what deploy waits on.
worker_pool_deployment_name() {
  local idx="$1"
  if [[ -z "${WORKER_POOLS}" ]]; then
    # Unchanged from the single-pool layout, so existing tooling that waits on
    # deployment/benchmark-ateom keeps working.
    echo "benchmark-ateom"
    return
  fi
  echo "benchmark-ateom-${POOL_NAMES[idx]}"
}

# prune_stale_worker_pools deletes benchmark WorkerPools that an earlier deploy
# created but the current layout no longer names (switching between the
# default and --worker-pools, or renaming/dropping a pool). They carry the
# same workload=benchmark-ateom label, so left alone they would keep their
# workers running alongside the current pools.
prune_stale_worker_pools() {
  local idx existing current=" "
  for idx in "${!POOL_NAMES[@]}"; do
    current+="$(worker_pool_deployment_name "${idx}") "
  done
  while IFS= read -r existing; do
    [[ -z "${existing}" || "${current}" == *" ${existing} "* ]] && continue
    echo "Deleting stale worker pool ${existing}..."
    kubectl delete workerpool -n benchmark-workloads "${existing}" --ignore-not-found
  done < <(kubectl get workerpool -n benchmark-workloads -l workload=benchmark-ateom \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
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
  resolve_otlp_endpoint
  parse_worker_pools
  echo "Deploying workloads (worker_count=${WORKER_COUNT}, pools=${#POOL_NAMES[@]}, actor_memory=${ACTOR_MEMORY}, worker_memory=${WORKER_MEMORY:-unset}, otlp_endpoint=${OTLP_ENDPOINT})..."
  substitute "${NAMESPACE_MANIFEST}" | kubectl apply -f -

  local idx deployment
  for idx in "${!POOL_NAMES[@]}"; do
    echo "  pool ${POOL_NAMES[idx]}: ${POOL_WORKER_COUNTS[idx]} worker(s)${POOL_NODE_SELECTORS[idx]:+ on ${POOL_NODE_SELECTORS[idx]}}"
  done
  for idx in "${!POOL_NAMES[@]}"; do
    (( idx > 0 )) && echo "---"
    render_worker_pool "${idx}"
  done | hack/run-tool.sh ko apply -f -
  prune_stale_worker_pools

  echo "Waiting for worker pools to be ready (timeout: ${WAIT_TIMEOUT_SECS}s)..."
  for idx in "${!POOL_NAMES[@]}"; do
    deployment="$(worker_pool_deployment_name "${idx}")"
    kubectl wait --for=create "deployment/${deployment}" \
      --namespace=benchmark-workloads --timeout="${WAIT_TIMEOUT_SECS}s"
    kubectl rollout status "deployment/${deployment}" \
      --namespace=benchmark-workloads --timeout="${WAIT_TIMEOUT_SECS}s"
  done

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
    substitute "${MANIFEST_DIR}/${template}-template.yaml.tmpl" \
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
  # Every benchmark WorkerPool carries the workload=benchmark-ateom label, so a
  # single label-selected delete removes all pools without invoking ko or
  # needing the caller to pass --worker-pools on teardown. The namespace goes
  # last, after the pools it holds.
  kubectl delete workerpool -n benchmark-workloads -l workload=benchmark-ateom --ignore-not-found
  substitute "${NAMESPACE_MANIFEST}" | kubectl delete --ignore-not-found -f -
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
    --worker-pools)
      shift
      WORKER_POOLS="$1"
      ;;
    --worker-pools=*)
      WORKER_POOLS="${1#*=}"
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
fi
