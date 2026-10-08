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
ISOLATE_MANIFEST="${MANIFEST_DIR}/fake-data-plane.yaml.tmpl"
# The benchmark ActorTemplates: <name>-template.yaml.tmpl each, created
# through the ate API in the benchmark-workloads atespace. WORKLOAD_TEMPLATES
# overrides the default set — the usermem and kernelmem templates (for the
# matching locust tests) are not deployed by default.
read -r -a TEMPLATES <<<"${WORKLOAD_TEMPLATES:-sleep glutton glutton-durdir-data glutton-durdir-full}"

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

# --isolate, isolate mode, serves the WorkerPool with a fake data plane,
# fake-atelet and fake-workersync, instead of worker pods, for benchmarking
# ate-api-server and Postgres past what real nodes can restore. See
# fake-data-plane.yaml.tmpl.
ISOLATE=false
ISOLATE_NODES=1
ISOLATE_RUN="bench"
ISOLATE_DELAY="0s"
ISOLATE_NODE_LABEL="ate.dev/fake-data-plane"

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
  echo "  --worker-memory SIZE        Memory request and limit for each WorkerPool pod"
  echo "                              (default: unset, the pod is unsized)"
  echo "  --otlp-endpoint URL         The address to which an instrumented actor container"
  echo "                              sends telemetry (default: the endpoint in the"
  echo "                              ate-otel-config ConfigMap)"
  echo "  --wait-timeout SECONDS      The timeout in seconds for waiting for the ateom workers to be ready (default: 300)"
  echo ""
  echo "Isolate mode: a fake data plane (benchmarks ate-api-server and Postgres; never on a cluster with real actors):"
  echo "  --isolate                   Serve the WorkerPool with fake-atelet and fake-workersync: --worker-count"
  echo "                              fake Workers that no pod backs, each reporting what a real worker would."
  echo "                              Moves atelet off the chosen nodes and scales ate-controller to"
  echo "                              zero; --delete undoes both."
  echo "  --isolate-nodes N           Nodes to run fake-atelet on (default: 1)"
  echo "  --isolate-run PREFIX        Prefix of the fake Worker names, up to 8 lowercase letters or"
  echo "                              digits (default: bench)"
  echo "  --isolate-delay DURATION    How long each fake atelet call takes, e.g. 500ms (default: 0s)"
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
  local sandbox_config_name sandbox_class_enum worker_resources=""
  case "${SANDBOX_CLASS}" in
    gvisor)  sandbox_config_name="gvisor-default" sandbox_class_enum="SANDBOX_CLASS_GVISOR" ;;
    microvm) sandbox_config_name="microvm"        sandbox_class_enum="SANDBOX_CLASS_MICROVM" ;;
  esac
  # One flow-style line, so an unset WORKER_MEMORY leaves only a blank line.
  if [[ -n "${WORKER_MEMORY}" ]]; then
    worker_resources="resources: {requests: {memory: \"${WORKER_MEMORY}\"}, limits: {memory: \"${WORKER_MEMORY}\"}}"
  fi
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${WORKER_COUNT}|${WORKER_COUNT}|g" \
      -e "s|\${SANDBOX_CLASS}|${SANDBOX_CLASS}|g" \
      -e "s|\${SANDBOX_CLASS_ENUM}|${sandbox_class_enum}|g" \
      -e "s|\${SANDBOX_CONFIG_NAME}|${sandbox_config_name}|g" \
      -e "s|\${OTLP_ENDPOINT}|${OTLP_ENDPOINT}|g" \
      -e "s|\${ACTOR_MEMORY}|${ACTOR_MEMORY}|g" \
      -e "s|\${WORKER_RESOURCES}|${worker_resources}|g" \
      -e "s|\${SWEPERF_IMAGE}|${SWEPERF_IMAGE:-}|g" \
      -e "s|\${ISOLATE_RUN}|${ISOLATE_RUN}|g" \
      -e "s|\${ISOLATE_DELAY}|${ISOLATE_DELAY}|g" \
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

# The real atelet DaemonSet(s): app=atelet without the fake-atelet label.
# Addressed by label, since the rendered name carries a version suffix.
REAL_ATELET_SELECTOR="app=atelet,!ate.dev/fake-atelet"

# wait_no_pods polls until no pod matching the selector is left in the
# namespace, optionally on one node. kubectl wait --for=delete errors when
# nothing matches, which is the state being waited for.
wait_no_pods() {
  local namespace="$1" selector="$2" node="${3:-}" timeout_secs="$4"
  local deadline=$((SECONDS + timeout_secs))
  local field_selector=()
  if [[ -n "${node}" ]]; then
    field_selector=(--field-selector "spec.nodeName=${node}")
  fi
  while ((SECONDS < deadline)); do
    if [[ -z "$(kubectl get pods --namespace="${namespace}" -l "${selector}" "${field_selector[@]}" -o name 2>/dev/null)" ]]; then
      return 0
    fi
    sleep 5
  done
  echo "timed out waiting for pods ${selector} in ${namespace}${node:+ on ${node}} to go away" >&2
  return 1
}

# FAKE_STORAGE_SECRET holds the real atelet's object-storage settings, which
# fake-atelet loads to write its snapshot placeholders where atelet would.
FAKE_STORAGE_SECRET="fake-atelet-storage"

# copy_storage_env copies ATE_STORAGE_BACKEND and the AWS_* variables from the
# real atelet container into FAKE_STORAGE_SECRET. An S3 install needs all of
# them, credentials included, hence a Secret. A variable set from valueFrom
# cannot be copied as a value, so it stops the deploy rather than being
# dropped.
copy_storage_env() {
  local env_json
  env_json="$(kubectl get daemonset --namespace=ate-system -l "${REAL_ATELET_SELECTOR}" -o json \
    | jq -c '[.items[0].spec.template.spec.containers[] | select(.name == "atelet") | .env // [] | .[]
             | select(.name == "ATE_STORAGE_BACKEND" or (.name | startswith("AWS_")))]')"
  if jq -e 'any(.[]; has("value") | not)' <<<"${env_json}" >/dev/null; then
    echo "Error: the real atelet sets $(jq -r '[.[] | select(has("value") | not) | .name] | join(", ")' <<<"${env_json}") from valueFrom; add it to the ${FAKE_STORAGE_SECRET} Secret by hand" >&2
    exit 1
  fi
  local literals=()
  mapfile -t literals < <(jq -r '.[] | "--from-literal=\(.name)=\(.value)"' <<<"${env_json}")
  kubectl create secret generic "${FAKE_STORAGE_SECRET}" --namespace=ate-system "${literals[@]}" \
    --dry-run=client -o yaml | kubectl apply -f -
}

# deploy_isolate takes atelet's place on --isolate-nodes nodes, then
# applies the WorkerPool, which fake-workersync serves with fake Workers on
# them. Runs before the templates are deployed, so the fake Workers have
# capacity when each golden actor is placed.
deploy_isolate() {
  # The chosen nodes are ones that run an atelet today, so they are nodes
  # substrate already schedules to.
  local nodes=()
  mapfile -t nodes < <(kubectl get pods --namespace=ate-system -l "${REAL_ATELET_SELECTOR}" \
    -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | head -n "${ISOLATE_NODES}")
  if ((${#nodes[@]} < ISOLATE_NODES)); then
    echo "Error: --isolate-nodes=${ISOLATE_NODES} but only ${#nodes[@]} node(s) run atelet" >&2
    exit 1
  fi
  echo "Deploying the fake data plane on ${nodes[*]} (worker_count=${WORKER_COUNT}, delay=${ISOLATE_DELAY})..."
  # Before the first change to the cluster, so a storage setting it cannot
  # copy stops the deploy with nothing to undo.
  copy_storage_env
  kubectl label nodes "${nodes[@]}" "${ISOLATE_NODE_LABEL}=true" --overwrite

  # Move the real atelet off the labeled nodes before the fake exists: a node
  # with two app=atelet pods is refused by ate-api-server's dialer.
  local ds
  for ds in $(kubectl get daemonset --namespace=ate-system -l "${REAL_ATELET_SELECTOR}" -o name); do
    kubectl patch --namespace=ate-system "${ds}" --type=merge -p \
      '{"spec":{"template":{"spec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"'"${ISOLATE_NODE_LABEL}"'","operator":"DoesNotExist"}]}]}}}}}}}'
  done
  # Each real atelet pod may take its full 330s grace period to drain.
  local node
  for node in "${nodes[@]}"; do
    wait_no_pods ate-system "${REAL_ATELET_SELECTOR}" "${node}" 400
  done

  # ate-controller's worker syncer deletes every Worker with no live pod when
  # it starts, which would remove every fake Worker.
  kubectl scale deployment/ate-controller --namespace=ate-system --replicas=0
  wait_no_pods ate-system app=ate-controller "" 120

  substitute "${ISOLATE_MANIFEST}" | hack/run-tool.sh ko apply -f -
  kubectl rollout status daemonset/fake-atelet \
    --namespace=ate-system --timeout="${WAIT_TIMEOUT_SECS}s"
  kubectl rollout status deployment/fake-workersync \
    --namespace=benchmark-workloads --timeout="${WAIT_TIMEOUT_SECS}s"

  # The same WorkerPool a real run deploys. With ate-controller down no
  # Deployment is made for it; fake-workersync counts a fake Worker ready
  # once its capacity is reported, and writes that to the pool's status.
  substitute "${POOL_MANIFEST}" | hack/run-tool.sh ko apply -f -
  if ((WORKER_COUNT > 0)); then
    echo "Waiting for ${WORKER_COUNT} fake Workers to report capacity (timeout: ${WAIT_TIMEOUT_SECS}s)..."
    kubectl wait --for=jsonpath='{.status.readyReplicas}'="${WORKER_COUNT}" workerpool/benchmark-ateom \
      --namespace=benchmark-workloads --timeout="${WAIT_TIMEOUT_SECS}s"
  fi
}

# isolate_present reports whether isolate mode is deployed, so
# --delete undoes it even when called without --isolate, as the
# benchmark automation's teardown does.
isolate_present() {
  [[ -n "$(kubectl get daemonset/fake-atelet --namespace=ate-system -o name --ignore-not-found)" ]] \
    || [[ -n "$(kubectl get nodes -l "${ISOLATE_NODE_LABEL}" -o name)" ]]
}

# delete_isolate undoes deploy_isolate. fake-workersync goes
# first and in the foreground: it deletes its fake Workers as it shuts down,
# which needs ate-api-server, and after the actors are gone.
delete_isolate() {
  echo "Deleting the fake data plane..."
  kubectl delete deployment/fake-workersync --namespace=benchmark-workloads \
    --cascade=foreground --ignore-not-found --timeout=400s
  substitute "${ISOLATE_MANIFEST}" | hack/run-tool.sh ko delete --ignore-not-found -f -
  kubectl delete secret "${FAKE_STORAGE_SECRET}" --namespace=ate-system --ignore-not-found
  local ds
  for ds in $(kubectl get daemonset --namespace=ate-system -l "${REAL_ATELET_SELECTOR}" -o name); do
    kubectl patch --namespace=ate-system "${ds}" --type=merge -p '{"spec":{"template":{"spec":{"affinity":null}}}}'
  done
  if [[ -n "$(kubectl get nodes -l "${ISOLATE_NODE_LABEL}" -o name)" ]]; then
    kubectl label nodes -l "${ISOLATE_NODE_LABEL}" "${ISOLATE_NODE_LABEL}-"
  fi
  # Its startup sweep removes any fake Worker fake-workersync left behind.
  kubectl scale deployment/ate-controller --namespace=ate-system --replicas=1
}

deploy() {
  resolve_otlp_endpoint
  if [[ "${ISOLATE}" == "true" ]]; then
    deploy_isolate
  else
    echo "Deploying workloads (worker_count=${WORKER_COUNT}, actor_memory=${ACTOR_MEMORY}, worker_memory=${WORKER_MEMORY:-unset}, otlp_endpoint=${OTLP_ENDPOINT})..."
    substitute "${POOL_MANIFEST}" | hack/run-tool.sh ko apply -f -
    echo "Waiting for worker pool to be ready (timeout: ${WAIT_TIMEOUT_SECS}s)..."
    kubectl wait --for=create deployment/benchmark-ateom \
      --namespace=benchmark-workloads --timeout="${WAIT_TIMEOUT_SECS}s"
    kubectl rollout status deployment/benchmark-ateom \
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
  if isolate_present; then
    delete_isolate
  fi
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
    --isolate)
      ISOLATE=true
      ;;
    --isolate-nodes)
      shift
      ISOLATE_NODES="$1"
      ;;
    --isolate-nodes=*)
      ISOLATE_NODES="${1#*=}"
      ;;
    --isolate-run)
      shift
      ISOLATE_RUN="$1"
      ;;
    --isolate-run=*)
      ISOLATE_RUN="${1#*=}"
      ;;
    --isolate-delay)
      shift
      ISOLATE_DELAY="$1"
      ;;
    --isolate-delay=*)
      ISOLATE_DELAY="${1#*=}"
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

if ! [[ "${ISOLATE_NODES}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: --isolate-nodes must be a positive whole number, got '${ISOLATE_NODES}'" >&2
  exit 1
fi
if ! [[ "${ISOLATE_RUN}" =~ ^[a-z0-9]{1,8}$ ]]; then
  echo "Error: --isolate-run must be 1 to 8 lowercase letters or digits, got '${ISOLATE_RUN}'" >&2
  exit 1
fi
# A Go duration, as fake-atelet parses --delay; anything else crash-loops it.
if ! [[ "${ISOLATE_DELAY}" =~ ^(0|([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+)$ ]]; then
  echo "Error: --isolate-delay must be a duration such as 500ms or 1m30s, got '${ISOLATE_DELAY}'" >&2
  exit 1
fi

if [[ "${action}" == "deploy" ]]; then
  build_kubectl_ate
  deploy
elif [[ "${action}" == "delete" ]]; then
  build_kubectl_ate
  delete
fi
