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

# Tests the --observability mode of hack/install-ate.sh: the selection of the
# mode, the tests of the flags, the ate-otel-config ConfigMap of each mode, and
# the preflight. The test needs no cluster: it gives hack/observability.sh a
# run_kubectl that answers from a list.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# The state of the cluster that the run_kubectl below reports.
#
#   FAKE_OBJECTS   one "kind/name" or "kind/name.namespace" for each object
#   FAKE_CONFIGMAP "<mode>|<endpoint>" of the ate-otel-config ConfigMap, and
#                  empty when the cluster has no such ConfigMap
#   FAKE_RESTARTS  the workloads that the test under way restarted
FAKE_OBJECTS=""
FAKE_CONFIGMAP=""
FAKE_RESTARTS=""

# run_kubectl answers a get from the state above, and records a rollout restart.
# Each other command is an error, because no function under test is permitted to
# change the cluster in another way.
run_kubectl() {
  local args=("$@") positional=() namespace="" i=0
  while ((i < ${#args[@]})); do
    case "${args[i]}" in
      -n | --namespace)
        namespace="${args[i + 1]}"
        ((i += 2))
        continue
        ;;
      -o)
        # The output format has no effect here: a get of the ConfigMap always
        # answers with the jsonpath that read_cluster_observability asks for.
        ((i += 2))
        continue
        ;;
      --namespace=* | -o* | --context=*) ;;
      *) positional+=("${args[i]}") ;;
    esac
    ((i++))
  done

  case "${positional[0]:-}" in
    get)
      local kind="${positional[1]:-}" name="${positional[2]:-}" key=""
      if [[ "${kind}" == "configmap" && "${name}" == "ate-otel-config" ]]; then
        [[ -n "${FAKE_CONFIGMAP}" ]] || return 1
        echo "${FAKE_CONFIGMAP}"
        return 0
      fi
      # A get takes "<kind> <name>", and also the "<kind>/<name>" of a rollout.
      key="${kind}"
      if [[ -n "${name}" ]]; then
        key="${key}/${name}"
      fi
      if [[ -n "${namespace}" ]]; then
        key="${key}.${namespace}"
      fi
      [[ " ${FAKE_OBJECTS} " == *" ${key} "* ]]
      ;;
    rollout)
      FAKE_RESTARTS="${FAKE_RESTARTS}${positional[2]:-} "
      ;;
    *)
      echo "run_kubectl: the test does not permit: $*" >&2
      return 1
      ;;
  esac
}

source "${ROOT}"/hack/observability.sh

# reset_state puts back the state that one install holds, thus each test below
# starts from an install that has done nothing yet.
reset_state() {
  ATE_OBSERVABILITY=""
  ATE_OTLP_ENDPOINT=""
  ATE_INSTALL_KIND=false
  ATE_OBSERVABILITY_SOURCE=""
  ATE_OBSERVABILITY_PREFLIGHT_DONE=false
  ATE_OBSERVABILITY_CLUSTER_READ=false
  ATE_OBSERVABILITY_CLUSTER_MODE=""
  ATE_OBSERVABILITY_CLUSTER_ENDPOINT=""
  ATE_OTEL_CONFIG_CHANGED=false
  FAKE_OBJECTS=""
  FAKE_CONFIGMAP=""
  FAKE_RESTARTS=""
}

failures=0

expect_eq() {
  local name="$1" want="$2" got="$3"
  if [[ "${want}" == "${got}" ]]; then
    echo "ok   ${name}"
    return 0
  fi
  echo "FAIL ${name}: want '${want}', got '${got}'"
  failures=$((failures + 1))
}

# expect_ok and expect_error run the given command in a subshell, because a
# function under test reports a fault of the operator with exit.
expect_ok() {
  local name="$1"
  shift
  if ("$@" >/dev/null 2>&1); then
    echo "ok   ${name}"
    return 0
  fi
  echo "FAIL ${name}: want success, got an error"
  failures=$((failures + 1))
}

expect_error() {
  local name="$1"
  shift
  if ("$@" >/dev/null 2>&1); then
    echo "FAIL ${name}: want an error, got success"
    failures=$((failures + 1))
    return 0
  fi
  echo "ok   ${name}"
}

# The mode, with and without the flag.
reset_state
ATE_OBSERVABILITY="" ATE_OTLP_ENDPOINT="" ATE_INSTALL_KIND=false
expect_eq "default mode is none" "none" "$(ate_observability)"

ATE_INSTALL_KIND=true
expect_eq "a kind install defaults to mode kind" "kind" "$(ate_observability)"

ATE_INSTALL_KIND=false ATE_OTLP_ENDPOINT="http://collector.otel-system.svc:4317"
expect_eq "--otlp-endpoint gives mode otlp" "otlp" "$(ate_observability)"

ATE_OTLP_ENDPOINT="" ATE_OBSERVABILITY="gke"
expect_eq "the flag selects the mode" "gke" "$(ate_observability)"

ATE_OBSERVABILITY="prometheus"
expect_error "an unknown mode is an error" ate_observability

reset_state
FAKE_CONFIGMAP="prometheus|http://collector.otel-system.svc:4317"
expect_error "an unknown mode on the ConfigMap is an error" ate_observability

# The tests of the flags.
reset_state
ATE_OBSERVABILITY="otlp" ATE_OTLP_ENDPOINT=""
expect_error "mode otlp with no endpoint is an error" validate_observability_flags

ATE_OBSERVABILITY="gke" ATE_OTLP_ENDPOINT="http://collector.otel-system.svc:4317"
expect_error "an endpoint with a different mode is an error" validate_observability_flags

ATE_OBSERVABILITY="kind" ATE_OTLP_ENDPOINT="" ATE_INSTALL_KIND=false
expect_error "mode kind outside a kind install is an error" validate_observability_flags

ATE_OBSERVABILITY="kind" ATE_INSTALL_KIND=true
expect_ok "mode kind in a kind install is correct" validate_observability_flags

ATE_INSTALL_KIND=false ATE_OBSERVABILITY="none"
expect_ok "mode none needs no other flag" validate_observability_flags

ATE_OBSERVABILITY="otlp" ATE_OTLP_ENDPOINT="collector.otel-system.svc:4317"
expect_error "an endpoint with no scheme is an error" validate_observability_flags

ATE_OTLP_ENDPOINT="http://collector.otel-system.svc:4317/v1"
expect_ok "an endpoint with a path is correct" validate_observability_flags

ATE_OTLP_ENDPOINT='http://collector.svc:4317;rm -rf /'
expect_error "an endpoint with a shell character is an error" validate_observability_flags

# The ConfigMap of each mode.
reset_state
ATE_OBSERVABILITY="none" ATE_OTLP_ENDPOINT="" ATE_INSTALL_KIND=false
expect_eq "mode none names no collector" "" "$(rendered_otlp_endpoint)"
expect_eq "mode none uses the file of mode none" \
  "${OTEL_CONFIG_NONE}" "$(otel_config_file)"

ATE_OBSERVABILITY="gke"
expect_eq "mode gke names the addon collector" \
  "http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317" \
  "$(rendered_otlp_endpoint)"

ATE_OBSERVABILITY="kind" ATE_INSTALL_KIND=true
expect_eq "mode kind names the in-cluster collector" \
  "http://opentelemetry-collector.otel-system.svc:4317" "$(rendered_otlp_endpoint)"
expect_eq "mode kind keeps the export interval of the kind file" \
  "10000" "$(render_otel_config | sed -n 's|^  OTEL_METRIC_EXPORT_INTERVAL: *||p' | tr -d '"')"

ATE_OBSERVABILITY="otlp" ATE_INSTALL_KIND=false
ATE_OTLP_ENDPOINT="http://telemetry-meter.benchmarking.svc.cluster.local:4317"
expect_eq "mode otlp names the given collector" \
  "${ATE_OTLP_ENDPOINT}" "$(rendered_otlp_endpoint)"
expect_eq "the rendered ConfigMap keeps its name" \
  "  name: ate-otel-config" "$(render_otel_config | grep '^  name:')"

# The Service of an endpoint.
reset_state
expect_eq "an endpoint of this cluster gives its Service" \
  "otel-system opentelemetry-collector" \
  "$(otlp_endpoint_service "http://opentelemetry-collector.otel-system.svc:4317")"
expect_eq "cluster.local gives the same Service" \
  "otel-system opentelemetry-collector" \
  "$(otlp_endpoint_service "http://opentelemetry-collector.otel-system.svc.cluster.local:4317")"
expect_eq "an address outside the cluster gives nothing" \
  "" "$(otlp_endpoint_service "https://otlp.example.com:443")"

# The preflight.
reset_state
ATE_OBSERVABILITY="gke" ATE_OTLP_ENDPOINT="" ATE_INSTALL_KIND=false
ATE_OBSERVABILITY_PREFLIGHT_DONE=false FAKE_OBJECTS=""
expect_error "mode gke without the addon stops the install" preflight_observability

FAKE_OBJECTS="namespace/gke-managed-otel"
expect_error "mode gke without the Service of the addon stops the install" preflight_observability

FAKE_OBJECTS="namespace/gke-managed-otel service/opentelemetry-collector.gke-managed-otel"
expect_ok "mode gke with the addon is correct" preflight_observability

ATE_OBSERVABILITY="otlp" ATE_OTLP_ENDPOINT="http://my-collector.my-ns.svc:4317"
FAKE_OBJECTS="namespace/my-ns"
expect_error "mode otlp without the collector stops the install" preflight_observability

FAKE_OBJECTS="namespace/my-ns service/my-collector.my-ns"
expect_ok "mode otlp with the collector is correct" preflight_observability

ATE_OTLP_ENDPOINT="https://otlp.example.com:443" FAKE_OBJECTS=""
expect_ok "mode otlp with an address outside the cluster is correct" preflight_observability

ATE_OBSERVABILITY="kind" ATE_OTLP_ENDPOINT="" ATE_INSTALL_KIND=true FAKE_OBJECTS=""
expect_ok "mode kind does not test the collector that it applies" preflight_observability

ATE_OBSERVABILITY="none" ATE_INSTALL_KIND=false
expect_ok "mode none tests nothing" preflight_observability

# The preflight reports one time for each install, because each deploy function
# applies the ConfigMap.
ATE_OBSERVABILITY_PREFLIGHT_DONE=false
preflight_observability >/dev/null
expect_eq "the preflight reports one time" "" "$(preflight_observability)"

# The mode of the cluster, which a deploy with no flag must keep.
reset_state
FAKE_CONFIGMAP="gke|http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317"
expect_eq "a deploy with no flag keeps the mode of the cluster" \
  "gke" "$(ate_observability)"

ATE_OBSERVABILITY="none"
expect_eq "the flag wins over the mode of the cluster" "none" "$(ate_observability)"

reset_state
FAKE_CONFIGMAP="otlp|http://my-collector.my-ns.svc:4317"
resolve_observability
expect_eq "mode otlp of the cluster keeps its address" \
  "http://my-collector.my-ns.svc:4317" "${ATE_OTLP_ENDPOINT}"
expect_eq "the report names the cluster as the source" "the cluster" \
  "${ATE_OBSERVABILITY_SOURCE}"

reset_state
FAKE_CONFIGMAP="kind|http://opentelemetry-collector.otel-system.svc:4317"
expect_ok "a kind cluster keeps mode kind with no kind install" validate_observability_flags

# A ConfigMap from an install that came before the modes carries no annotation.
reset_state
FAKE_CONFIGMAP="|http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317"
expect_eq "the endpoint of the GKE addon gives mode gke" "gke" "$(ate_observability)"

reset_state
FAKE_CONFIGMAP="|http://my-collector.my-ns.svc:4317"
expect_eq "an endpoint of your own gives mode otlp" "otlp" "$(ate_observability)"

reset_state
FAKE_CONFIGMAP="|"
expect_eq "an empty endpoint gives mode none" "none" "$(ate_observability)"

reset_state
ATE_OBSERVABILITY="otlp" ATE_OTLP_ENDPOINT="http://meter.benchmarking.svc:4317"
expect_eq "mode otlp writes its own mode on the ConfigMap" \
  "    ate.dev/observability-mode: otlp" \
  "$(render_otel_config | grep 'observability-mode')"

# The exporter of each signal, which internal/serverboot reads.
reset_state
ATE_OBSERVABILITY="none"
expect_eq "mode none turns the exporters off" \
  '  OTEL_METRICS_EXPORTER: "none"   OTEL_TRACES_EXPORTER: "none"' \
  "$(render_otel_config | grep '_EXPORTER:' | sort | tr '\n' ' ' | sed 's/ *$//')"

ATE_OBSERVABILITY="otlp" ATE_OTLP_ENDPOINT="http://meter.benchmarking.svc:4317"
expect_eq "mode otlp turns the exporters on" \
  '  OTEL_METRICS_EXPORTER: "otlp"   OTEL_TRACES_EXPORTER: "otlp"' \
  "$(render_otel_config | grep '_EXPORTER:' | sort | tr '\n' ' ' | sed 's/ *$//')"

ATE_OBSERVABILITY="gke" ATE_OTLP_ENDPOINT=""
expect_eq "mode gke leaves the exporters at their default" \
  "" "$(render_otel_config | grep '_EXPORTER:' || true)"

ATE_OBSERVABILITY="kind" ATE_INSTALL_KIND=true
expect_eq "mode kind leaves the exporters at their default" \
  "" "$(render_otel_config | grep '_EXPORTER:' || true)"

# The change of collector, and the restart that it needs.
reset_state
ATE_OBSERVABILITY="gke"
note_otel_config_change
expect_eq "a first install changes nothing" "false" "${ATE_OTEL_CONFIG_CHANGED}"

reset_state
ATE_OBSERVABILITY="gke"
FAKE_CONFIGMAP="gke|http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317"
note_otel_config_change
expect_eq "the same mode changes nothing" "false" "${ATE_OTEL_CONFIG_CHANGED}"
restart_otel_consumers >/dev/null
expect_eq "no change restarts nothing" "" "${FAKE_RESTARTS}"

reset_state
ATE_OBSERVABILITY="none"
FAKE_CONFIGMAP="gke|http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317"
note_otel_config_change
expect_eq "a different mode is a change" "true" "${ATE_OTEL_CONFIG_CHANGED}"

reset_state
ATE_OBSERVABILITY="otlp" ATE_OTLP_ENDPOINT="http://meter.benchmarking.svc:4317"
FAKE_CONFIGMAP="otlp|http://my-collector.my-ns.svc:4317"
note_otel_config_change
expect_eq "a different endpoint of the same mode is a change" \
  "true" "${ATE_OTEL_CONFIG_CHANGED}"

# One command line can name more than one deploy target, and each target applies
# the ConfigMap. Only the first one changes the collector.
reset_state
ATE_OBSERVABILITY="gke"
FAKE_OBJECTS="deployment/ate-api-server.ate-system"
FAKE_CONFIGMAP="none|"
note_otel_config_change
restart_otel_consumers >/dev/null
expect_eq "the first deploy target restarts the workloads" \
  "deployment/ate-api-server " "${FAKE_RESTARTS}"
note_otel_config_change
expect_eq "a second deploy target finds no change" "false" "${ATE_OTEL_CONFIG_CHANGED}"
restart_otel_consumers >/dev/null
expect_eq "a second deploy target restarts nothing again" \
  "deployment/ate-api-server " "${FAKE_RESTARTS}"

reset_state
ATE_OTEL_CONFIG_CHANGED=true
FAKE_OBJECTS="deployment/ate-api-server.ate-system daemonset/atelet.ate-system"
restart_otel_consumers >/dev/null
expect_eq "a change restarts the workloads that exist" \
  "deployment/ate-api-server daemonset/atelet " "${FAKE_RESTARTS}"
expect_eq "the restart happens one time" "false" "${ATE_OTEL_CONFIG_CHANGED}"

if ((failures > 0)); then
  echo "${failures} test(s) failed" >&2
  exit 1
fi
echo "All observability tests passed"
