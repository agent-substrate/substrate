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

# The observability mode of hack/install-ate.sh, which sources this file.
# hack/verify/observability.sh sources it too, thus keep the functions here free
# of the state of the install.
#
# The telemetry stack is optional, because substrate must run on a cluster that
# has no collector. The mode is explicit for the same reason: a default that
# names one collector is correct on one type of cluster only, and it is wrong
# with no message on each other type. The modes are:
#
#   none   No collector. The components export no telemetry. ateapi, atelet,
#          and atenet-router still serve their own /metrics endpoints, and
#          ate-controller and the ateoms push only, thus they emit nothing.
#          This is the default.
#   otlp   The collector at --otlp-endpoint. Use this for a collector that you
#          operate, and for a measurement (read benchmarking/telemetry).
#   gke    The collector of the GKE managed OTel addon.
#   kind   The in-cluster collector that a kind install applies.
#
# Each mode supplies the ate-otel-config ConfigMap from a different file, and
# preflight_observability tests the mode before the install applies anything.
#
# The mode of a cluster is state, and not only a flag. The install writes the
# mode on the ConfigMap, and reads it back when no flag gives one. Thus a deploy
# of one component keeps the collector of the cluster, and a change of mode
# restarts the workloads that read the ConfigMap.

OTEL_CONFIG_NONE="manifests/ate-install/otel/none/ate-otel-config.yaml"
OTEL_CONFIG_GKE="manifests/ate-install/otel/gke/ate-otel-config.yaml"
OTEL_CONFIG_KIND="manifests/ate-install/otel/kind/ate-otel-config.yaml"

# The namespace and the Service of the GKE managed OTel addon. The addon makes
# both; the endpoint in ${OTEL_CONFIG_GKE} names both.
GKE_OTEL_NAMESPACE="gke-managed-otel"

# The ate-otel-config ConfigMap of the cluster, read one time. The read tells a
# deploy of one component which collector the cluster already has.
ATE_OBSERVABILITY_CLUSTER_READ=false
ATE_OBSERVABILITY_CLUSTER_MODE=""
ATE_OBSERVABILITY_CLUSTER_ENDPOINT=""

# endpoint_in_file echoes the endpoint that a ConfigMap manifest names.
endpoint_in_file() {
  sed -n 's|^  OTEL_EXPORTER_OTLP_ENDPOINT: *||p' "$1" | tr -d '"'
}

# mode_of_endpoint echoes the mode that an endpoint belongs to. It is for a
# ConfigMap that carries no mode annotation, which is the ConfigMap of an
# install that came before the modes: the endpoint is the only evidence there,
# and without this the next deploy of one component would put the default over a
# collector that works.
mode_of_endpoint() {
  local endpoint="$1"
  if [[ -z "${endpoint}" ]]; then
    echo "none"
  elif [[ "${endpoint}" == "$(endpoint_in_file "${OTEL_CONFIG_GKE}")" ]]; then
    echo "gke"
  elif [[ "${endpoint}" == "$(endpoint_in_file "${OTEL_CONFIG_KIND}")" ]]; then
    echo "kind"
  else
    echo "otlp"
  fi
}

# read_cluster_observability reads the mode and the endpoint of the
# ate-otel-config ConfigMap in the cluster. Both stay empty when the cluster has
# no such ConfigMap, and when there is no cluster to ask.
read_cluster_observability() {
  if [[ "${ATE_OBSERVABILITY_CLUSTER_READ}" == "true" ]]; then
    return 0
  fi
  ATE_OBSERVABILITY_CLUSTER_READ=true

  local raw=""
  raw="$(run_kubectl -n ate-system get configmap ate-otel-config \
    -o jsonpath='{.metadata.annotations.ate\.dev/observability-mode}|{.data.OTEL_EXPORTER_OTLP_ENDPOINT}' \
    2>/dev/null || true)"
  # The separator is present for each ConfigMap that exists, and absent for a
  # read that failed, thus it tells the two apart.
  if [[ "${raw}" != *"|"* ]]; then
    return 0
  fi
  ATE_OBSERVABILITY_CLUSTER_MODE="${raw%%|*}"
  ATE_OBSERVABILITY_CLUSTER_ENDPOINT="${raw#*|}"
  if [[ -z "${ATE_OBSERVABILITY_CLUSTER_MODE}" ]]; then
    ATE_OBSERVABILITY_CLUSTER_MODE="$(mode_of_endpoint "${ATE_OBSERVABILITY_CLUSTER_ENDPOINT}")"
  fi
}

# resolve_observability decides the mode, and stops the install if the mode is
# not a known one. The order is:
#
#   1. --observability, or ATE_OBSERVABILITY.
#   2. --otlp-endpoint, which gives mode otlp.
#   3. The mode of the cluster, thus a deploy of one component does not change
#      the collector that the cluster has.
#   4. Mode kind on a kind install, and mode none on each other install.
#
# Call it in the shell of the install, and not in a subshell: for mode otlp it
# also takes the address from the cluster, which no manifest holds.
# ate_observability echoes the result.
resolve_observability() {
  local source="the --observability flag"

  if [[ -z "${ATE_OBSERVABILITY:-}" ]]; then
    if [[ -n "${ATE_OTLP_ENDPOINT:-}" ]]; then
      ATE_OBSERVABILITY="otlp"
      source="the --otlp-endpoint flag"
    else
      read_cluster_observability
      if [[ -n "${ATE_OBSERVABILITY_CLUSTER_MODE}" ]]; then
        ATE_OBSERVABILITY="${ATE_OBSERVABILITY_CLUSTER_MODE}"
        ATE_OTLP_ENDPOINT="${ATE_OBSERVABILITY_CLUSTER_ENDPOINT}"
        source="the cluster"
        # Only mode otlp keeps its address in the ConfigMap. For each other
        # mode the manifest holds it, and the flag must stay empty, because
        # validate_observability_flags rejects the two together.
        if [[ "${ATE_OBSERVABILITY}" != "otlp" ]]; then
          ATE_OTLP_ENDPOINT=""
        fi
      elif [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
        ATE_OBSERVABILITY="kind"
        source="the kind install"
      else
        ATE_OBSERVABILITY="none"
        source="the default"
      fi
    fi
  fi

  case "${ATE_OBSERVABILITY}" in
    none | otlp | gke | kind) ;;
    *)
      echo "Error: the mode must be none, otlp, gke, or kind, got '${ATE_OBSERVABILITY}'" >&2
      echo "  from ${source}." >&2
      if [[ "${source}" == "the cluster" ]]; then
        echo "  The ate.dev/observability-mode annotation on the ate-otel-config" >&2
        echo "  ConfigMap in ate-system holds that value. Give --observability to" >&2
        echo "  replace it." >&2
      fi
      exit 1
      ;;
  esac
  # The first resolution decides the source. A later call has a mode already,
  # thus it must not report that mode as one that a flag gave.
  if [[ -z "${ATE_OBSERVABILITY_SOURCE:-}" ]]; then
    ATE_OBSERVABILITY_SOURCE="${source}"
  fi
}

# ate_observability echoes the mode. It resolves the mode each time, because a
# resolution that a subshell made does not reach the shell of the install.
ate_observability() {
  resolve_observability
  echo "${ATE_OBSERVABILITY}"
}

# validate_observability_flags rejects a combination of flags that no install
# can satisfy. Call it in the pre-scan, ahead of the first apply.
validate_observability_flags() {
  local mode
  resolve_observability
  mode="${ATE_OBSERVABILITY}"

  if [[ "${mode}" == "otlp" && -z "${ATE_OTLP_ENDPOINT:-}" ]]; then
    echo "Error: --observability=otlp needs the address of a collector." >&2
    echo "  Give --otlp-endpoint URL, or select a different mode:" >&2
    echo "  --observability=gke for the GKE managed collector, or" >&2
    echo "  --observability=none to install with no telemetry export." >&2
    exit 1
  fi

  if [[ "${mode}" != "otlp" && -n "${ATE_OTLP_ENDPOINT:-}" ]]; then
    echo "Error: --otlp-endpoint is for --observability=otlp, but the mode is ${mode}." >&2
    echo "  Remove one of the two flags." >&2
    exit 1
  fi

  # Only for a mode that a flag gives. A kind cluster that keeps mode kind is
  # correct, and hack/install-ate.sh can deploy to it with no flag.
  if [[ "${mode}" == "kind" && "${ATE_INSTALL_KIND:-false}" != "true" \
    && "${ATE_OBSERVABILITY_SOURCE}" != "the cluster" ]]; then
    echo "Error: --observability=kind needs a kind install, because the collector" >&2
    echo "  of that mode is the one that hack/install-ate-kind.sh applies." >&2
    echo "  Use hack/install-ate-kind.sh, or --observability=otlp with the" >&2
    echo "  address of your own collector." >&2
    exit 1
  fi

  validate_otlp_endpoint "${ATE_OTLP_ENDPOINT:-}"
}

# validate_otlp_endpoint tests the format of the URL of --otlp-endpoint. An
# empty value is correct, because each other mode reads its endpoint from a
# file. The set of permitted characters is small on purpose: the value goes into
# a ConfigMap and into the replacement of render_otel_config.
validate_otlp_endpoint() {
  local endpoint="$1"
  if [[ -z "${endpoint}" ]]; then
    return 0
  fi
  if [[ ! "${endpoint}" =~ ^https?://[A-Za-z0-9._-]+(:[0-9]+)?(/[A-Za-z0-9._~/-]*)?$ ]]; then
    echo "Error: --otlp-endpoint must be a URL with a scheme, for example" >&2
    echo "  http://opentelemetry-collector.otel-system.svc:4317" >&2
    echo "  Got '${endpoint}'." >&2
    exit 1
  fi
}

# otel_config_file echoes the manifest that supplies the ate-otel-config
# ConfigMap for the selected mode. Mode otlp uses the file of mode none, and
# render_otel_config puts the given endpoint in it.
otel_config_file() {
  case "$(ate_observability)" in
    kind) echo "${OTEL_CONFIG_KIND}" ;;
    gke) echo "${OTEL_CONFIG_GKE}" ;;
    *) echo "${OTEL_CONFIG_NONE}" ;;
  esac
}

# render_otel_config echoes the ate-otel-config ConfigMap of the selected mode.
# The endpoint is in the manifest before the apply, thus the install needs no
# patch after it. Mode otlp takes the file of mode none, thus it replaces the
# endpoint and the mode annotation in it.
render_otel_config() {
  local file
  file="$(otel_config_file)"
  if [[ "$(ate_observability)" != "otlp" ]]; then
    cat "${file}"
    return
  fi
  sed -e "s|^  OTEL_EXPORTER_OTLP_ENDPOINT:.*|  OTEL_EXPORTER_OTLP_ENDPOINT: ${ATE_OTLP_ENDPOINT}|" \
    -e "s|^    ate.dev/observability-mode:.*|    ate.dev/observability-mode: otlp|" \
    -e 's|^  OTEL_TRACES_EXPORTER:.*|  OTEL_TRACES_EXPORTER: "otlp"|' \
    -e 's|^  OTEL_METRICS_EXPORTER:.*|  OTEL_METRICS_EXPORTER: "otlp"|' \
    "${file}"
}

# rendered_otlp_endpoint echoes the endpoint of the ConfigMap above, or nothing
# in mode none. It reads the rendered manifest, thus the manifest stays the one
# source of the value.
rendered_otlp_endpoint() {
  render_otel_config | sed -n 's|^  OTEL_EXPORTER_OTLP_ENDPOINT: *||p' | tr -d '"'
}

# otlp_endpoint_service echoes "namespace service" for an endpoint that names a
# Service of this cluster, and nothing for each other address.
otlp_endpoint_service() {
  local host="$1"
  # Cut the scheme, then the path, then the port.
  host="${host#*://}"
  host="${host%%/*}"
  host="${host%%:*}"
  # A cluster address is service.namespace.svc, with or without cluster.local.
  if [[ "${host}" =~ ^([a-z0-9-]+)\.([a-z0-9-]+)\.svc(\.cluster\.local)?\.?$ ]]; then
    echo "${BASH_REMATCH[2]} ${BASH_REMATCH[1]}"
  fi
}

# check_otlp_endpoint_reachable tests that the Service of an in-cluster endpoint
# exists. An absent Service is an error: the components would fail to find the
# collector one time each minute, and the telemetry would stop with no message,
# which reads as a fault of the network and not as an absent dependency.
#
# The second argument is the line that tells the operator what to do. It differs
# with the mode, because the correction differs.
#
# An address outside the cluster gets a note only, because the installer cannot
# test it.
check_otlp_endpoint_reachable() {
  local endpoint="$1"
  local remedy="$2"
  local namespace="" service=""

  read -r namespace service <<<"$(otlp_endpoint_service "${endpoint}")"
  if [[ -z "${namespace}" ]]; then
    echo "  The installer does not test ${endpoint}, because the address is not one of this cluster."
    return 0
  fi

  if ! run_kubectl get namespace "${namespace}" >/dev/null 2>&1; then
    echo "Error: the collector at ${endpoint} is absent: there is no namespace ${namespace}." >&2
    echo "${remedy}" >&2
    exit 1
  fi
  if ! run_kubectl get service "${service}" --namespace "${namespace}" >/dev/null 2>&1; then
    echo "Error: the collector at ${endpoint} is absent: there is no Service ${service} in ${namespace}." >&2
    echo "${remedy}" >&2
    exit 1
  fi
}

# preflight_observability tests the selected mode, then reports it. It runs one
# time for each install: the deploy functions each apply the ConfigMap, and the
# operator needs the report one time only.
preflight_observability() {
  if [[ "${ATE_OBSERVABILITY_PREFLIGHT_DONE:-false}" == "true" ]]; then
    return 0
  fi
  ATE_OBSERVABILITY_PREFLIGHT_DONE=true

  local mode endpoint from
  mode="$(ate_observability)"
  endpoint="$(rendered_otlp_endpoint)"
  from=" (from ${ATE_OBSERVABILITY_SOURCE:-the default})"

  case "${mode}" in
    none)
      echo "Observability: mode none${from}. The control plane exports no telemetry."
      echo "  ateapi, atelet, and atenet-router still serve their own /metrics"
      echo "  endpoints. ate-controller and the ateoms push only, thus they emit nothing."
      if run_kubectl get namespace "${GKE_OTEL_NAMESPACE}" >/dev/null 2>&1; then
        echo "  The namespace ${GKE_OTEL_NAMESPACE} is present. To use that collector,"
        echo "  install again with --observability=gke."
      fi
      ;;
    gke)
      echo "Observability: mode gke${from}, endpoint ${endpoint}"
      check_otlp_endpoint_reachable "${endpoint}" \
        "  Enable the managed OTel addon on the cluster, or install with
  --observability=otlp and the address of your own collector, or with
  --observability=none for no telemetry export."
      ;;
    otlp)
      echo "Observability: mode otlp${from}, endpoint ${endpoint}"
      check_otlp_endpoint_reachable "${endpoint}" \
        "  Install the collector first, correct --otlp-endpoint, or install with
  --observability=none for no telemetry export."
      ;;
    kind)
      # No test of the Service: the collector is in the same bundle as the
      # components, thus it does not exist before this install applies it.
      echo "Observability: mode kind${from}, endpoint ${endpoint}"
      ;;
  esac
}

# ATE_OTEL_CONFIG_CHANGED records that this install gives the cluster a
# different collector than the one it had. restart_otel_consumers reads it.
ATE_OTEL_CONFIG_CHANGED=false

# note_otel_config_change compares the ConfigMap of this install with the one in
# the cluster. Nothing to compare on a first install, thus no change: the
# workloads that come after it read the new ConfigMap when they start.
#
# It then takes the new values as the values of the cluster, because the caller
# applies them. One command line can name more than one deploy target
# (--deploy-atelet --deploy-atenet), and each target applies the ConfigMap. Thus
# without this, each target after the first one would compare with the state
# before the install, find the same change again, and restart the workloads a
# second time. Each of those restarts rolls every WorkerPool.
note_otel_config_change() {
  local mode endpoint
  read_cluster_observability
  mode="$(ate_observability)"
  endpoint="$(rendered_otlp_endpoint)"

  if [[ -n "${ATE_OBSERVABILITY_CLUSTER_MODE}" ]] \
    && { [[ "${mode}" != "${ATE_OBSERVABILITY_CLUSTER_MODE}" ]] \
      || [[ "${endpoint}" != "${ATE_OBSERVABILITY_CLUSTER_ENDPOINT}" ]]; }; then
    ATE_OTEL_CONFIG_CHANGED=true
  fi

  ATE_OBSERVABILITY_CLUSTER_MODE="${mode}"
  ATE_OBSERVABILITY_CLUSTER_ENDPOINT="${endpoint}"
}

# restart_otel_consumers restarts the workloads that read ate-otel-config, but
# only when the collector changed. They read it with envFrom, thus each one
# takes a new endpoint at the start of a pod, and a new ConfigMap on its own
# starts no pod: the pod template stays the same, thus the apply above starts no
# rollout and each running pod keeps the collector of the install before it.
#
# Call this at the end of a deploy path, after the waits for the rollouts. A
# restart during the rollout of the bundle makes the two rollouts compete, and
# `kubectl rollout status` can then exceed its timeout. An absent workload is
# not an error, because a deploy of one component has only that component.
restart_otel_consumers() {
  if [[ "${ATE_OTEL_CONFIG_CHANGED}" != "true" ]]; then
    return 0
  fi
  ATE_OTEL_CONFIG_CHANGED=false

  echo "The collector changed. Restarting the workloads that read ate-otel-config."
  local workload
  for workload in deployment/ate-api-server deployment/ate-controller \
    deployment/atenet-router daemonset/atelet; do
    if run_kubectl -n ate-system get "${workload}" >/dev/null 2>&1; then
      run_kubectl -n ate-system rollout restart "${workload}"
    fi
  done
  echo "  ate-controller rolls each WorkerPool when it restarts, thus the running"
  echo "  workers, and the actors on them, are replaced."
}
