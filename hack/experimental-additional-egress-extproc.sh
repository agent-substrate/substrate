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
#
# This is sourced as part of install-ate.sh. Do not run directly.
#
# --experimental-additional-egress-extproc-service=NS/SVC:PORT runs an ext_proc
# authorization filter on the egress gateway's decrypted leg, where a request is
# a hostname, method, and path rather than the IP:port the CONNECT checkpoint
# sees.

additional_egress_extproc_enabled() {
  [[ -n "${ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE:-}" ]]
}

# additional_egress_extproc_endpoint validates
# --experimental-additional-egress-extproc-service and echoes the DNS name to
# dial, the port, and the name to verify the server certificate against,
# space-separated.
#
# Example input: ate-system/foo:50051
# Example output: foo.ate-system.svc.cluster.local 50051 foo.ate-system.svc
additional_egress_extproc_endpoint() {
  local spec="$1"
  local label='[a-z0-9]([-a-z0-9]*[a-z0-9])?'
  if [[ ! "${spec}" =~ ^${label}/${label}:[0-9]+$ ]]; then
    echo "Error: --experimental-additional-egress-extproc-service must be <namespace>/<service>:<port>, got '${spec}'" >&2
    return 1
  fi

  local namespace="${spec%%/*}"
  local rest="${spec#*/}"
  local service="${rest%%:*}"
  local port="${rest##*:}"
  if (( port < 1 || port > 65535 )); then
    echo "Error: --experimental-additional-egress-extproc-service port must be 1-65535, got '${port}'" >&2
    return 1
  fi

  echo "${service}.${namespace}.svc.cluster.local ${port} ${service}.${namespace}.svc"
}

# The Envoy cluster the additional ext_proc filter dials.
readonly ADDITIONAL_EGRESS_EXTPROC_CLUSTER="additional_egress_ext_proc"

# Note: the heredoc sections are are written at column zero; the awk in
# patch_atenet_egress_manifest will re-indent to the right column.

emit_additional_egress_extproc_filter() {
  cat <<EOF
# Added by hack/install-ate.sh
# --experimental-additional-egress-extproc-service=${ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE}.
# Spliced over each #ATE_MITM_EXTPROC_FILTER marker in
# atenet-egress-with-sdsmint.yaml.
- name: envoy.filters.http.ext_proc
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
    grpc_service:
      envoy_grpc:
        cluster_name: ${ADDITIONAL_EGRESS_EXTPROC_CLUSTER}
      timeout: 2s
    failure_mode_allow: false
    # Default is 200ms, which is tuned for the co-located sidecar
    # on the CONNECT leg. This processor is a Service somewhere
    # else in the cluster, and under failure_mode_allow: false a
    # message that lands late is a denied request rather than a
    # slow one.
    message_timeout: 2s
    # The actor's verified identity
    request_attributes:
    - filter_state['ate.actor.identity']
    processing_mode:
      request_header_mode: SEND
      response_header_mode: SKIP
      request_body_mode: NONE
      response_body_mode: NONE
      request_trailer_mode: SKIP
      response_trailer_mode: SKIP
    mutation_rules:
      disallow_system: true
      disallow_is_error: true
EOF
}

# Arguments:
#
# $1 = address to dial
# $2 = port
# $3 = server_name to verify the server certificate against
emit_additional_egress_extproc_cluster() {
  local address="$1" port="$2" server_name="$3"
  cat <<EOF
# Added by hack/install-ate.sh
# --experimental-additional-egress-extproc-service=${ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE}.
# Spliced over the #ATE_MITM_EXTPROC_CLUSTER marker in
# atenet-egress-with-sdsmint.yaml.
- name: ${ADDITIONAL_EGRESS_EXTPROC_CLUSTER}
  type: STRICT_DNS
  lb_policy: ROUND_ROBIN
  connect_timeout: 1s
  # ext_proc is gRPC, so this leg has to be HTTP/2.
  typed_extension_protocol_options:
    envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
      "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
      explicit_http_config:
        http2_protocol_options: {}
  transport_socket:
    name: envoy.transport_sockets.tls
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
      sni: ${server_name}
      common_tls_context:
        # Pinned, because Envoy's default ceiling for an *upstream*
        # context is TLS 1.2 -- only downstream defaults to 1.3 -- while
        # extprocd, like every other Go server in this install, will not
        # negotiate below 1.3. Left to the defaults the two never agree,
        # and the handshake fails with TLSV1_ALERT_PROTOCOL_VERSION on a
        # config where both ends name TLS 1.3.
        tls_params:
          tls_minimum_protocol_version: TLSv1_3
          tls_maximum_protocol_version: TLSv1_3
        # The gateway's own pod identity, via filesystem SDS:
        # watched_directory only works on SDS-delivered secrets, so an
        # inline cert would never pick up kubelet's rotation.
        tls_certificate_sds_secret_configs:
        - name: podidentity_client_cert
          sds_config:
            resource_api_version: V3
            path_config_source:
              path: /etc/envoy/sds-podidentity-cert.yaml
        combined_validation_context:
          default_validation_context:
            # Chaining to the servicedns CA only proves the peer is some
            # pod serving some Service. Pinning the name is what makes
            # this the processor the operator asked for. The flag-derived
            # pin stays inline; the trust bundle rides SDS so it rotates.
            match_typed_subject_alt_names:
            - san_type: DNS
              matcher:
                exact: ${server_name}
          validation_context_sds_secret_config:
            name: servicedns_validation_context
            sds_config:
              resource_api_version: V3
              path_config_source:
                path: /etc/envoy/sds-servicedns-validation.yaml
  load_assignment:
    cluster_name: ${ADDITIONAL_EGRESS_EXTPROC_CLUSTER}
    endpoints:
    - lb_endpoints:
      - endpoint:
          address:
            socket_address:
              address: ${address}
              port_value: ${port}
EOF
}

# patch_atenet_egress_manifest writes the egress manifest to stdout with the
# #ATE_MITM_EXTPROC_FILTER and #ATE_MITM_EXTPROC_CLUSTER marker comments in
# manifests/ate-install/atenet-egress-with-sdsmint.yaml replaced by an ext_proc
# filter -- and the cluster it dials.
#
# Only used by--experimental-additional-egress-extproc-service.
patch_atenet_egress_manifest() {
  local manifest
  manifest="$(atenet_egress_manifest)"

  # Only the sdsmint manifest carries the markers. Refuse rather than apply an
  # unpatched manifest: silently ignoring the flag would deploy a gateway with
  # no additional checkpoint on it while the install reported success.
  if [[ "${ATE_EXPERIMENTAL_USE_SDSMINT:-false}" != "true" ]]; then
    echo "Error: --experimental-additional-egress-extproc-service requires --experimental-use-sdsmint" >&2
    return 1
  fi

  local endpoint address port server_name
  endpoint="$(additional_egress_extproc_endpoint "${ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE}")" || return 1
  read -r address port server_name <<<"${endpoint}"

  local filter_block cluster_block
  filter_block="$(emit_additional_egress_extproc_filter)" || return 1
  cluster_block="$(emit_additional_egress_extproc_cluster \
    "${address}" "${port}" "${server_name}")" || return 1

  local expected_filter_markers=2

  # Anchored to the start of the line so that prose mentioning a marker -- the
  # mitm_listener comment in the manifest names both of them -- is not itself
  # replaced by a config block.
  #
  # Pass ATE_EXTPROC_... as env vars in ENVIRON to work around differences
  # between gawk and BSD awk.
  ATE_EXTPROC_FILTER_BLOCK="${filter_block}" \
  ATE_EXTPROC_CLUSTER_BLOCK="${cluster_block}" \
  awk -v want_filters="${expected_filter_markers}" '
    BEGIN {
      filter  = ENVIRON["ATE_EXTPROC_FILTER_BLOCK"]
      cluster = ENVIRON["ATE_EXTPROC_CLUSTER_BLOCK"]
    }

    /^[ \t]*#ATE_MITM_EXTPROC_FILTER/  { splice(filter);  filters++;  next }
    /^[ \t]*#ATE_MITM_EXTPROC_CLUSTER/ { splice(cluster); clusters++; next }
    { print }

    END {
      if (filters != want_filters || clusters != 1) {
        printf("Error: expected %d #ATE_MITM_EXTPROC_FILTER and 1 #ATE_MITM_EXTPROC_CLUSTER marker in %s, found %d and %d\n",
               want_filters, FILENAME, filters, clusters) > "/dev/stderr"
        exit 1
      }
    }

    # Prints block at the indentation of the marker line being replaced. The
    # heredocs above are written at column zero.
    function splice(block, indent, lines, n, i) {
      match($0, /^[ \t]*/)
      indent = substr($0, 1, RLENGTH)
      n = split(block, lines, "\n")
      for (i = 1; i <= n; i++) {
        print (lines[i] == "" ? "" : indent lines[i])
      }
    }
  ' "${manifest}"
}
