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

# Test setup only: keep public origin verification and also trust local origins
# signed by the cluster's service-DNS CA. Run after deploying sdsmint and before
# the MITM networking tests. No CA signing key is mounted on the gateway.
context="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME:-kind}}"
kubectl_args=(--context "${context}" -n ate-system)
gateway="$(kubectl "${kubectl_args[@]}" get deployment atenet-egress -o jsonpath='{.spec.template.spec.containers[?(@.name=="envoy")].name}')"
if [[ -z "${gateway}" ]]; then
  gateway="$(kubectl "${kubectl_args[@]}" get deployment atenet-egress -o jsonpath='{.spec.template.spec.containers[?(@.name=="agentgateway")].name}')"
fi
image="$(kubectl "${kubectl_args[@]}" get deployment atenet-egress -o jsonpath="{.spec.template.spec.containers[?(@.name==\"${gateway}\")].image}")"
sdsmint="$(kubectl "${kubectl_args[@]}" get deployment atenet-egress -o jsonpath='{.spec.template.spec.initContainers[?(@.name=="sdsmint")].name}')"
if [[ -z "${gateway}" || -z "${image}" ]]; then
  echo "error: deploy the Envoy or AgentGateway egress before configuring E2E TLS origins" >&2
  exit 1
fi
if [[ "${gateway}" == envoy && "${sdsmint}" != sdsmint ]]; then
  echo "error: deploy the Envoy sdsmint gateway before configuring E2E TLS origins" >&2
  exit 1
fi

# AgentGateway is distroless, so use the pinned Envoy image as a shell-capable
# init helper. Its public roots plus the service-DNS CA are mounted only for
# this E2E test setup; production keeps the gateway's normal root behavior.
root_image="${image}"
if [[ "${gateway}" == agentgateway ]]; then
  root_image="envoyproxy/envoy:v1.39-latest@sha256:57e14a549d7bd43c8d3f6d03e8cfa653e037d4b38e133acd9b54f38c524401b4"
fi

# Use the helper image's public roots on every Pod start. The separate init
# container prevents re-runs from appending to an existing bundle.
kubectl "${kubectl_args[@]}" patch deployment atenet-egress --type=strategic --patch "$(cat <<EOF
spec:
  template:
    spec:
      initContainers:
      - name: e2e-egress-upstream-roots
        image: ${root_image}
        command:
        - sh
        - -ec
        - cat /etc/ssl/certs/ca-certificates.crt /run/servicedns.podcert.ate.dev/trust-bundle.pem > /run/e2e-egress-roots/ca-certificates.crt
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          capabilities:
            drop: [ALL]
        volumeMounts:
        - name: servicedns
          mountPath: /run/servicedns.podcert.ate.dev
          readOnly: true
        - name: e2e-egress-upstream-roots
          mountPath: /run/e2e-egress-roots
      containers:
      - name: ${gateway}
        volumeMounts:
        - name: e2e-egress-upstream-roots
          mountPath: /etc/ssl/certs/ca-certificates.crt
          subPath: ca-certificates.crt
          readOnly: true
      volumes:
      - name: e2e-egress-upstream-roots
        emptyDir: {}
EOF
)"
kubectl "${kubectl_args[@]}" rollout status deployment/atenet-egress --timeout=120s
