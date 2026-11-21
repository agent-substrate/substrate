#!/usr/bin/env bash
# Copyright 2026 The Agent Substrate Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Sets up a pod on an infra node that runs wakestorm inside the cluster, so
# each call's time does not include a kubectl port-forward.
#
# - ServiceAccount benchmarking/wakestorm, allowed to read ClusterTrustBundles
#   to verify ate-api-server's serving certificate.
# - Secret benchmarking/wakestorm-token: an ate-client token, valid 6 h.
# - Pod benchmarking/wakestorm-runner on a bench-role=infra node, with a
#   wakestorm binary built for that node.
#
# Usage: runner_setup.sh

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
kubectl create namespace benchmarking --dry-run=client -o yaml | kubectl apply -f -
# ate-system's NetworkPolicy admits API clients only from labeled namespaces.
kubectl label namespace benchmarking ate.dev/api-access=true --overwrite
kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: wakestorm
  namespace: benchmarking
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: wakestorm-trust-bundles
rules:
- apiGroups: ["certificates.k8s.io"]
  resources: ["clustertrustbundles"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: wakestorm-trust-bundles
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: wakestorm-trust-bundles
subjects:
- kind: ServiceAccount
  name: wakestorm
  namespace: benchmarking
EOF

token="$(kubectl create token ate-client -n ate-system --audience api.ate-system.svc --duration 6h)"
kubectl create secret generic wakestorm-token -n benchmarking --from-literal=token="${token}" \
  --dry-run=client -o yaml | kubectl apply -f -

# Any image with a shell and tar works. The Postgres image is already on the
# infra nodes.
image="$(kubectl get statefulset postgres -n ate-system -o jsonpath='{.spec.template.spec.containers[0].image}')"
kubectl delete pod wakestorm-runner -n benchmarking --ignore-not-found --wait
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: wakestorm-runner
  namespace: benchmarking
spec:
  serviceAccountName: wakestorm
  nodeSelector:
    bench-role: infra
  containers:
  - name: runner
    image: ${image}
    command: ["sleep", "infinity"]
    resources:
      requests: {cpu: "500m", memory: "512Mi"}
    volumeMounts:
    - name: token
      mountPath: /var/run/ate-token
      readOnly: true
  volumes:
  - name: token
    secret:
      secretName: wakestorm-token
EOF
kubectl wait --for=condition=Ready pod/wakestorm-runner -n benchmarking --timeout=300s

node="$(kubectl get pod wakestorm-runner -n benchmarking -o jsonpath='{.spec.nodeName}')"
arch="$(kubectl get node "${node}" -o jsonpath='{.status.nodeInfo.architecture}')"
bin="$(mktemp -d)/wakestorm"
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go build -o "${bin}" "${ROOT}/benchmarking/storage/cmd/wakestorm"
kubectl cp "${bin}" benchmarking/wakestorm-runner:/tmp/wakestorm -c runner
kubectl exec -n benchmarking wakestorm-runner -c runner -- sh -c 'chmod +x /tmp/wakestorm && mkdir -p /tmp/out'
echo "wakestorm-runner is ready on ${node} (${arch})"
