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

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
reg_name="kind-registry"
reg_port="5001"

mkdir -p "${ROOT}/bin"

# 1. Create registry container unless it already exists
echo "Setting up local docker registry '${reg_name}' on port ${reg_port}..."
if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" == "true" ]; then
  if ! docker port "${reg_name}" | grep -q "${reg_port}"; then
    echo "Registry exists but is not mapped to port ${reg_port}. Recreating..."
    docker rm -f "${reg_name}"
  fi
fi

if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != "true" ]; then
  docker run \
    -d --restart=always \
    --label created-by=agent-substrate \
    -p "127.0.0.1:${reg_port}:5000" \
    -p "[::1]:${reg_port}:5000" \
    --network bridge --name "${reg_name}" \
    registry:3
fi

# 2. Create kind configuration with containerdConfigPatches and feature gates.
#
# Probe for /dev/kvm where the kind nodes will actually run — inside the Docker
# provider VM on macOS (Lima/Colima/Docker Desktop), the host itself on Linux —
# and only wire up micro-VM (kata + cloud-hypervisor) support when present.
# Without KVM the cluster still works for gVisor.
echo "Probing for /dev/kvm in the Docker environment..."
HAS_KVM=0
if docker run --rm --device /dev/kvm busybox true >/dev/null 2>&1; then
  HAS_KVM=1
  echo "/dev/kvm found: micro-VM (kata + cloud-hypervisor) support will be enabled."
else
  echo "/dev/kvm not available: micro-VM support disabled (gVisor still works)."
fi

echo "Creating kind configuration for cluster '${KIND_CLUSTER_NAME}'..."
cat <<EOF > "${ROOT}/bin/kind-config.yaml"
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
EOF
if [ "${HAS_KVM}" = "1" ]; then
  cat <<EOF >> "${ROOT}/bin/kind-config.yaml"
  # Bind-mount /dev/kvm into the node so micro-VM (kata + cloud-hypervisor)
  # worker pods can use KVM.
  extraMounts:
  - hostPath: /dev/kvm
    containerPath: /dev/kvm
EOF
fi
cat <<EOF >> "${ROOT}/bin/kind-config.yaml"
# cmd/podcertcontroller depends on ClusterTrustBundle & PodCertificateRequest.
# They are not enabled by default as of Kubernetes v1.36
# https://github.com/kubernetes/kubernetes/blob/master/test/compatibility_lifecycle/reference/versioned_feature_list.yaml
featureGates:
  ClusterTrustBundle: true
  ClusterTrustBundleProjection: true
  PodCertificateRequest: true
runtimeConfig:
  "certificates.k8s.io/v1beta1": "true"
EOF

echo "Deleting existing kind cluster '${KIND_CLUSTER_NAME}' if it exists..."
"${ROOT}"/hack/kind.sh delete cluster --name "${KIND_CLUSTER_NAME}" || true

echo "Creating kind cluster '${KIND_CLUSTER_NAME}'..."
"${ROOT}"/hack/kind.sh create cluster --name "${KIND_CLUSTER_NAME}" --config "${ROOT}/bin/kind-config.yaml"

# 2.5 Enable Proxy ARP on kind nodes for gVisor loopback pod-to-pod networking
echo "Enabling Proxy ARP on kind nodes..."
for node in $("${ROOT}"/hack/kind.sh get nodes --name "${KIND_CLUSTER_NAME}"); do
  docker exec "${node}" sysctl net.ipv4.conf.all.proxy_arp=1
done

# 2.6 When KVM is available: make /dev/kvm usable inside the node and label
# nodes so micro-VM WorkerPools (nodeSelector ate.dev/sandboxClass=microvm) schedule.
if [ "${HAS_KVM}" = "1" ]; then
  echo "Preparing kind nodes for micro-VM (kata + cloud-hypervisor) runtime..."
  for node in $("${ROOT}"/hack/kind.sh get nodes --name "${KIND_CLUSTER_NAME}"); do
    docker exec "${node}" chmod 666 /dev/kvm
    kubectl label node "${node}" ate.dev/sandboxClass=microvm --overwrite
  done
fi

# 3. Add the registry config to the nodes
echo "Adding registry config to kind nodes..."
REGISTRY_DIR="/etc/containerd/certs.d/localhost:${reg_port}"
for node in $("${ROOT}"/hack/kind.sh get nodes --name "${KIND_CLUSTER_NAME}"); do
  docker exec "${node}" mkdir -p "${REGISTRY_DIR}"
  cat <<EOF | docker exec -i "${node}" cp /dev/stdin "${REGISTRY_DIR}/hosts.toml"
[host."http://${reg_name}:5000"]
EOF
done

# 4. Connect the registry to the cluster network if not already connected
echo "Connecting local registry to cluster network..."
if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${reg_name}")" = "null" ]; then
  docker network connect "kind" "${reg_name}"
fi

# 5. Document the local registry in kube-public ConfigMap
echo "Documenting local registry in cluster..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${reg_port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF
