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
KUBECTL_CONTEXT="kind-${KIND_CLUSTER_NAME}"
reg_name="kind-registry"
reg_port="${KIND_REGISTRY_PORT:-5001}"

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      echo "Usage: $0"
      echo "Creates the kind cluster '${KIND_CLUSTER_NAME}' and a local registry container on port ${reg_port}."
      echo
      echo "Configured through the environment:"
      echo "  KIND_CLUSTER_NAME  Name of the cluster to create (default: kind)."
      echo "  IP_FAMILY          Address families for pods and Services: ipv4, ipv6 or dual (default: ipv4)."
      exit 0
      ;;
  esac
fi

# Only ipFamily is set; kind's per-family podSubnet/serviceSubnet defaults are
# already what we want.
IP_FAMILY="${IP_FAMILY:-ipv4}"
case "${IP_FAMILY}" in
  ipv4|ipv6|dual)
    ;;
  *)
    echo "error: IP_FAMILY must be one of ipv4, ipv6, dual (got '${IP_FAMILY}')" >&2
    exit 1
    ;;
esac

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

echo "Creating kind configuration for cluster '${KIND_CLUSTER_NAME}' (ipFamily=${IP_FAMILY})..."
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
networking:
  ipFamily: ${IP_FAMILY}
# The install pulls ~570MB of third-party images (postgres, prometheus, the otel
# collector, rustfs, envoy, jaeger) onto this one node. kubelet serializes image
# pulls by default, so they queue behind one another and whichever workload draws
# the back of the queue can miss its readiness deadline.
# TODO: kind should probably default to parallel pulls for its single-node
# clusters rather than leaving every user to patch it in.
kubeadmConfigPatches:
- |
  kind: KubeletConfiguration
  serializeImagePulls: false
  maxParallelImagePulls: 4
EOF

echo "Deleting existing kind cluster '${KIND_CLUSTER_NAME}' if it exists..."
"${ROOT}"/hack/kind.sh delete cluster --name "${KIND_CLUSTER_NAME}" || true

# kind reuses an existing "kind" network as-is and deleting the cluster does not
# drop it, so a v4-only one (older kind, or a daemon with IPv6 off) leaves nodes
# without a v6 address — surfacing much later as pods stuck at ContainerCreating.
if [[ "${IP_FAMILY}" != "ipv4" &&
      "$(docker network inspect kind --format '{{.EnableIPv6}}' 2>/dev/null || echo absent)" == "false" ]]; then
  echo "The 'kind' Docker network exists without IPv6; recreating it..."
  docker network disconnect kind "${reg_name}" 2>/dev/null || true
  if ! docker network rm kind >/dev/null; then
    echo "error: could not remove the 'kind' Docker network. Something else is still" >&2
    echo "       attached to it; disconnect it and re-run:" >&2
    echo "         docker network inspect kind --format '{{json .Containers}}'" >&2
    exit 1
  fi
fi

echo "Creating kind cluster '${KIND_CLUSTER_NAME}'..."
"${ROOT}"/hack/kind.sh create cluster --name "${KIND_CLUSTER_NAME}" --config "${ROOT}/bin/kind-config.yaml"

# A daemon with IPv6 off hands kind a v4-only network whatever it asked for.
if [[ "${IP_FAMILY}" != "ipv4" &&
      "$(docker network inspect kind --format '{{.EnableIPv6}}')" != "true" ]]; then
  echo "error: the 'kind' Docker network has no IPv6, so the nodes have no v6 address." >&2
  echo "       Enable IPv6 in the Docker daemon and re-run. On Linux, add to" >&2
  echo "       /etc/docker/daemon.json and restart dockerd:" >&2
  echo '         {"ipv6": true, "ip6tables": true}' >&2
  exit 1
fi

# 2.5 Enable Proxy ARP/NDP on kind nodes for gVisor loopback pod-to-pod networking
echo "Enabling Proxy ARP/NDP on kind nodes..."
for node in $("${ROOT}"/hack/kind.sh get nodes --name "${KIND_CLUSTER_NAME}"); do
  # Unconditional: nodes of either family carry the other's addresses on the
  # Docker bridge, and a proxy sysctl is inert without proxy entries. -e skips
  # proxy_ndp on a kernel built without IPv6 instead of failing an ipv4 run.
  docker exec "${node}" sysctl net.ipv4.conf.all.proxy_arp=1
  docker exec "${node}" sysctl -e net.ipv6.conf.all.proxy_ndp=1
done

# 2.6 When KVM is available: make /dev/kvm usable inside the node. Micro-VM
# WorkerPools reach these nodes through the device atelet advertises for them,
# so no node label is needed.
if [ "${HAS_KVM}" = "1" ]; then
  echo "Preparing kind nodes for micro-VM (kata + cloud-hypervisor) runtime..."
  for node in $("${ROOT}"/hack/kind.sh get nodes --name "${KIND_CLUSTER_NAME}"); do
    docker exec "${node}" chmod 666 /dev/kvm
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
cat <<EOF | kubectl --context="${KUBECTL_CONTEXT}" apply -f -
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
