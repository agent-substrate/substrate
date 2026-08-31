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
IPV6_DNS_UPSTREAM="${IPV6_DNS_UPSTREAM:-}"
IPV6_DNS64_PREFIX="${IPV6_DNS64_PREFIX:-}"
NAT64_PROBE_URL="${NAT64_PROBE_URL:-http://connectivitycheck.gstatic.com/generate_204}"
NAT64_IMAGE="${NAT64_IMAGE:-}"
# curl, not the busybox in manifests/: busybox wget takes the first address the
# resolver hands back, which here can be one no pod can reach.
probe_image="curlimages/curl:8.11.1@sha256:c1fe1679c34d9784c1b0d1e5f62ac0a79fca01fb6377cdd33e90473c6f9f9a69"

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      echo "Usage: $0"
      echo "Creates the kind cluster '${KIND_CLUSTER_NAME}' and a local registry container on port ${reg_port}."
      echo
      echo "Configured through the environment:"
      echo "  KIND_CLUSTER_NAME  Name of the cluster to create (default: kind)."
      echo "  IP_FAMILY          Address families for pods and Services: ipv4, ipv6 or dual (default: ipv4)."
      echo "  IPV6_DNS_UPSTREAM  Space-separated IPv6 resolvers CoreDNS forwards to when IP_FAMILY=ipv6"
      echo "                     (default: Google Public DNS, reached through IPV6_DNS64_PREFIX when"
      echo "                     that is set). These replace the host's resolver, so any split-horizon"
      echo "                     names it served stop resolving from pods."
      echo "  IPV6_DNS64_PREFIX  NAT64 prefix to route external traffic through when IP_FAMILY=ipv6."
      echo "                     Must be 64:ff9b::/96, the only prefix the vendored agent translates"
      echo "                     (default: empty, no NAT64). Needed where the host has no IPv6 egress"
      echo "                     of its own; leave unset where it does. Setup, prerequisites and"
      echo "                     caveats: docs/dev/ipv6-local.md."
      echo "  NAT64_PROBE_URL    URL a pod fetches to prove translation works (default:"
      echo "                     ${NAT64_PROBE_URL})."
      echo "  NAT64_IMAGE        Override the NAT64 agent image; required on arm64."
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

if [[ -n "${IPV6_DNS64_PREFIX}" ]]; then
  if [[ "${IP_FAMILY}" != "ipv6" ]]; then
    echo "error: IPV6_DNS64_PREFIX only applies to IP_FAMILY=ipv6 (got '${IP_FAMILY}')" >&2
    exit 1
  fi
  # The vendored manifest leaves the agent's --nat-v6-cidr at its 64:ff9b::/96
  # default, so any other prefix has DNS64 synthesizing addresses nothing
  # translates.
  if [[ "${IPV6_DNS64_PREFIX}" != "64:ff9b::/96" ]]; then
    echo "error: IPV6_DNS64_PREFIX must be 64:ff9b::/96, the only prefix the deployed agent" >&2
    echo "       translates (got '${IPV6_DNS64_PREFIX}')" >&2
    exit 1
  fi
fi

# The resolver cluster DNS forwards to. With NAT64 it has to be an IPv4 resolver
# reached through the prefix, because a host that needs NAT64 has no reachable
# IPv6 resolver to send pods at.
derive_dns_upstream() {
  if [[ -z "${IPV6_DNS64_PREFIX}" ]]; then
    echo "2001:4860:4860::8888 2001:4860:4860::8844"
    return
  fi
  local p="${IPV6_DNS64_PREFIX%/*}"
  echo "${p}0808:0808 ${p}0808:0404"
}

if [[ -z "${IPV6_DNS_UPSTREAM}" ]]; then
  IPV6_DNS_UPSTREAM="$(derive_dns_upstream)"
fi

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
EOF
if [[ -n "${IPV6_DNS64_PREFIX}" ]]; then
  # The manifest leaves the agent's IPv4 pool (--nat-v4-cidr) at its /24
  # default, so a node's Pod CIDR cannot be wider than /120. This /112 is
  # what makes kube-controller-manager hand out /120s; kind's IPv6 default
  # /56 does not.
  cat <<EOF >> "${ROOT}/bin/kind-config.yaml"
  podSubnet: "fd00:10:244::/112"
EOF
fi
cat <<EOF >> "${ROOT}/bin/kind-config.yaml"
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
if [[ -n "${IPV6_DNS64_PREFIX}" ]]; then
  # Appends an item to the list above. A second kubeadmConfigPatches key makes
  # the config unparseable and kind rejects it before creating anything.
  cat <<EOF >> "${ROOT}/bin/kind-config.yaml"
- |
  kind: ClusterConfiguration
  controllerManager:
    extraArgs:
    - name: node-cidr-mask-size-ipv6
      value: "120"
EOF
fi

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

# kind create returns before the apiserver answers, and every kubectl below races
# it. Poll from inside the node, where the answer does not depend on how the
# daemon published the port.
echo "Waiting for the control plane to answer..."
for attempt in $(seq 60); do
  if docker exec "${KIND_CLUSTER_NAME}-control-plane" \
    kubectl --kubeconfig=/etc/kubernetes/admin.conf get --raw /healthz >/dev/null 2>&1; then
    break
  fi
  if [[ "${attempt}" == 60 ]]; then
    echo "error: the control plane did not answer /healthz within 2m of create:" >&2
    echo "         docker logs ${KIND_CLUSTER_NAME}-control-plane" >&2
    exit 1
  fi
  sleep 2
done

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

# 4.4. Deploy the NAT64 agent, before cluster DNS starts answering with the
# prefix it translates.
if [[ -n "${IPV6_DNS64_PREFIX}" ]]; then
  echo "Deploying the NAT64 agent for ${IPV6_DNS64_PREFIX}..."
  kubectl --context="${KUBECTL_CONTEXT}" apply -f "${ROOT}/hack/third_party/nat64/install.yaml"
  if [[ -n "${NAT64_IMAGE}" ]]; then
    kubectl --context="${KUBECTL_CONTEXT}" -n kube-system set image ds/nat64 \
      "nat64=${NAT64_IMAGE}"
  fi

  nat64_fail() {
    echo "error: ${1}" >&2
    kubectl --context="${KUBECTL_CONTEXT}" -n kube-system logs ds/nat64 --tail=50 >&2 || true
    exit 1
  }
  kubectl --context="${KUBECTL_CONTEXT}" -n kube-system rollout status ds/nat64 \
    --timeout=180s || nat64_fail "the NAT64 agent did not roll out"

  # ds/nat64 has no readiness probe, so the rollout above returns as soon as the
  # container is created. Poll instead.
  echo "Waiting for the NAT64 agent to stay up..."
  clean=0
  for attempt in $(seq 45); do
    sleep 2
    total=0
    running=0
    while read -r count started; do
      if [[ -z "${count}" ]]; then
        continue
      fi
      total=$((total + 1))
      if [[ "${count}" != 0 ]]; then
        nat64_fail "the NAT64 agent restarted ${count} time(s)"
      fi
      if [[ -n "${started}" ]]; then
        running=$((running + 1))
      fi
    done <<< "$(kubectl --context="${KUBECTL_CONTEXT}" -n kube-system get pod -l app=nat64 \
      -o jsonpath='{range .items[*].status.containerStatuses[*]}{.restartCount}{" "}{.state.running.startedAt}{"\n"}{end}')"

    if [[ "${total}" -gt 0 && "${running}" -eq "${total}" ]]; then
      clean=$((clean + 1))
      if [[ "${clean}" -eq 5 ]]; then
        break
      fi
    else
      clean=0
    fi

    if [[ "${attempt}" -eq 45 ]]; then
      nat64_fail "the NAT64 agent never ran for 10s without restarting"
    fi
  done
fi

# 4.5. Point CoreDNS at an IPv6 resolver and teach it the registry's name
if [[ "${IP_FAMILY}" == "ipv6" ]]; then
  echo "Repointing CoreDNS at an IPv6 resolver and teaching it '${reg_name}'..."
  reg_v6="$(docker inspect "${reg_name}" \
    --format '{{.NetworkSettings.Networks.kind.GlobalIPv6Address}}' 2>/dev/null || true)"
  if [[ -z "${reg_v6}" ]]; then
    echo "error: '${reg_name}' has no IPv6 address on the 'kind' network" >&2
    exit 1
  fi

  # CoreDNS runs dnsPolicy: Default and inherits the node's IPv4 resolver, which
  # no pod here can reach.
  corefile="$(kubectl --context="${KUBECTL_CONTEXT}" -n kube-system get cm coredns \
    -o jsonpath='{.data.Corefile}')"
  search="forward . /etc/resolv.conf"
  # $search unquoted: bash 3.2 splices the quotes in literally.
  patched="${corefile/$search/forward . ${IPV6_DNS_UPSTREAM}}"
  if [[ "${patched}" == "${corefile}" ]]; then
    echo "error: '${search}' not found in the CoreDNS Corefile" >&2
    echo "       the Corefile layout changed upstream; update this block" >&2
    exit 1
  fi

  # Step 3's registry wiring is node-side, while atelet pulls from its own netns,
  # where "kind-registry" does not resolve. Own zone, so no fallthrough is needed:
  # only this name reaches the hosts stanza.
  patched="${patched}
${reg_name}:53 {
    errors
    hosts {
        ${reg_v6} ${reg_name}
    }
}"

  if [[ -n "${IPV6_DNS64_PREFIX}" ]]; then
    echo "Synthesizing external names into ${IPV6_DNS64_PREFIX}..."
    # dns64 answers AAAA by synthesizing from A, so it cannot share a block with
    # the cluster zones: an AAAA-only ClusterIP would synthesize to nothing. Narrow
    # the block kind ships to those zones and lift its forwarder out.
    rezoned="$(printf '%s\n' "${patched}" | awk '
      NR == 1 && /^\.:53[[:space:]]*\{/ {
        print "cluster.local:53 in-addr.arpa:53 ip6.arpa:53 {"; first = 1; next
      }
      first && /^    forward([[:space:]].*)?\{$/ { skip = 1; next }
      first && skip && /^    \}$/                { skip = 0; next }
      first && skip                              { next }
      first && /^\}$/                            { first = 0 }
      { print }
    ')"
    case "${rezoned}" in
      "cluster.local:53"*) ;;
      *) echo "error: the Corefile does not open with the '.:53' block kind ships" >&2
         echo "       the Corefile layout changed upstream; update this block" >&2
         exit 1 ;;
    esac
    if printf '%s' "${rezoned}" | grep -q 'forward'; then
      echo "error: a forward block survived the re-zone" >&2
      exit 1
    fi
    # The prefix goes inside the block: `dns64 PREFIX { ... }` parses and then
    # silently drops the block.
    patched="${rezoned}
.:53 {
    errors
    dns64 {
        prefix ${IPV6_DNS64_PREFIX}
        translate_all
    }
    forward . ${IPV6_DNS_UPSTREAM} {
        max_concurrent 1000
    }
    cache 30
    loop
    reload
}"
  fi

  # A YAML patch file avoids escaping the Corefile's newlines into JSON.
  { printf 'data:\n  Corefile: |\n'; printf '%s\n' "${patched}" | sed 's/^/    /'; } \
    > "${ROOT}/bin/coredns-patch.yaml"
  kubectl --context="${KUBECTL_CONTEXT}" -n kube-system patch cm coredns \
    --type=merge --patch-file "${ROOT}/bin/coredns-patch.yaml"
  kubectl --context="${KUBECTL_CONTEXT}" -n kube-system rollout restart deploy/coredns
  kubectl --context="${KUBECTL_CONTEXT}" -n kube-system rollout status deploy/coredns \
    --timeout=120s
fi

# 4.6. Prove the two halves meet.
if [[ -n "${IPV6_DNS64_PREFIX}" ]]; then
  echo "Probing ${NAT64_PROBE_URL} from a pod..."
  for attempt in $(seq 30); do
    kubectl --context="${KUBECTL_CONTEXT}" -n default get sa default >/dev/null 2>&1 && break
    if [[ "${attempt}" == 30 ]]; then
      echo "error: the 'default' ServiceAccount never appeared" >&2
      exit 1
    fi
    sleep 2
  done

  kubectl --context="${KUBECTL_CONTEXT}" -n default delete pod nat64-probe \
    --ignore-not-found --wait >/dev/null
  kubectl --context="${KUBECTL_CONTEXT}" -n default run nat64-probe \
    --image="${probe_image}" --restart=Never --command -- \
    sh -c "curl -6 -sS -m 25 -o /dev/null -w 'reached %{remote_ip}\n' '${NAT64_PROBE_URL}' \
      && echo NAT64-OK" >/dev/null

  # Read the log rather than attaching: attach races the container's exit.
  kubectl --context="${KUBECTL_CONTEXT}" -n default wait --for=jsonpath='{.status.phase}'=Succeeded \
    pod/nat64-probe --timeout=120s >/dev/null || true
  probe_out="$(kubectl --context="${KUBECTL_CONTEXT}" -n default logs nat64-probe 2>&1 || true)"
  kubectl --context="${KUBECTL_CONTEXT}" -n default delete pod nat64-probe \
    --ignore-not-found --wait=false >/dev/null

  if [[ "${probe_out}" != *NAT64-OK* ]]; then
    echo "error: a pod could not reach ${NAT64_PROBE_URL} through ${IPV6_DNS64_PREFIX}" >&2
    echo "       probe output: ${probe_out:-(none)}" >&2
    kubectl --context="${KUBECTL_CONTEXT}" -n kube-system logs ds/nat64 --tail=50 >&2 || true
    exit 1
  fi
  echo "NAT64 is translating."
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
