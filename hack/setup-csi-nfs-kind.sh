#!/usr/bin/env bash

# Copyright 2026 Google LLC
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

set -o errexit -o nounset -o pipefail

# Check if NFS server module is loaded on the host
if [ -f /proc/filesystems ] && ! grep -q "nfsd" /proc/filesystems; then
  echo "ERROR: NFS server support (nfsd) is not active in the host kernel."
  echo "Please load the nfsd module on your host by running:"
  echo "  sudo modprobe nfsd"
  exit 1
fi

ROOT="$(git rev-parse --show-toplevel)"

# 1. Deploy NFS Server (In-Cluster)
echo "Deploying NFS server..."
kubectl apply -f "${ROOT}/hack/csi/nfs-server.yaml"


# 2. Expose the CSI NFS Controller over a TLS Service, and render the Envoy
# proxy configuration it fronts. Both before the driver: servicednssigner
# issues the proxy's serving certificate from the Services selecting its pod,
# and refuses to issue one for a pod no Service selects yet.
echo "Exposing CSI NFS Controller over Service..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: csi-nfs-controller
  namespace: kube-system
spec:
  selector:
    app: csi-nfs-controller
  ports:
  - port: 50052
    targetPort: 10000
    name: grpc
EOF

# The only client the proxy accepts is ate-api-server's Pod Identity.
ATE_NAMESPACE="${ATE_NAMESPACE:-ate-system}"
echo "Creating the CSI NFS Controller Envoy configuration..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: csi-nfs-envoy-config
  namespace: kube-system
data:
  envoy.yaml: |
    # SDS (used for the TLS certs below) requires a node id and cluster.
    node:
      id: csi-nfs-envoy
      cluster: csi-nfs-envoy
    static_resources:
      listeners:
      - name: csi_tls_listener
        address:
          socket_address: { address: 0.0.0.0, port_value: 10000 }
        filter_chains:
        - transport_socket:
            name: envoy.transport_sockets.tls
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
              require_client_certificate: true
              common_tls_context:
                alpn_protocols:
                - h2
                tls_certificate_sds_secret_configs:
                - name: servicedns_serving_cert
                  sds_config:
                    resource_api_version: V3
                    path_config_source:
                      path: /etc/envoy/sds-servicedns-cert.yaml
                combined_validation_context:
                  default_validation_context:
                    match_typed_subject_alt_names:
                    - san_type: URI
                      matcher:
                        exact: "spiffe://cluster.local/ns/${ATE_NAMESPACE}/sa/ate-api-server"
                  validation_context_sds_secret_config:
                    name: podidentity_validation_context
                    sds_config:
                      resource_api_version: V3
                      path_config_source:
                        path: /etc/envoy/sds-podidentity-validation.yaml
          filters:
          - name: envoy.filters.network.tcp_proxy
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
              stat_prefix: csi_proxy
              cluster: csi_unix_socket
      clusters:
      - name: csi_unix_socket
        connect_timeout: 0.25s
        type: STATIC
        load_assignment:
          cluster_name: csi_unix_socket
          endpoints:
          - lb_endpoints:
            - endpoint:
                address:
                  pipe:
                    path: /csi/csi.sock
  sds-servicedns-cert.yaml: |
    resources:
    - "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret
      name: servicedns_serving_cert
      tls_certificate:
        certificate_chain: { filename: /run/servicedns/credential-bundle.pem }
        private_key: { filename: /run/servicedns/credential-bundle.pem }
        watched_directory: { path: /run/servicedns }
  sds-podidentity-validation.yaml: |
    resources:
    - "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret
      name: podidentity_validation_context
      validation_context:
        trusted_ca: { filename: /run/podidentity-ca/trust-bundle.pem }
        watched_directory: { path: /run/podidentity-ca }
EOF

# 3. Deploy CSI NFS Driver
echo "Deploying CSI NFS Driver..."
kubectl apply -f "${ROOT}/hack/third_party/csi-driver-nfs/deploy/rbac-csi-nfs.yaml"
kubectl apply -f "${ROOT}/hack/third_party/csi-driver-nfs/deploy/csi-nfs-driverinfo.yaml"
kubectl apply -f "${ROOT}/hack/third_party/csi-driver-nfs/deploy/csi-nfs-controller.yaml"
kubectl apply -f "${ROOT}/hack/third_party/csi-driver-nfs/deploy/csi-nfs-node.yaml"

# 4. Patch CSI NFS Node DaemonSet to propagate mounts
echo "Patching CSI NFS Node DaemonSet..."
kubectl patch daemonset csi-nfs-node -n kube-system --patch '
spec:
  template:
    spec:
      containers:
      - name: nfs
        volumeMounts:
        - name: ate-base
          mountPath: /var/lib/ate
          mountPropagation: Bidirectional
      volumes:
      - name: ate-base
        hostPath:
          path: /var/lib/ate
          type: DirectoryOrCreate
'

# 5. Patch CSI NFS Controller Deployment to add the Envoy mTLS proxy. The
# delete directive removes the plaintext socat sidecar an earlier run added;
# a fresh install has none and it does nothing. The proxy runs as root because
# the driver creates csi.sock mode 0755, but is not privileged.
echo "Patching CSI NFS Controller Deployment..."
# shellcheck disable=SC2016 # $patch is a strategic merge directive, not a variable
kubectl patch deployment csi-nfs-controller -n kube-system --patch '
spec:
  template:
    spec:
      containers:
      - $patch: delete
        name: socat
      - name: envoy
        image: envoyproxy/envoy:v1.34-latest
        command:
        - /usr/local/bin/envoy
        args:
        - -c
        - /etc/envoy/envoy.yaml
        readinessProbe:
          tcpSocket:
            port: 10000
          periodSeconds: 5
        securityContext:
          runAsUser: 0
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          capabilities:
            drop:
            - ALL
        volumeMounts:
        - mountPath: /etc/envoy
          name: envoy-config
          readOnly: true
        - mountPath: /csi
          name: socket-dir
        - mountPath: /run/servicedns
          name: servicedns-certs
          readOnly: true
        - mountPath: /run/podidentity-ca
          name: podidentity-ca
          readOnly: true
      volumes:
      - name: envoy-config
        configMap:
          name: csi-nfs-envoy-config
      - name: servicedns-certs
        projected:
          sources:
          - podCertificate:
              signerName: servicedns.podcert.ate.dev/identity
              keyType: ECDSAP256
              credentialBundlePath: credential-bundle.pem
      - name: podidentity-ca
        projected:
          sources:
          - clusterTrustBundle:
              signerName: podidentity.podcert.ate.dev/identity
              labelSelector:
                matchLabels:
                  podcert.ate.dev/canarying: live
              path: trust-bundle.pem
'

# 6. Create NFS StorageClass (pointing to the in-cluster NFS server)
# We wait for the NFS server service to get an IP first, but we can use its DNS name
# since kube-dns should resolve it. The CSI driver will resolve it when provisioning.
echo "Creating csi-nfs-sc StorageClass..."
kubectl apply -f "${ROOT}/hack/third_party/csi-driver-nfs/deploy/example/storageclass-nfs.yaml"

# 7. Wait for deployments to be ready
echo "Waiting for NFS server and CSI driver to be ready..."
kubectl rollout status deployment/nfs-server -n default --timeout=120s
kubectl rollout status deployment/csi-nfs-controller -n kube-system --timeout=120s
kubectl rollout status daemonset/csi-nfs-node -n kube-system --timeout=120s

# 8. Create the CSIDriverConfig for Substrate
echo "Creating nfs.csi.k8s.io CSIDriverConfig..."
cat <<EOF | kubectl apply -f -
apiVersion: ate.dev/v1
kind: CSIDriverConfig
metadata:
  name: nfs.csi.k8s.io
spec:
  driverName: nfs.csi.k8s.io
  controllerEndpoint: tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock
  tls:
    enabled: true
    usePodIdentity: true
    # The one DNS name servicednssigner puts in the proxy's certificate.
    serverName: csi-nfs-controller.kube-system.svc
EOF

# 9. Restart atelet to ensure it reconnects to the new CSI socket.
echo "Restarting atelet DaemonSet (if present)..."
kubectl rollout restart daemonset/atelet -n "${ATE_NAMESPACE}" >/dev/null 2>&1 || true

echo "CSI NFS setup complete!"
