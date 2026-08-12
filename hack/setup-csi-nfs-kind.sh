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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 1. Deploy NFS Server (In-Cluster)
echo "Deploying sample NFS server..."
kubectl apply -f "${ROOT}/hack/third_party/csi-driver-nfs/deploy/example/nfs-provisioner/nfs-server.yaml"
echo "Patching NFS server to use emptyDir..."
kubectl patch deployment nfs-server -n default --type=json -p='[{"op": "replace", "path": "/spec/template/spec/volumes/0", "value": {"name": "nfs-vol", "emptyDir": {}}}]'


# 2. Deploy CSI NFS Driver
echo "Deploying CSI NFS Driver..."
kubectl apply -f "${ROOT}/hack/third_party/csi-driver-nfs/deploy/rbac-csi-nfs.yaml"
kubectl apply -f "${ROOT}/hack/third_party/csi-driver-nfs/deploy/csi-nfs-driverinfo.yaml"
kubectl apply -f "${ROOT}/hack/third_party/csi-driver-nfs/deploy/csi-nfs-controller.yaml"
kubectl apply -f "${ROOT}/hack/third_party/csi-driver-nfs/deploy/csi-nfs-node.yaml"

# 3. Patch CSI NFS Node DaemonSet to propagate mounts
echo "Patching CSI NFS Node DaemonSet..."
kubectl patch daemonset csi-nfs-node -n kube-system --patch '
spec:
  template:
    spec:
      containers:
      - name: nfs
        volumeMounts:
        - name: ateom-dir
          mountPath: /var/lib/ateom-gvisor
          mountPropagation: Bidirectional
      volumes:
      - name: ateom-dir
        hostPath:
          path: /var/lib/ateom-gvisor
          type: DirectoryOrCreate
'

# 4. Patch CSI NFS Controller Deployment to add socat proxy
echo "Patching CSI NFS Controller Deployment..."
kubectl patch deployment csi-nfs-controller -n kube-system --patch '
spec:
  template:
    spec:
      containers:
      - name: socat
        image: docker.io/alpine/socat:1.7.4.3-r0
        args:
        - tcp-listen:10000,fork,reuseaddr
        - unix-connect:/csi/csi.sock
        securityContext:
          privileged: true
        volumeMounts:
        - mountPath: /csi
          name: socket-dir
'

# 5. Expose CSI NFS Controller over Service
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

# 6. Create NFS StorageClass (pointing to the in-cluster NFS server)
# We wait for the NFS server service to get an IP first, but we can use its DNS name
# since kube-dns should resolve it. The CSI driver will resolve it when provisioning.
echo "Creating csi-nfs-sc StorageClass..."
cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: csi-nfs-sc
provisioner: nfs.csi.k8s.io
parameters:
  server: nfs-server.default.svc.cluster.local
  share: /
reclaimPolicy: Delete
volumeBindingMode: Immediate
mountOptions:
  - nfsvers=3
  - nolock
EOF

# 7. Wait for deployments to be ready
echo "Waiting for NFS server and CSI driver to be ready..."
kubectl rollout status deployment/nfs-server -n default --timeout=120s
kubectl rollout status deployment/csi-nfs-controller -n kube-system --timeout=120s
kubectl rollout status daemonset/csi-nfs-node -n kube-system --timeout=120s

# 8. Create the CSIDriverConfig for Substrate
echo "Creating nfs.csi.k8s.io CSIDriverConfig..."
cat <<EOF | kubectl apply -f -
apiVersion: ate.dev/v1alpha1
kind: CSIDriverConfig
metadata:
  name: nfs.csi.k8s.io
spec:
  driverName: nfs.csi.k8s.io
  controllerEndpoint: tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock
EOF

# 9. Restart atelet to ensure it reconnects to the new CSI socket
echo "Restarting atelet DaemonSet (if present)..."
kubectl rollout restart daemonset/atelet -n ate-system >/dev/null 2>&1 || true

echo "CSI NFS setup complete!"
