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

# TODO: Consider integrating this script into install-ate-kind.sh (perhaps as an optional flag).

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Define paths
DRIVER_DIR="${ROOT}/hack/third_party/csi-driver-host-path"
DEPLOY_DIR="${DRIVER_DIR}/deploy"

# 1. Clean up existing deployment if present
echo "Cleaning up existing CSI Hostpath resources..."
# Use kustomize to delete if kustomization is valid, ignore errors
kubectl delete -k "${DEPLOY_DIR}" >/dev/null 2>&1 || true
# Also delete our custom service and SC
kubectl delete service csi-hostpath-controller -n default >/dev/null 2>&1 || true
kubectl delete storageclass csi-hostpath-sc >/dev/null 2>&1 || true

# Also clean up the host directories inside Kind node (best effort)
echo "Cleaning up CSI directories on Kind node..."
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
KIND_NODE=$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o name | head -n 1 | cut -d'/' -f2)
if [ -z "${KIND_NODE}" ]; then
  KIND_NODE=$(kubectl get nodes -o name | head -n 1 | cut -d'/' -f2)
fi
echo "Using Kind node: ${KIND_NODE}"
if docker ps | grep -q "${KIND_NODE}"; then
  # Unmount any stale mounts to prevent "device or resource busy"
  docker exec "${KIND_NODE}" sh -c '
    for mnt in $(mount | grep /var/lib/ateom-gvisor | awk "{print \$3}"); do
      echo "Unmounting stale mount: ${mnt}"
      umount -f "${mnt}" || true
    done
  ' || true
  # Only delete contents, keep the directory itself to preserve mounts of running pods (like atelet)
  docker exec "${KIND_NODE}" sh -c 'rm -rf /var/lib/ateom-gvisor/*' || true
else
  echo "Warning: Kind node ${KIND_NODE} not running. Skipping directory cleanup."
fi

# 3. Deploy the CSI Hostpath Driver
echo "Deploying CSI Hostpath Driver..."
# The apply might fail at snapshotclass due to missing CRDs. We catch and ignore this.
kubectl apply -k "${DEPLOY_DIR}" || {
  echo "Warning: kubectl apply -k exited with error (likely VolumeSnapshotClass CRD missing). Checking if plugin StatefulSet is created..."
}

# Verify StatefulSet exists
if ! kubectl get statefulset csi-hostpathplugin -n default >/dev/null 2>&1; then
  echo "Error: csi-hostpathplugin StatefulSet was not created!"
  exit 1
fi

# 4. Patch the CSI Driver to mount Substrate directory
echo "Patching CSI Hostpath StatefulSet..."
kubectl patch statefulset csi-hostpathplugin -n default --patch "
spec:
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: ${KIND_NODE}
      containers:
      - name: hostpath
        volumeMounts:
        - name: ateom-dir
          mountPath: /var/lib/ateom-gvisor
          mountPropagation: Bidirectional
      volumes:
      - name: ateom-dir
        hostPath:
          path: /var/lib/ateom-gvisor
          type: DirectoryOrCreate
"

# 5. Create the TCP Service mapping port 50051 to socat port 10000
echo "Exposing CSI Controller over TCP Service..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: csi-hostpath-controller
  namespace: default
spec:
  selector:
    app.kubernetes.io/name: csi-hostpath-socat
  ports:
  - port: 50051
    targetPort: 10000
    name: grpc
EOF

# 6. Create the StorageClass
echo "Creating csi-hostpath-sc StorageClass..."
kubectl apply -f "${DRIVER_DIR}/examples/csi-storageclass.yaml"

# 7. Wait for pods to be ready
echo "Waiting for CSI Hostpath and Proxy pods to be Ready..."
kubectl rollout status statefulset/csi-hostpathplugin -n default --timeout=120s
kubectl rollout status statefulset/csi-hostpath-socat -n default --timeout=120s

# 8. Create the CSIDriverConfig for Substrate
echo "Creating hostpath.csi.k8s.io CSIDriverConfig..."
cat <<EOF | kubectl apply -f -
apiVersion: ate.dev/v1alpha1
kind: CSIDriverConfig
metadata:
  name: hostpath.csi.k8s.io
spec:
  driverName: hostpath.csi.k8s.io
  controllerEndpoint: tcp://csi-hostpath-controller.default.svc.cluster.local:50051
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-hostpath/csi.sock
EOF

# 9. Restart atelet to recreate image cache directories if they were wiped
echo "Restarting atelet DaemonSet (if present)..."
kubectl rollout restart daemonset/atelet -n ate-system >/dev/null 2>&1 || true

echo "CSI Hostpath setup complete!"
