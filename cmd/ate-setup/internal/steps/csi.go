// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package steps

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kustomize"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// ateomHostDir is the host directory atelet mounts sandbox images under. Both
// CSI drivers need it bind-mounted with bidirectional propagation so volumes
// they mount inside it are visible to atelet.
const ateomHostDir = "/var/lib/ateom-gvisor"

// csiNFSStorageClass is the StorageClass the external volume demos provision
// from.
const csiNFSStorageClass = `
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
  - nfsvers=4.1
`

// SetupCSI installs the hostpath and NFS CSI drivers used by external volume
// demos. Kind only: both drivers are patched for the single-node Kind layout
// and reach into the node container over docker.
func (e *Env) SetupCSI(ctx context.Context) error {
	log.Step("setup_csi")
	if err := e.RequireKind("CSI setup"); err != nil {
		return err
	}
	// Both drivers register themselves with a CSIDriverConfig, so the ate CRDs
	// have to be in place even when CSI setup runs on its own rather than as
	// part of deploy ate-system.
	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}
	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}
	if err := e.EnsurePodCertificateCAs(ctx); err != nil {
		return err
	}
	if err := e.ResolveAndApply(ctx, e.Cfg.Manifest("pod-certificate-controller.yaml")); err != nil {
		return err
	}
	if err := e.applyPodcertWorkersOverride(ctx); err != nil {
		return err
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, NamespacePodCert, "podcertificate-controller", e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
		return err
	}
	if err := e.WaitForPodCertificateTrustBundles(ctx); err != nil {
		return err
	}
	if err := e.setupCSIHostpath(ctx); err != nil {
		return err
	}
	return e.setupCSINFS(ctx)
}

func (e *Env) setupCSIHostpath(ctx context.Context) error {
	log.Step("setup_csi_hostpath")

	driverDir := e.Cfg.Path("hack", "third_party", "csi-driver-host-path")
	deployDir := driverDir + "/deploy"

	bundle, err := kustomize.Build(deployDir)
	if err != nil {
		return err
	}
	objs, err := kube.DecodeManifestBytes(bundle)
	if err != nil {
		return err
	}

	// Remove any previous deployment first: the plugin StatefulSet holds
	// mounts under ateomHostDir, and re-applying over a running one leaves
	// stale mounts behind.
	log.Infof("Cleaning up existing CSI hostpath resources...")
	if err := e.Kube.Delete(ctx, objs); err != nil {
		return err
	}
	if err := e.deleteService(ctx, "default", "csi-hostpath-controller"); err != nil {
		return err
	}
	if err := e.deleteStorageClass(ctx, "csi-hostpath-sc"); err != nil {
		return err
	}
	if err := e.Kube.WaitDeleted(ctx, schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "StatefulSet"}, "default", "csi-hostpathplugin", e.Cfg.RolloutTimeout); err != nil {
		return err
	}

	node, err := e.kindNodeName(ctx)
	if err != nil {
		return err
	}
	log.Infof("Using Kind node: %s", node)
	cleanupKindAteomDir(node)

	log.Infof("Deploying the CSI hostpath driver...")
	err = e.Kube.ApplyTolerant(ctx, objs, func(obj *unstructured.Unstructured, _ error) {
		log.Warnf("skipping %s: its CRD is not installed on this cluster", kube.Describe(obj))
	})
	if err != nil {
		return err
	}

	if _, err := e.Kube.Typed.AppsV1().StatefulSets("default").
		Get(ctx, "csi-hostpathplugin", metav1.GetOptions{}); err != nil {
		return fmt.Errorf("csi-hostpathplugin StatefulSet was not created: %w", err)
	}

	// Pin the plugin to the node whose ateomHostDir it bind-mounts, and mount
	// that directory with bidirectional propagation.
	log.Infof("Patching the CSI hostpath StatefulSet...")
	patch := fmt.Sprintf(`
spec:
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: %s
      containers:
      - name: hostpath
        volumeMounts:
        - name: ateom-dir
          mountPath: %s
          mountPropagation: Bidirectional
      volumes:
      - name: ateom-dir
        hostPath:
          path: %s
          type: DirectoryOrCreate
`, node, ateomHostDir, ateomHostDir)
	patchJSON, err := yaml.YAMLToJSON([]byte(patch))
	if err != nil {
		return fmt.Errorf("while converting csi-hostpathplugin patch to JSON: %w", err)
	}
	if _, err := e.Kube.Typed.AppsV1().StatefulSets("default").Patch(
		ctx, "csi-hostpathplugin", types.StrategicMergePatchType, patchJSON, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("while patching the csi-hostpathplugin StatefulSet: %w", err)
	}

	// atelet talks to the controller over TCP; the socat sidecar bridges that
	// to the driver's unix socket.
	log.Infof("Exposing the CSI hostpath controller over a TCP Service...")
	if err := e.applyInline(ctx, `
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
`); err != nil {
		return err
	}

	log.Infof("Creating the csi-hostpath-sc StorageClass...")
	if err := e.Kube.ApplyPath(ctx, driverDir+"/examples/csi-storageclass.yaml"); err != nil {
		return err
	}

	log.Infof("Waiting for the CSI hostpath and proxy pods to be ready...")
	for _, name := range []string{"csi-hostpathplugin", "csi-hostpath-socat"} {
		if err := e.Kube.RolloutStatus(ctx, kube.KindStatefulSet, "default", name, e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
			return err
		}
	}

	log.Infof("Creating the hostpath.csi.k8s.io CSIDriverConfig...")
	if err := e.applyInline(ctx, `
apiVersion: ate.dev/v1alpha1
kind: CSIDriverConfig
metadata:
  name: hostpath.csi.k8s.io
spec:
  driverName: hostpath.csi.k8s.io
  controllerEndpoint: tcp://csi-hostpath-controller.default.svc.cluster.local:50051
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-hostpath/csi.sock
  tls:
    enabled: true
    usePodIdentity: true
    serverName: csi-hostpath-controller.default.svc
`); err != nil {
		return err
	}

	// The image cache directories under ateomHostDir were just wiped, so
	// atelet has to recreate them.
	log.Infof("Restarting the atelet DaemonSets (if present)...")
	return e.RestartAteletDaemonSets(ctx)
}

func (e *Env) setupCSINFS(ctx context.Context) error {
	log.Step("setup_csi_nfs")

	if err := checkNFSDSupport(); err != nil {
		log.Warnf("%v; skipping NFS CSI driver setup", err)
		return nil
	}

	deployDir := e.Cfg.Path("hack", "third_party", "csi-driver-nfs", "deploy")

	log.Infof("Deploying the NFS server...")
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Path("hack", "csi", "nfs-server.yaml")); err != nil {
		return err
	}

	log.Infof("Deploying the CSI NFS driver...")
	for _, name := range []string{
		"rbac-csi-nfs.yaml",
		"csi-nfs-driverinfo.yaml",
		"csi-nfs-controller.yaml",
		"csi-nfs-node.yaml",
	} {
		if err := e.Kube.ApplyPath(ctx, deployDir+"/"+name); err != nil {
			return err
		}
	}

	log.Infof("Patching the CSI NFS node DaemonSet...")
	nodePatch := fmt.Sprintf(`
spec:
  template:
    spec:
      containers:
      - name: nfs
        volumeMounts:
        - name: ateom-dir
          mountPath: %s
          mountPropagation: Bidirectional
      volumes:
      - name: ateom-dir
        hostPath:
          path: %s
          type: DirectoryOrCreate
`, ateomHostDir, ateomHostDir)
	nodePatchJSON, err := yaml.YAMLToJSON([]byte(nodePatch))
	if err != nil {
		return fmt.Errorf("while converting csi-nfs-node patch to JSON: %w", err)
	}
	if _, err := e.Kube.Typed.AppsV1().DaemonSets("kube-system").Patch(
		ctx, "csi-nfs-node", types.StrategicMergePatchType, nodePatchJSON, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("while patching the csi-nfs-node DaemonSet: %w", err)
	}

	log.Infof("Patching the CSI NFS controller Deployment...")
	const controllerPatch = `
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
`
	controllerPatchJSON, err := yaml.YAMLToJSON([]byte(controllerPatch))
	if err != nil {
		return fmt.Errorf("while converting csi-nfs-controller patch to JSON: %w", err)
	}
	if _, err := e.Kube.Typed.AppsV1().Deployments("kube-system").Patch(
		ctx, "csi-nfs-controller", types.StrategicMergePatchType, controllerPatchJSON, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("while patching the csi-nfs-controller Deployment: %w", err)
	}

	log.Infof("Exposing the CSI NFS controller over a Service...")
	if err := e.applyInline(ctx, `
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
`); err != nil {
		return err
	}

	// The driver resolves the server's DNS name at provisioning time, so the
	// StorageClass can be created before the NFS server has an address.
	log.Infof("Creating the csi-nfs-sc StorageClass...")
	if err := e.applyInline(ctx, csiNFSStorageClass); err != nil {
		return err
	}

	log.Infof("Waiting for the NFS server and CSI driver to be ready...")
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, "default", "nfs-server", e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
		return err
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, "kube-system", "csi-nfs-controller", e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
		return err
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDaemonSet, "kube-system", "csi-nfs-node", e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
		return err
	}

	log.Infof("Creating the nfs.csi.k8s.io CSIDriverConfig...")
	if err := e.applyInline(ctx, `
apiVersion: ate.dev/v1alpha1
kind: CSIDriverConfig
metadata:
  name: nfs.csi.k8s.io
spec:
  driverName: nfs.csi.k8s.io
  controllerEndpoint: tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock
`); err != nil {
		return err
	}

	log.Infof("Restarting the atelet DaemonSets (if present)...")
	return e.RestartAteletDaemonSets(ctx)
}

// checkNFSDSupport verifies the host kernel can serve NFS. The in-cluster NFS
// server runs in a container on the host kernel, so without nfsd the driver
// fails at mount time with a much less obvious error.
func checkNFSDSupport() error {
	filesystems, err := os.ReadFile("/proc/filesystems")
	if err != nil {
		// Not Linux, or no procfs: nothing to check against.
		return nil
	}
	if !strings.Contains(string(filesystems), "nfsd") {
		return fmt.Errorf("NFS server support (nfsd) is not active in the host kernel; run: sudo modprobe nfsd")
	}
	return nil
}

// applyInline applies a manifest defined in this file.
func (e *Env) applyInline(ctx context.Context, manifest string) error {
	return e.Kube.ApplyBytes(ctx, []byte(manifest))
}

func (e *Env) deleteService(ctx context.Context, namespace, name string) error {
	err := e.Kube.Typed.CoreV1().Services(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("while deleting service %s/%s: %w", namespace, name, err)
	}
	return nil
}

func (e *Env) deleteStorageClass(ctx context.Context, name string) error {
	err := e.Kube.Typed.StorageV1().StorageClasses().Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("while deleting storageclass %s: %w", name, err)
	}
	return nil
}

// kindNodeName returns the worker node to pin the hostpath driver to,
// preferring a non-control-plane node and falling back to the only node on a
// single-node cluster.
func (e *Env) kindNodeName(ctx context.Context) (string, error) {
	nodes, err := e.Kube.Typed.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "!node-role.kubernetes.io/control-plane",
	})
	if err != nil {
		return "", fmt.Errorf("while listing worker nodes: %w", err)
	}
	if len(nodes.Items) == 0 {
		nodes, err = e.Kube.Typed.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return "", fmt.Errorf("while listing nodes: %w", err)
		}
	}
	if len(nodes.Items) == 0 {
		return "", fmt.Errorf("the cluster reports no nodes")
	}
	return nodes.Items[0].Name, nil
}

// cleanupKindAteomDir clears the sandbox image directory inside the Kind node
// container.
//
// Best effort throughout, as in the shell script: a stale mount from a
// previous run makes the removal fail with EBUSY, and the node container may
// not be running at all when only the CSI drivers are being reinstalled. The
// directory itself is kept so that mounts held by running pods such as atelet
// survive.
func cleanupKindAteomDir(node string) {
	log.Infof("Cleaning up CSI directories on the Kind node...")
	if !dockerContainerRunning(node) {
		log.Warnf("Kind node %s is not running. Skipping directory cleanup.", node)
		return
	}

	unmount := fmt.Sprintf(`for mnt in $(mount | grep %s | awk '{print $3}'); do umount -f "${mnt}" || true; done`, ateomHostDir)
	runQuiet("docker", "exec", node, "sh", "-c", unmount)
	runQuiet("docker", "exec", node, "sh", "-c", "rm -rf "+ateomHostDir+"/*")
}

func dockerContainerRunning(name string) bool {
	out, err := exec.Command("docker", "ps", "--filter", "name="+name, "--format", "{{.Names}}").Output()
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == name {
			return true
		}
	}
	return false
}

// runQuiet runs a best-effort command, discarding its output and status.
func runQuiet(name string, args ...string) {
	cmd := exec.Command(name, args...)
	_ = cmd.Run()
}
