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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kustomize"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/nodepath"
)

// workerHostDir is the host directory atelet mounts sandbox images under. Both
// CSI drivers need it bind-mounted with bidirectional propagation so volumes
// they mount inside it are visible to atelet.
const workerHostDir = nodepath.BasePath

// csiHostpathControllerService fronts the hostpath driver's Envoy sidecar,
// which terminates mTLS on the TCP endpoint ateapi dials and forwards to the
// driver's unix socket. The CSIDriverConfig applied later names this Service
// in both its controllerEndpoint and its TLS server name.
const csiHostpathControllerService = `
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
`

// csiDriverSpec is which drivers a --setup-csi value installs.
type csiDriverSpec struct {
	hostpath bool
	nfs      bool
}

// installsNothing reports whether the value selects no driver at all.
func (s csiDriverSpec) installsNothing() bool { return !s.hostpath && !s.nfs }

// csiDrivers is every accepted --setup-csi value. "true", "false" and the empty
// string are the shell installer's boolean spellings, from before the flag grew
// driver names, and still work.
var csiDrivers = map[string]csiDriverSpec{
	"nfs":      {nfs: true},
	"hostpath": {hostpath: true},
	"both":     {hostpath: true, nfs: true},
	"true":     {hostpath: true, nfs: true},
	"none":     {},
	"false":    {},
	"":         {},
}

// ValidateCSIDriver reports whether driver names something --setup-csi accepts.
//
// This is the half of the check that needs neither configuration nor a cluster,
// so the commands run it while cobra is still validating arguments: a typo
// should not cost a credential fetch, let alone an install.
func ValidateCSIDriver(driver string) error {
	if _, ok := csiDrivers[driver]; !ok {
		return fmt.Errorf("unknown CSI driver %q (valid options: nfs, hostpath, both, none)", driver)
	}
	return nil
}

// CheckCSIDriver is ValidateCSIDriver plus the part that needs the resolved
// configuration: the hostpath driver exists only for Kind.
//
// DeployAteSystem calls it before it installs anything, so that
// `--setup-csi=hostpath` against a cluster that is not Kind is refused up front
// rather than after a full control-plane install.
func (e *Env) CheckCSIDriver(driver string) error {
	if err := ValidateCSIDriver(driver); err != nil {
		return err
	}
	if csiDrivers[driver].hostpath {
		return e.RequireKind("csi-hostpath")
	}
	return nil
}

// SetupCSI installs the hostpath and NFS CSI drivers used by external volume
// demos. The hostpath driver is Kind only: it is patched for the single-node
// Kind layout and reaches into the node container over docker.
func (e *Env) SetupCSI(ctx context.Context, driver string) error {
	log.Step("setup_csi")

	// Both halves of the check come before any of the work: leaving a
	// podcertificate controller behind on the way to reporting a typo, or on
	// the way to refusing hostpath on a cluster that was never going to support
	// it, is worse than reporting it straight away.
	if err := e.CheckCSIDriver(driver); err != nil {
		return err
	}
	// Installing nothing is the default for deploy ate-system, so this is the
	// common path. Everything below is prerequisites for a driver, and the
	// caller that had none to install has already done its own bootstrap.
	spec := csiDrivers[driver]
	if spec.installsNothing() {
		log.Infof("Skipping CSI driver setup.")
		return nil
	}

	if err := e.ensureCSIPrerequisites(ctx); err != nil {
		return err
	}
	if spec.hostpath {
		if err := e.setupCSIHostpath(ctx); err != nil {
			return err
		}
	}
	if spec.nfs {
		return e.setupCSINFS(ctx)
	}
	return nil
}

// ensureCSIPrerequisites brings up what either driver needs before it can
// register: the ate CRDs, because each driver registers itself with a
// CSIDriverConfig, and the podcertificate controller, because the driver
// controllers are issued serving certificates from it.
//
// deploy ate-system has already done all of this by the time it calls SetupCSI.
// It is repeated because `setup csi` also runs on its own, against a cluster
// where none of it need be present; every step is an apply or a wait that
// returns immediately when the work is already done.
func (e *Env) ensureCSIPrerequisites(ctx context.Context) error {
	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}
	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}
	return e.DeployPodCertificateController(ctx)
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
	// mounts under workerHostDir, and re-applying over a running one leaves
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
	cleanupKindWorkerDir(node)

	// Before the driver, not after: ateapi talks to the controller over TCP,
	// the Envoy sidecar bridges that to the driver's unix socket, and
	// servicednssigner issues the sidecar's serving certificate from the
	// Service. A sidecar that starts before the Service exists has no
	// certificate to be issued against and never becomes ready.
	log.Infof("Exposing the CSI hostpath controller over a TCP Service...")
	if err := e.applyInline(ctx, csiHostpathControllerService); err != nil {
		return err
	}

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

	// Pin the plugin to the node whose workerHostDir it bind-mounts, and mount
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
        - name: ate-base
          mountPath: %s
          mountPropagation: Bidirectional
      volumes:
      - name: ate-base
        hostPath:
          path: %s
          type: DirectoryOrCreate
`, node, workerHostDir, workerHostDir)
	patchJSON, err := yaml.YAMLToJSON([]byte(patch))
	if err != nil {
		return fmt.Errorf("while converting csi-hostpathplugin patch to JSON: %w", err)
	}
	if _, err := e.Kube.Typed.AppsV1().StatefulSets("default").Patch(
		ctx, "csi-hostpathplugin", types.StrategicMergePatchType, patchJSON, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("while patching the csi-hostpathplugin StatefulSet: %w", err)
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
apiVersion: ate.dev/v1
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

	// The image cache directories under workerHostDir were just wiped, so
	// atelet has to recreate them.
	log.Infof("Restarting the atelet DaemonSet (if present)...")
	return e.Kube.RolloutRestart(ctx, e.Namespace(), "atelet", time.Now())
}

// csiNFSControllerService fronts the Envoy sidecar csiNFSControllerPatch adds to the NFS controller pod.
const csiNFSControllerService = `
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
`

const csiNFSEnvoyConfigMapName = "csi-nfs-envoy-config"

// csiNFSEnvoyConfigMap renders the NFS controller's Envoy config: an mTLS listener accepting only apiServerSPIFFEID, proxied to the driver's unix socket.
func csiNFSEnvoyConfigMap(apiServerSPIFFEID string) string {
	return strings.ReplaceAll(csiNFSEnvoyConfigMapTemplate, "{{API_SERVER_SPIFFE_ID}}", apiServerSPIFFEID)
}

const csiNFSEnvoyConfigMapTemplate = `
apiVersion: v1
kind: ConfigMap
metadata:
  name: ` + csiNFSEnvoyConfigMapName + `
  namespace: kube-system
data:
  envoy.yaml: |
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
                        exact: "{{API_SERVER_SPIFFE_ID}}"
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
`

// csiNFSControllerPatch adds the Envoy TLS proxy to the upstream csi-nfs-controller Deployment and removes the socat sidecar earlier installs added.
const csiNFSControllerPatch = `
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
          # Root because the driver creates csi.sock mode 0755; nothing else is needed.
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
          name: ` + csiNFSEnvoyConfigMapName + `
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
`

// csiNFSDriverConfig registers the NFS driver; its serverName is the one DNS name servicednssigner issues, <service>.<namespace>.svc.
const csiNFSDriverConfig = `
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
    serverName: csi-nfs-controller.kube-system.svc
`

func (e *Env) setupCSINFS(ctx context.Context) error {
	log.Step("setup_csi_nfs")

	if err := checkNFSDSupport(); err != nil {
		return err
	}

	deployDir := e.Cfg.Path("hack", "third_party", "csi-driver-nfs", "deploy")

	log.Infof("Deploying the NFS server...")
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Path("hack", "csi", "nfs-server.yaml")); err != nil {
		return err
	}

	// Before the driver: servicednssigner only issues the sidecar's certificate once a Service selects its pod.
	log.Infof("Exposing the CSI NFS controller over a TLS Service...")
	if err := e.applyInline(ctx, csiNFSControllerService); err != nil {
		return err
	}
	if err := e.applyInline(ctx, csiNFSEnvoyConfigMap(installdefaults.APIServerSPIFFEID(e.Namespace()))); err != nil {
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
        - name: ate-base
          mountPath: %s
          mountPropagation: Bidirectional
      volumes:
      - name: ate-base
        hostPath:
          path: %s
          type: DirectoryOrCreate
`, workerHostDir, workerHostDir)
	nodePatchJSON, err := yaml.YAMLToJSON([]byte(nodePatch))
	if err != nil {
		return fmt.Errorf("while converting csi-nfs-node patch to JSON: %w", err)
	}
	if _, err := e.Kube.Typed.AppsV1().DaemonSets("kube-system").Patch(
		ctx, "csi-nfs-node", types.StrategicMergePatchType, nodePatchJSON, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("while patching the csi-nfs-node DaemonSet: %w", err)
	}

	log.Infof("Patching the CSI NFS controller Deployment with the Envoy TLS proxy...")
	controllerPatchJSON, err := yaml.YAMLToJSON([]byte(csiNFSControllerPatch))
	if err != nil {
		return fmt.Errorf("while converting csi-nfs-controller patch to JSON: %w", err)
	}
	if _, err := e.Kube.Typed.AppsV1().Deployments("kube-system").Patch(
		ctx, "csi-nfs-controller", types.StrategicMergePatchType, controllerPatchJSON, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("while patching the csi-nfs-controller Deployment: %w", err)
	}

	// The driver resolves the server's DNS name at provisioning time, so the
	// StorageClass can be created before the NFS server has an address.
	log.Infof("Creating the csi-nfs-sc StorageClass...")
	if err := e.Kube.ApplyPath(ctx, deployDir+"/example/storageclass-nfs.yaml"); err != nil {
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
	if err := e.applyInline(ctx, csiNFSDriverConfig); err != nil {
		return err
	}

	log.Infof("Restarting the atelet DaemonSet (if present)...")
	return e.Kube.RolloutRestart(ctx, e.Namespace(), "atelet", time.Now())
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

// cleanupKindWorkerDir clears the sandbox image directory inside the Kind node
// container.
//
// Best effort throughout, as in the shell script: a stale mount from a
// previous run makes the removal fail with EBUSY, and the node container may
// not be running at all when only the CSI drivers are being reinstalled. The
// directory itself is kept so that mounts held by running pods such as atelet
// survive.
func cleanupKindWorkerDir(node string) {
	log.Infof("Cleaning up CSI directories on the Kind node...")
	if !dockerContainerRunning(node) {
		log.Warnf("Kind node %s is not running. Skipping directory cleanup.", node)
		return
	}

	unmount := fmt.Sprintf(`for mnt in $(mount | grep %s | awk '{print $3}'); do umount -f "${mnt}" || true; done`, workerHostDir)
	runQuiet("docker", "exec", node, "sh", "-c", unmount)
	runQuiet("docker", "exec", node, "sh", "-c", "rm -rf "+workerHostDir+"/*")
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
