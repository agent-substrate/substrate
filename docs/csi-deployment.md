# Deploying CSI Drivers for Agent Substrate

This guide explains how to deploy and configure Container Storage Interface (CSI) drivers to work with Agent Substrate.

In standard Kubernetes, CSI drivers are deployed to interact with asynchronous sidecars and kubelet. Substrate directly orchestrates storage operations from its control plane (`ateapi`) and node daemon (`atelet`), introducing specific deployment and networking requirements for both the **CSI Controller** and the **CSI Node DaemonSet**.

---

## 1. Overview of Deployment Differences

| Component | Standard Kubernetes CSI Deployment | Agent Substrate CSI Deployment |
| :--- | :--- | :--- |
| **CSI Controller Communication** | Controller listens strictly on an in-pod Unix domain socket (`/csi/csi.sock`). Sidecars (`csi-provisioner`, `csi-attacher`) watch `etcd` and talk to the driver locally. | The Substrate control plane (`ateapi`) communicates directly with the CSI Controller via gRPC over the cluster network. The controller must be exposed over TCP via a Kubernetes `Service`. |
| **Controller Authentication** | None (in-pod socket communication only). | Optional but recommended: Mutual TLS (mTLS) authenticated via Substrate's SPIFFE Pod Identity certificates and dynamic CA rotation. |
| **CSI Node Communication** | `kubelet` communicates with the node plugin Unix domain socket. | Substrate's node daemon (`atelet`) connects directly to the CSI node plugin Unix socket (discovered via `CSIDriverConfig`). |
| **Node Mount Propagation** | `kubelet` manages mounts under `/var/lib/kubelet/pods`. | The CSI Node plugin must mount Substrate target directories on the host (e.g. `/var/lib/ateom-gvisor`) with `mountPropagation: Bidirectional` so that `atelet` can bind-mount them into actor sandboxes. |

---

## 2. CSI Controller Deployment Requirements

Because Substrate's control plane (`ateapi`) invokes CSI Controller methods directly (`CreateVolume`, `ControllerPublishVolume`, `ControllerUnpublishVolume`, `DeleteVolume`), the CSI Controller's gRPC endpoint must be reachable across the cluster network.

### Exposing the Controller Endpoint

Standard CSI controller deployments can be made network-accessible and secured in one of two ways:

1. **TLS Proxy Sidecar with Envoy (Recommended):** Add an Envoy reverse proxy sidecar container to the CSI Controller Deployment/StatefulSet. Envoy terminates incoming TLS/mTLS gRPC connections from `ateapi` and forwards the requests over HTTP/2 to the local Unix domain socket (`/csi/csi.sock`).
2. **Native TCP Endpoint:** If the CSI driver binary natively supports listening on a network socket (e.g. `--endpoint=tcp://0.0.0.0:<port>`), configure the driver container to bind directly to a network port.

### Exposing via a Kubernetes `Service`

Create a Kubernetes `Service` targeting the CSI Controller pods. This provides a stable DNS name and load balancing across controller replicas.

### Securing with mTLS and Pod Identity

When exposing the CSI Controller over the network, communication should be secured with mutual TLS (mTLS). In the [`CSIDriverConfig`](csi-volumes.md#2-dynamic-csi-driver-discovery-csidriverconfig) resource, configure the following fields under `spec.tls`:

* `enabled: true`: Enables TLS/mTLS for gRPC communication between `ateapi` and the CSI Controller service. If `false` (or omitted), communication falls back to unencrypted plaintext gRPC.
* `usePodIdentity: true`: Instructs `ateapi` to authenticate using Substrate's SPIFFE Pod Identity client certificate (`/run/podidentity.podcert.ate.dev/credential-bundle.pem`) and verify the controller's server certificate using Substrate's dynamic Service DNS CA trust bundle (`/run/servicedns.podcert.ate.dev/trust-bundle.pem`).

> [!IMPORTANT]
> **What happens if `usePodIdentity` is `false`?**
> Currently, Substrate requires `usePodIdentity: true` whenever `spec.tls.enabled` is `true`. If `usePodIdentity` is set to `false` (or omitted when TLS is enabled), the `CSIDriverConfig` resource will be rejected by CRD validation (`tls.usePodIdentity must be true when tls.enabled is true; manual certificates are not yet supported`), and `ateapi` will return an error at runtime.

### Restricting Controller Communication to `ateapi`

Because the CSI Controller executes privileged storage operations (such as creating and deleting volumes), network access should be restricted to the Substrate control plane (`ateapi`). This can be enforced via the Envoy proxy sidecar.

In the Envoy reverse proxy sidecar, enable client certificate validation (`require_client_certificate: true`) and configure the validation context with the Substrate Pod Identity CA (`signerName: podidentity.podcert.ate.dev/identity`). In `match_typed_subject_alt_names`, specify `ateapi`'s exact SPIFFE SAN:
```yaml
match_typed_subject_alt_names:
- san_type: URI
  matcher:
    exact: "spiffe://cluster.local/ns/ate-system/sa/ate-api-server"
```
Envoy will reject any connection from clients that do not present a valid certificate issued to `ateapi`.

### Example: `csi-nfs` Controller Deployment with Envoy TLS Proxy

The following example demonstrates configuring the `csi-nfs-controller` Deployment with an **Envoy sidecar proxy** to terminate mTLS, authenticate with Service DNS certificates, validate `ateapi`'s SPIFFE identity via Pod Identity CA, and forward gRPC traffic to `/csi/csi.sock`:

#### 1. Envoy Proxy ConfigMap

```yaml
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
      - name: grpc_listener
        address:
          socket_address:
            address: 0.0.0.0
            port_value: 10000
        filter_chains:
        - transport_socket:
            name: envoy.transport_sockets.tls
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
              require_client_certificate: true
              common_tls_context:
                alpn_protocols:
                - h2
                # Serving cert and trust bundle via filesystem SDS (the
                # sds-*.yaml keys below): watched_directory only works on
                # SDS-delivered secrets.
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
                        exact: "spiffe://cluster.local/ns/ate-system/sa/ate-api-server"
                  validation_context_sds_secret_config:
                    name: podidentity_validation_context
                    sds_config:
                      resource_api_version: V3
                      path_config_source:
                        path: /etc/envoy/sds-podidentity-validation.yaml
          filters:
          - name: envoy.filters.network.http_connection_manager
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
              stat_prefix: grpc_csi
              http2_protocol_options: {}
              route_config:
                name: local_route
                virtual_hosts:
                - name: csi_controller
                  domains: ["*"]
                  routes:
                  - match:
                      prefix: "/"
                    route:
                      cluster: csi_unix_socket
              http_filters:
              - name: envoy.filters.http.router
                typed_config:
                  "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
      clusters:
      - name: csi_unix_socket
        connect_timeout: 0.25s
        type: STATIC
        typed_extension_protocol_options:
          envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
            "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
            explicit_http2_config: {}
        load_assignment:
          cluster_name: csi_unix_socket
          endpoints:
          - lb_endpoints:
            - endpoint:
                address:
                  pipe:
                    path: /csi/csi.sock
  # SDS resources referenced by the listener above.
  sds-servicedns-cert.yaml: |
    resources:
    - "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret
      name: servicedns_serving_cert
      tls_certificate:
        certificate_chain:
          filename: /run/servicedns/credential-bundle.pem
        private_key:
          filename: /run/servicedns/credential-bundle.pem
        watched_directory:
          path: /run/servicedns
  sds-podidentity-validation.yaml: |
    resources:
    - "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret
      name: podidentity_validation_context
      validation_context:
        trusted_ca:
          filename: /run/podidentity-ca/trust-bundle.pem
        watched_directory:
          path: /run/podidentity-ca
```

> **Note:** `watched_directory` takes effect only on SDS-delivered secrets. Attaching it to an inline `tls_certificates` or `validation_context` entry does nothing: Envoy reads those files once at config load, so a rotated certificate is never picked up and the proxy eventually serves (or trusts) expired material.

#### 2. Envoy Sidecar in `csi-nfs-controller` Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: csi-nfs-controller
  namespace: kube-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: csi-nfs-controller
  template:
    metadata:
      labels:
        app: csi-nfs-controller
    spec:
      containers:
      # Upstream CSI NFS driver container listening on local Unix socket
      - name: nfs
        image: registry.k8s.io/sig-storage/nfsplugin:v4.13.4
        args:
        - "--nodeid=$(NODE_ID)"
        - "--endpoint=unix:///csi/csi.sock"
        volumeMounts:
        - name: socket-dir
          mountPath: /csi
      # Envoy proxy sidecar terminating mTLS and forwarding to /csi/csi.sock
      - name: envoy-proxy
        image: envoyproxy/envoy:v1.34-latest
        args:
        - "-c"
        - "/etc/envoy/envoy.yaml"
        volumeMounts:
        - name: socket-dir
          mountPath: /csi
        - name: envoy-config
          mountPath: /etc/envoy
        - name: servicedns-certs
          mountPath: /run/servicedns
          readOnly: true
        - name: podidentity-ca
          mountPath: /run/podidentity-ca
          readOnly: true
      volumes:
      - name: socket-dir
        emptyDir: {}
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
```

#### 3. Kubernetes `Service` for the Controller

```yaml
apiVersion: v1
kind: Service
metadata:
  name: csi-nfs-controller
  namespace: kube-system
spec:
  selector:
    app: csi-nfs-controller
  ports:
  - name: grpc
    port: 50052
    targetPort: 10000
```

#### 4. Corresponding `CSIDriverConfig`

```yaml
apiVersion: ate.dev/v1alpha1
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
    serverName: csi-nfs-controller.kube-system.svc.cluster.local
```

---

## 3. CSI Node DaemonSet Mount Requirements

The CSI Node plugin runs as a DaemonSet on each worker node and handles `NodeStageVolume` and `NodePublishVolume` operations when an actor is resumed.

### Mount Propagation Requirements

When the CSI Node plugin mounts an external volume (such as an NFS share or a formatted block device), the filesystem mount is created inside the plugin container. For Substrate's node supervisor (`atelet`) and worker sandboxes (`ateom-gvisor`) on the host to see this filesystem mount, the following requirements must be met:

1. **Bidirectional Mount Propagation (`mountPropagation: Bidirectional`):** The volume mount on the host Substrate target directory (e.g. `/var/lib/ateom-gvisor`) in the CSI Node plugin container must have `mountPropagation: Bidirectional`. This ensures that any mounts made by the CSI plugin inside the container propagate back to the host filesystem.
2. **Unix Domain Socket Accessibility:** The CSI Node plugin must place its Unix domain socket under `/var/lib/kubelet/plugins/<driverName>/` (or the path defined in `CSIDriverConfig.spec.nodeSocketOverride`), which is shared with `atelet`.

### Example: `csi-nfs-node` DaemonSet Configuration

In the `csi-nfs` driver deployment (see [`hack/third_party/csi-driver-nfs/deploy/csi-nfs-node.yaml`](../hack/third_party/csi-driver-nfs/deploy/csi-nfs-node.yaml) and [`hack/setup-csi-nfs-kind.sh`](../hack/setup-csi-nfs-kind.sh)), the DaemonSet is configured with bidirectional mount propagation to `/var/lib/ateom-gvisor`:

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: csi-nfs-node
  namespace: kube-system
spec:
  selector:
    matchLabels:
      app: csi-nfs-node
  template:
    metadata:
      labels:
        app: csi-nfs-node
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      serviceAccountName: csi-nfs-node-sa
      nodeSelector:
        kubernetes.io/os: linux
      containers:
      - name: nfs
        image: registry.k8s.io/sig-storage/nfsplugin:v4.13.4
        securityContext:
          privileged: true
          capabilities:
            add: ["SYS_ADMIN"]
          allowPrivilegeEscalation: true
        args:
        - "--nodeid=$(NODE_ID)"
        - "--endpoint=unix:///csi/csi.sock"
        env:
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        volumeMounts:
        # CSI communication socket shared with host/atelet
        - name: socket-dir
          mountPath: /csi
        # Target directory with bidirectional mount propagation
        - name: ateom-dir
          mountPath: /var/lib/ateom-gvisor
          mountPropagation: Bidirectional
      volumes:
      - name: socket-dir
        hostPath:
          path: /var/lib/kubelet/plugins/csi-nfsplugin
          type: DirectoryOrCreate
      - name: ateom-dir
        hostPath:
          path: /var/lib/ateom-gvisor
          type: DirectoryOrCreate
```
