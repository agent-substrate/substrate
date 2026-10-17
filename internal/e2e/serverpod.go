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

package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// serverPodTemplate is the one manifest every ServerPod is rendered from.
const serverPodTemplate = "internal/e2e/fixtures/serverpod.yaml.tmpl"

// serverPodReadyTimeout covers a cold image pull on a kind node, which is what
// dominates: the servers themselves listen immediately.
const serverPodReadyTimeout = 3 * time.Minute

// ServerPod describes a plain server to stand up beside the code under test:
// the origin an Actor's egress lands on, or the probe that dials a gateway.
// Everything those have in common — the pod shape, the Service, the security
// context — lives in the shared template, so a suite only names what differs.
type ServerPod struct {
	// Name names the Pod, its container and the Service alike, and is what
	// appears in kubectl output when a test fails.
	Name string
	// ImportPath is the server binary's package, as a ko:// reference. The
	// template's contract is that the binary takes --listen=:<port>, which is
	// how one manifest serves fixtures that share nothing else.
	ImportPath string
	// Args are passed to the binary ahead of --listen=:<port>, which the
	// template always appends. One image (internal/e2e/fixtures/testserver)
	// backs every server, so this is where a caller names the subcommand that
	// picks its behavior -- []string{"grpc"}, say.
	Args []string
	// Port is what the Service publishes, so an address a suite grafts into an
	// assertion — a CONNECT authority in a gateway's access log, say — is this
	// number.
	Port int
	// TargetPort is what the binary listens on, defaulting to Port. Set it to
	// publish a port the container -- uid 65532, every capability dropped --
	// cannot bind, such as 80; the Service maps Port down to it.
	TargetPort int
	// TLSPort, when set, publishes the HTTPS listener testserver's http and
	// websocket subcommands open on container port 8443 when given a
	// certificate, beside the cleartext one on Port. The readiness probe then
	// checks HTTPS, the listener that needs the certificate.
	TLSPort int
	// Namespace deploys into an existing namespace instead of a fresh one, for
	// a suite that has to populate that namespace first: credentials the pod
	// mounts have to exist before it is scheduled, and DeployServerPod cannot
	// hand back a namespace it has not created yet.
	Namespace string
	// GRPCProbe asks kubelet to probe with the gRPC health protocol instead of
	// an HTTP GET. A gRPC server answers an HTTP request with a protocol error,
	// so a server speaking grpc must set this and register the health service.
	GRPCProbe bool
	// HealthPath is the HTTP readiness path, defaulting to /healthz. Ignored
	// when GRPCProbe is set.
	HealthPath string
	// Volumes and VolumeMounts carry whatever credentials the server needs.
	// Typed, rather than more YAML in the template, so the Secret names here
	// sit beside the code that creates them instead of drifting from it.
	Volumes      []corev1.Volume
	VolumeMounts []corev1.VolumeMount
}

// serverTLSTargetPort is the container port testserver serves HTTPS on when
// given a certificate (its tlsListenAddress), which ServerPod.TLSPort publishes.
const serverTLSTargetPort = 8443

// serviceDNSBundle is where ServiceDNSTLS mounts the server's certificate
// chain and key, in one file.
const serviceDNSBundle = "/run/servicedns.podcert.ate.dev/credential-bundle.pem"

// ServiceDNSTLS returns spec serving HTTPS as well as HTTP, with a certificate
// from the cluster's service-DNS signer that covers only Server.SvcName.
// Unless spec sets them, HTTP is on port 80 and HTTPS on 443. The server must
// be testserver's http or websocket subcommand.
func ServiceDNSTLS(spec ServerPod) ServerPod {
	if spec.Port == 0 {
		spec.Port, spec.TargetPort = 80, 8080
	}
	if spec.TLSPort == 0 {
		spec.TLSPort = 443
	}
	spec.Args = append(slices.Clone(spec.Args), "--tls-cert="+serviceDNSBundle, "--tls-key="+serviceDNSBundle)
	spec.Volumes = append(slices.Clone(spec.Volumes), corev1.Volume{
		Name: "servicedns",
		VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{{PodCertificate: &corev1.PodCertificateProjection{
				SignerName:           "servicedns.podcert.ate.dev/identity",
				KeyType:              "ECDSAP256",
				CredentialBundlePath: path.Base(serviceDNSBundle),
			}}},
		}},
	})
	spec.VolumeMounts = append(slices.Clone(spec.VolumeMounts), corev1.VolumeMount{
		Name:      "servicedns",
		MountPath: path.Dir(serviceDNSBundle),
		ReadOnly:  true,
	})
	return spec
}

// Server is a deployed ServerPod, as the address a caller dials it at.
type Server struct {
	// Name is the Service's name, which is also the Pod's.
	Name string
	// Namespace is the namespace the server was deployed into, for a suite that
	// wants to port-forward to it or read its logs on failure.
	Namespace string
	// ClusterIP is the Service's address. Address dials it rather than a DNS
	// name: an IP keeps a caller inside a sandbox off that sandbox's resolver,
	// and makes the authority in a gateway's access log exactly what the test
	// deployed. A TLS client needs the name its certificate covers instead; see
	// SvcName.
	ClusterIP string
	Port      int
	// TLSPort is ServerPod.TLSPort: where the Service publishes HTTPS, or 0.
	TLSPort int
}

// Address is the host:port to dial the server at.
func (s Server) Address() string {
	return net.JoinHostPort(s.ClusterIP, strconv.Itoa(s.Port))
}

// TLSAddress is the host:port to dial the server's HTTPS listener at.
func (s Server) TLSAddress() string {
	return net.JoinHostPort(s.ClusterIP, strconv.Itoa(s.TLSPort))
}

// SvcName is the Service's <name>.<namespace>.svc, the one name a ServiceDNSTLS
// server's certificate covers. It resolves through the search path cluster DNS
// gives every pod, an actor's sandbox and the egress gateway included.
func (s Server) SvcName() string {
	return s.Name + "." + s.Namespace + ".svc"
}

// DeployServerPod builds spec's image, applies the shared server manifest, waits
// for readiness and returns the address to dial.
//
// It registers no cleanup: everything the manifest creates is namespaced, so it
// goes with the namespace CreateNamespace made — and, on failure, is retained
// with it for `kubectl logs`.
func DeployServerPod(t *testing.T, ctx context.Context, spec ServerPod) Server {
	t.Helper()
	if _, err := CheckEnv("KO_DOCKER_REPO"); err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	namespace := spec.Namespace
	if namespace == "" {
		namespace = CreateNamespace(t).Name
	}

	koApply(t, renderServerPod(t, spec, namespace))
	WaitForPodReady(t, ctx, namespace, spec.Name, serverPodReadyTimeout)

	service, err := GetClients().K8s.CoreV1().Services(namespace).Get(ctx, spec.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting service %s/%s: %v", namespace, spec.Name, err)
	}
	if service.Spec.ClusterIP == "" || service.Spec.ClusterIP == corev1.ClusterIPNone {
		t.Fatalf("service %s/%s has no ClusterIP to dial: %q", namespace, spec.Name, service.Spec.ClusterIP)
	}

	server := Server{Name: spec.Name, Namespace: namespace, ClusterIP: service.Spec.ClusterIP, Port: spec.Port, TLSPort: spec.TLSPort}
	if server.TLSPort != 0 {
		t.Logf("server %s is serving at %s and HTTPS at %s (namespace %s)", spec.Name, server.Address(), server.TLSAddress(), namespace)
	} else {
		t.Logf("server %s is serving at %s (namespace %s)", spec.Name, server.Address(), namespace)
	}
	return server
}

// renderServerPod writes spec's manifest into the test's temp dir and returns
// the path. Split out of DeployServerPod so the rendering has a unit test that
// does not need a cluster.
func renderServerPod(t *testing.T, spec ServerPod, namespace string) string {
	t.Helper()
	targetPort := spec.TargetPort
	if targetPort == 0 {
		targetPort = spec.Port
	}
	if spec.TLSPort != 0 && targetPort == serverTLSTargetPort {
		t.Fatalf("server %s listens in cleartext on %d, which its HTTPS listener (TLSPort) needs; set TargetPort to another port", spec.Name, serverTLSTargetPort)
	}
	targetPortStr := strconv.Itoa(targetPort)
	inline := map[string]string{
		"${NAME}":        spec.Name,
		"${NAMESPACE}":   namespace,
		"${IMAGE}":       "ko://" + spec.ImportPath,
		"${PORT}":        strconv.Itoa(spec.Port),
		"${TARGET_PORT}": targetPortStr,
	}
	blocks := map[string]string{
		"${ARGS}":            serverArgs(spec, targetPortStr),
		"${READINESS_PROBE}": serverReadinessProbe(spec, targetPortStr),
		// Indented to the container's and the Service's `ports:` lists.
		"${TLS_CONTAINER_PORT}": serverTLSPort(spec, "    - name: serve-tls\n      containerPort: %d", serverTLSTargetPort),
		"${TLS_SERVICE_PORT}":   serverTLSPort(spec, "  - name: serve-tls\n    port: %d\n    targetPort: %d", spec.TLSPort, serverTLSTargetPort),
		// Indented to their parents: volumeMounts is a container field, volumes
		// a pod one. An empty list takes its whole line, key included.
		"${VOLUME_MOUNTS}": yamlListBlock(t, "volumeMounts", spec.VolumeMounts, 4),
		"${VOLUMES}":       yamlListBlock(t, "volumes", spec.Volumes, 2),
	}
	return renderManifest(t, serverPodTemplate, inline, blocks)
}

// serverArgs renders the container's `args:` list -- spec.Args followed by the
// --listen the template's contract always appends -- indented to replace the
// template's `${ARGS}` line. It is never empty: every server takes --listen.
func serverArgs(spec ServerPod, targetPort string) string {
	const pad = "    "
	out := []string{pad + "args:"}
	for _, arg := range spec.Args {
		out = append(out, fmt.Sprintf("%s- %q", pad, arg))
	}
	out = append(out, fmt.Sprintf("%s- %q", pad, "--listen=:"+targetPort))
	return strings.Join(out, "\n")
}

// serverReadinessProbe renders the probe fragment for spec, indented to sit
// under the template's `readinessProbe:` key. kubelet dials the container
// directly, so the probe names the listen port rather than the published one.
// A server with TLSPort is probed over HTTPS, on the listener that depends on
// its certificate.
func serverReadinessProbe(spec ServerPod, targetPort string) string {
	if spec.GRPCProbe {
		return "      grpc:\n        port: " + targetPort
	}
	healthPath := spec.HealthPath
	if healthPath == "" {
		healthPath = "/healthz"
	}
	scheme := corev1.URISchemeHTTP
	if spec.TLSPort != 0 {
		targetPort, scheme = strconv.Itoa(serverTLSTargetPort), corev1.URISchemeHTTPS
	}
	return fmt.Sprintf("      httpGet:\n        path: %s\n        port: %s\n        scheme: %s", healthPath, targetPort, scheme)
}

// serverTLSPort renders format with args for a server with TLSPort, and
// nothing, which drops the placeholder's line, for one without.
func serverTLSPort(spec ServerPod, format string, args ...any) string {
	if spec.TLSPort == 0 {
		return ""
	}
	return fmt.Sprintf(format, args...)
}

func serverEndpointReady(endpointSlices []discoveryv1.EndpointSlice, pod *corev1.Pod, port int32) bool {
	for _, slice := range endpointSlices {
		hasPort := false
		for _, p := range slice.Ports {
			if p.Port != nil && *p.Port == port && p.Protocol != nil && *p.Protocol == corev1.ProtocolTCP {
				hasPort = true
			}
		}
		if !hasPort {
			continue
		}
		for _, endpoint := range slice.Endpoints {
			ref := endpoint.TargetRef
			if ref == nil || ref.Kind != "Pod" || ref.Namespace != pod.Namespace || ref.Name != pod.Name || ref.UID != pod.UID {
				continue
			}
			if endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready {
				continue
			}
			if endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating {
				continue
			}
			if slices.Contains(endpoint.Addresses, pod.Status.PodIP) {
				return true
			}
		}
	}
	return false
}

// CreateServerService reserves the origin address before its Pod starts, so
// callers can issue a certificate for that IP. Namespace must already exist.
// DeployServerPod subsequently applies the same Service without replacing it.
func CreateServerService(t *testing.T, ctx context.Context, spec ServerPod) Server {
	t.Helper()
	if spec.Namespace == "" {
		t.Fatal("CreateServerService requires a test namespace")
	}
	raw, err := os.ReadFile(renderServerPod(t, spec, spec.Namespace))
	if err != nil {
		t.Fatalf("reading origin manifest: %v", err)
	}
	for doc := range strings.SplitSeq(string(raw), "\n---\n") {
		var kind metav1.TypeMeta
		if err := yaml.Unmarshal([]byte(doc), &kind); err != nil {
			t.Fatalf("decoding origin kind: %v", err)
		}
		if kind.Kind != "Service" {
			continue
		}
		var service corev1.Service
		if err := yaml.UnmarshalStrict([]byte(doc), &service); err != nil {
			t.Fatalf("decoding origin Service: %v", err)
		}
		created, err := GetClients().K8s.CoreV1().Services(spec.Namespace).Create(ctx, &service, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("creating origin Service: %v", err)
		}
		if net.ParseIP(created.Spec.ClusterIP) == nil {
			t.Fatalf("origin Service has invalid IP %q", created.Spec.ClusterIP)
		}
		return Server{Name: spec.Name, Namespace: spec.Namespace, ClusterIP: created.Spec.ClusterIP, Port: spec.Port, TLSPort: spec.TLSPort}
	}
	t.Fatal("origin manifest has no Service")
	return Server{}
}

// WaitForServerEndpoint waits for publication of the current origin Pod as a
// ready Service backend. It does not perform the protocol exchange under test.
func WaitForServerEndpoint(t *testing.T, ctx context.Context, spec ServerPod) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	clients := GetClients()
	pod, err := clients.K8s.CoreV1().Pods(spec.Namespace).Get(ctx, spec.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading origin Pod for endpoint readiness: %v", err)
	}
	port := spec.TargetPort
	if port == 0 {
		port = spec.Port
	}
	for {
		endpoints, err := clients.K8s.DiscoveryV1().EndpointSlices(spec.Namespace).List(ctx, metav1.ListOptions{LabelSelector: discoveryv1.LabelServiceName + "=" + spec.Name})
		if err != nil {
			t.Fatalf("reading origin EndpointSlices: %v", err)
		}
		if serverEndpointReady(endpoints.Items, pod, int32(port)) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("origin %s/%s has no ready endpoint for Pod %s on port %d: %v", spec.Namespace, spec.Name, pod.UID, port, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}
