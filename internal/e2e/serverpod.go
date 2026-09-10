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
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// Server is a deployed ServerPod, as the address a caller dials it at.
type Server struct {
	// Namespace is the namespace the server was deployed into, for a suite that
	// wants to port-forward to it or read its logs on failure.
	Namespace string
	// ClusterIP is the Service's address. Deliberately not its DNS name: an IP
	// keeps a caller inside a sandbox off that sandbox's resolver, and makes
	// the authority in a gateway's access log exactly what the test deployed.
	ClusterIP string
	Port      int
}

// Address is the host:port to dial the server at.
func (s Server) Address() string {
	return net.JoinHostPort(s.ClusterIP, strconv.Itoa(s.Port))
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

	server := Server{Namespace: namespace, ClusterIP: service.Spec.ClusterIP, Port: spec.Port}
	t.Logf("server %s is serving at %s (namespace %s)", spec.Name, server.Address(), namespace)
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
func serverReadinessProbe(spec ServerPod, targetPort string) string {
	if spec.GRPCProbe {
		return "      grpc:\n        port: " + targetPort
	}
	path := spec.HealthPath
	if path == "" {
		path = "/healthz"
	}
	return fmt.Sprintf("      httpGet:\n        path: %s\n        port: %s", path, targetPort)
}
