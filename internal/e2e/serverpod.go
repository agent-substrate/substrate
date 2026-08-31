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
	// Port is what the binary listens on and what the Service publishes,
	// unchanged, so an address a suite grafts into an assertion — a CONNECT
	// authority in a gateway's access log, say — is this number.
	Port int
	// Namespace deploys into an existing namespace instead of a fresh one, for
	// a suite that has to populate that namespace first: credentials the pod
	// mounts have to exist before it is scheduled, and DeployServerPod cannot
	// hand back a namespace it has not created yet.
	Namespace string
	// GRPCProbe asks kubelet to probe with the gRPC health protocol instead of
	// an HTTP GET. A gRPC server answers an HTTP request with a protocol error,
	// so a server speaking grpc must set this and register the health service.
	GRPCProbe bool
	// TCPProbe asks kubelet to probe by opening a TCP connection and closing
	// it, which is all readiness can mean for a server that speaks neither HTTP
	// nor gRPC. Ignored when GRPCProbe is set.
	TCPProbe bool
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
func DeployServerPod(t *testing.T, ctx context.Context, spec ServerPod) Server {
	t.Helper()
	return DeployServerPods(t, ctx, spec)[0]
}

// DeployServerPods deploys several servers at once, into one namespace and
// through one ko invocation, and returns their addresses in the order given.
//
// Worth having rather than a loop over DeployServerPod, because a serial loop
// pays for each server end to end: ko starts, builds and pushes, then the pod
// schedules, pulls and goes ready, and only then does the next one begin. One
// apply overlaps all of that. ko caches builds and pushes by image reference
// within an invocation, so servers sharing an ImportPath -- which is the common
// case, since one testserver binary backs them all -- build once; and every pod
// is admitted at the same instant, so the second readiness wait has usually
// already been satisfied by the time the first returns.
//
// It registers no cleanup: everything the manifest creates is namespaced, so it
// goes with the namespace CreateNamespace made — and, on failure, is retained
// with it for `kubectl logs`.
func DeployServerPods(t *testing.T, ctx context.Context, specs ...ServerPod) []Server {
	t.Helper()
	if len(specs) == 0 {
		t.Fatalf("DeployServerPods was given no servers to deploy")
	}
	if _, err := CheckEnv("KO_DOCKER_REPO"); err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	namespace := serverPodNamespace(t, specs)

	koApply(t, writeManifest(t, "serverpods.yaml", renderServerPods(t, specs, namespace)))

	servers := make([]Server, 0, len(specs))
	for _, spec := range specs {
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
		servers = append(servers, server)
	}
	return servers
}

// serverPodNamespace picks the one namespace specs are deployed into: the one
// they name, or a fresh one when none does. Sharing it is what lets a single
// apply cover them all, so specs that disagree are a mistake in the caller
// rather than something to paper over by applying twice.
func serverPodNamespace(t *testing.T, specs []ServerPod) string {
	t.Helper()
	names := map[string]bool{}
	namespace := ""
	for _, spec := range specs {
		// Distinct names, because the Pod, the Service and the app label are all
		// spec.Name: two servers sharing one would apply over each other and
		// leave the suite dialing a Service that selects both.
		if names[spec.Name] {
			t.Fatalf("two servers are both named %q", spec.Name)
		}
		names[spec.Name] = true

		if spec.Namespace == "" {
			continue
		}
		if namespace != "" && namespace != spec.Namespace {
			t.Fatalf("servers deployed together must share a namespace, got %q and %q", namespace, spec.Namespace)
		}
		namespace = spec.Namespace
	}
	if namespace == "" {
		namespace = CreateNamespace(t).Name
	}
	return namespace
}

// renderServerPods renders specs into one multi-document manifest.
func renderServerPods(t *testing.T, specs []ServerPod, namespace string) string {
	t.Helper()
	docs := make([]string, 0, len(specs))
	for _, spec := range specs {
		docs = append(docs, renderServerPod(t, spec, namespace))
	}
	return strings.Join(docs, "\n---\n")
}

// renderServerPod renders spec's Pod and Service. Split out of DeployServerPods
// so the rendering has a unit test that does not need a cluster.
func renderServerPod(t *testing.T, spec ServerPod, namespace string) string {
	t.Helper()
	port := strconv.Itoa(spec.Port)
	inline := map[string]string{
		"${NAME}":      spec.Name,
		"${NAMESPACE}": namespace,
		"${IMAGE}":     "ko://" + spec.ImportPath,
		"${PORT}":      port,
	}
	blocks := map[string]string{
		"${ARGS}":            serverArgs(spec, port),
		"${READINESS_PROBE}": serverReadinessProbe(spec, port),
		// Indented to their parents: volumeMounts is a container field, volumes
		// a pod one. An empty list takes its whole line, key included.
		"${VOLUME_MOUNTS}": yamlListBlock(t, "volumeMounts", spec.VolumeMounts, 4),
		"${VOLUMES}":       yamlListBlock(t, "volumes", spec.Volumes, 2),
	}
	return renderManifestText(t, serverPodTemplate, inline, blocks)
}

// serverArgs renders the container's `args:` list -- spec.Args followed by the
// --listen the template's contract always appends -- indented to replace the
// template's `${ARGS}` line. It is never empty: every server takes --listen.
func serverArgs(spec ServerPod, port string) string {
	const pad = "    "
	out := []string{pad + "args:"}
	for _, arg := range spec.Args {
		out = append(out, fmt.Sprintf("%s- %q", pad, arg))
	}
	out = append(out, fmt.Sprintf("%s- %q", pad, "--listen=:"+port))
	return strings.Join(out, "\n")
}

// serverReadinessProbe renders the probe fragment for spec, indented to sit
// under the template's `readinessProbe:` key.
func serverReadinessProbe(spec ServerPod, port string) string {
	switch {
	case spec.GRPCProbe:
		return "      grpc:\n        port: " + port
	case spec.TCPProbe:
		return "      tcpSocket:\n        port: " + port
	}
	path := spec.HealthPath
	if path == "" {
		path = "/healthz"
	}
	return fmt.Sprintf("      httpGet:\n        path: %s\n        port: %s", path, port)
}
