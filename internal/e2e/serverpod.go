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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	// number. It is the primary port: the one the template hands the binary as
	// --listen, the one kubelet probes, and the one Server.Address dials.
	Port int
	// TargetPort is what the binary listens on, defaulting to Port. Set it to
	// publish a port the container -- uid 65532, every capability dropped --
	// cannot bind, such as 80; the Service maps Port down to it.
	TargetPort int
	// ExtraPorts are published alongside Port, for a server that speaks several
	// protocols at once. A test reaches one by name with Server.AddressFor.
	//
	// The template appends --listen for the primary port only, so the flag that
	// opens each extra listener goes in Args: a spec names an extra port twice,
	// once for the binary and once for the Service.
	//
	// A container gets one readiness probe, and it is on the primary port, so a
	// server with extra ports has to bind every listener before it serves any
	// and exit if a bind fails -- otherwise Ready says nothing about the rest of
	// them, and a test dialing one races the listener into existence. The
	// `serve` subcommand of internal/e2e/fixtures/testserver does that.
	ExtraPorts []ServerPort
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

// ServerPort is one of the ports a ServerPod publishes past its primary one.
type ServerPort struct {
	// Name is what Server.AddressFor looks the port up by, and what the Service
	// calls it. Kubernetes wants an IANA_SVC_NAME here: at most 15 characters,
	// lowercase alphanumeric and dashes, at least one letter.
	Name string
	// Port is what the Service publishes.
	Port int
	// TargetPort is what the binary listens on, defaulting to Port. Two
	// published ports may share one: putting both 80 and 8080 in front of a
	// single listener is a Service with two ports and a container with one.
	TargetPort int
}

// primaryPortName names ServerPod.Port wherever the ports are handled as a
// list -- in the Service, and in Server.Ports, where it makes AddressFor agree
// with Address.
const primaryPortName = "serve"

// serverPorts is spec's ports as one list, the primary first, with every
// TargetPort resolved to the number the binary actually listens on.
func serverPorts(spec ServerPod) []ServerPort {
	all := append([]ServerPort{{Name: primaryPortName, Port: spec.Port, TargetPort: spec.TargetPort}}, spec.ExtraPorts...)
	for i, port := range all {
		if port.TargetPort == 0 {
			all[i].TargetPort = port.Port
		}
	}
	return all
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
	// Ports is every published port by name, the primary one included. Read it
	// through AddressFor.
	Ports map[string]int
}

// Address is the host:port to dial the server's primary port at.
func (s Server) Address() string {
	return net.JoinHostPort(s.ClusterIP, strconv.Itoa(s.Port))
}

// AddressFor is Address for one of the server's other ports, named as its
// ServerPort named it. An unknown name fails the test rather than handing back
// an address: the alternative is a dial to port 0, reported as a connection
// the thing under test refused.
func (s Server) AddressFor(t *testing.T, name string) string {
	t.Helper()
	port, found := s.Ports[name]
	if !found {
		t.Fatalf("server at %s has no port named %q; it publishes %v", s.ClusterIP, name, s.Ports)
	}
	return net.JoinHostPort(s.ClusterIP, strconv.Itoa(port))
}

// DeployServerPod builds spec's image, applies the shared server manifest, waits
// for readiness and returns the address to dial. The pod goes when the test that
// deployed it is done; for an origin several tests in a suite dial, see
// DeploySharedServerPod.
//
// It registers no cleanup: everything the manifest creates is namespaced, so it
// goes with the namespace CreateNamespace made — and, on failure, is retained
// with it for `kubectl logs`.
func DeployServerPod(t *testing.T, ctx context.Context, spec ServerPod) Server {
	t.Helper()
	return deployServerPod(t, ctx, spec, CreateNamespace)
}

// sharedServer is one memoized origin and the spec it was deployed from.
type sharedServer struct {
	spec ServerPod
	once sync.Once
	// server and ok are written inside once.Do and read after it returns,
	// which the Once orders for us.
	server Server
	ok     bool
}

// sharedServers memoizes DeploySharedServerPod's origins by name. Package-level
// state is suite-scoped state: go test builds one binary per package, so this
// map is born and dies with the suite that uses it.
var (
	sharedServersMu sync.Mutex
	sharedServers   = map[string]*sharedServer{}
)

// DeploySharedServerPod is DeployServerPod for an origin several tests in a
// suite dial: the first caller deploys it, every later one gets the same Server
// back.
//
// Worth it because an origin is the cheap half of a fixture — a Pod asking for
// 10m of CPU, and a Service — while standing one up costs a namespace, an apply,
// a schedule and a readiness wait every time. Four tests dialing four identical
// HTTP origins pay all of that four times to observe the same thing.
//
// The pod outlives the test that deployed it, in a namespace from
// CreateSuiteNamespace, and goes at the end of the run with every other
// namespace: deleted if the suite passed, kept for `kubectl logs` if it did not.
//
// Two things to weigh before reaching for it:
//
//   - The deploy is attributed to whichever test ran first, so its logs, its
//     failure and its serverPodReadyTimeout land on that test — and move when
//     -run picks a different one. An origin a single test dials is better served
//     by DeployServerPod, which hands the namespace back as soon as that test is
//     done.
//   - One server now answers several tests, so it has to be one that carries no
//     state between them. Every fixture in internal/e2e/fixtures/testserver
//     qualifies; something that recorded what it was sent would not.
func DeploySharedServerPod(t *testing.T, ctx context.Context, spec ServerPod) Server {
	t.Helper()
	shared, conflict := sharedServerEntry(spec)
	if conflict {
		t.Fatalf("shared server pod %q was already deployed from a different spec\n have: %+v\n want: %+v",
			spec.Name, shared.spec, spec)
	}

	shared.once.Do(func() {
		shared.server = deployServerPod(t, ctx, spec, CreateSuiteNamespace)
		// Reached only if the deploy did not Fatalf. sync.Once marks itself
		// done on the way out even when its function ends in a Goexit, which
		// is what t.Fatalf does -- so a failed deploy is remembered as one
		// rather than retried by the next test, which would spend another
		// serverPodReadyTimeout learning the same thing.
		shared.ok = true
	})
	if !shared.ok {
		t.Fatalf("shared server pod %q is not running: the test that deployed it failed", spec.Name)
	}
	return shared.server
}

// sharedServerEntry returns the memo entry for spec's name, creating it on first
// use. conflict reports that the name is already held by a different spec, which
// is two servers colliding over one name rather than a request for a second
// server -- and worth saying so, since the alternative is a test quietly dialing
// someone else's origin.
func sharedServerEntry(spec ServerPod) (entry *sharedServer, conflict bool) {
	sharedServersMu.Lock()
	defer sharedServersMu.Unlock()

	if existing, found := sharedServers[spec.Name]; found {
		return existing, !reflect.DeepEqual(existing.spec, spec)
	}
	entry = &sharedServer{spec: spec}
	sharedServers[spec.Name] = entry
	return entry, false
}

// deployServerPod is the body both variants share. newNamespace is what decides
// the pod's lifetime: a namespace released with the deploying test, or one held
// for the suite.
func deployServerPod(t *testing.T, ctx context.Context, spec ServerPod, newNamespace func(*testing.T) *Namespace) Server {
	t.Helper()
	if _, err := CheckEnv("KO_DOCKER_REPO"); err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	namespace := spec.Namespace
	if namespace == "" {
		namespace = newNamespace(t).Name
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

	published := map[string]int{}
	for _, port := range serverPorts(spec) {
		published[port.Name] = port.Port
	}
	server := Server{Namespace: namespace, ClusterIP: service.Spec.ClusterIP, Port: spec.Port, Ports: published}
	t.Logf("server %s is serving at %s (namespace %s)", spec.Name, server.Address(), namespace)
	return server
}

// renderServerPod writes spec's manifest into the test's temp dir and returns
// the path. Split out of DeployServerPod so the rendering has a unit test that
// does not need a cluster.
func renderServerPod(t *testing.T, spec ServerPod, namespace string) string {
	t.Helper()
	ports := serverPorts(spec)
	if err := checkServerPorts(ports); err != nil {
		t.Fatalf("server pod %q: %v", spec.Name, err)
	}
	targetPortStr := strconv.Itoa(ports[0].TargetPort)
	inline := map[string]string{
		"${NAME}":      spec.Name,
		"${NAMESPACE}": namespace,
		"${IMAGE}":     "ko://" + spec.ImportPath,
	}
	containerPorts, servicePorts := serverPortBlocks(t, ports)
	blocks := map[string]string{
		"${ARGS}":            serverArgs(spec, targetPortStr),
		"${READINESS_PROBE}": serverReadinessProbe(spec, targetPortStr),
		"${CONTAINER_PORTS}": containerPorts,
		"${SERVICE_PORTS}":   servicePorts,
		// Indented to their parents: volumeMounts is a container field, volumes
		// a pod one. An empty list takes its whole line, key included.
		"${VOLUME_MOUNTS}": yamlListBlock(t, "volumeMounts", spec.VolumeMounts, 4),
		"${VOLUMES}":       yamlListBlock(t, "volumes", spec.Volumes, 2),
	}
	return renderManifest(t, serverPodTemplate, inline, blocks)
}

// checkServerPorts rejects a spec the API server would reject, or that would
// apply cleanly and mean something other than what it says.
func checkServerPorts(ports []ServerPort) error {
	names, numbers := map[string]bool{}, map[int]bool{}
	for _, port := range ports {
		switch {
		case port.Name == "":
			return fmt.Errorf("port %+v has no name; a Service with several ports must name each one", port)
		case port.Port == 0:
			return fmt.Errorf("port %q publishes nothing", port.Name)
		case names[port.Name]:
			return fmt.Errorf("two ports are named %q", port.Name)
		case numbers[port.Port]:
			// The Service keeps one of the two and AddressFor still answers for
			// both, so nothing downstream can notice: the test that asked for
			// the loser quietly reaches whatever the winner serves.
			return fmt.Errorf("two ports publish %d; the Service can carry it once", port.Port)
		}
		names[port.Name], numbers[port.Port] = true, true
	}
	return nil
}

// serverPortBlocks renders the container's ports and the Service's, which do
// not line up one to one: several published ports may share a listener, and a
// container may declare a given number only once -- a duplicate containerPort
// is a pod the API server refuses.
func serverPortBlocks(t *testing.T, ports []ServerPort) (container, service string) {
	t.Helper()
	var containerPorts []corev1.ContainerPort
	var servicePorts []corev1.ServicePort
	declared := map[int]bool{}
	for _, port := range ports {
		if !declared[port.TargetPort] {
			declared[port.TargetPort] = true
			containerPorts = append(containerPorts, corev1.ContainerPort{
				Name:          port.Name,
				ContainerPort: int32(port.TargetPort),
			})
		}
		servicePorts = append(servicePorts, corev1.ServicePort{
			Name:       port.Name,
			Port:       int32(port.Port),
			TargetPort: intstr.FromInt32(int32(port.TargetPort)),
		})
	}
	return yamlListBlock(t, "ports", containerPorts, 4), yamlListBlock(t, "ports", servicePorts, 2)
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
