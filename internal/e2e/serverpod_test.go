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
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

// renderServerPodDocs renders spec and decodes the Pod and Service out of it.
//
// Strict decoding against the real API types is the point: the probe and the
// volumes are injected as pre-indented text, so a fragment at the wrong depth
// yields YAML that still parses but hangs its fields off the wrong parent —
// which strict mode reports as an unknown field, instead of applying a pod that
// silently has no readiness gate or no credentials.
func renderServerPodDocs(t *testing.T, spec ServerPod) (*corev1.Pod, *corev1.Service) {
	t.Helper()
	raw, err := os.ReadFile(renderServerPod(t, spec, "test-namespace"))
	if err != nil {
		t.Fatalf("reading the rendered server manifest: %v", err)
	}
	if strings.Contains(string(raw), "${") {
		t.Errorf("rendered server manifest still carries a placeholder:\n%s", raw)
	}

	pod, service := &corev1.Pod{}, &corev1.Service{}
	for doc := range strings.SplitSeq(string(raw), "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatalf("rendered server manifest is not valid YAML: %v\n%s", err, doc)
		}
		var into any
		switch meta.Kind {
		case "Pod":
			into = pod
		case "Service":
			into = service
		default:
			continue
		}
		if err := yaml.UnmarshalStrict([]byte(doc), into); err != nil {
			t.Fatalf("rendered server %s does not match the API type: %v\n%s", meta.Kind, err, doc)
		}
	}
	if pod.Name == "" || service.Name == "" {
		t.Fatalf("rendered server manifest is missing a Pod or a Service:\n%s", raw)
	}
	return pod, service
}

// TestRenderServerPod_GRPCProbe covers the shape the networking suite deploys:
// a bare origin, no credentials.
func TestRenderServerPod_GRPCProbe(t *testing.T) {
	pod, service := renderServerPodDocs(t, ServerPod{
		Name:       "grpcecho",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"grpc"},
		Port:       50051,
		GRPCProbe:  true,
	})

	container := pod.Spec.Containers[0]
	if got, want := container.Image, "ko://github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver"; got != want {
		t.Errorf("container image = %q, want %q", got, want)
	}
	// Args carry the subcommand ahead of the --listen the template appends. The
	// port has to reach the binary, the container port and the Service alike:
	// the gateway's access log records whatever the caller dialed, and the
	// networking suite greps for exactly this number.
	if got, want := container.Args, []string{"grpc", "--listen=:50051"}; !slices.Equal(got, want) {
		t.Errorf("container args = %v, want %v", got, want)
	}
	if got := container.Ports[0].ContainerPort; got != 50051 {
		t.Errorf("containerPort = %d, want 50051", got)
	}
	if got := service.Spec.Ports[0].Port; got != 50051 {
		t.Errorf("service port = %d, want 50051", got)
	}
	if got := service.Spec.Selector["app"]; got != pod.Labels["app"] {
		t.Errorf("service selects app=%q but the pod is labelled app=%q", got, pod.Labels["app"])
	}

	probe := container.ReadinessProbe
	if probe == nil || probe.GRPC == nil {
		t.Fatalf("readinessProbe = %+v, want a gRPC probe", probe)
	}
	if probe.GRPC.Port != 50051 {
		t.Errorf("gRPC probe port = %d, want 50051", probe.GRPC.Port)
	}
	if probe.HTTPGet != nil {
		t.Errorf("readinessProbe also carries an httpGet: %+v", probe.HTTPGet)
	}
	// A distroless base declares no USER, so runAsNonRoot alone makes kubelet
	// refuse to start the container rather than pick a uid.
	if sc := container.SecurityContext; sc == nil || sc.RunAsUser == nil || *sc.RunAsUser != 65532 {
		t.Errorf("container securityContext = %+v, want an explicit runAsUser 65532", sc)
	}

	// An empty list must take its whole line, `volumes:` key included: a key
	// with nothing under it decodes as null, which is not what a caller that
	// asked for no volumes meant.
	if len(pod.Spec.Volumes) != 0 || len(container.VolumeMounts) != 0 {
		t.Errorf("a server that asked for no credentials got volumes %+v / mounts %+v",
			pod.Spec.Volumes, container.VolumeMounts)
	}
}

// TestRenderServerPod_HTTPProbe covers the other probe kind, and the default
// health path a server gets when it does not name one.
func TestRenderServerPod_HTTPProbe(t *testing.T) {
	pod, _ := renderServerPodDocs(t, ServerPod{
		Name:       "httporigin",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http"},
		Port:       8080,
	})

	probe := pod.Spec.Containers[0].ReadinessProbe
	if probe == nil || probe.HTTPGet == nil {
		t.Fatalf("readinessProbe = %+v, want an httpGet probe", probe)
	}
	if got, want := probe.HTTPGet.Path, "/healthz"; got != want {
		t.Errorf("probe path = %q, want the default %q", got, want)
	}
	if got := probe.HTTPGet.Port.IntValue(); got != 8080 {
		t.Errorf("probe port = %d, want 8080", got)
	}
	if probe.GRPC != nil {
		t.Errorf("readinessProbe also carries a gRPC probe: %+v", probe.GRPC)
	}
}

// TestRenderServerPod covers where each port lands: every field kubelet or
// the binary reaches follows the listener, while the Service alone keeps the
// published port.
func TestRenderServerPod(t *testing.T) {
	tests := []struct {
		name          string
		spec          ServerPod
		wantPublished int32 // the Service's port
		wantListen    int   // args, containerPort, probe and Service targetPort
	}{{
		name: "maps a privileged published port to an unprivileged listener",
		spec: ServerPod{
			Name:       "egresshttp",
			ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
			Args:       []string{"http"},
			Port:       80,
			TargetPort: 8080,
		},
		wantPublished: 80,
		wantListen:    8080,
	}, {
		name: "defaults the listener to Port when TargetPort is unset",
		spec: ServerPod{
			Name:       "httporigin",
			ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
			Args:       []string{"http"},
			Port:       8080,
		},
		wantPublished: 8080,
		wantListen:    8080,
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod, service := renderServerPodDocs(t, tc.spec)

			container := pod.Spec.Containers[0]
			if got, want := container.Args, []string{"http", "--listen=:" + strconv.Itoa(tc.wantListen)}; !slices.Equal(got, want) {
				t.Errorf("container args = %v, want %v", got, want)
			}
			if got := container.Ports[0].ContainerPort; got != int32(tc.wantListen) {
				t.Errorf("containerPort = %d, want the listen port %d", got, tc.wantListen)
			}
			probe := container.ReadinessProbe
			if probe == nil || probe.HTTPGet == nil {
				t.Fatalf("readinessProbe = %+v, want an httpGet probe", probe)
			}
			if got := probe.HTTPGet.Port.IntValue(); got != tc.wantListen {
				t.Errorf("probe port = %d, want the listen port %d", got, tc.wantListen)
			}

			if got := service.Spec.Ports[0].Port; got != tc.wantPublished {
				t.Errorf("service port = %d, want the published port %d", got, tc.wantPublished)
			}
			if got := service.Spec.Ports[0].TargetPort.IntValue(); got != tc.wantListen {
				t.Errorf("service targetPort = %d, want the listen port %d", got, tc.wantListen)
			}
		})
	}
}

// TestRenderServerPod_UDPPorts covers the shape the UDP-egress test deploys:
// several published UDP ports onto one listener, beside the TCP port that
// carries readiness. The assertion that matters is that the published ports
// stay distinct while every target port is the same, since a test that reads a
// missing datagram as policy depends on all of them reaching one server.
func TestRenderServerPod_UDPPorts(t *testing.T) {
	pod, service := renderServerPodDocs(t, ServerPod{
		Name:       "udpecho",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"udpecho"},
		Port:       8053,
		UDPPorts:   []int{53, 443, 9999},
	})

	// The TCP port keeps its place at the head of both lists: the readiness
	// probe and anything dialing Address() still address it.
	ports := service.Spec.Ports
	if len(ports) != 4 {
		t.Fatalf("rendered %d service ports, want the TCP one plus three UDP: %+v", len(ports), ports)
	}
	if got := ports[0].Port; got != 8053 || ports[0].Protocol == corev1.ProtocolUDP {
		t.Errorf("first service port = %+v, want TCP 8053", ports[0])
	}
	names := map[string]bool{}
	for _, port := range ports[1:] {
		if port.Protocol != corev1.ProtocolUDP {
			t.Errorf("service port %+v is not UDP", port)
		}
		if got := port.TargetPort.IntValue(); got != 8053 {
			t.Errorf("service port %d targets %d, want the one listener on 8053", port.Port, got)
		}
		// Kubernetes requires a name once a Service carries more than one
		// port, and rejects a duplicate.
		if port.Name == "" || names[port.Name] {
			t.Errorf("service port %d has an empty or duplicate name %q", port.Port, port.Name)
		}
		names[port.Name] = true
	}
	if got := []int32{ports[1].Port, ports[2].Port, ports[3].Port}; !slices.Equal(got, []int32{53, 443, 9999}) {
		t.Errorf("published UDP ports = %v, want [53 443 9999]", got)
	}

	// One container port for the one listener, however many the Service
	// publishes onto it.
	containerPorts := pod.Spec.Containers[0].Ports
	if len(containerPorts) != 2 {
		t.Fatalf("rendered %d container ports, want the TCP one plus one UDP: %+v", len(containerPorts), containerPorts)
	}
	udp := containerPorts[1]
	if udp.Protocol != corev1.ProtocolUDP || udp.ContainerPort != 8053 {
		t.Errorf("UDP container port = %+v, want UDP 8053", udp)
	}
}

// TestRenderServerPod_NoUDPPorts pins the default: a server that asked for no
// UDP gets neither list extended, and in particular no bare entry left behind
// by the placeholder's own line.
func TestRenderServerPod_NoUDPPorts(t *testing.T) {
	pod, service := renderServerPodDocs(t, ServerPod{
		Name:       "httporigin",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http"},
		Port:       8080,
	})

	if got := len(service.Spec.Ports); got != 1 {
		t.Errorf("rendered %d service ports, want only the TCP one: %+v", got, service.Spec.Ports)
	}
	if got := len(pod.Spec.Containers[0].Ports); got != 1 {
		t.Errorf("rendered %d container ports, want only the TCP one: %+v", got, pod.Spec.Containers[0].Ports)
	}
}

// TestRenderServerPod_UDPTargetPort covers a listener that is not the TCP one.
func TestRenderServerPod_UDPTargetPort(t *testing.T) {
	pod, service := renderServerPodDocs(t, ServerPod{
		Name:          "udpecho",
		ImportPath:    "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:          []string{"udpecho"},
		Port:          8080,
		UDPPorts:      []int{53},
		UDPTargetPort: 9053,
	})

	if got := service.Spec.Ports[1].TargetPort.IntValue(); got != 9053 {
		t.Errorf("service targetPort for UDP 53 = %d, want the UDP listener on 9053", got)
	}
	if got := pod.Spec.Containers[0].Ports[1].ContainerPort; got != 9053 {
		t.Errorf("UDP containerPort = %d, want 9053", got)
	}
}

// TestRenderServerPod_Volumes covers the credential-carrying shape the sdsmint
// suite deploys, with both volume kinds it needs: a plain Secret and a
// projection. A projection is the interesting one — it nests three levels, so
// it is what an off-by-two in the block indentation shows up in.
func TestRenderServerPod_Volumes(t *testing.T) {
	pod, _ := renderServerPodDocs(t, ServerPod{
		Name:       "egressprobe",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"egressprobe"},
		Port:       8080,
		VolumeMounts: []corev1.VolumeMount{
			{Name: "actor-identity", MountPath: "/run/actor-identity"},
			{Name: "podidentity", MountPath: "/run/podidentity.podcert.ate.dev"},
		},
		Volumes: []corev1.Volume{{
			Name:         "actor-identity",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "probe-actor-identity"}},
		}, {
			Name: "podidentity",
			VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{
					ClusterTrustBundle: &corev1.ClusterTrustBundleProjection{
						SignerName:    ptr.To("servicedns.podcert.ate.dev/identity"),
						LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"podcert.ate.dev/canarying": "live"}},
						Path:          "trust-bundle.pem",
					},
				}},
			}},
		}},
	})

	mounts := pod.Spec.Containers[0].VolumeMounts
	if len(mounts) != 2 {
		t.Fatalf("rendered %d volumeMounts, want 2: %+v", len(mounts), mounts)
	}
	if got, want := mounts[0].MountPath, "/run/actor-identity"; got != want {
		t.Errorf("first mountPath = %q, want %q", got, want)
	}

	if len(pod.Spec.Volumes) != 2 {
		t.Fatalf("rendered %d volumes, want 2: %+v", len(pod.Spec.Volumes), pod.Spec.Volumes)
	}
	secret := pod.Spec.Volumes[0].Secret
	if secret == nil || secret.SecretName != "probe-actor-identity" {
		t.Errorf("first volume = %+v, want the actor-identity Secret", pod.Spec.Volumes[0])
	}
	projected := pod.Spec.Volumes[1].Projected
	if projected == nil || len(projected.Sources) != 1 || projected.Sources[0].ClusterTrustBundle == nil {
		t.Fatalf("second volume = %+v, want a clusterTrustBundle projection", pod.Spec.Volumes[1])
	}
	bundle := projected.Sources[0].ClusterTrustBundle
	if got, want := ptr.Deref(bundle.SignerName, ""), "servicedns.podcert.ate.dev/identity"; got != want {
		t.Errorf("projected signerName = %q, want %q", got, want)
	}
	if got := bundle.LabelSelector.MatchLabels["podcert.ate.dev/canarying"]; got != "live" {
		t.Errorf("projected label selector = %+v, want the live canary label", bundle.LabelSelector)
	}

	// Every mount has to name a volume that exists: a typo here yields a pod
	// kubelet refuses to start, long after the manifest applied cleanly.
	volumes := map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		volumes[v.Name] = true
	}
	for _, m := range mounts {
		if !volumes[m.Name] {
			t.Errorf("volumeMount %q names no volume; the pod has %v", m.Name, volumes)
		}
	}
}
