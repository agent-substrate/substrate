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
	"slices"
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
	pods, services := decodeServerPodManifest(t, renderServerPod(t, spec, "test-namespace"))
	if len(pods) != 1 || len(services) != 1 {
		t.Fatalf("rendered %d Pods and %d Services, want one of each", len(pods), len(services))
	}
	return pods[0], services[0]
}

// decodeServerPodManifest decodes every Pod and Service out of a rendered
// server manifest, in the order they appear.
func decodeServerPodManifest(t *testing.T, raw string) ([]*corev1.Pod, []*corev1.Service) {
	t.Helper()
	if strings.Contains(raw, "${") {
		t.Errorf("rendered server manifest still carries a placeholder:\n%s", raw)
	}

	var pods []*corev1.Pod
	var services []*corev1.Service
	for doc := range strings.SplitSeq(raw, "\n---\n") {
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
			pod := &corev1.Pod{}
			pods, into = append(pods, pod), pod
		case "Service":
			service := &corev1.Service{}
			services, into = append(services, service), service
		default:
			t.Fatalf("rendered server manifest carries an unexpected %q document:\n%s", meta.Kind, doc)
		}
		if err := yaml.UnmarshalStrict([]byte(doc), into); err != nil {
			t.Fatalf("rendered server %s does not match the API type: %v\n%s", meta.Kind, err, doc)
		}
	}
	return pods, services
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

// TestRenderServerPod_TCPProbe covers the third probe kind, for an origin that
// speaks neither HTTP nor gRPC, and the arg quoting a raw-TCP server needs: its
// greeting is a command-line flag, and the CRLF in it has to survive the
// round trip through YAML or the origin greets with something the suite is not
// expecting.
func TestRenderServerPod_TCPProbe(t *testing.T) {
	const banner = "TESTBANNER/1.0\r\n"
	pod, _ := renderServerPodDocs(t, ServerPod{
		Name:       "tcpbanner",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"tcpecho", "--banner=" + banner},
		Port:       2222,
		TCPProbe:   true,
	})

	container := pod.Spec.Containers[0]
	if got, want := container.Args, []string{"tcpecho", "--banner=" + banner, "--listen=:2222"}; !slices.Equal(got, want) {
		t.Errorf("container args = %q, want %q", got, want)
	}

	probe := container.ReadinessProbe
	if probe == nil || probe.TCPSocket == nil {
		t.Fatalf("readinessProbe = %+v, want a tcpSocket probe", probe)
	}
	if got := probe.TCPSocket.Port.IntValue(); got != 2222 {
		t.Errorf("tcpSocket probe port = %d, want 2222", got)
	}
	// Either of the others would probe an origin that speaks no HTTP and no
	// gRPC, so the pod would never go ready.
	if probe.HTTPGet != nil || probe.GRPC != nil {
		t.Errorf("readinessProbe also carries httpGet %+v / gRPC %+v", probe.HTTPGet, probe.GRPC)
	}
}

// TestRenderServerPods covers the manifest a batched deploy applies: the whole
// point of it is that one ko invocation and one apply cover every server, so
// what matters is that the documents concatenate into something that still
// decodes as separate objects, each keeping the name and port its caller asked
// for and all of them landing in the shared namespace.
func TestRenderServerPods(t *testing.T) {
	specs := []ServerPod{{
		Name:       "tcpbanner",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"tcpecho", "--banner=TESTBANNER/1.0\r\n"},
		Port:       2222,
		TCPProbe:   true,
	}, {
		Name:       "tcpquiet",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"tcpecho"},
		Port:       2223,
		TCPProbe:   true,
	}}
	pods, services := decodeServerPodManifest(t, renderServerPods(t, specs, "test-namespace"))

	if len(pods) != len(specs) || len(services) != len(specs) {
		t.Fatalf("rendered %d Pods and %d Services, want %d of each", len(pods), len(services), len(specs))
	}
	for i, spec := range specs {
		pod, service := pods[i], services[i]
		if pod.Name != spec.Name || service.Name != spec.Name {
			t.Errorf("document %d is Pod %q / Service %q, want both named %q", i, pod.Name, service.Name, spec.Name)
		}
		// A shared namespace is what makes one apply legal in the first place.
		if pod.Namespace != "test-namespace" || service.Namespace != "test-namespace" {
			t.Errorf("%s landed in namespace %q / %q, want test-namespace", spec.Name, pod.Namespace, service.Namespace)
		}
		// Distinct ports per server, since the suite tells them apart by the
		// port in the egress gateway's access log.
		if got := int(pod.Spec.Containers[0].Ports[0].ContainerPort); got != spec.Port {
			t.Errorf("%s containerPort = %d, want %d", spec.Name, got, spec.Port)
		}
		if got := int(service.Spec.Ports[0].Port); got != spec.Port {
			t.Errorf("%s service port = %d, want %d", spec.Name, got, spec.Port)
		}
		if got := service.Spec.Selector["app"]; got != pod.Labels["app"] {
			t.Errorf("%s Service selects app=%q but its Pod is labelled app=%q", spec.Name, got, pod.Labels["app"])
		}
	}
	// The Services select on a label that is the server's name, so two servers
	// sharing a namespace must not share it.
	if services[0].Spec.Selector["app"] == services[1].Spec.Selector["app"] {
		t.Errorf("both Services select app=%q, so each would reach either pod", services[0].Spec.Selector["app"])
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
