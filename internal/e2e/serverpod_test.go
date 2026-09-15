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

// TestRenderServerPod_ExtraPorts covers the multi-protocol origin: one pod
// serving several ports, which the Service and the container spell differently
// — the Service publishes every port, while the container declares each
// listener once, however many published ports point at it.
func TestRenderServerPod_ExtraPorts(t *testing.T) {
	pod, service := renderServerPodDocs(t, ServerPod{
		Name:       "origin",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"serve", "--grpc=:50051"},
		Port:       80,
		TargetPort: 8080,
		ExtraPorts: []ServerPort{
			{Name: "http-alt", Port: 8080, TargetPort: 8080},
			{Name: "grpc", Port: 50051},
		},
	})

	container := pod.Spec.Containers[0]
	// Only the primary port is appended as --listen; the rest are flags the
	// caller wrote, because only the caller knows which listener is which.
	if got, want := container.Args, []string{"serve", "--grpc=:50051", "--listen=:8080"}; !slices.Equal(got, want) {
		t.Errorf("container args = %v, want %v", got, want)
	}

	// 80 and 8080 are published in front of the same listener, and a container
	// that declared 8080 twice is a pod the API server refuses.
	wantContainer := []struct {
		name string
		port int32
	}{{"serve", 8080}, {"grpc", 50051}}
	if len(container.Ports) != len(wantContainer) {
		t.Fatalf("container declares %d ports, want %d: %+v", len(container.Ports), len(wantContainer), container.Ports)
	}
	for i, want := range wantContainer {
		if got := container.Ports[i]; got.Name != want.name || got.ContainerPort != want.port {
			t.Errorf("containerPort %d = %s/%d, want %s/%d", i, got.Name, got.ContainerPort, want.name, want.port)
		}
	}

	wantService := []struct {
		name             string
		port, targetPort int
	}{{"serve", 80, 8080}, {"http-alt", 8080, 8080}, {"grpc", 50051, 50051}}
	if len(service.Spec.Ports) != len(wantService) {
		t.Fatalf("service publishes %d ports, want %d: %+v", len(service.Spec.Ports), len(wantService), service.Spec.Ports)
	}
	for i, want := range wantService {
		got := service.Spec.Ports[i]
		if got.Name != want.name || int(got.Port) != want.port || got.TargetPort.IntValue() != want.targetPort {
			t.Errorf("service port %d = %s %d->%d, want %s %d->%d",
				i, got.Name, got.Port, got.TargetPort.IntValue(), want.name, want.port, want.targetPort)
		}
	}

	// The one probe kubelet gets stays on the primary listener. What makes that
	// enough is the server binding every port before it serves any; see the
	// ExtraPorts doc comment.
	probe := container.ReadinessProbe
	if probe == nil || probe.HTTPGet == nil {
		t.Fatalf("readinessProbe = %+v, want an httpGet probe", probe)
	}
	if got := probe.HTTPGet.Port.IntValue(); got != 8080 {
		t.Errorf("probe port = %d, want the primary listen port 8080", got)
	}
}

// TestServerAddressFor covers the lookup a test dials an extra port through.
func TestServerAddressFor(t *testing.T) {
	server := Server{
		ClusterIP: "10.0.0.7",
		Port:      80,
		Ports:     map[string]int{"serve": 80, "grpc": 50051},
	}
	if got, want := server.Address(), "10.0.0.7:80"; got != want {
		t.Errorf("Address() = %q, want %q", got, want)
	}
	if got, want := server.AddressFor(t, "grpc"), "10.0.0.7:50051"; got != want {
		t.Errorf("AddressFor(grpc) = %q, want %q", got, want)
	}
	// The primary port is in the map under its own name, so a caller that
	// reaches for every port by name does not have to special-case one.
	if got, want := server.AddressFor(t, "serve"), server.Address(); got != want {
		t.Errorf("AddressFor(serve) = %q, want Address() = %q", got, want)
	}
}

// TestCheckServerPorts covers the specs that have to fail here rather than in
// the cluster: a rendered manifest the API server rejects costs a suite the
// whole apply, and a duplicate published port costs it a test dialing the
// wrong server with no sign anything went wrong.
func TestCheckServerPorts(t *testing.T) {
	tests := []struct {
		name string
		spec ServerPod
		ok   bool // whether checkServerPorts should accept it
	}{{
		name: "the single-port shape every existing caller passes",
		spec: ServerPod{Port: 80, TargetPort: 8080},
		ok:   true,
	}, {
		name: "two published ports sharing one listener",
		spec: ServerPod{Port: 80, TargetPort: 8080, ExtraPorts: []ServerPort{{Name: "http-alt", Port: 8080}}},
		ok:   true,
	}, {
		name: "an extra port with no name",
		spec: ServerPod{Port: 80, ExtraPorts: []ServerPort{{Port: 50051}}},
	}, {
		name: "an extra port that publishes nothing",
		spec: ServerPod{Port: 80, ExtraPorts: []ServerPort{{Name: "grpc"}}},
	}, {
		name: "an extra port colliding with the primary one's name",
		spec: ServerPod{Port: 80, ExtraPorts: []ServerPort{{Name: primaryPortName, Port: 50051}}},
	}, {
		name: "an extra port colliding with the primary published port",
		spec: ServerPod{Port: 80, TargetPort: 8080, ExtraPorts: []ServerPort{{Name: "http-alt", Port: 80}}},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkServerPorts(serverPorts(tc.spec))
			if got := err == nil; got != tc.ok {
				t.Errorf("checkServerPorts accepted = %t, want %t (err = %v)", got, tc.ok, err)
			}
		})
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

// resetSharedServers isolates a test from the package-level memo, and restores
// it afterwards so the suite it belongs to is unaffected.
func resetSharedServers(t *testing.T) {
	t.Helper()
	sharedServersMu.Lock()
	saved := sharedServers
	sharedServers = map[string]*sharedServer{}
	sharedServersMu.Unlock()

	t.Cleanup(func() {
		sharedServersMu.Lock()
		sharedServers = saved
		sharedServersMu.Unlock()
	})
}

// TestSharedServerEntry covers the memo DeploySharedServerPod is built on. The
// deploy itself needs a cluster; which callers are handed the same entry does
// not, and it is the part that decides whether a suite shares one origin or
// silently stands up several.
func TestSharedServerEntry(t *testing.T) {
	resetSharedServers(t)

	spec := ServerPod{
		Name:       "egresshttp",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http"},
		Port:       80,
		TargetPort: 8080,
	}

	first, conflict := sharedServerEntry(spec)
	if conflict {
		t.Fatalf("the first call for %q reported a conflict", spec.Name)
	}

	// An equal spec built separately, which is what a suite's callers actually
	// pass: egressHTTPTarget() returns a fresh value every time, so matching on
	// identity rather than contents would deploy an origin per caller and
	// quietly undo the sharing.
	same := ServerPod{
		Name:       "egresshttp",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http"},
		Port:       80,
		TargetPort: 8080,
	}
	second, conflict := sharedServerEntry(same)
	if conflict {
		t.Errorf("an equal spec for %q reported a conflict", spec.Name)
	}
	if second != first {
		t.Errorf("an equal spec got a different entry, so its caller would deploy a second origin")
	}
}

// TestSharedServerEntryConflict covers the name collision. Handing the second
// caller the first one's origin would leave it dialing a server it never asked
// for, so the mismatch has to surface.
func TestSharedServerEntryConflict(t *testing.T) {
	resetSharedServers(t)

	spec := ServerPod{Name: "origin", ImportPath: "example.com/a", Port: 80}
	if _, conflict := sharedServerEntry(spec); conflict {
		t.Fatalf("the first call for %q reported a conflict", spec.Name)
	}

	for _, differs := range []ServerPod{
		{Name: "origin", ImportPath: "example.com/b", Port: 80},
		{Name: "origin", ImportPath: "example.com/a", Port: 8080},
		{Name: "origin", ImportPath: "example.com/a", Port: 80, Args: []string{"grpc"}},
	} {
		if _, conflict := sharedServerEntry(differs); !conflict {
			t.Errorf("spec %+v reused the entry for %+v without reporting a conflict", differs, spec)
		}
	}
}

// TestSharedServerEntryDistinctNames covers the ordinary case of a suite with
// more than one shared origin: different names are different servers.
func TestSharedServerEntryDistinctNames(t *testing.T) {
	resetSharedServers(t)

	http, conflict := sharedServerEntry(ServerPod{Name: "egresshttp", Port: 80})
	if conflict {
		t.Fatal("the first call for egresshttp reported a conflict")
	}
	grpc, conflict := sharedServerEntry(ServerPod{Name: "grpcecho", Port: 50051})
	if conflict {
		t.Fatal("grpcecho conflicted with egresshttp, which it does not share a name with")
	}
	if http == grpc {
		t.Error("two names got one entry, so one origin would answer for both")
	}
}
