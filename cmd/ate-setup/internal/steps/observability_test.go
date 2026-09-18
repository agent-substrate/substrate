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
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// testEnv builds an Env whose manifests come from the checkout and whose
// cluster holds the given objects.
func testEnv(t *testing.T, cfg *config.Config, objects ...runtime.Object) *Env {
	t.Helper()
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}
	cfg.Root = root
	typed := fake.NewSimpleClientset(objects...)
	return &Env{Cfg: cfg, Kube: &kube.Client{Typed: typed}}
}

// otelConfigMap is the ate-otel-config ConfigMap of a cluster that was
// installed with the given mode and endpoint.
func otelConfigMap(mode, endpoint string) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: otelConfigMapName, Namespace: NamespaceAteSystem},
		Data:       map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint},
	}
	if mode != "" {
		cm.Annotations = map[string]string{otelModeAnnotation: mode}
	}
	return cm
}

func namespace(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func service(namespace, name string) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
}

// A mode that no flag gives comes from the cluster, thus a deploy of one
// component keeps the collector that the cluster has.
func TestResolveObservability(t *testing.T) {
	gkeEndpoint := endpointInFileForTest(t, config.ObservabilityGKE)
	kindEndpoint := endpointInFileForTest(t, config.ObservabilityKind)

	tests := []struct {
		name         string
		cfg          config.Config
		objects      []runtime.Object
		wantMode     string
		wantEndpoint string
	}{{
		name:     "no flag and no cluster gives none",
		wantMode: config.ObservabilityNone,
	}, {
		name:         "a kind install gives kind",
		cfg:          config.Config{Kind: true},
		wantMode:     config.ObservabilityKind,
		wantEndpoint: kindEndpoint,
	}, {
		name:         "the endpoint flag gives otlp",
		cfg:          config.Config{OtlpEndpoint: "http://collector.obs.svc:4317"},
		wantMode:     config.ObservabilityOTLP,
		wantEndpoint: "http://collector.obs.svc:4317",
	}, {
		name:         "the mode flag wins over the cluster",
		cfg:          config.Config{Observability: config.ObservabilityNone},
		objects:      []runtime.Object{otelConfigMap(config.ObservabilityGKE, gkeEndpoint)},
		wantMode:     config.ObservabilityNone,
		wantEndpoint: "",
	}, {
		name:         "no flag keeps the mode of the cluster",
		objects:      []runtime.Object{otelConfigMap(config.ObservabilityGKE, gkeEndpoint)},
		wantMode:     config.ObservabilityGKE,
		wantEndpoint: gkeEndpoint,
	}, {
		name:         "no flag keeps the endpoint of an otlp cluster",
		objects:      []runtime.Object{otelConfigMap(config.ObservabilityOTLP, "http://collector.obs.svc:4317")},
		wantMode:     config.ObservabilityOTLP,
		wantEndpoint: "http://collector.obs.svc:4317",
	}, {
		// An install that came before the modes has no annotation, thus the
		// endpoint is the only evidence of the collector it uses.
		name:         "a ConfigMap with no annotation keeps its collector",
		objects:      []runtime.Object{otelConfigMap("", gkeEndpoint)},
		wantMode:     config.ObservabilityGKE,
		wantEndpoint: gkeEndpoint,
	}, {
		name:         "a ConfigMap with no annotation and an unknown endpoint is otlp",
		objects:      []runtime.Object{otelConfigMap("", "http://collector.obs.svc:4317")},
		wantMode:     config.ObservabilityOTLP,
		wantEndpoint: "http://collector.obs.svc:4317",
	}, {
		name:     "a ConfigMap with no endpoint is none",
		objects:  []runtime.Object{otelConfigMap("", "")},
		wantMode: config.ObservabilityNone,
	}, {
		// A first install of a cluster that has the addon uses it. The mode
		// names the Service that this test finds, thus it cannot select a
		// collector that is absent.
		name: "the addon gives gke on a first install",
		objects: []runtime.Object{
			namespace(gkeOtelNamespace),
			service(gkeOtelNamespace, "opentelemetry-collector"),
		},
		wantMode:     config.ObservabilityGKE,
		wantEndpoint: gkeEndpoint,
	}, {
		name:     "the namespace alone does not give gke",
		objects:  []runtime.Object{namespace(gkeOtelNamespace)},
		wantMode: config.ObservabilityNone,
	}, {
		name: "the mode of the cluster wins over the addon",
		objects: []runtime.Object{
			otelConfigMap(config.ObservabilityNone, ""),
			namespace(gkeOtelNamespace),
			service(gkeOtelNamespace, "opentelemetry-collector"),
		},
		wantMode: config.ObservabilityNone,
	}, {
		name: "a kind install does not use the addon",
		cfg:  config.Config{Kind: true},
		objects: []runtime.Object{
			namespace(gkeOtelNamespace),
			service(gkeOtelNamespace, "opentelemetry-collector"),
		},
		wantMode:     config.ObservabilityKind,
		wantEndpoint: kindEndpoint,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := testEnv(t, &tc.cfg, tc.objects...)
			got, err := e.ResolveObservability(context.Background())
			if err != nil {
				t.Fatalf("ResolveObservability: %v", err)
			}
			if got.mode != tc.wantMode {
				t.Errorf("mode = %q, want %q", got.mode, tc.wantMode)
			}
			if got.endpoint != tc.wantEndpoint {
				t.Errorf("endpoint = %q, want %q", got.endpoint, tc.wantEndpoint)
			}
		})
	}
}

// The ConfigMap has more than one writer: hack/install-ate.sh patches the
// collector of a measurement into it and leaves the annotation of the mode
// before it. The endpoint is what the components read, thus the install must
// keep it and not put the collector of the stale annotation back over it.
func TestResolveObservabilityEndpointWinsOverStaleAnnotation(t *testing.T) {
	const meter = "http://telemetry-meter.benchmarking.svc:4317"
	gkeEndpoint := endpointInFileForTest(t, config.ObservabilityGKE)

	tests := []struct {
		name         string
		annotation   string
		endpoint     string
		wantMode     string
		wantEndpoint string
	}{{
		name:         "an annotation that agrees with the endpoint stays",
		annotation:   config.ObservabilityGKE,
		endpoint:     gkeEndpoint,
		wantMode:     config.ObservabilityGKE,
		wantEndpoint: gkeEndpoint,
	}, {
		name:         "a patched endpoint wins over the annotation of mode gke",
		annotation:   config.ObservabilityGKE,
		endpoint:     meter,
		wantMode:     config.ObservabilityOTLP,
		wantEndpoint: meter,
	}, {
		name:         "a patched endpoint wins over the annotation of mode none",
		annotation:   config.ObservabilityNone,
		endpoint:     meter,
		wantMode:     config.ObservabilityOTLP,
		wantEndpoint: meter,
	}, {
		// No manifest holds the address of mode otlp, thus the ConfigMap is its
		// only source and the two cannot disagree.
		name:         "mode otlp keeps its own address",
		annotation:   config.ObservabilityOTLP,
		endpoint:     meter,
		wantMode:     config.ObservabilityOTLP,
		wantEndpoint: meter,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := testEnv(t, &config.Config{}, otelConfigMap(tc.annotation, tc.endpoint))
			got, err := e.ResolveObservability(context.Background())
			if err != nil {
				t.Fatalf("ResolveObservability: %v", err)
			}
			if got.mode != tc.wantMode {
				t.Errorf("mode = %q, want %q", got.mode, tc.wantMode)
			}
			if got.endpoint != tc.wantEndpoint {
				t.Errorf("endpoint = %q, want %q", got.endpoint, tc.wantEndpoint)
			}
		})
	}
}

// Mode otlp takes the file of mode none, thus the endpoint and the two exporter
// switches in it must carry the given collector and not the empty default.
func TestRenderOtelConfigOTLP(t *testing.T) {
	e := testEnv(t, &config.Config{OtlpEndpoint: "http://collector.obs.svc:4317"})
	got, err := e.ResolveObservability(context.Background())
	if err != nil {
		t.Fatalf("ResolveObservability: %v", err)
	}

	data, _, err := unstructured.NestedStringMap(got.obj.Object, "data")
	if err != nil {
		t.Fatalf("reading data: %v", err)
	}
	for key, want := range map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.obs.svc:4317",
		"OTEL_TRACES_EXPORTER":        "otlp",
		"OTEL_METRICS_EXPORTER":       "otlp",
	} {
		if data[key] != want {
			t.Errorf("data[%s] = %q, want %q", key, data[key], want)
		}
	}
	if got := got.obj.GetAnnotations()[otelModeAnnotation]; got != config.ObservabilityOTLP {
		t.Errorf("%s = %q, want %s", otelModeAnnotation, got, config.ObservabilityOTLP)
	}
}

// An absent collector must stop the install: it presents as a fault of the
// network, and not as an absent dependency.
func TestPreflightObservability(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.Config
		objects []runtime.Object
		wantErr string
	}{{
		name:    "gke with no addon namespace fails",
		cfg:     config.Config{Observability: config.ObservabilityGKE},
		wantErr: "there is no namespace " + gkeOtelNamespace,
	}, {
		name:    "gke with the namespace but no Service fails",
		cfg:     config.Config{Observability: config.ObservabilityGKE},
		objects: []runtime.Object{namespace(gkeOtelNamespace)},
		wantErr: "there is no Service opentelemetry-collector",
	}, {
		name: "gke with the addon passes",
		cfg:  config.Config{Observability: config.ObservabilityGKE},
		objects: []runtime.Object{
			namespace(gkeOtelNamespace),
			service(gkeOtelNamespace, "opentelemetry-collector"),
		},
	}, {
		name:    "otlp with an absent in-cluster collector fails",
		cfg:     config.Config{OtlpEndpoint: "http://collector.obs.svc:4317"},
		wantErr: "there is no namespace obs",
	}, {
		// The installer cannot test an address outside the cluster.
		name: "otlp outside the cluster passes",
		cfg:  config.Config{OtlpEndpoint: "http://collector.example.com:4317"},
	}, {
		name: "none passes with no collector",
		cfg:  config.Config{Observability: config.ObservabilityNone},
	}, {
		// The collector of mode kind is in the bundle of the same install, thus
		// it does not exist before the install applies it.
		name: "kind passes before its collector exists",
		cfg:  config.Config{Kind: true},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := testEnv(t, &tc.cfg, tc.objects...)
			otel, err := e.ResolveObservability(context.Background())
			if err != nil {
				t.Fatalf("ResolveObservability: %v", err)
			}
			err = e.preflightObservability(context.Background(), otel)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("preflightObservability: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("preflightObservability succeeded, want an error with %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("error = %v, want one with %q", err, tc.wantErr)
			}
		})
	}
}

// A restart is for a collector that changed only: it rolls every WorkerPool,
// which replaces the running actors.
func TestNoteOtelConfigChange(t *testing.T) {
	gkeEndpoint := endpointInFileForTest(t, config.ObservabilityGKE)

	tests := []struct {
		name        string
		objects     []runtime.Object
		wantChanged bool
	}{{
		name:        "a first install changes nothing",
		wantChanged: false,
	}, {
		name:        "the same collector changes nothing",
		objects:     []runtime.Object{otelConfigMap(config.ObservabilityNone, "")},
		wantChanged: false,
	}, {
		name:        "a different collector is a change",
		objects:     []runtime.Object{otelConfigMap(config.ObservabilityGKE, gkeEndpoint)},
		wantChanged: true,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := testEnv(t, &config.Config{Observability: config.ObservabilityNone}, tc.objects...)
			otel, err := e.ResolveObservability(context.Background())
			if err != nil {
				t.Fatalf("ResolveObservability: %v", err)
			}
			e.noteOtelConfigChange(context.Background(), otel)
			if e.observability.changed != tc.wantChanged {
				t.Errorf("changed = %v, want %v", e.observability.changed, tc.wantChanged)
			}
			// A second deploy target in the same command must not find the
			// change again and restart the workloads a second time.
			e.observability.changed = false
			e.noteOtelConfigChange(context.Background(), otel)
			if e.observability.changed {
				t.Error("the second apply of the same ConfigMap reported a change")
			}
		})
	}
}

func TestEndpointService(t *testing.T) {
	tests := []struct {
		endpoint      string
		wantNamespace string
		wantService   string
	}{
		{"http://collector.obs.svc:4317", "obs", "collector"},
		{"http://collector.obs.svc.cluster.local:4317", "obs", "collector"},
		{"https://collector.obs.svc.cluster.local/v1/traces", "obs", "collector"},
		{"collector.obs.svc:4317", "obs", "collector"},
		{"http://collector.example.com:4317", "", ""},
		{"http://localhost:4317", "", ""},
		{"", "", ""},
	}
	for _, tc := range tests {
		gotNamespace, gotService := endpointService(tc.endpoint)
		if gotNamespace != tc.wantNamespace || gotService != tc.wantService {
			t.Errorf("endpointService(%q) = %q, %q, want %q, %q",
				tc.endpoint, gotNamespace, gotService, tc.wantNamespace, tc.wantService)
		}
	}
}

// endpointInFileForTest reads the endpoint of a mode from its manifest, thus
// the test asserts against the file and not against a copy of the address.
func endpointInFileForTest(t *testing.T, mode string) string {
	t.Helper()
	e := testEnv(t, &config.Config{})
	endpoint := endpointInFile(e.otelConfigPath(mode))
	if endpoint == "" {
		t.Fatalf("no endpoint in the manifest of mode %s", mode)
	}
	return endpoint
}

// readsOtelConfig reports whether a workload takes the ate-otel-config
// ConfigMap through envFrom on one of its containers.
func readsOtelConfig(obj *unstructured.Unstructured) bool {
	for _, field := range []string{"containers", "initContainers"} {
		containers, _, _ := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", field)
		for _, c := range containers {
			container, ok := c.(map[string]any)
			if !ok {
				continue
			}
			sources, _, _ := unstructured.NestedSlice(container, "envFrom")
			for _, s := range sources {
				source, ok := s.(map[string]any)
				if !ok {
					continue
				}
				name, _, _ := unstructured.NestedString(source, "configMapRef", "name")
				if name == otelConfigMapName {
					return true
				}
			}
		}
	}
	return false
}

// A workload that reads the ConfigMap and is absent from the restart keeps the
// collector of the install before it, with no message. Thus the list must
// follow the manifests, and this test fails when a new consumer arrives.
func TestOtelConsumersMatchTheManifests(t *testing.T) {
	e := testEnv(t, &config.Config{})
	objs, err := kube.LoadPath(e.Cfg.Manifest())
	if err != nil {
		t.Fatalf("loading the manifests: %v", err)
	}

	deployments := map[string]bool{}
	others := map[string]string{}
	for _, obj := range objs {
		if !readsOtelConfig(obj) {
			continue
		}
		if obj.GetKind() == "Deployment" {
			deployments[obj.GetName()] = true
			continue
		}
		others[obj.GetKind()] = obj.GetName()
	}
	if len(deployments) == 0 {
		t.Fatal("no Deployment in the manifests reads the ConfigMap; the scan is broken")
	}

	for name := range deployments {
		if !slices.Contains(otelConsumerDeployments, name) {
			t.Errorf("Deployment %s reads %s and is absent from otelConsumerDeployments", name, otelConfigMapName)
		}
	}
	for _, name := range otelConsumerDeployments {
		if !deployments[name] {
			t.Errorf("otelConsumerDeployments names %s, which reads no %s in the manifests", name, otelConfigMapName)
		}
	}

	// The DaemonSets take the restart by label, thus they are correct outside
	// the list. Each other kind would take a restart call of its own.
	delete(others, "DaemonSet")
	for kind, name := range others {
		t.Errorf("%s %s reads %s and no restart covers it", kind, name, otelConfigMapName)
	}
}

// restartedAtAnnotation is what a rollout restart stamps on a pod template; see
// kube.RolloutRestart.
const restartedAtAnnotation = "kubectl.kubernetes.io/restartedAt"

// The name of an atelet DaemonSet carries the version suffix of the install
// that made it, thus a restart by the bare name reaches nothing and the node
// keeps the collector of the install before it, with no message.
func TestRestartOtelConsumersReachesEachWorkload(t *testing.T) {
	deployment := func(name string) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: NamespaceAteSystem},
		}
	}
	daemonSet := func(name string) *appsv1.DaemonSet {
		return &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: NamespaceAteSystem,
			Labels:    map[string]string{"app": "atelet"},
		}}
	}

	objects := []runtime.Object{
		// Two versions, as a cluster in a rolling upgrade holds.
		daemonSet("atelet-v1-2-3"), daemonSet("atelet-v1-2-4"),
	}
	for _, name := range otelConsumerDeployments {
		objects = append(objects, deployment(name))
	}

	e := testEnv(t, &config.Config{Observability: config.ObservabilityNone}, objects...)
	e.observability.changed = true
	if err := e.restartOtelConsumers(context.Background()); err != nil {
		t.Fatalf("restartOtelConsumers: %v", err)
	}

	ctx := context.Background()
	for _, name := range otelConsumerDeployments {
		got, err := e.Kube.Typed.AppsV1().Deployments(NamespaceAteSystem).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("reading deployment %s: %v", name, err)
		}
		if got.Spec.Template.Annotations[restartedAtAnnotation] == "" {
			t.Errorf("deployment %s was not restarted", name)
		}
	}
	for _, name := range []string{"atelet-v1-2-3", "atelet-v1-2-4"} {
		got, err := e.Kube.Typed.AppsV1().DaemonSets(NamespaceAteSystem).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("reading daemonset %s: %v", name, err)
		}
		if got.Spec.Template.Annotations[restartedAtAnnotation] == "" {
			t.Errorf("daemonset %s was not restarted", name)
		}
	}
}

// The shortened metric export tick is a property of a kind cluster, and not of
// its collector: a component is invisible until its first tick, and the metrics
// e2e suite asserts on a bounded deadline. Thus a kind cluster that names its
// own collector keeps the tick, and each other install keeps the SDK default.
func TestRenderOtelConfigKeepsTheKindExportInterval(t *testing.T) {
	const key = "OTEL_METRIC_EXPORT_INTERVAL"
	kindValue := func(t *testing.T) string {
		t.Helper()
		e := testEnv(t, &config.Config{})
		objs, err := kube.LoadPath(e.otelConfigPath(config.ObservabilityKind))
		if err != nil || len(objs) != 1 {
			t.Fatalf("loading the ConfigMap of mode kind: %v", err)
		}
		value, _, _ := unstructured.NestedString(objs[0].Object, "data", key)
		if value == "" {
			t.Fatalf("the ConfigMap of mode kind names no %s", key)
		}
		return value
	}(t)

	tests := []struct {
		name  string
		cfg   config.Config
		want  string
		wantK string
	}{{
		name:  "mode otlp on a kind install keeps the tick",
		cfg:   config.Config{Kind: true, OtlpEndpoint: "http://collector.obs.svc:4317"},
		want:  kindValue,
		wantK: "http://collector.obs.svc:4317",
	}, {
		name:  "mode otlp on each other install keeps the SDK default",
		cfg:   config.Config{OtlpEndpoint: "http://collector.obs.svc:4317"},
		want:  "",
		wantK: "http://collector.obs.svc:4317",
	}, {
		name:  "mode kind keeps the tick",
		cfg:   config.Config{Kind: true},
		want:  kindValue,
		wantK: endpointInFileForTest(t, config.ObservabilityKind),
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := testEnv(t, &tc.cfg)
			got, err := e.ResolveObservability(context.Background())
			if err != nil {
				t.Fatalf("ResolveObservability: %v", err)
			}
			interval, _, _ := unstructured.NestedString(got.obj.Object, "data", key)
			if interval != tc.want {
				t.Errorf("data[%s] = %q, want %q", key, interval, tc.want)
			}
			if got.endpoint != tc.wantK {
				t.Errorf("endpoint = %q, want %q", got.endpoint, tc.wantK)
			}
		})
	}
}
