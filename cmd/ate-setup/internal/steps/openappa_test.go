// Copyright 2026 The Agent Substrate Authors
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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
)

// The deploy step and the egress filter refer to OpenAPPA's objects by name
// and address. This checks them against the manifests, so a rename fails here
// rather than in the next install.
func TestOpenAPPAManifestAgrees(t *testing.T) {
	e := &Env{Cfg: &config.Config{Root: repoRoot(t)}}
	objs, err := kube.LoadPath(e.openAPPAPath(openAPPAManifest))
	if err != nil {
		t.Fatalf("loading the OpenAPPA manifest: %v", err)
	}
	for _, obj := range objs {
		if obj.GetNamespace() != NamespaceAteSystem {
			t.Errorf("%s is in namespace %q, want %q", kube.Describe(obj), obj.GetNamespace(), NamespaceAteSystem)
		}
	}
	dep := findObject(objs, "Deployment", openAPPADeployment)
	if dep == nil {
		t.Fatalf("manifest has no deployment/%s", openAPPADeployment)
	}
	if findObject(objs, "ConfigMap", openAPPAPolicyConfigMap) != nil {
		t.Errorf("manifest carries configmap/%s itself; the deploy step must own whether it is applied", openAPPAPolicyConfigMap)
	}
	if got := mountedConfigMaps(dep); len(got) != 1 || got[0] != openAPPAPolicyConfigMap {
		t.Errorf("deployment/%s mounts ConfigMaps %v, want only %q", openAPPADeployment, got, openAPPAPolicyConfigMap)
	}
	svc := findObject(objs, "Service", openAPPADeployment)
	if svc == nil {
		t.Fatalf("manifest has no service/%s", openAPPADeployment)
	}
	ports, _, _ := unstructured.NestedSlice(svc.Object, "spec", "ports")
	if len(ports) != 1 {
		t.Fatalf("service/%s has %d ports, want 1", openAPPADeployment, len(ports))
	}
	port, _ := asInt(ports[0].(map[string]any)["port"])
	if want := svc.GetNamespace() + "/" + svc.GetName() + ":" + strconv.FormatInt(port, 10); want != config.OpenAPPAExtprocService {
		t.Errorf("config.OpenAPPAExtprocService = %q, but the Service is %q", config.OpenAPPAExtprocService, want)
	}
	if got := containerPort(t, dep, "grpc"); got != port {
		t.Errorf("the extproc container serves on %d, but the Service forwards to %d", got, port)
	}

	// The processor admits only the egress gateway's identity.
	egress, err := kube.LoadPath(e.atenetEgressManifestPath())
	if err != nil {
		t.Fatalf("loading the egress manifest: %v", err)
	}
	gateway := findObject(egress, "Deployment", "atenet-egress")
	if gateway == nil {
		t.Fatal("egress manifest has no deployment/atenet-egress")
	}
	sa, _, _ := unstructured.NestedString(gateway.Object, "spec", "template", "spec", "serviceAccountName")
	wantID := "--tls-client-id=spiffe://cluster.local/ns/" + gateway.GetNamespace() + "/sa/" + sa
	if args := deploymentContainerArgs(t, dep, "extproc"); !slices.Contains(args, wantID) {
		t.Errorf("extproc args %v lack %s", args, wantID)
	}
}

func TestRenderOpenAPPA(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cfg         config.Config
		wantServers []string
	}{
		{
			name: "defaults",
			cfg: config.Config{
				OpenAPPAMCPServers:   []string{"api.githubcopilot.com=github"},
				OpenAPPARuntimeImage: config.OpenAPPARuntimeImage,
				OpenAPPAExtprocImage: config.OpenAPPAExtprocImage,
			},
			wantServers: []string{"--mcp-server=api.githubcopilot.com=github"},
		},
		{
			name: "overrides",
			cfg: config.Config{
				OpenAPPAMCPServers:   []string{"a.example.com=a", "b.example.com=b"},
				OpenAPPARuntimeImage: "example.com/runtime:v1",
				OpenAPPAExtprocImage: "example.com/extproc:v1",
			},
			wantServers: []string{"--mcp-server=a.example.com=a", "--mcp-server=b.example.com=b"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.Root = repoRoot(t)
			e := &Env{Cfg: &cfg}
			objs, err := e.renderOpenAPPA("policy")
			if err != nil {
				t.Fatalf("renderOpenAPPA: %v", err)
			}
			dep := findObject(objs, "Deployment", openAPPADeployment)
			if got := deploymentContainerImage(t, dep, "extproc"); got != cfg.OpenAPPAExtprocImage {
				t.Errorf("extproc image = %q, want %q", got, cfg.OpenAPPAExtprocImage)
			}
			if got := deploymentContainerImage(t, dep, "runtime"); got != cfg.OpenAPPARuntimeImage {
				t.Errorf("runtime image = %q, want %q", got, cfg.OpenAPPARuntimeImage)
			}
			var servers []string
			for _, arg := range deploymentContainerArgs(t, dep, "extproc") {
				if strings.HasPrefix(arg, "--mcp-server=") {
					servers = append(servers, arg)
				}
			}
			if !slices.Equal(servers, tc.wantServers) {
				t.Errorf("extproc --mcp-server args = %v, want %v", servers, tc.wantServers)
			}
		})
	}
}

// A changed policy changes the pod template, so the runtime restarts and
// reads it.
func TestRenderOpenAPPAStampsPolicyHash(t *testing.T) {
	e := &Env{Cfg: &config.Config{Root: repoRoot(t)}}
	hash := func(policy string) string {
		t.Helper()
		objs, err := e.renderOpenAPPA(policy)
		if err != nil {
			t.Fatalf("renderOpenAPPA: %v", err)
		}
		dep := findObject(objs, "Deployment", openAPPADeployment)
		got, _, _ := unstructured.NestedString(dep.Object, "spec", "template", "metadata", "annotations", openAPPAPolicyHashAnnotation)
		if got == "" {
			t.Fatalf("deployment/%s has no %s", openAPPADeployment, openAPPAPolicyHashAnnotation)
		}
		return got
	}
	if hash("a") == hash("b") {
		t.Error("two policies stamp the same digest")
	}
}

// The bundled policy fills a cluster that has none and never replaces one an
// operator edited. A policy file always replaces it.
func TestApplyOpenAPPAPolicy(t *testing.T) {
	root := repoRoot(t)
	bundled, err := os.ReadFile(filepath.Join(root, openAPPADir, openAPPAPolicy))
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "appa.toml")
	if err := os.WriteFile(file, []byte("from file"), 0o600); err != nil {
		t.Fatal(err)
	}
	edited := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: openAPPAPolicyConfigMap},
		Data:       map[string]string{openAPPAPolicy: "edited"},
	}
	for _, tc := range []struct {
		name     string
		existing runtime.Object
		policy   string
		want     string
	}{
		{name: "fresh install gets the bundled policy", want: string(bundled)},
		{name: "redeploy keeps an edited policy", existing: edited, want: "edited"},
		{name: "fresh install with a file", policy: file, want: "from file"},
		{name: "a file replaces an edited policy", existing: edited.DeepCopy(), policy: file, want: "from file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var existing []runtime.Object
			if tc.existing != nil {
				existing = append(existing, tc.existing)
			}
			// NewClientset rather than fakeKube: only it supports apply.
			k := &kube.Client{Typed: fake.NewClientset(existing...)}
			e := &Env{Cfg: &config.Config{Root: root, OpenAPPAPolicy: tc.policy}, Kube: k}
			got, err := e.applyOpenAPPAPolicy(context.Background())
			if err != nil {
				t.Fatalf("applyOpenAPPAPolicy: %v", err)
			}
			if got != tc.want {
				t.Errorf("applyOpenAPPAPolicy() = %q, want %q", got, tc.want)
			}
			cm, err := e.Kube.GetConfigMap(context.Background(), NamespaceAteSystem, openAPPAPolicyConfigMap)
			if err != nil || cm == nil {
				t.Fatalf("GetConfigMap = %v, %v", cm, err)
			}
			if cm.Data[openAPPAPolicy] != tc.want {
				t.Errorf("configmap/%s holds %q, want %q", openAPPAPolicyConfigMap, cm.Data[openAPPAPolicy], tc.want)
			}
		})
	}
}

// A redeploy without --openappa removes what an earlier one deployed, and
// keeps the operator's policy.
func TestRemoveOpenAPPA(t *testing.T) {
	services := schema.GroupVersionResource{Version: "v1", Resource: "services"}
	for _, tc := range []struct {
		name    string
		running bool
	}{
		{name: "earlier install had it", running: true},
		{name: "nothing to remove"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: openAPPAPolicyConfigMap}}
			typed := []runtime.Object{policy}
			var dyn []runtime.Object
			if tc.running {
				typed = append(typed, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: openAPPADeployment}})
				dyn = append(dyn, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: openAPPADeployment}})
			}
			k, dc := fakeDeleteKube(t, typed, dyn)
			e := &Env{Cfg: &config.Config{Root: repoRoot(t)}, Kube: k}
			if err := e.removeOpenAPPA(context.Background()); err != nil {
				t.Fatalf("removeOpenAPPA() = %v", err)
			}
			if tc.running {
				_, err := dc.Resource(services).Namespace(NamespaceAteSystem).Get(context.Background(), openAPPADeployment, metav1.GetOptions{})
				if !apierrors.IsNotFound(err) {
					t.Errorf("service/%s survived removeOpenAPPA (err %v)", openAPPADeployment, err)
				}
			}
			if cm, err := k.GetConfigMap(context.Background(), NamespaceAteSystem, openAPPAPolicyConfigMap); err != nil || cm == nil {
				t.Errorf("configmap/%s is gone after removeOpenAPPA (err %v)", openAPPAPolicyConfigMap, err)
			}
		})
	}
}

// deploymentContainerArgs returns the args of a Deployment's named container.
func deploymentContainerArgs(t *testing.T, dep *unstructured.Unstructured, name string) []string {
	t.Helper()
	args, _, _ := unstructured.NestedStringSlice(deploymentContainer(t, dep, name), "args")
	return args
}

// deploymentContainerImage returns the image of a Deployment's named container.
func deploymentContainerImage(t *testing.T, dep *unstructured.Unstructured, name string) string {
	t.Helper()
	image, _, _ := unstructured.NestedString(deploymentContainer(t, dep, name), "image")
	return image
}

func deploymentContainer(t *testing.T, dep *unstructured.Unstructured, name string) map[string]any {
	t.Helper()
	containers, _, _ := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
	for _, c := range containers {
		if c.(map[string]any)["name"] == name {
			return c.(map[string]any)
		}
	}
	t.Fatalf("deployment/%s has no container %q", dep.GetName(), name)
	return nil
}
