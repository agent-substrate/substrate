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

package chartcompare

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	// Registers the settings of the deploy commands.
	_ "go.ate.dev/substrate/cmd/ate-setup/internal/cmd"
	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/steps"
	"go.ate.dev/substrate/internal/testenv"
)

const deployAteSystem = "deploy ate-system"

var updateDifferences = flag.Bool("update-differences", false, "write the differences that each scenario has to its differences.yaml")

// excluded reports whether the comparison leaves an object out. These are the
// objects that ate-setup creates in code, which the charts get in phase 2, the
// bundled PostgreSQL and RustFS, and the test fixtures of the kind overlay.
func excluded(kind, namespace, name string) bool {
	switch kind + "/" + namespace + "/" + name {
	case "Secret/ate-system/actor-id-ca-certs",
		"Secret/ate-system/actor-id-ca-pool",
		"Secret/ate-system/actor-id-jwt-pool",
		"Secret/ate-system/ate-api-server-secret-envvars",
		"Secret/ate-system/egress-mitm-ca-pool",
		"Secret/podcertificate-controller-system/pod-identity-ca-pool",
		"Secret/podcertificate-controller-system/postgres-ca-pool",
		"Secret/podcertificate-controller-system/service-dns-ca-pool",
		"ConfigMap/ate-system/ate-api-config",
		"ConfigMap/ate-system/ate-api-authentication",
		"ConfigMap/ate-system/ate-api-server-envvars",
		"ConfigMap/ate-system/k8s-credential-provider-namespace-policy",
		"ClusterRole//ate-prometheus",
		"RoleBinding/ate-system/ate-prometheus":
		return true
	}
	switch {
	case kind == "IPAddress":
		// The API server allocates the Service IPs at random.
		return true
	case namespace == "otel-system", kind == "Namespace" && name == "otel-system":
		return true
	case namespace == "ate-system" && (name == "postgres" || strings.HasPrefix(name, "postgres-") || strings.HasPrefix(name, "rustfs")):
		return true
	}
	return false
}

// enablePodCertificates turns on the APIs that the pod certificate volumes
// use. Without them the API server drops those volumes from both installs.
func enablePodCertificates(env *envtest.Environment) {
	env.ControlPlane.GetAPIServer().Configure().
		Append("feature-gates", "ClusterTrustBundle=true,ClusterTrustBundleProjection=true,PodCertificateRequest=true").
		Append("runtime-config", "certificates.k8s.io/v1beta1=true")
}

// TestChartsMatchAteSetup installs each scenario that has an ate-setup-flags
// file twice, each time on its own envtest API server: once with
// `ate-setup deploy ate-system` and those flags, and once from the scenario's
// rendered.yaml. It then compares the objects that each install created.
func TestChartsMatchAteSetup(t *testing.T) {
	if testing.Short() {
		t.Skip("starts envtest API servers")
	}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	flagFiles, err := filepath.Glob(filepath.Join(root, "charts", "scenarios", "*", "ate-setup-flags"))
	if err != nil {
		t.Fatal(err)
	}
	if len(flagFiles) == 0 {
		t.Fatal("no scenario in charts/scenarios has an ate-setup-flags file")
	}
	for _, flagFile := range flagFiles {
		dir := filepath.Dir(flagFile)
		t.Run(filepath.Base(dir), func(t *testing.T) {
			setup := installWithAteSetup(t, flagFile)
			charts := installCharts(t, filepath.Join(dir, "rendered.yaml"))
			got := differences(t, setup, charts)
			path := filepath.Join(dir, "differences.yaml")
			if *updateDifferences {
				writeDifferences(t, path, got)
				return
			}
			want := readDifferences(t, path)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("the installs differ other than %s lists (-listed +found). Fix the charts, or run this test with -update-differences and review the change:\n%s", path, diff)
			}
		})
	}
}

func installWithAteSetup(t *testing.T, flagFile string) map[string]map[string]any {
	t.Helper()
	fs := pflag.NewFlagSet("ate-setup", pflag.ContinueOnError)
	config.BindFlags(fs)
	config.BindCommandFlags(deployAteSystem, fs)
	if err := fs.Parse(readFlags(t, flagFile)); err != nil {
		t.Fatalf("%s: %v", flagFile, err)
	}
	cfg, err := config.LoadFlags(fs, config.LoadOptions{NoDevEnv: true})
	if err != nil {
		t.Fatal(err)
	}
	restCfg, client, baseline := startAPIServer(t)
	r := cfg.Resolved()
	env := &steps.Env{Cfg: cfg, Kube: client}
	opts := steps.DeployOptions{SetupCSI: r.String("csi.setup"), SandboxClasses: r.String("sandboxClasses")}
	if err := env.DeployAteSystem(t.Context(), opts); err != nil {
		t.Fatalf("ate-setup deploy ate-system: %v", err)
	}
	return installed(t, restCfg, baseline)
}

func installCharts(t *testing.T, rendered string) map[string]map[string]any {
	t.Helper()
	objs, err := kube.LoadPath(rendered)
	if err != nil {
		t.Fatal(err)
	}
	restCfg, client, baseline := startAPIServer(t)
	// helm install --create-namespace creates the release namespaces.
	var namespaces []string
	for _, obj := range objs {
		if ns := obj.GetNamespace(); ns != "" && !slices.Contains(namespaces, ns) {
			namespaces = append(namespaces, ns)
		}
	}
	for _, ns := range namespaces {
		if err := client.EnsureNamespace(t.Context(), ns); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Apply(t.Context(), objs); err != nil {
		t.Fatalf("applying %s: %v", rendered, err)
	}
	return installed(t, restCfg, baseline)
}

// readFlags reads one flag for each line, and skips empty lines and comments.
func readFlags(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var flags []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			flags = append(flags, line)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return flags
}

// startAPIServer starts an envtest API server with the repo CRDs. It returns
// a client for it and the keys of the objects that it holds before an install.
func startAPIServer(t *testing.T) (*rest.Config, *kube.Client, map[string]bool) {
	t.Helper()
	cfg, stop := testenv.Start(enablePodCertificates)
	t.Cleanup(stop)
	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	disco, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client := kube.NewFromClients(cfg, typed, dyn, disco)
	client.APIServerOnly = true
	baseline := map[string]bool{}
	for key := range snapshot(t, cfg) {
		baseline[key] = true
	}
	return cfg, client, baseline
}

// snapshot lists every object on the API server, keyed by kind/namespace/name.
func snapshot(t *testing.T, cfg *rest.Config) map[string]*unstructured.Unstructured {
	t.Helper()
	disco, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	lists, err := disco.ServerPreferredResources()
	if err != nil && !discovery.IsGroupDiscoveryFailedError(err) {
		t.Fatal(err)
	}
	objs := map[string]*unstructured.Unstructured{}
	for _, list := range lists {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			t.Fatal(err)
		}
		for _, res := range list.APIResources {
			if strings.Contains(res.Name, "/") || !slices.Contains(res.Verbs, "list") {
				continue
			}
			items, err := dyn.Resource(gv.WithResource(res.Name)).List(t.Context(), metav1.ListOptions{})
			if err != nil {
				t.Fatalf("listing %s: %v", res.Name, err)
			}
			for i := range items.Items {
				obj := &items.Items[i]
				objs[key(obj)] = obj
			}
		}
	}
	return objs
}

func key(obj *unstructured.Unstructured) string {
	return obj.GetKind() + "/" + obj.GetNamespace() + "/" + obj.GetName()
}

// installed returns the normalized objects that an install created.
func installed(t *testing.T, cfg *rest.Config, baseline map[string]bool) map[string]map[string]any {
	t.Helper()
	objs := map[string]map[string]any{}
	for k, obj := range snapshot(t, cfg) {
		if !baseline[k] && !excluded(obj.GetKind(), obj.GetNamespace(), obj.GetName()) {
			objs[k] = normalize(obj)
		}
	}
	return objs
}

// digest matches the digest that ate-setup pins on each image reference.
var digest = regexp.MustCompile(`@sha256:[0-9a-f]{64}`)

// normalize removes the fields that the API server sets, the labels and
// annotations that Helm adds, and the image digests that ate-setup adds. It
// removes the pod template annotations that roll pods when an excluded
// configuration object changes. It also sorts each list of flags and each list
// of named items, such as volumes and env, because their order does not change
// what gets installed. The order of env can matter to a $(VAR) reference, which
// this does not check.
func normalize(obj *unstructured.Unstructured) map[string]any {
	obj = obj.DeepCopy()
	delete(obj.Object, "status")
	for _, field := range []string{"uid", "resourceVersion", "creationTimestamp", "generation", "managedFields"} {
		unstructured.RemoveNestedField(obj.Object, "metadata", field)
	}
	if obj.GetKind() == "Service" {
		unstructured.RemoveNestedField(obj.Object, "spec", "clusterIP")
		unstructured.RemoveNestedField(obj.Object, "spec", "clusterIPs")
	}
	if annotations, ok, _ := unstructured.NestedStringMap(obj.Object, "spec", "template", "metadata", "annotations"); ok {
		for k := range annotations {
			if k == "ate.dev/env-hash" || strings.HasPrefix(k, "checksum/") {
				delete(annotations, k)
			}
		}
		if len(annotations) == 0 {
			unstructured.RemoveNestedField(obj.Object, "spec", "template", "metadata", "annotations")
		} else if err := unstructured.SetNestedStringMap(obj.Object, annotations, "spec", "template", "metadata", "annotations"); err != nil {
			panic(err)
		}
	}
	sortFlags(obj.Object)
	obj.SetLabels(withoutHelmKeys(obj.GetLabels()))
	obj.SetAnnotations(withoutHelmKeys(obj.GetAnnotations()))
	return withoutDigests(obj.Object).(map[string]any)
}

// sortFlags sorts each args list whose items are all flags, and each list
// whose items all have a name.
func sortFlags(v any) {
	switch v := v.(type) {
	case map[string]any:
		for k, item := range v {
			if args, ok := item.([]any); ok && k == "args" && allFlags(args) {
				slices.SortFunc(args, func(a, b any) int { return strings.Compare(a.(string), b.(string)) })
			}
			sortFlags(item)
		}
	case []any:
		if allNamed(v) {
			slices.SortStableFunc(v, func(a, b any) int {
				return strings.Compare(a.(map[string]any)["name"].(string), b.(map[string]any)["name"].(string))
			})
		}
		for _, item := range v {
			sortFlags(item)
		}
	}
}

func allNamed(items []any) bool {
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			return false
		}
		if _, ok := m["name"].(string); !ok {
			return false
		}
	}
	return len(items) > 0
}

func allFlags(args []any) bool {
	for _, arg := range args {
		if s, ok := arg.(string); !ok || !strings.HasPrefix(s, "--") {
			return false
		}
	}
	return true
}

func withoutDigests(v any) any {
	switch v := v.(type) {
	case map[string]any:
		for k, item := range v {
			v[k] = withoutDigests(item)
		}
	case []any:
		for i, item := range v {
			v[i] = withoutDigests(item)
		}
	case string:
		return digest.ReplaceAllString(v, "")
	}
	return v
}

func withoutHelmKeys(m map[string]string) map[string]string {
	for k, v := range m {
		if strings.HasPrefix(k, "helm.sh/") || strings.HasPrefix(k, "meta.helm.sh/") || (k == "app.kubernetes.io/managed-by" && v == "Helm") {
			delete(m, k)
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// differences returns, for each object that the installs created
// differently, each field path that differs with the value from each install.
// An object that only one install created maps to "only ate-setup" or "only
// charts". The result is in the form that readDifferences returns.
func differences(t *testing.T, setup, charts map[string]map[string]any) map[string]any {
	t.Helper()
	found := map[string]any{}
	for k, a := range setup {
		b, ok := charts[k]
		if !ok {
			found[k] = "only ate-setup"
			continue
		}
		fields := map[string]any{}
		diffPaths("", a, b, fields)
		if len(fields) > 0 {
			found[k] = fields
		}
	}
	for k := range charts {
		if _, ok := setup[k]; !ok {
			found[k] = "only charts"
		}
	}
	// A round trip through YAML gives the number types that readDifferences
	// gives.
	data, err := yaml.Marshal(found)
	if err != nil {
		t.Fatal(err)
	}
	found = map[string]any{}
	if err := yaml.Unmarshal(data, &found); err != nil {
		t.Fatal(err)
	}
	return found
}

// diffPaths adds each path at which a and b differ, with both values. It goes
// into maps, and into lists of the same length. A list that changes length
// differs as a whole.
func diffPaths(path string, a, b any, fields map[string]any) {
	switch a := a.(type) {
	case map[string]any:
		if b, ok := b.(map[string]any); ok {
			keys := map[string]bool{}
			for k := range a {
				keys[k] = true
			}
			for k := range b {
				keys[k] = true
			}
			for k := range keys {
				diffPaths(strings.TrimPrefix(path+"."+k, "."), a[k], b[k], fields)
			}
			return
		}
	case []any:
		if b, ok := b.([]any); ok && len(a) == len(b) {
			for i := range a {
				diffPaths(fmt.Sprintf("%s[%d]", path, i), a[i], b[i], fields)
			}
			return
		}
	}
	if !cmp.Equal(a, b) {
		fields[path] = map[string]any{"ate-setup": a, "charts": b}
	}
}

func readDifferences(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]any{}
	}
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]any{}
	if err := yaml.Unmarshal(data, &listed); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return listed
}

func writeDifferences(t *testing.T, path string, found map[string]any) {
	t.Helper()
	header, err := os.ReadFile(filepath.Join(filepath.Dir(path), "..", "..", "..", "hack", "boilerplate", "sh.txt"))
	if err != nil {
		t.Fatal(err)
	}
	body, err := yaml.Marshal(found)
	if err != nil {
		t.Fatal(err)
	}
	intro := "\n# The differences between the ate-setup install and the chart install of this\n" +
		"# scenario, by object and field path, with the value from each install.\n" +
		"# cmd/ate-setup/internal/chartcompare fails when the installs differ in any\n" +
		"# other way. Run it with -update-differences to rewrite this file.\n"
	if len(found) == 0 {
		body = []byte("{}\n")
	}
	if err := os.WriteFile(path, append(append(header, intro...), body...), 0o644); err != nil {
		t.Fatal(err)
	}
}
