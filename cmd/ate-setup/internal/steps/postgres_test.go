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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// postgresObjects loads the bundled postgres manifest of the given
// environment, as applyPostgres would before any resizing.
func postgresObjects(t *testing.T, kind bool) []*unstructured.Unstructured {
	t.Helper()
	e := &Env{Cfg: &config.Config{Root: repoRoot(t), Kind: kind}}
	manifest, err := e.render(e.postgresManifestPath())
	if err != nil {
		t.Fatalf("render(%s): %v", e.postgresManifestPath(), err)
	}
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		t.Fatalf("decoding the postgres manifest: %v", err)
	}
	return objs
}

func findObject(objs []*unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	for _, obj := range objs {
		if obj.GetKind() == kind && obj.GetName() == name {
			return obj
		}
	}
	return nil
}

// The kind overlay has to be what a kind install applies: the base file asks
// for more CPU than a laptop cluster schedules.
func TestPostgresManifestPathSelectsKindOverlay(t *testing.T) {
	objs := postgresObjects(t, true)
	ss := findObject(objs, "StatefulSet", "postgres")
	if ss == nil {
		t.Fatal("kind postgres overlay renders no statefulset/postgres")
	}
	cpu, _, _ := unstructured.NestedString(ss.Object, "spec", "template", "spec", "containers", "0", "resources", "requests", "cpu")
	containers, _, _ := unstructured.NestedSlice(ss.Object, "spec", "template", "spec", "containers")
	if len(containers) > 0 {
		cpu, _, _ = unstructured.NestedString(containers[0].(map[string]any), "resources", "requests", "cpu")
	}
	if cpu != "250m" {
		t.Errorf("kind postgres cpu request = %q, want the overlay's 250m", cpu)
	}
}

// Runs the size10 resize over the real manifest and the real config patch, so
// a drift between the two (a renamed ConfigMap key, a second container) fails
// here rather than on a size10 install.
func TestApplyPostgresSize10Overrides(t *testing.T) {
	objs := postgresObjects(t, false)
	base := findObject(objs, "ConfigMap", "postgres-config")
	if base == nil {
		t.Fatal("postgres manifest has no configmap/postgres-config")
	}
	baseData, _, _ := unstructured.NestedStringMap(base.Object, "data")

	root := repoRoot(t)
	conf, err := os.ReadFile(filepath.Join(root, "manifests", "ate-install", "postgres-size10", "postgres-config-patch.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := applyPostgresSize10Overrides(objs, conf); err != nil {
		t.Fatalf("applyPostgresSize10Overrides: %v", err)
	}

	cm := findObject(objs, "ConfigMap", "postgres-config")
	data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
	if !strings.Contains(data["postgresql.conf"], "max_connections = 300") {
		t.Errorf("postgresql.conf was not replaced with the size10 tuning:\n%s", data["postgresql.conf"])
	}
	// The merge semantics: the base's other keys survive untouched.
	for _, key := range []string{"pg_hba.conf", "reload-tls.sh"} {
		if data[key] != baseData[key] {
			t.Errorf("data[%s] changed; the size10 patch must only replace postgresql.conf", key)
		}
	}
	// And the TLS wiring the base's postgresql.conf carried must not be lost
	// by the replacement, or the server refuses connections.
	for _, want := range []string{"ssl = on", "hba_file = "} {
		if !strings.Contains(data["postgresql.conf"], want) {
			t.Errorf("size10 postgresql.conf lacks %q", want)
		}
	}

	ss := findObject(objs, "StatefulSet", "postgres")
	containers, _, _ := unstructured.NestedSlice(ss.Object, "spec", "template", "spec", "containers")
	resources, _, _ := unstructured.NestedMap(containers[0].(map[string]any), "resources")
	requests, _ := resources["requests"].(map[string]any)
	limits, _ := resources["limits"].(map[string]any)
	if requests["cpu"] != size10PostgresCPURequest {
		t.Errorf("requests.cpu = %v, want %s", requests["cpu"], size10PostgresCPURequest)
	}
	if requests["memory"] != size10PostgresMemory || limits["memory"] != size10PostgresMemory {
		t.Errorf("memory request/limit = %v/%v, want %s for both", requests["memory"], limits["memory"], size10PostgresMemory)
	}
	if _, ok := limits["cpu"]; ok {
		t.Errorf("limits.cpu = %v, want it removed", limits["cpu"])
	}
}

func TestApplyPostgresSize10OverridesRejectsMissingObjects(t *testing.T) {
	root := repoRoot(t)
	conf, err := os.ReadFile(filepath.Join(root, "manifests", "ate-install", "postgres-size10", "postgres-config-patch.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	objs := postgresObjects(t, false)

	var withoutStatefulSet []*unstructured.Unstructured
	for _, obj := range objs {
		if obj.GetKind() != "StatefulSet" {
			withoutStatefulSet = append(withoutStatefulSet, obj)
		}
	}
	if err := applyPostgresSize10Overrides(withoutStatefulSet, conf); err == nil {
		t.Error("applyPostgresSize10Overrides succeeded without a StatefulSet, want an error")
	}
	if err := applyPostgresSize10Overrides(objs, []byte("data: {}\n")); err == nil {
		t.Error("applyPostgresSize10Overrides succeeded with an empty patch, want an error")
	}
}

func TestUseBundledPostgres(t *testing.T) {
	for _, tc := range []struct {
		name       string
		connString string
		want       bool
	}{
		{name: "no external database", connString: "", want: true},
		{
			name:       "external database configured",
			connString: "postgresql://user@db.example.com:5432/atepg",
			want:       false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &config.Config{PostgresConnectionString: tc.connString}}
			if got := e.useBundledPostgres(); got != tc.want {
				t.Errorf("useBundledPostgres() = %v, want %v", got, tc.want)
			}
		})
	}
}

// The StatefulSet lives in a subdirectory that the bundle render does not
// descend into, so DeployAteSystem has to apply it by name. A rename that
// misses Manifest("postgres", "postgres.yaml") would otherwise only surface
// as a failed GKE install.
func TestBundledPostgresManifestExists(t *testing.T) {
	cfg := &config.Config{Root: repoRoot(t)}
	path := cfg.Manifest("postgres", "postgres.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat(%s) = %v, want the bundled PostgreSQL manifest", path, err)
	}
	// It must not also sit at the top level, where `ko resolve -f
	// manifests/ate-install` would apply it regardless of the skip.
	stray := filepath.Join(cfg.Root, "manifests", "ate-install", "postgres.yaml")
	if _, err := os.Stat(stray); err == nil {
		t.Errorf("%s exists; the bundle render would apply it even for external databases", stray)
	}
}
