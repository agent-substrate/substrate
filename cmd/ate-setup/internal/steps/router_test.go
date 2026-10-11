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
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
)

func routerObjects(t *testing.T, cfg *config.Config) ([]*unstructured.Unstructured, *Env) {
	t.Helper()
	cfg.Root = repoRoot(t)
	e := &Env{Cfg: cfg}
	manifest, err := e.render(e.Cfg.Manifest("atenet-router.yaml"))
	if err != nil {
		t.Fatalf("render(atenet-router.yaml): %v", err)
	}
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		t.Fatalf("decoding the router manifest: %v", err)
	}
	return objs, e
}

func routerPodSpec(t *testing.T, objs []*unstructured.Unstructured) ([]any, int64) {
	t.Helper()
	dep := findObject(objs, "Deployment", "atenet-router")
	if dep == nil {
		t.Fatal("manifest has no deployment/atenet-router")
	}
	grace, err := gracePeriod(dep)
	if err != nil {
		t.Fatal(err)
	}
	containers, _, _ := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
	for _, c := range containers {
		if m := c.(map[string]any); m["name"] == "atenet-router" {
			args, _, _ := unstructured.NestedSlice(m, "args")
			return args, grace
		}
	}
	t.Fatal("deployment/atenet-router has no atenet-router container")
	return nil, 0
}

func flagValues(args []any, flag string) []string {
	var out []string
	for _, a := range args {
		if s, ok := a.(string); ok && strings.HasPrefix(s, flag+"=") {
			out = append(out, strings.TrimPrefix(s, flag+"="))
		}
	}
	return out
}

func TestConfigureAtenetRouter(t *testing.T) {
	objs, e := routerObjects(t, &config.Config{RouterRouteTimeout: 30 * time.Minute, RouterParkedRequestBudget: 2 * time.Minute})
	_, grace := routerPodSpec(t, objs)
	if err := e.configureAtenetRouter(objs); err != nil {
		t.Fatalf("configureAtenetRouter: %v", err)
	}
	args, got := routerPodSpec(t, objs)
	if v := flagValues(args, "--route-timeout"); len(v) != 1 || v[0] != "30m0s" {
		t.Errorf("--route-timeout = %v, want [30m0s]", v)
	}
	if v := flagValues(args, "--parked-request-budget"); len(v) != 1 || v[0] != "2m0s" {
		t.Errorf("--parked-request-budget = %v, want [2m0s]", v)
	}
	if want := grace + 115; got != want {
		t.Errorf("terminationGracePeriodSeconds = %d, want %d", got, want)
	}
}

// Unset settings keep the manifest as checked in, which must not set the flags itself.
func TestConfigureAtenetRouterUnsetLeavesManifestAlone(t *testing.T) {
	objs, e := routerObjects(t, &config.Config{})
	before, grace := routerPodSpec(t, objs)
	for _, flag := range []string{"--route-timeout", "--parked-request-budget"} {
		if v := flagValues(before, flag); len(v) != 0 {
			t.Errorf("the manifest sets %s=%v; the settings expect it unset", flag, v)
		}
	}
	if err := e.configureAtenetRouter(objs); err != nil {
		t.Fatalf("configureAtenetRouter: %v", err)
	}
	after, got := routerPodSpec(t, objs)
	if !reflect.DeepEqual(before, after) || got != grace {
		t.Errorf("args or grace period changed with no setting set: %v %d, want %v %d", after, got, before, grace)
	}
}

// The agentgateway overlay replaces the atenet-router container.
func TestConfigureAtenetRouterRejectsMissingContainer(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"namespace": NamespaceAteSystem, "name": "atenet-router"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "agentgateway"}},
		}}},
	}}
	e := &Env{Cfg: &config.Config{RouterRouteTimeout: time.Minute}}
	if err := e.configureAtenetRouter([]*unstructured.Unstructured{dep}); err == nil {
		t.Error("configureAtenetRouter succeeded without an atenet-router container, want an error")
	}
}
