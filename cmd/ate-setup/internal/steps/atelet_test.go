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
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
)

// Both deploy ate-system, which renders the system overlay, and deploy atelet,
// which renders the atelet manifest alone, have to add the configured
// tolerations after the built-in ones and leave everything else alone.
func TestRenderAddsAteletTolerations(t *testing.T) {
	seconds := int64(300)
	extra := []corev1.Toleration{
		{Key: "session-size", Operator: corev1.TolerationOpEqual, Value: "large", Effect: corev1.TaintEffectNoSchedule},
		{Key: "maintenance", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
	}
	for _, kind := range []bool{false, true} {
		plain := &Env{Cfg: &config.Config{Root: repoRoot(t), Router: config.RouterEnvoy, Kind: kind}}
		tolerating := &Env{Cfg: &config.Config{Root: repoRoot(t), Router: config.RouterEnvoy, Kind: kind, AteletTolerations: extra}}
		for _, path := range []string{
			plain.Cfg.Path(SystemOverlay(plain.Cfg)),
			plain.ateletManifestPath(),
		} {
			want := renderObjects(t, plain, path)
			got := renderObjects(t, tolerating, path)

			wantAtelet := findObject(want, "DaemonSet", "atelet")
			gotAtelet := findObject(got, "DaemonSet", "atelet")
			if wantAtelet == nil || gotAtelet == nil {
				t.Fatalf("kind=%t: %s has no daemonset/atelet", kind, path)
			}
			builtin := ateletTolerations(t, wantAtelet)
			if len(builtin) == 0 {
				t.Fatalf("kind=%t: %s: daemonset/atelet has no built-in tolerations", kind, path)
			}
			if diff := cmp.Diff(append(builtin, extra...), ateletTolerations(t, gotAtelet)); diff != "" {
				t.Errorf("kind=%t: %s: atelet tolerations (-want +got):\n%s", kind, path, diff)
			}
			for _, obj := range []*unstructured.Unstructured{wantAtelet, gotAtelet} {
				unstructured.RemoveNestedField(obj.Object, "spec", "template", "spec", "tolerations")
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("kind=%t: %s (-want +got):\n%s", kind, path, diff)
			}
		}
	}
}

func renderObjects(t *testing.T, e *Env, path string) []*unstructured.Unstructured {
	t.Helper()
	manifest, err := e.render(path)
	if err != nil {
		t.Fatalf("render(%s): %v", path, err)
	}
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		t.Fatalf("decoding %s: %v", path, err)
	}
	return objs
}

func ateletTolerations(t *testing.T, obj *unstructured.Unstructured) []corev1.Toleration {
	t.Helper()
	var spec corev1.PodSpec
	raw, _, _ := unstructured.NestedMap(obj.Object, "spec", "template", "spec")
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &spec); err != nil {
		t.Fatal(err)
	}
	return spec.Tolerations
}
