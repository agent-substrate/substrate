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
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
)

// ate-idp-server has to publish under the issuer ate-api-server signs with,
// and the keys of the pool it signs from, or no relying party can verify a
// token.
func TestIDPServerGetsTheIssuerAndSharesTheAPIServerPool(t *testing.T) {
	const issuer = "https://idp.example.com"
	root := repoRoot(t)
	for _, kind := range []bool{false, true} {
		cfg := config.Config{Root: root, Router: config.RouterEnvoy, Kind: kind}
		e := &Env{Cfg: &cfg}
		rendered, err := e.render(e.Cfg.Path(SystemOverlay(e.Cfg)))
		if err != nil {
			t.Fatalf("kind=%t: render: %v", kind, err)
		}
		// Twice, so the second call has an --issuer to replace.
		for range 2 {
			if rendered, err = setIDPServerIssuer(rendered, issuer); err != nil {
				t.Fatalf("kind=%t: setIDPServerIssuer: %v", kind, err)
			}
		}
		objs, err := kube.DecodeManifestBytes(rendered)
		if err != nil {
			t.Fatalf("kind=%t: DecodeManifestBytes: %v", kind, err)
		}
		pods := map[string]corev1.PodSpec{}
		for _, obj := range objs {
			if obj.GetKind() != "Deployment" {
				continue
			}
			var dep appsv1.Deployment
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &dep); err != nil {
				t.Fatalf("kind=%t: deployment/%s: %v", kind, obj.GetName(), err)
			}
			pods[dep.Name] = dep.Spec.Template.Spec
		}
		api, ok := pods["ate-api-server"]
		if !ok {
			t.Fatalf("kind=%t: bundle has no deployment/ate-api-server", kind)
		}
		idp, ok := pods["ate-idp-server"]
		if !ok {
			t.Fatalf("kind=%t: bundle has no deployment/ate-idp-server", kind)
		}

		args := idp.Containers[0].Args
		issuerArgs := slices.DeleteFunc(slices.Clone(args), func(arg string) bool { return !strings.HasPrefix(arg, "--issuer=") })
		if diff := cmp.Diff([]string{"--issuer=" + issuer}, issuerArgs); diff != "" {
			t.Errorf("kind=%t: ate-idp-server --issuer args (-want +got):\n%s", kind, diff)
		}
		if len(idp.Containers[0].Env) != 0 {
			t.Errorf("kind=%t: ate-idp-server sets env %v, want its configuration in flags", kind, idp.Containers[0].Env)
		}

		volume := func(spec corev1.PodSpec) *corev1.Volume {
			i := slices.IndexFunc(spec.Volumes, func(v corev1.Volume) bool { return v.Name == "actor-id-jwt-pool" })
			if i < 0 {
				return nil
			}
			return &spec.Volumes[i]
		}
		if volume(api) == nil {
			t.Fatalf("kind=%t: ate-api-server has no actor-id-jwt-pool volume", kind)
		}
		if diff := cmp.Diff(volume(api), volume(idp)); diff != "" {
			t.Errorf("kind=%t: actor-id-jwt-pool volume (-ate-api-server +ate-idp-server):\n%s", kind, diff)
		}
	}
}

// deploy apiserver applies ate-idp-server on its own, so it has to set the
// issuer as deploy ate-system does.
func TestRenderIDPServerSetsTheIssuer(t *testing.T) {
	const issuer = "https://idp.example.com"
	e := &Env{Cfg: &config.Config{Root: repoRoot(t), ActorJWTIssuer: issuer}}
	rendered, err := e.renderIDPServer()
	if err != nil {
		t.Fatalf("renderIDPServer: %v", err)
	}
	objs, err := kube.DecodeManifestBytes(rendered)
	if err != nil {
		t.Fatalf("DecodeManifestBytes: %v", err)
	}
	idp := findObject(objs, "Deployment", "ate-idp-server")
	if idp == nil {
		t.Fatal("renderIDPServer has no deployment/ate-idp-server")
	}
	var dep appsv1.Deployment
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(idp.Object, &dep); err != nil {
		t.Fatal(err)
	}
	if args := dep.Spec.Template.Spec.Containers[0].Args; !slices.Contains(args, "--issuer="+issuer) {
		t.Errorf("ate-idp-server args = %v, want --issuer=%s", args, issuer)
	}
}

func TestSetIDPServerIssuerWithoutTheDeployment(t *testing.T) {
	manifest := []byte("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: ate-api-server\n")
	if _, err := setIDPServerIssuer(manifest, "https://idp.example.com"); err == nil {
		t.Error("setIDPServerIssuer succeeded on a manifest without deployment/ate-idp-server")
	}
}
