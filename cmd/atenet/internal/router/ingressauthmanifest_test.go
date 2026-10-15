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

package router

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/yaml"
)

const routerStaticMTLSComponentPath = "../../../../manifests/ate-install/components/router-static-mtls/kustomization.yaml"

// TestRouterIngressAuthManifestsAreValid renders the router manifest as
// ate-setup installs it, alone and with the component it layers on under
// --ingress-auth-mode=static-mtls, and checks the router would accept the
// resulting flags. The component appends to an argument list it does not own,
// and the base manifest's TLS listeners need the client auth settings, so a
// new listener in the manifest, or a new startup check here, would otherwise
// surface only as a router that refuses to start in the e2e cluster.
func TestRouterIngressAuthManifestsAreValid(t *testing.T) {
	for _, tc := range []struct {
		name          string
		componentPath string
		wantMode      IngressAuthMode
	}{
		{name: "base", wantMode: IngressAuthDeprecatedInsecure},
		{name: "router-static-mtls", componentPath: routerStaticMTLSComponentPath, wantMode: IngressAuthStaticMTLS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := renderedRouterArgs(t, tc.componentPath)

			var cfg routerConfig
			fs := pflag.NewFlagSet("router", pflag.ContinueOnError)
			bindRouterFlags(fs, &cfg)
			if err := fs.Parse(args); err != nil {
				t.Fatalf("the rendered atenet-router flags are not ones the binary accepts: %v", err)
			}

			if got := cfg.IngressAuth.mode(); got != tc.wantMode {
				t.Fatalf("--ingress-auth-mode = %q, want %q", got, tc.wantMode)
			}
			if !cfg.tlsIngressEnabled() {
				t.Fatalf("the rendered flags enable no TLS ingress listener, so client auth is never exercised")
			}
			if err := cfg.validate(); err != nil {
				t.Fatalf("the router would refuse the rendered flags: %v", err)
			}
			// The router reads the CA file, but Envoy is what enforces it, so
			// it has to be mounted in the envoy container at the same path.
			envoyMounts := renderedEnvoyMountPaths(t, tc.componentPath)
			if !slices.ContainsFunc(envoyMounts, func(dir string) bool {
				return strings.HasPrefix(cfg.IngressAuth.ClientCAFile, dir+"/")
			}) {
				t.Errorf("--ingress-client-ca-file=%s is not under any of the envoy container's mounts %v", cfg.IngressAuth.ClientCAFile, envoyMounts)
			}
		})
	}
}

// renderedRouterDeployment builds the router manifest with the component at
// componentPath layered on, the way ate-setup composes them, and returns the
// atenet-router Deployment. An empty componentPath renders the manifest alone.
func renderedRouterDeployment(t *testing.T, componentPath string) appsv1.Deployment {
	t.Helper()
	manifest, err := os.ReadFile(routerManifestPath)
	if err != nil {
		t.Fatalf("reading %s: %v", routerManifestPath, err)
	}
	files := map[string][]byte{
		"/app/atenet-router.yaml": manifest,
		"/app/kustomization.yaml": []byte("resources:\n- atenet-router.yaml\n"),
	}
	if componentPath != "" {
		component, err := os.ReadFile(componentPath)
		if err != nil {
			t.Fatalf("reading %s: %v", componentPath, err)
		}
		files["/app/component/kustomization.yaml"] = component
		files["/app/kustomization.yaml"] = []byte("resources:\n- atenet-router.yaml\ncomponents:\n- component\n")
	}

	fsys := filesys.MakeFsInMemory()
	for path, content := range files {
		if err := fsys.WriteFile(path, content); err != nil {
			t.Fatalf("staging %s: %v", path, err)
		}
	}
	resMap, err := krusty.MakeKustomizer(krusty.MakeDefaultOptions()).Run(fsys, "/app")
	if err != nil {
		t.Fatalf("rendering the router manifest with %s: %v", componentPath, err)
	}
	for _, res := range resMap.Resources() {
		if res.GetKind() != "Deployment" || res.GetName() != "atenet-router" {
			continue
		}
		raw, err := res.AsYAML()
		if err != nil {
			t.Fatalf("serializing the rendered atenet-router Deployment: %v", err)
		}
		var deployment appsv1.Deployment
		if err := yaml.Unmarshal(raw, &deployment); err != nil {
			t.Fatalf("decoding the rendered atenet-router Deployment: %v", err)
		}
		return deployment
	}
	t.Fatalf("rendering with %s produced no atenet-router Deployment", componentPath)
	return appsv1.Deployment{}
}

// renderedRouterArgs returns the atenet-router container's flags, without
// the subcommand, from the manifest rendered with componentPath.
func renderedRouterArgs(t *testing.T, componentPath string) []string {
	t.Helper()
	for _, c := range renderedRouterDeployment(t, componentPath).Spec.Template.Spec.Containers {
		if c.Name != "atenet-router" {
			continue
		}
		if len(c.Args) == 0 || c.Args[0] != "router" {
			t.Fatalf("the rendered atenet-router container's args are %v; they should invoke the router subcommand", c.Args)
		}
		return c.Args[1:]
	}
	t.Fatal("the rendered atenet-router Deployment has no container named atenet-router")
	return nil
}

// renderedEnvoyMountPaths returns the envoy container's volume mount paths
// from the manifest rendered with componentPath.
func renderedEnvoyMountPaths(t *testing.T, componentPath string) []string {
	t.Helper()
	for _, c := range renderedRouterDeployment(t, componentPath).Spec.Template.Spec.Containers {
		if c.Name != "envoy" {
			continue
		}
		var paths []string
		for _, m := range c.VolumeMounts {
			paths = append(paths, m.MountPath)
		}
		return paths
	}
	t.Fatal("the rendered atenet-router Deployment has no container named envoy")
	return nil
}
