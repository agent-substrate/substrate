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
	"bufio"
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kustomize"
)

// TestEgressDNSLookupFamily requires ALL on every dynamic forward proxy DNS
// cache in the install tree, so a new egress variant is checked the day it is
// added rather than the day it is installed. atenet-egress.yaml says why ALL.
func TestEgressDNSLookupFamily(t *testing.T) {
	for _, path := range manifestPaths(t) {
		caches := dnsCacheConfigs(t, path)
		if len(caches) == 0 {
			continue
		}
		t.Run(filepath.Base(path), func(t *testing.T) {
			requireLookupFamilyALL(t, caches)
		})
	}
}

// TestRenderedOverlaysDNSLookupFamily repeats the check against the output of
// every overlay, because a manifest is not what gets applied: a patch or a
// configMapGenerator can put back a pin that no file in the tree contains.
func TestRenderedOverlaysDNSLookupFamily(t *testing.T) {
	for _, dir := range overlayDirs(t) {
		t.Run(filepath.Base(dir), func(t *testing.T) {
			rendered, err := kustomize.Build(dir)
			if err != nil {
				t.Fatalf("rendering %s: %v", dir, err)
			}
			requireLookupFamilyALL(t, dnsCacheConfigsIn(t, dir, bytes.NewReader(rendered)))
		})
	}
}

// TestEgressManifestsCarryDNSCaches pins the cache count of each manifest the
// installer can select, so a walk that quietly stopped matching fails here
// instead of passing TestEgressDNSLookupFamily vacuously. Ranging over
// ExperimentalUseSDSMint covers the whole input domain of the selection.
func TestEgressManifestsCarryDNSCaches(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sdsmint bool
		want    int
	}{
		{name: "envoy", want: 2},
		{name: "sdsmint", sdsmint: true, want: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &Env{Cfg: &config.Config{Root: repoRoot(t), ExperimentalUseSDSMint: tc.sdsmint}}
			path := env.atenetEgressManifestPath()
			if got := len(dnsCacheConfigs(t, path)); got != tc.want {
				t.Errorf("%s has %d dns_cache_config blocks, want %d", path, got, tc.want)
			}
		})
	}
}

// TestOverlayDiscoveryCoversTheInstaller keeps the discovery in overlayDirs
// honest: a renamed or unparseable kustomization would drop an overlay from
// the rendered check silently, leaving it to pass over an empty set.
func TestOverlayDiscoveryCoversTheInstaller(t *testing.T) {
	root := repoRoot(t)
	discovered := overlayDirs(t)

	want := []string{
		filepath.Join(root, installDir, "agentgateway-egress"),
		filepath.Join(root, installDir, "agentgateway-router"),
	}
	for _, kind := range []bool{false, true} {
		for _, router := range []string{config.RouterEnvoy, config.RouterAgentgateway} {
			if overlay := SystemOverlay(&config.Config{Kind: kind, Router: router}); overlay != "" {
				want = append(want, filepath.Join(root, overlay))
			}
		}
	}

	for _, dir := range want {
		if !slices.Contains(discovered, dir) {
			t.Errorf("the installer renders %s, but overlay discovery did not find it", dir)
		}
	}
}

func requireLookupFamilyALL(t *testing.T, caches []map[string]any) {
	t.Helper()
	for _, cache := range caches {
		if got := cache["dns_lookup_family"]; got != "ALL" {
			t.Errorf("dns_cache_config %v: dns_lookup_family = %v, want ALL", cache["name"], got)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving the repository root: %v", err)
	}
	return root
}

// manifestPaths is every YAML file that ships, not a fixture list, so the
// guard covers the config the install actually applies.
func manifestPaths(t *testing.T) []string {
	t.Helper()
	var paths []string
	walkManifests(t, func(path string) {
		if strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml") {
			paths = append(paths, path)
		}
	})
	return paths
}

// overlayDirs is every buildable kustomization root. Components are skipped:
// they are fragments that only render through the overlay that includes them.
func overlayDirs(t *testing.T) []string {
	t.Helper()
	var dirs []string
	walkManifests(t, func(path string) {
		if filepath.Base(path) != "kustomization.yaml" {
			return
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		var head struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal(raw, &head); err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		if head.Kind != "Component" {
			dirs = append(dirs, filepath.Dir(path))
		}
	})
	return dirs
}

func walkManifests(t *testing.T, visit func(path string)) {
	t.Helper()
	root := filepath.Join(repoRoot(t), "manifests")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			visit(path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
}

func dnsCacheConfigs(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer f.Close()
	return dnsCacheConfigsIn(t, path, f)
}

func dnsCacheConfigsIn(t *testing.T, name string, r io.Reader) []map[string]any {
	t.Helper()
	var caches []map[string]any
	reader := k8syaml.NewYAMLReader(bufio.NewReader(r))
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return caches
		}
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		var object struct {
			Kind string            `json:"kind"`
			Data map[string]string `json:"data"`
		}
		if err := yaml.Unmarshal(doc, &object); err != nil {
			t.Fatalf("parsing a document of %s: %v", name, err)
		}
		if object.Kind != "ConfigMap" {
			continue
		}
		for _, value := range object.Data {
			var parsed any
			if err := yaml.Unmarshal([]byte(value), &parsed); err != nil {
				// Not every ConfigMap value is YAML.
				continue
			}
			caches = append(caches, collectDNSCacheConfigs(parsed)...)
		}
	}
}

func collectDNSCacheConfigs(node any) []map[string]any {
	var caches []map[string]any
	switch node := node.(type) {
	case map[string]any:
		for key, value := range node {
			if cache, ok := value.(map[string]any); ok && key == "dns_cache_config" {
				caches = append(caches, cache)
			}
			caches = append(caches, collectDNSCacheConfigs(value)...)
		}
	case []any:
		for _, value := range node {
			caches = append(caches, collectDNSCacheConfigs(value)...)
		}
	}
	return caches
}
