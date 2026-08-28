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

package extproc

import (
	"bufio"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// The install tree, not a fixture, so this guards the Envoy config that ships.
const manifestsDir = "../../../../../manifests"

// What the install ships today. Falling below it means the walk stopped
// matching, not that the config got better.
const minDNSCacheConfigs = 7

// TestEgressDNSLookupFamily requires ALL on every dynamic forward proxy DNS
// cache the install ships; atenet-egress.yaml says why ALL.
func TestEgressDNSLookupFamily(t *testing.T) {
	found := 0
	for _, path := range manifestPaths(t) {
		caches := dnsCacheConfigs(t, path)
		if len(caches) == 0 {
			continue
		}
		found += len(caches)
		t.Run(filepath.Base(path), func(t *testing.T) {
			for _, cache := range caches {
				if got := cache["dns_lookup_family"]; got != "ALL" {
					t.Errorf("dns_cache_config %v: dns_lookup_family = %v, want ALL", cache["name"], got)
				}
			}
		})
	}
	if found < minDNSCacheConfigs {
		t.Errorf("found %d dns_cache_config blocks under %s, want at least %d", found, manifestsDir, minDNSCacheConfigs)
	}
}

// manifestPaths covers the whole install tree, so a new egress variant is
// checked the day it is added.
func manifestPaths(t *testing.T) []string {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(manifestsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && (strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml")) {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", manifestsDir, err)
	}
	return paths
}

func dnsCacheConfigs(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer f.Close()

	var caches []map[string]any
	reader := k8syaml.NewYAMLReader(bufio.NewReader(f))
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return caches
		}
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		var object struct {
			Kind string            `json:"kind"`
			Data map[string]string `json:"data"`
		}
		if err := yaml.Unmarshal(doc, &object); err != nil {
			t.Fatalf("parsing a document of %s: %v", path, err)
		}
		if object.Kind != "ConfigMap" {
			continue
		}
		for _, value := range object.Data {
			var config any
			if err := yaml.Unmarshal([]byte(value), &config); err != nil {
				// Not every ConfigMap value is YAML.
				continue
			}
			caches = append(caches, collectDNSCacheConfigs(config)...)
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
