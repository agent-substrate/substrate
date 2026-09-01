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

// The component list is checked against the real manifests, so this is an
// external test package: config imports images, and only the test needs config.
package images_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
)

// installTrees are the directories the installer reads manifests from.
//
// benchmarking/ and internal/e2e/ are left out deliberately: neither is
// installed through the image resolvers -- the benchmark workloads go through
// a shell script that runs ko itself, and the e2e fixtures are built by the
// test suite.
var installTrees = []string{"manifests", "demos"}

var koRefPattern = regexp.MustCompile(`ko://[^\s"',\]}]+`)

// componentsInTree collects the package of every ko:// reference under dir.
//
// A reference that does not carry the module path is recorded whole, so it
// shows up in the failure rather than being silently dropped.
func componentsInTree(t *testing.T, dir string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		switch filepath.Ext(strings.TrimSuffix(p, ".tmpl")) {
		case ".yaml", ".yml", ".json":
		default:
			return nil
		}
		body, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		for _, match := range koRefPattern.FindAllString(string(body), -1) {
			pkg, ok := strings.CutPrefix(match, "ko://"+images.ModulePath+"/")
			if !ok {
				pkg = match
			}
			if !slices.Contains(found, pkg) {
				found = append(found, pkg)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
	return found
}

// images.Components is a closed list, so a component added to a manifest but
// not to the list would install fine from source and fail only when someone
// tried to install a release. Catch it here instead.
//
// The same comparison holds the spelling of the references. ko accepts a
// "./"-relative import path as well as a full one, and images.KoReference only
// builds the full form, so a relative reference would resolve to no image. It
// lands here as an entry the list cannot contain.
func TestComponentsCoverTheManifests(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}

	var found []string
	for _, tree := range installTrees {
		for _, pkg := range componentsInTree(t, filepath.Join(root, tree)) {
			if !slices.Contains(found, pkg) {
				found = append(found, pkg)
			}
		}
	}
	slices.Sort(found)

	want := slices.Clone(images.Components)
	slices.Sort(want)

	if !slices.Equal(found, want) {
		t.Errorf("the components referenced by %s and images.Components disagree:\n"+
			" in the manifests: %v\n"+
			" in images.Components: %v\n"+
			"add the missing entries to images.Components, or remove the stale ones; "+
			"an entry still carrying a ko:// prefix is a reference that does not name "+
			"its package by the full import path %s/...",
			strings.Join(installTrees, "/ and "), found, want, images.ModulePath)
	}
}

func TestSourceValidate(t *testing.T) {
	tests := []struct {
		name  string
		src   images.Source
		error string
	}{
		{
			name: "build from source",
			src:  images.Source{},
		},
		{
			name: "prebuilt",
			src:  images.Source{Repo: "example.com/substrate", Tag: "v1"},
		},
		{
			name:  "no tag",
			src:   images.Source{Repo: "example.com/substrate"},
			error: "--image-repo (or ATE_IMAGE_REPO) requires --image-tag",
		},
		{
			// A tag with nowhere to pull from would otherwise be dropped, and
			// the source build that followed would look like the release.
			name:  "no repo",
			src:   images.Source{Tag: "v1"},
			error: "--image-tag (or ATE_IMAGE_TAG) requires --image-repo",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.src.Validate()
			if tc.error == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error containing %q", tc.error)
			}
			if !strings.Contains(err.Error(), tc.error) {
				t.Errorf("Validate() error = %v, want it to contain %q", err, tc.error)
			}
		})
	}
}
