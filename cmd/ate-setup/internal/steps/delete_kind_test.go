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
	"os"
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
)

// notInstalledOnKind are the top-level manifests a kind install never applies:
// the PodMonitoring CRD is a GKE one.
var notInstalledOnKind = []string{"atenet-router-monitoring.yaml"}

// Every object in a top-level manifest must be removed by the kind branch of
// DeleteAteSystem, either through the kind bundle or by name. A new manifest
// that deploy applies by path fails here until it is listed in
// kindDeleteManifests.
func TestKindDeleteCoversTopLevelManifests(t *testing.T) {
	e := &Env{Cfg: &config.Config{Root: repoRoot(t), Kind: true}}
	manifest, err := e.Kustomize(installDir + "/kind")
	if err != nil {
		t.Fatalf("rendering the kind bundle: %v", err)
	}
	bundle, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		t.Fatalf("decoding the kind bundle: %v", err)
	}
	inBundle := map[string]bool{}
	for _, obj := range bundle {
		inBundle[kube.Describe(obj)] = true
	}

	entries, err := os.ReadDir(e.Cfg.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		file := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(file, ".yaml") || slices.Contains(notInstalledOnKind, file) {
			continue
		}
		if slices.Contains(kindDeleteManifests, file) {
			continue
		}
		objs, err := kube.LoadPath(e.Cfg.Manifest(file))
		if err != nil {
			t.Fatalf("loading %s: %v", file, err)
		}
		for _, obj := range objs {
			if !inBundle[kube.Describe(obj)] {
				t.Errorf("%s: %s is in neither the kind bundle nor kindDeleteManifests, so a kind delete leaves it behind", file, kube.Describe(obj))
			}
		}
	}

	// The named files must exist, or DeletePath skips them silently.
	for _, file := range kindDeleteManifests {
		if _, err := os.Stat(e.Cfg.Manifest(file)); err != nil {
			t.Errorf("kindDeleteManifests: %v", err)
		}
	}
}
