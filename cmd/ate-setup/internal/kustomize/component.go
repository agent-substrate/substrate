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

package kustomize

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"sigs.k8s.io/kustomize/api/konfig"
	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/api/types"
)

// Patch is a patch in a component and the resources it selects.
type Patch struct {
	// Name is the patch's path, or its field and index for an inline patch.
	Name   string
	Target types.Selector
}

// ComponentPatches reads the kustomize Component in dir and returns its
// patches. It fails unless dir holds a kustomization of kind Component with no
// unknown fields, and on any patch without a target. ate-setup composes a
// component into applies that each render part of the control plane, and
// kustomize fails an untargeted patch on every apply that lacks its resource.
func ComponentPatches(dir string) ([]Patch, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}
	for _, name := range konfig.RecognizedKustomizationFileNames() {
		path := filepath.Join(dir, name)
		raw, err := os.ReadFile(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var k types.Kustomization
		if err := k.Unmarshal(raw); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", path, err)
		}
		if k.Kind != types.ComponentKind {
			return nil, fmt.Errorf("%s must declare kind: %s, got %q", path, types.ComponentKind, k.Kind)
		}
		return targetedPatches(path, &k)
	}
	return nil, fmt.Errorf("%s has no kustomization.yaml", dir)
}

// targetedPatches returns the patches in k, the kustomization at path. Entries
// of the deprecated patchesStrategicMerge field have no target. kustomize
// still applies the deprecated fields, so they are checked too.
func targetedPatches(path string, k *types.Kustomization) ([]Patch, error) {
	if len(k.PatchesStrategicMerge) > 0 { //nolint:staticcheck // SA1019: see the note above.
		return nil, fmt.Errorf("%s: patchesStrategicMerge cannot set a target; move each entry to patches and set its target", path)
	}
	var patches []Patch
	var untargeted []string
	for _, field := range []struct {
		name    string
		patches []types.Patch
	}{
		{"patches", k.Patches},
		{"patchesJson6902", k.PatchesJson6902}, //nolint:staticcheck // SA1019: see the note on targetedPatches.
	} {
		for i, p := range field.patches {
			name := p.Path
			if name == "" {
				name = field.name + "[" + strconv.Itoa(i) + "]"
			}
			if p.Target == nil {
				untargeted = append(untargeted, name)
				continue
			}
			patches = append(patches, Patch{Name: name, Target: *p.Target})
		}
	}
	if len(untargeted) > 0 {
		return nil, fmt.Errorf("%s: patch %s has no target; each control plane apply renders only part of the control plane, so every patch must set one",
			path, strings.Join(untargeted, ", "))
	}
	return patches, nil
}

// Unmatched returns the patches whose target selects no resource in any of
// manifests, matching resources the way kustomize does when it applies a
// patch. Each manifest is read on its own, as each is applied on its own.
func Unmatched(patches []Patch, manifests [][]byte) ([]Patch, error) {
	if len(patches) == 0 {
		return nil, nil
	}
	factory := resmap.NewFactory(provider.NewDefaultDepProvider().GetResourceFactory())
	resources := make([]resmap.ResMap, 0, len(manifests))
	for _, manifest := range manifests {
		m, err := factory.NewResMapFromBytes(manifest)
		if err != nil {
			return nil, fmt.Errorf("while parsing a rendered manifest: %w", err)
		}
		resources = append(resources, m)
	}
	var unmatched []Patch
	for _, p := range patches {
		matched := false
		for _, m := range resources {
			selected, err := m.Select(p.Target)
			if err != nil {
				return nil, fmt.Errorf("patch %s: target %s: %w", p.Name, DescribeSelector(p.Target), err)
			}
			if len(selected) > 0 {
				matched = true
				break
			}
		}
		if !matched {
			unmatched = append(unmatched, p)
		}
	}
	return unmatched, nil
}

// DescribeSelector lists the fields s sets, as in a target: block.
func DescribeSelector(s types.Selector) string {
	var fields []string
	for _, f := range []struct{ name, value string }{
		{"group", s.Group},
		{"version", s.Version},
		{"kind", s.Kind},
		{"namespace", s.Namespace},
		{"name", s.Name},
		{"labelSelector", s.LabelSelector},
		{"annotationSelector", s.AnnotationSelector},
	} {
		if f.value != "" {
			fields = append(fields, f.name+"="+strconv.Quote(f.value))
		}
	}
	if len(fields) == 0 {
		return "{}"
	}
	return "{" + strings.Join(fields, " ") + "}"
}
