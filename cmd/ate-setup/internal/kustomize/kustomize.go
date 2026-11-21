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

// Package kustomize renders the overlays under manifests/ate-install, standing
// in for `kubectl kustomize <dir> --load-restrictor LoadRestrictionsNone`.
package kustomize

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/yaml"
)

// Build renders an overlay directory to a multi-document YAML manifest.
//
// Load restrictions are disabled to match the shell scripts: the kind overlays
// reference base manifests that sit outside their own directory, which the
// default root-relative restriction rejects.
func Build(dir string) ([]byte, error) {
	opts := krusty.MakeDefaultOptions()
	opts.LoadRestrictions = types.LoadRestrictionsNone

	k := krusty.MakeKustomizer(opts)
	resMap, err := k.Run(filesys.MakeFsOnDisk(), dir)
	if err != nil {
		return nil, fmt.Errorf("while building the kustomization at %s: %w", dir, err)
	}

	out, err := resMap.AsYaml()
	if err != nil {
		return nil, fmt.Errorf("while serializing the kustomization at %s: %w", dir, err)
	}
	return out, nil
}

// Compose renders path, a manifest file or a kustomization directory, with the
// given component directories layered on top. It is how one component, such as
// the control plane node pinning, reaches every apply path whichever form the
// manifests take there.
//
// The composition is a throwaway kustomization in a temporary directory.
// kustomize refuses absolute paths as resource or component roots even with
// the load restrictor off, so both are written relative to that directory.
func Compose(path string, components ...string) ([]byte, error) {
	dir, err := os.MkdirTemp("", "ate-setup-kustomize-")
	if err != nil {
		return nil, fmt.Errorf("while creating a scratch kustomization: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	return compose(dir, path, components)
}

// ComposeBytes is Compose for a manifest already held in memory, such as one
// patched on the way in.
func ComposeBytes(manifest []byte, components ...string) ([]byte, error) {
	dir, err := os.MkdirTemp("", "ate-setup-kustomize-")
	if err != nil {
		return nil, fmt.Errorf("while creating a scratch kustomization: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	resource := filepath.Join(dir, "manifest.yaml")
	if err := os.WriteFile(resource, manifest, 0o600); err != nil {
		return nil, fmt.Errorf("while staging a manifest for kustomize: %w", err)
	}
	return compose(dir, resource, components)
}

func compose(dir, resource string, components []string) ([]byte, error) {
	k := types.Kustomization{TypeMeta: types.TypeMeta{APIVersion: types.KustomizationVersion, Kind: types.KustomizationKind}}
	rel, err := relativeTo(dir, resource)
	if err != nil {
		return nil, err
	}
	k.Resources = []string{rel}
	for _, component := range components {
		rel, err := relativeTo(dir, component)
		if err != nil {
			return nil, err
		}
		k.Components = append(k.Components, rel)
	}
	// Marshaling quotes paths that YAML would otherwise misread, such as a
	// directory named "a: b" or "my provider #1".
	data, err := yaml.Marshal(k)
	if err != nil {
		return nil, fmt.Errorf("while serializing a scratch kustomization: %w", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), data, 0o600); err != nil {
		return nil, fmt.Errorf("while writing a scratch kustomization: %w", err)
	}
	out, err := Build(dir)
	if err != nil {
		return nil, fmt.Errorf("while composing %s with %s: %w", resource, strings.Join(components, ", "), err)
	}
	return out, nil
}

// relativeTo returns target as a path relative to dir. Both are resolved
// through symlinks first: the temporary directory commonly sits behind one
// (macOS's /tmp), and a `..` walk computed from the unresolved path would leave
// the real tree.
func relativeTo(dir, target string) (string, error) {
	realDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", fmt.Errorf("while resolving %s: %w", dir, err)
	}
	realTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		return "", fmt.Errorf("while resolving %s: %w", target, err)
	}
	rel, err := filepath.Rel(realDir, realTarget)
	if err != nil {
		return "", fmt.Errorf("while relativizing %s to %s: %w", target, dir, err)
	}
	return filepath.ToSlash(rel), nil
}
