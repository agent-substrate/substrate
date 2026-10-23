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

// Package kustomize renders the overlays under manifests/ate-install, standing
// in for `kubectl kustomize <dir> --load-restrictor LoadRestrictionsNone`.
package kustomize

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/konfig"
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

// ComponentDirs checks that each directory holds a kustomization file of kind
// Component and returns the directories as absolute paths, in order.
//
// ate-setup composes each Component over every manifest it renders, and each
// manifest carries only part of the install, so a Component may only change
// objects that are already there. checkComponent rejects fields that add,
// rename or move objects, change selectors, or fail on a manifest that lacks
// the object they name. docs/operator-install.md lists them.
func ComponentDirs(dirs []string) ([]string, error) {
	out := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return nil, fmt.Errorf("while resolving component %s: %w", dir, err)
		}
		if err := checkComponent(abs, nil); err != nil {
			return nil, fmt.Errorf("component %s: %w", dir, err)
		}
		out = append(out, abs)
	}
	return out, nil
}

// checkComponent checks the Component in dir and the components it includes.
// parents are the directories that include dir, to catch a cycle.
func checkComponent(dir string, parents []string) error {
	if slices.Contains(parents, dir) {
		return fmt.Errorf("includes itself through %s", dir)
	}
	name, data, err := readKustomization(dir)
	if err != nil {
		return err
	}
	var k types.Kustomization
	if err := yaml.Unmarshal(data, &k); err != nil {
		return fmt.Errorf("while parsing %s: %w", name, err)
	}
	if k.Kind != types.ComponentKind {
		return fmt.Errorf("%s has kind %q, want %s", name, k.Kind, types.ComponentKind)
	}
	if deprecated := *k.CheckDeprecatedFields(); len(deprecated) > 0 {
		return fmt.Errorf("uses deprecated fields: %s", strings.Join(deprecated, "; "))
	}
	for _, f := range []struct {
		field string
		set   bool
	}{
		{"resources", len(k.Resources) > 0},
		{"configMapGenerator", len(k.ConfigMapGenerator) > 0},
		{"secretGenerator", len(k.SecretGenerator) > 0},
		{"namePrefix", k.NamePrefix != ""},
		{"nameSuffix", k.NameSuffix != ""},
		{"namespace", k.Namespace != ""},
		{"replacements", len(k.Replacements) > 0},
		{"replicas", len(k.Replicas) > 0},
	} {
		if f.set {
			return fmt.Errorf("%s is not supported in a --component", f.field)
		}
	}
	for i, l := range k.Labels {
		if l.IncludeSelectors {
			return fmt.Errorf("labels %d sets includeSelectors, which is not supported in a --component", i)
		}
	}
	for i, p := range k.Patches {
		if p.Target == nil {
			return fmt.Errorf("patch %d has no target", i)
		}
	}
	for _, c := range k.Components {
		if !filepath.IsAbs(c) {
			c = filepath.Join(dir, c)
		}
		if err := checkComponent(c, append(parents, dir)); err != nil {
			return fmt.Errorf("nested component %s: %w", c, err)
		}
	}
	return nil
}

// readKustomization reads the one kustomization file in dir, under any name
// kustomize recognizes, and returns its name and contents.
func readKustomization(dir string) (string, []byte, error) {
	var name string
	var data []byte
	for _, n := range konfig.RecognizedKustomizationFileNames() {
		d, err := os.ReadFile(filepath.Join(dir, n))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		if name != "" {
			return "", nil, fmt.Errorf("has both %s and %s", name, n)
		}
		name, data = n, d
	}
	if name == "" {
		return "", nil, fmt.Errorf("no kustomization file, want one of %s", strings.Join(konfig.RecognizedKustomizationFileNames(), ", "))
	}
	return name, data, nil
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
	var b strings.Builder
	b.WriteString("apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n")
	rel, err := relativeTo(dir, resource)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(&b, "  - %s\n", rel)
	b.WriteString("components:\n")
	for _, component := range components {
		rel, err := relativeTo(dir, component)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(&b, "  - %s\n", rel)
	}

	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(b.String()), 0o600); err != nil {
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
