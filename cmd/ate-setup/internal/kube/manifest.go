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

package kube

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// manifestExtensions are the file suffixes kubectl treats as manifests when
// given a directory.
var manifestExtensions = map[string]bool{
	".yaml": true,
	".yml":  true,
	".json": true,
}

// DecodeManifest splits a multi-document YAML stream into objects, preserving
// document order. Empty documents are skipped the way kubectl skips them.
func DecodeManifest(r io.Reader) ([]*unstructured.Unstructured, error) {
	var objs []*unstructured.Unstructured
	decoder := yaml.NewYAMLOrJSONDecoder(r, 4096)
	for {
		raw := map[string]any{}
		err := decoder.Decode(&raw)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("while decoding a manifest document: %w", err)
		}
		if len(raw) == 0 {
			continue
		}
		obj := &unstructured.Unstructured{Object: raw}
		if obj.GetKind() == "" {
			return nil, fmt.Errorf("manifest document has no kind")
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

// DecodeManifestBytes decodes a multi-document YAML stream held in memory.
func DecodeManifestBytes(data []byte) ([]*unstructured.Unstructured, error) {
	return DecodeManifest(bytes.NewReader(data))
}

// LoadPath reads a manifest file, or every manifest directly inside a
// directory.
//
// Directory handling deliberately mirrors `kubectl apply -f <dir>`: it is
// non-recursive and processes files in lexical order. deploy_ate_system and
// delete_ate_system in the shell installer depend on that ordering, and the
// comments there call out specific filename-ordering hazards, so recursing or
// reordering here would change install behavior.
func LoadPath(path string) ([]*unstructured.Unstructured, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", path, err)
	}
	if !info.IsDir() {
		return loadFile(path)
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("while listing %s: %w", path, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !manifestExtensions[strings.ToLower(filepath.Ext(entry.Name()))] {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)

	var objs []*unstructured.Unstructured
	for _, name := range names {
		fileObjs, err := loadFile(filepath.Join(path, name))
		if err != nil {
			return nil, err
		}
		objs = append(objs, fileObjs...)
	}
	return objs, nil
}

func loadFile(path string) ([]*unstructured.Unstructured, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("while opening %s: %w", path, err)
	}
	defer f.Close()

	objs, err := DecodeManifest(f)
	if err != nil {
		return nil, fmt.Errorf("in %s: %w", path, err)
	}
	return objs, nil
}

// Describe renders an object reference for log output, in kubectl's
// kind/name form.
func Describe(obj *unstructured.Unstructured) string {
	kind := strings.ToLower(obj.GetKind())
	if ns := obj.GetNamespace(); ns != "" {
		return fmt.Sprintf("%s/%s -n %s", kind, obj.GetName(), ns)
	}
	return fmt.Sprintf("%s/%s", kind, obj.GetName())
}
