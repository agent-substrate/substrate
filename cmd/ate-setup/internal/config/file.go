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

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	yaml "gopkg.in/yaml.v3"
)

// The identifying header every configuration document carries. Versioning the
// document from the start means a later schema change can be detected rather
// than mis-parsed.
const (
	FileAPIVersion = "install.ate.dev/v1alpha1"
	FileKind       = "SubstrateInstall"
)

// File is a parsed configuration document. Settings are held flattened, keyed
// by the same dotted keys Registry declares, so lookups need no knowledge of
// how the YAML was nested.
type File struct {
	// Path is where the document was read from, for error messages.
	Path string

	values map[string]string
}

// reserved names the top-level keys that are not settings: the two header
// fields, and the metadata ate-setup writes into its own records. Skipping
// the last is what lets a record be handed straight back to --config with no
// step to strip anything first.
var reserved = map[string]bool{
	"apiVersion":        true,
	"kind":              true,
	documentMetadataKey: true,
}

// ParseFile reads a configuration document.
//
// Unknown keys are rejected rather than ignored: a typo in a setting name
// would otherwise leave the install silently running on the default.
func ParseFile(path string) (*File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		// Name the absolute path: a relative one means different files from
		// different working directories, and the shell installer changes to
		// the repository root before running.
		abs, absErr := filepath.Abs(path)
		if absErr != nil {
			abs = path
		}
		return nil, fmt.Errorf("reading config %s: %w", abs, err)
	}

	// Decoded as a node tree rather than into a map, so a scalar keeps the
	// text that was written. Resolving it to a Go type first would hand a
	// string setting 0.1 for "0.10" and 83 for "0123", and an image tag or an
	// account id is exactly the kind of value typed unquoted by hand.
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	var doc *yaml.Node
	if len(root.Content) > 0 {
		doc = root.Content[0]
	}
	if doc != nil && doc.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s: the document must be a mapping", path)
	}

	if got := headerField(doc, "apiVersion"); got != FileAPIVersion {
		return nil, fmt.Errorf("%s: apiVersion must be %s, got %q", path, FileAPIVersion, got)
	}
	if got := headerField(doc, "kind"); got != FileKind {
		return nil, fmt.Errorf("%s: kind must be %s, got %q", path, FileKind, got)
	}

	flat := make(map[string]string)
	if err := flatten("", doc, flat); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	var unknown []string
	for k := range flat {
		if _, ok := Lookup(k); !ok {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("%s: unknown setting%s %s", path,
			plural(len(unknown)), strings.Join(quoteAll(unknown), ", "))
	}

	return &File{Path: path, values: flat}, nil
}

// Get returns the raw value for a canonical key.
func (f *File) Get(key string) (string, bool) {
	v, ok := f.values[key]
	return v, ok
}

// Keys returns every key the document set, sorted.
func (f *File) Keys() []string {
	out := make([]string, 0, len(f.values))
	for k := range f.values {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// headerField reads a top-level scalar, for the two fields checked before any
// setting is.
func headerField(doc *yaml.Node, name string) string {
	if doc == nil {
		return ""
	}
	for i := 0; i+1 < len(doc.Content); i += 2 {
		if doc.Content[i].Value == name && doc.Content[i+1].Kind == yaml.ScalarNode {
			return doc.Content[i+1].Value
		}
	}
	return ""
}

// flatten walks the document into dotted keys, carrying each scalar's text
// through unchanged so the file and the environment feed the resolver
// identical input.
//
// A key given twice is an error rather than a silent win for one of them. The
// two spellings reach it differently: repeated inside one mapping, caught by
// the per-mapping set; written dotted beside its nested form, caught by the
// collision in out, which would otherwise resolve by map iteration order and
// make the same document parse two ways.
func flatten(prefix string, node *yaml.Node, out map[string]string) error {
	if node == nil {
		return nil
	}
	seen := make(map[string]bool, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		name, value := node.Content[i].Value, node.Content[i+1]
		if prefix == "" && reserved[name] {
			continue
		}
		key := name
		if prefix != "" {
			key = prefix + "." + name
		}
		if seen[name] {
			return fmt.Errorf("setting %q is set twice", key)
		}
		seen[name] = true

		switch value.Kind {
		case yaml.MappingNode:
			if err := flatten(key, value, out); err != nil {
				return err
			}
			continue
		case yaml.SequenceNode:
			return fmt.Errorf("setting %q is a list; no setting takes a list", key)
		case yaml.ScalarNode:
		default:
			return fmt.Errorf("setting %q has an unsupported value", key)
		}

		if _, dup := out[key]; dup {
			return fmt.Errorf("setting %q is set twice", key)
		}
		if value.Tag == "!!null" {
			out[key] = ""
			continue
		}
		out[key] = value.Value
	}
	return nil
}

func quoteAll(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strconv.Quote(s)
	}
	return out
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
