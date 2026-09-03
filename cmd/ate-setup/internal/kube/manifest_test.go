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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// writeFiles populates a temporary directory and returns its path.
func writeFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// manifest returns a minimal object whose name identifies it in assertions.
func manifest(name string) string {
	return "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\n"
}

func names(t *testing.T, data []byte) []string {
	t.Helper()
	objs, err := DecodeManifestBytes(data)
	if err != nil {
		t.Fatalf("DecodeManifestBytes() = %v", err)
	}
	out := make([]string, 0, len(objs))
	for _, obj := range objs {
		out = append(out, obj.GetName())
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ReadPath feeds the pre-built image resolver, and its output is applied in
// place of what LoadPath would have applied. The two must therefore cover the
// same documents in the same order: the install depends on that ordering, and
// a divergence would be a silently different install rather than an error.
func TestReadPathMatchesLoadPath(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name:  "single file",
			files: map[string]string{"a.yaml": manifest("only")},
			want:  []string{"only"},
		},
		{
			name: "directory is lexical, not filesystem order",
			files: map[string]string{
				"20-second.yaml": manifest("second"),
				"10-first.yaml":  manifest("first"),
				"30-third.yaml":  manifest("third"),
			},
			want: []string{"first", "second", "third"},
		},
		{
			name: "non-manifest extensions are skipped",
			files: map[string]string{
				"keep.yaml":     manifest("keep"),
				"keep2.yml":     manifest("keep2"),
				"keep3.json":    `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"keep3"}}`,
				"skip.tmpl":     manifest("skip"),
				"skip.md":       "# not a manifest\n",
				"skip.yaml.bak": manifest("skip"),
			},
			want: []string{"keep", "keep2", "keep3"},
		},
		{
			name: "nested directories are not recursed into",
			files: map[string]string{
				"top.yaml":         manifest("top"),
				"nested/deep.yaml": manifest("deep"),
			},
			want: []string{"top"},
		},
		{
			name: "a file with no trailing newline does not merge into the next",
			files: map[string]string{
				"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a",
				"b.yaml": manifest("b"),
			},
			want: []string{"a", "b"},
		},
		{
			name: "an empty file contributes no document",
			files: map[string]string{
				"a.yaml":     manifest("a"),
				"empty.yaml": "",
			},
			want: []string{"a"},
		},
		{
			name: "multi-document files keep their internal order",
			files: map[string]string{
				"a.yaml": manifest("one") + "---\n" + manifest("two"),
			},
			want: []string{"one", "two"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeFiles(t, tc.files)

			// A single-file case addresses the file directly, which is the
			// shape the manifest-path call sites use.
			path := dir
			if len(tc.files) == 1 {
				for name := range tc.files {
					path = filepath.Join(dir, name)
				}
			}

			data, err := ReadPath(path)
			if err != nil {
				t.Fatalf("ReadPath() = %v", err)
			}
			got := names(t, data)
			if !equal(got, tc.want) {
				t.Errorf("ReadPath() objects = %v, want %v", got, tc.want)
			}

			objs, err := LoadPath(path)
			if err != nil {
				t.Fatalf("LoadPath() = %v", err)
			}
			loaded := make([]string, 0, len(objs))
			for _, obj := range objs {
				loaded = append(loaded, obj.GetName())
			}
			if !equal(loaded, got) {
				t.Errorf("LoadPath() objects = %v, ReadPath() objects = %v; want identical", loaded, got)
			}
		})
	}
}

// The manifest-path call sites hand ReadPath a path that may not exist on a
// teardown, and DeletePath distinguishes that from a real failure by checking
// for fs.ErrNotExist. Keep the wrapping transparent to errors.Is.
func TestReadPathMissing(t *testing.T) {
	if _, err := ReadPath(filepath.Join(t.TempDir(), "absent.yaml")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadPath(absent) = %v, want a not-exist error", err)
	}
}
