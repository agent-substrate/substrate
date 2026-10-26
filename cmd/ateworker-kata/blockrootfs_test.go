//go:build linux

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

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestImageBytes(t *testing.T) {
	tests := []struct {
		name string
		used int64
		want int64
	}{
		{"empty rootfs gets the minimum", 0, blockRootfsMin},
		{"small rootfs gets the minimum", 10 << 20, blockRootfsMin},
		{"large rootfs gets half again", 1 << 30, 3 << 29},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := imageBytes(tt.used); got != tt.want {
				t.Errorf("imageBytes(%d) = %d, want %d", tt.used, got, tt.want)
			}
		})
	}
}

func TestTreeSize(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	for path, size := range map[string]int{"x": 100, "a/y": 4097, "a/b/z": 0} {
		if err := os.WriteFile(filepath.Join(dir, path), make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A symlink takes no blocks: its target is not copied into the image.
	if err := os.Symlink("/etc/passwd", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	// 100 bytes take one block, 4097 take two, and none take none. The tree
	// has the directory, a, b, the three files and the link.
	bytes, entries, err := treeSize(dir)
	if err != nil || bytes != 3*blockSize || entries != 7 {
		t.Errorf("treeSize() = %d, %d, %v, want %d, 7", bytes, entries, err, 3*blockSize)
	}
	if _, _, err := treeSize(filepath.Join(dir, "missing")); err == nil {
		t.Error("treeSize() of a missing directory = nil, want an error")
	}
}
