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

package atomicfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWrite(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "state.json")

	// One shared write over an existing file; each subtest checks one
	// postcondition.
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatalf("seeding target: %v", err)
	}
	if err := Write(target, []byte("new"), 0o644); err != nil {
		t.Fatalf("Write: %v", err)
	}

	t.Run("replaces content", func(t *testing.T) {
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("reading target: %v", err)
		}
		if string(got) != "new" {
			t.Errorf("content = %q, want %q", got, "new")
		}
	})

	t.Run("sets permissions", func(t *testing.T) {
		info, err := os.Stat(target)
		if err != nil {
			t.Fatalf("stat target: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o644 {
			t.Errorf("perm = %o, want 644", perm)
		}
	})

	t.Run("leaves no temp files", func(t *testing.T) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading dir: %v", err)
		}
		if len(entries) != 1 {
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Errorf("files in the target's directory = %v, want only the target", names)
		}
	})
}

// TestWriteSync covers the durable variant: same contract as Write, the
// file holds exactly the data afterwards and no temp file is left.
func TestWriteSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runtime.json")
	if err := WriteSync(path, []byte("v1"), 0o600); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := WriteSync(path, []byte("v2"), 0o600); err != nil {
		t.Fatalf("WriteSync over an existing file: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "v2" {
		t.Fatalf("ReadFile = %q, %v; want v2", got, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("dir holds %d entries after WriteSync, want only the file", len(entries))
	}
}
