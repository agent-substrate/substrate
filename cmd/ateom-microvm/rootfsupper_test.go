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
	"strings"
	"testing"
)

// upperDirWith returns a rootfs upper directory laid out the way the host
// overlay staging builds one: <containerID>/{fs,work} per container, with the
// given files created under it (paths relative to the directory).
func upperDirWith(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for rel, content := range files {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("creating %q: %v", filepath.Dir(p), err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatalf("writing %q: %v", rel, err)
		}
	}
	return dir
}

func TestRootfsUpperRoundTrip(t *testing.T) {
	// Checkpoint: every container's upper, archived while the guest is paused.
	// The fs/ layout under the dir is exactly what find-paths re-opens; the
	// workdirs are deliberately left out of the archive (inert with index=off,
	// recreated at mount).
	files := map[string]string{
		"app_ovl/fs/home/agent/notes.txt": "rootfs write",
		"app_ovl/work/index":              "",
		"app_ovl/work/#1/tmp.bin":         "in-flight copy-up temp",
		"sidecar_ovl/fs/var/log/s.log":    "sidecar write",
	}
	src := upperDirWith(t, files)
	checkpointDir := t.TempDir()
	if err := tarRootfsUpper(t.Context(), src, checkpointDir); err != nil {
		t.Fatalf("tarRootfsUpper: %v", err)
	}
	// Restore: onto a directory holding a stale previous activation's contents,
	// which must not leak into the restored overlay state.
	dst := upperDirWith(t, map[string]string{"app_ovl/fs/stale.txt": "stale"})
	if err := untarRootfsUpper(dst, checkpointDir); err != nil {
		t.Fatalf("untarRootfsUpper: %v", err)
	}
	for rel, want := range files {
		if strings.Contains(rel, "/work/") {
			continue // asserted absent below
		}
		got, err := os.ReadFile(filepath.Join(dst, rel))
		if err != nil {
			t.Errorf("reading restored %q: %v", rel, err)
			continue
		}
		if string(got) != want {
			t.Errorf("restored %q = %q, want %q", rel, got, want)
		}
	}
	// The workdirs must NOT survive the round trip: they are excluded from the
	// archive (dead weight; overlayfs rebuilds them at mount).
	if _, err := os.Stat(filepath.Join(dst, "app_ovl/work")); !os.IsNotExist(err) {
		t.Errorf("workdir survived the snapshot round trip (stat err = %v), want it excluded", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "app_ovl/fs/stale.txt")); !os.IsNotExist(err) {
		t.Errorf("stale pre-restore content survived untarRootfsUpper (stat err = %v), want it wiped", err)
	}
}
