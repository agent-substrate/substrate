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

	"github.com/agent-substrate/substrate/internal/tarutil"
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
	containers := []string{"app_ovl", "sidecar_ovl"}
	src := upperDirWith(t, files)
	// The upperdir's own mode is the container's /, so it must survive too.
	if err := os.Chmod(filepath.Join(src, "app_ovl/fs"), 0o750); err != nil {
		t.Fatal(err)
	}
	checkpointDir := t.TempDir()
	if err := tarRootfsUpper(t.Context(), src, checkpointDir, containers); err != nil {
		t.Fatalf("tarRootfsUpper: %v", err)
	}
	// Restore: onto a directory holding a stale previous activation's contents,
	// which must not leak into the restored overlay state.
	dst := upperDirWith(t, map[string]string{"app_ovl/fs/stale.txt": "stale"})
	if err := untarRootfsUpper(dst, checkpointDir, containers); err != nil {
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
	if fi, err := os.Stat(filepath.Join(dst, "app_ovl/fs")); err != nil || fi.Mode().Perm() != 0o750 {
		t.Errorf("restored upperdir: Stat = %v, %v; want mode 0750", fi, err)
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

// The snapshot is untrusted, so it must not decide any directory above the
// upperdirs: a symlink there would point the overlay mount, or the workdir
// wipe in kata.StageMergedRootfs, outside the actor's directory.
func TestUntarRootfsUpperCannotPlantLayout(t *testing.T) {
	victim := t.TempDir()
	// A symlink at the tar's root has no name to land on, and one at "fs"
	// would only land inside the upperdir as guest content.
	planted := upperDirWith(t, nil)
	for _, link := range []string{"fs", "work"} {
		if err := os.Symlink(victim, filepath.Join(planted, link)); err != nil {
			t.Fatal(err)
		}
	}
	snapshotDir := t.TempDir()
	if err := tarutil.Create(t.Context(), filepath.Join(snapshotDir, "rootfs-upper-app.tar"), planted); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	if err := untarRootfsUpper(dst, snapshotDir, []string{"app"}); err != nil {
		t.Fatalf("untarRootfsUpper: %v", err)
	}
	if fi, err := os.Lstat(filepath.Join(dst, "app")); err != nil || !fi.IsDir() {
		t.Errorf("app: Lstat = %v, %v; want a real directory", fi, err)
	}
	if fi, err := os.Lstat(filepath.Join(dst, "app", "fs")); err != nil || !fi.IsDir() {
		t.Errorf("app/fs: Lstat = %v, %v; want a real directory", fi, err)
	}
	if _, err := os.Lstat(filepath.Join(dst, "app", "work")); !os.IsNotExist(err) {
		t.Errorf("app/work: Lstat = %v; want it absent (created by the overlay staging)", err)
	}
}

func TestRootfsUpperTarFileRejectsBadNames(t *testing.T) {
	for _, cid := range []string{"", ".", "..", "a/b", "/abs", "../x"} {
		if _, err := rootfsUpperTarFile(cid); err == nil {
			t.Errorf("rootfsUpperTarFile(%q) = nil error, want one", cid)
		}
	}
	if err := untarRootfsUpper(t.TempDir(), t.TempDir(), []string{"../x"}); err == nil {
		t.Error("untarRootfsUpper with container ../x = nil error, want one")
	}
}

// A container in the request without a tar in the snapshot fails the restore
// rather than starting it on a silently empty rootfs upper.
func TestUntarRootfsUpperMissingTar(t *testing.T) {
	if err := untarRootfsUpper(t.TempDir(), t.TempDir(), []string{"app"}); err == nil {
		t.Error("untarRootfsUpper with no tar = nil error, want one")
	}
}
