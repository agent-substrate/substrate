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
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/tarutil"
)

// mkVolumeDirs creates the per-volume directories atelet prepares.
func mkVolumeDirs(t *testing.T, dir string, vols ...string) {
	t.Helper()
	for _, v := range vols {
		if err := os.MkdirAll(filepath.Join(dir, v), 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDurableVolumesRoundTrip(t *testing.T) {
	src := t.TempDir()
	mkVolumeDirs(t, src, "data", "cache")
	for rel, content := range map[string]string{
		"data/notes.txt":        "kept",
		"data/.gvisor.filestat": "internal",
		"cache/c.bin":           "cached",
	} {
		if err := os.WriteFile(filepath.Join(src, rel), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// The volume root's own mode is the mount point's, so it must survive.
	if err := os.Chmod(filepath.Join(src, "data"), 0o750); err != nil {
		t.Fatal(err)
	}
	checkpointDir := t.TempDir()
	files, err := tarDurableVolumes(t.Context(), src, checkpointDir, []string{"cache", "data"})
	if err != nil {
		t.Fatalf("tarDurableVolumes: %v", err)
	}
	if want := []string{"durable-dir-cache.tar", "durable-dir-data.tar"}; !slices.Equal(files, want) {
		t.Errorf("tarDurableVolumes files = %v, want %v", files, want)
	}

	dst := t.TempDir()
	mkVolumeDirs(t, dst, "data", "cache")
	if err := untarDurableVolumes(dst, checkpointDir, []string{"cache", "data"}); err != nil {
		t.Fatalf("untarDurableVolumes: %v", err)
	}
	for rel, want := range map[string]string{"data/notes.txt": "kept", "cache/c.bin": "cached"} {
		if got, err := os.ReadFile(filepath.Join(dst, rel)); err != nil || string(got) != want {
			t.Errorf("restored %q = %q, %v; want %q", rel, got, err, want)
		}
	}
	if _, err := os.Lstat(filepath.Join(dst, "data/.gvisor.filestat")); !os.IsNotExist(err) {
		t.Errorf(".gvisor.filestat: Lstat = %v; want it skipped", err)
	}
	if fi, err := os.Stat(filepath.Join(dst, "data")); err != nil || fi.Mode().Perm() != 0o750 {
		t.Errorf("restored volume dir: Stat = %v, %v; want mode 0750", fi, err)
	}
}

// runsc bind-mounts <dir>/<volume> by path, so the snapshot must not be able
// to replace that directory with a symlink out of the actor's tree.
func TestUntarDurableVolumesCannotPlantVolumeDir(t *testing.T) {
	victim := t.TempDir()
	planted := t.TempDir()
	if err := os.Symlink(victim, filepath.Join(planted, "data")); err != nil {
		t.Fatal(err)
	}
	snapshotDir := t.TempDir()
	// An entry named after the volume, as the old single-tar layout had.
	if err := tarutil.Create(t.Context(), filepath.Join(snapshotDir, "durable-dir-data.tar"), planted); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	mkVolumeDirs(t, dst, "data")
	if err := untarDurableVolumes(dst, snapshotDir, []string{"data"}); err != nil {
		t.Fatalf("untarDurableVolumes: %v", err)
	}
	if fi, err := os.Lstat(filepath.Join(dst, "data")); err != nil || !fi.IsDir() {
		t.Errorf("data: Lstat = %v, %v; want a real directory", fi, err)
	}
	entries, err := os.ReadDir(victim)
	if err != nil || len(entries) != 0 {
		t.Errorf("victim: ReadDir = %v, %v; want it untouched", entries, err)
	}
}

// A volume added to the template after the snapshot has no tar: it restores
// empty rather than failing.
func TestUntarDurableVolumesMissingTarIsEmpty(t *testing.T) {
	dst := t.TempDir()
	mkVolumeDirs(t, dst, "new")
	if err := untarDurableVolumes(dst, t.TempDir(), []string{"new"}); err != nil {
		t.Fatalf("untarDurableVolumes: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(dst, "new"))
	if err != nil || len(entries) != 0 {
		t.Errorf("new: ReadDir = %v, %v; want an empty directory", entries, err)
	}
}

func TestDurableTarFileRejectsBadNames(t *testing.T) {
	for _, v := range []string{"", ".", "..", "a/b", "/abs", "../x"} {
		if _, err := durableTarFile(v); err == nil {
			t.Errorf("durableTarFile(%q) = nil error, want one", v)
		}
	}
}
