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
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/agent-substrate/substrate/internal/apierror"
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

// writeLegacyDurableTar writes the snapshot file of the layout before one tar
// per volume as that layout's checkpoint did: one archive of the whole
// durable-dir volumes directory, its entries named <volume>/... .
func writeLegacyDurableTar(t *testing.T, snapshotDir string, files map[string]string) {
	t.Helper()
	src := t.TempDir()
	for rel, body := range files {
		p := filepath.Join(src, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarutil.Create(t.Context(), filepath.Join(snapshotDir, legacyDurableTarFile), src); err != nil {
		t.Fatal(err)
	}
}

// A snapshot in the layout before one tar per volume restores its volumes:
// the restore converts it into one tar per volume first.
func TestUntarDurableVolumesConvertsLegacyLayout(t *testing.T) {
	dst, snap := t.TempDir(), t.TempDir()
	mkVolumeDirs(t, dst, "data", "cache", "new")
	writeLegacyDurableTar(t, snap, map[string]string{
		"data/app.db":           "state",
		"data/sessions/s1.json": "turn",
		"data/.gvisor.filestat": "internal",
		"cache/c.bin":           "cached",
	})

	if err := untarDurableVolumes(dst, snap, []string{"cache", "data", "new"}); err != nil {
		t.Fatalf("untarDurableVolumes: %v", err)
	}
	for rel, want := range map[string]string{
		"data/app.db":           "state",
		"data/sessions/s1.json": "turn",
		"cache/c.bin":           "cached",
	} {
		if got, err := os.ReadFile(filepath.Join(dst, rel)); err != nil || string(got) != want {
			t.Errorf("restored %q = %q, %v; want %q", rel, got, err, want)
		}
	}
	if _, err := os.Lstat(filepath.Join(dst, "data/.gvisor.filestat")); !os.IsNotExist(err) {
		t.Errorf(".gvisor.filestat: Lstat = %v; want it removed", err)
	}
	if entries, err := os.ReadDir(filepath.Join(dst, "new")); err != nil || len(entries) != 0 {
		t.Errorf("new: ReadDir = %v, %v; want an empty directory", entries, err)
	}
	for _, name := range []string{"durable-dir-data.tar", "durable-dir-cache.tar"} {
		if _, err := os.Stat(filepath.Join(snap, name)); err != nil {
			t.Errorf("converted %s: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(snap, "durable-dir-new.tar")); !os.IsNotExist(err) {
		t.Errorf("durable-dir-new.tar: Stat = %v; want no tar for a volume without entries", err)
	}

	// The conversion is idempotent: a second restore of the same directory
	// converts again and restores the same data.
	dst2 := t.TempDir()
	mkVolumeDirs(t, dst2, "cache", "data", "new")
	if err := untarDurableVolumes(dst2, snap, []string{"cache", "data", "new"}); err != nil {
		t.Fatalf("second untarDurableVolumes: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(dst2, "data/app.db")); err != nil || string(got) != "state" {
		t.Errorf("second restore data/app.db = %q, %v; want %q", got, err, "state")
	}
}

// A snapshot in the current layout is restored as it is, the snapshot
// directory untouched.
func TestUntarDurableVolumesCurrentLayoutUnchanged(t *testing.T) {
	src := t.TempDir()
	mkVolumeDirs(t, src, "data")
	if err := os.WriteFile(filepath.Join(src, "data/app.db"), []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	snap := t.TempDir()
	if _, err := tarDurableVolumes(t.Context(), src, snap, []string{"data"}); err != nil {
		t.Fatal(err)
	}
	before := snapshotFiles(t, snap)

	dst := t.TempDir()
	mkVolumeDirs(t, dst, "data")
	if err := untarDurableVolumes(dst, snap, []string{"data"}); err != nil {
		t.Fatalf("untarDurableVolumes: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(dst, "data/app.db")); err != nil || string(got) != "state" {
		t.Errorf("restored data/app.db = %q, %v; want %q", got, err, "state")
	}
	if after := snapshotFiles(t, snap); !maps.Equal(before, after) {
		t.Errorf("snapshot directory changed by the restore: before %v, after %v", slices.Sorted(maps.Keys(before)), slices.Sorted(maps.Keys(after)))
	}
}

// snapshotFiles returns the contents of each file in dir by name.
func snapshotFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[e.Name()] = string(b)
	}
	return files
}

// A snapshot in the layout before one tar per volume that does not convert is
// refused with the cause instead of restoring the volumes empty, and the
// snapshot directory gains no partial conversion.
func TestUntarDurableVolumesRefusesUnconvertibleLegacyLayout(t *testing.T) {
	for name, write := range map[string]func(t *testing.T, snap string){
		"entry outside every volume": func(t *testing.T, snap string) {
			writeLegacyDurableTar(t, snap, map[string]string{"data/app.db": "state", "stray/x": "x"})
		},
		"truncated archive": func(t *testing.T, snap string) {
			writeLegacyDurableTar(t, snap, map[string]string{"data/app.db": strings.Repeat("s", 4096)})
			p := filepath.Join(snap, legacyDurableTarFile)
			if err := os.Truncate(p, 2048); err != nil {
				t.Fatal(err)
			}
		},
		"not a tar": func(t *testing.T, snap string) {
			if err := os.WriteFile(filepath.Join(snap, legacyDurableTarFile), []byte(strings.Repeat("garbage", 200)), 0o600); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			dst, snap := t.TempDir(), t.TempDir()
			mkVolumeDirs(t, dst, "data")
			write(t, snap)
			before := snapshotFiles(t, snap)

			err := untarDurableVolumes(dst, snap, []string{"data"})
			if !errors.Is(err, errLegacySnapshotLayout) {
				t.Fatalf("untarDurableVolumes = %v; want %v", err, errLegacySnapshotLayout)
			}
			if got := apierror.Code(err); got != codes.FailedPrecondition {
				t.Errorf("code = %v; want %v", got, codes.FailedPrecondition)
			}
			if entries, rerr := os.ReadDir(filepath.Join(dst, "data")); rerr != nil || len(entries) != 0 {
				t.Errorf("data: ReadDir = %v, %v; want it untouched", entries, rerr)
			}
			if after := snapshotFiles(t, snap); !maps.Equal(before, after) {
				t.Errorf("snapshot directory changed by the refused restore: %v", slices.Sorted(maps.Keys(after)))
			}
		})
	}
}

func TestDurableTarFileRejectsBadNames(t *testing.T) {
	for _, v := range []string{"", ".", "..", "a/b", "/abs", "../x"} {
		if _, err := durableTarFile(v); err == nil {
			t.Errorf("durableTarFile(%q) = nil error, want one", v)
		}
	}
}
