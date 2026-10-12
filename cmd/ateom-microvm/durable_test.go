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

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/tarutil"
)

func TestHasDurableVolumes(t *testing.T) {
	tests := []struct {
		name       string
		containers []*ateompb.Container
		want       bool
	}{
		{name: "no containers"},
		{
			name:       "container without durable volumes",
			containers: []*ateompb.Container{{Name: "app"}},
		},
		{
			name: "one of several containers has a durable volume",
			containers: []*ateompb.Container{
				{Name: "sidecar"},
				{Name: "app", DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
					{VolumeName: "data", MountPath: "/home/counter"},
				}},
			},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasDurableVolumes(tc.containers); got != tc.want {
				t.Errorf("hasDurableVolumes() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDurableVolumeNames(t *testing.T) {
	containers := []*ateompb.Container{
		{Name: "app", DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
			{VolumeName: "data", MountPath: "/data"},
			{VolumeName: "cache", MountPath: "/cache"},
		}},
		{Name: "sidecar", DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
			{VolumeName: "data", MountPath: "/shared"},
		}},
	}
	got := durableVolumeNames(containers)
	if want := []string{"cache", "data"}; !slices.Equal(got, want) {
		t.Errorf("durableVolumeNames() = %v, want %v", got, want)
	}
}

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
	// Checkpoint: every volume the actor has, archived while the guest is paused.
	src := t.TempDir()
	mkVolumeDirs(t, src, "data", "cache")
	for vol, content := range map[string]string{"data": "42", "cache": "7"} {
		if err := os.WriteFile(filepath.Join(src, vol, "a.txt"), []byte(content), 0o644); err != nil {
			t.Fatalf("writing %q content: %v", vol, err)
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

	// Restore: onto the empty directory atelet re-creates for the actor.
	dst := t.TempDir()
	mkVolumeDirs(t, dst, "data", "cache")
	if err := untarDurableVolumes(dst, checkpointDir, []string{"cache", "data"}); err != nil {
		t.Fatalf("untarDurableVolumes: %v", err)
	}
	// Both volumes come back, each under its own name: the names are what the
	// guest mount paths are built from after a restore onto another node.
	for vol, want := range map[string]string{"data": "42", "cache": "7"} {
		got, err := os.ReadFile(filepath.Join(dst, vol, "a.txt"))
		if err != nil {
			t.Errorf("reading restored %q content: %v", vol, err)
			continue
		}
		if string(got) != want {
			t.Errorf("restored %q content = %q, want %q", vol, got, want)
		}
	}
	if fi, err := os.Stat(filepath.Join(dst, "data")); err != nil || fi.Mode().Perm() != 0o750 {
		t.Errorf("restored volume dir: Stat = %v, %v; want mode 0750", fi, err)
	}
}

// kata-agent bind-mounts <dir>/<volume> by path inside the guest, so the
// snapshot must not be able to replace that directory with a symlink.
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
