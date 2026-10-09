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
)

func TestRootfsFSCheckpointPaths(t *testing.T) {
	containers := []*ateompb.Container{
		{Name: "app"},
		{Name: "sidecar"},
	}

	got := rootfsFSCheckpointPaths(containers)
	want := []string{
		"app_rootfs=app:/",
		"sidecar_rootfs=sidecar:/",
	}
	if !slices.Equal(got, want) {
		t.Errorf("rootfsFSCheckpointPaths() = %v, want %v", got, want)
	}
}

func TestDurableFSCheckpointPaths(t *testing.T) {
	containers := []*ateompb.Container{
		{
			Name: "app",
			DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
				{VolumeName: "data", MountPath: "/var/data"},
				{VolumeName: "cache", MountPath: "/var/cache"},
			},
		},
		{
			Name: "sidecar",
			DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
				{VolumeName: "logs", MountPath: "/var/log/app"},
			},
		},
	}

	got := durableFSCheckpointPaths(containers)
	want := []string{
		"data=app:/var/data",
		"cache=app:/var/cache",
		"logs=sidecar:/var/log/app",
	}
	if !slices.Equal(got, want) {
		t.Errorf("durableFSCheckpointPaths() = %v, want %v", got, want)
	}
}

func TestDurableSnapshotFiles(t *testing.T) {
	snapshotFiles := []string{
		"app_rootfs_fscheckpoint.pb",
		"app_rootfs_multitar.img",
		"app_rootfs_pages.img",
		"app_rootfs_pages_meta.img",
		"cache_fscheckpoint.pb",
		"cache_multitar.img",
		"cache_pages.img",
		"cache_pages_meta.img",
		"checkpoint.img",
		"data_fscheckpoint.pb",
		"data_multitar.img",
		"data_pages.img",
		"data_pages_meta.img",
		"pages.img",
		"pages_meta.img",
	}

	got := durableSnapshotFiles(snapshotFiles, []string{"cache", "data"})
	want := []string{
		"cache_fscheckpoint.pb",
		"cache_multitar.img",
		"cache_pages.img",
		"cache_pages_meta.img",
		"data_fscheckpoint.pb",
		"data_multitar.img",
		"data_pages.img",
		"data_pages_meta.img",
	}
	if !slices.Equal(got, want) {
		t.Errorf("durableSnapshotFiles() = %v, want %v", got, want)
	}
}

func TestFSRestoreArgs(t *testing.T) {
	checkpointDir := t.TempDir()
	for _, name := range []string{"cache_fscheckpoint.pb", "data_fscheckpoint.pb"} {
		if err := os.WriteFile(filepath.Join(checkpointDir, name), []byte("manifest"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// "new" has no manifest in checkpointDir (added to template after snapshot),
	// so it is skipped and starts empty.
	got, err := fsRestoreArgs(checkpointDir, []string{"cache", "data", "new"})
	if err != nil {
		t.Fatalf("fsRestoreArgs() = %v", err)
	}
	want := []string{
		"--fs-restore-image-path", filepath.Join(checkpointDir, "cache"),
		"--fs-restore-image-path", filepath.Join(checkpointDir, "data"),
	}
	if !slices.Equal(got, want) {
		t.Errorf("fsRestoreArgs() = %v, want %v", got, want)
	}
}

func TestSplitRootfsAndDurableDirsRestoreModes(t *testing.T) {
	checkpointDir := t.TempDir()
	containers := []*ateompb.Container{
		{
			Name: "app",
			DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
				{VolumeName: "data", MountPath: "/var/data"},
				{VolumeName: "cache", MountPath: "/var/cache"},
			},
		},
	}
	durableVolumes := []string{"cache", "data"}

	// Verify checkpoint paths split rootfs and each durable-dir volume into its own bundle.
	gotCheckpointPaths := append(rootfsFSCheckpointPaths(containers), durableFSCheckpointPaths(containers)...)
	wantCheckpointPaths := []string{
		"app_rootfs=app:/",
		"data=app:/var/data",
		"cache=app:/var/cache",
	}
	if !slices.Equal(gotCheckpointPaths, wantCheckpointPaths) {
		t.Fatalf("split checkpoint paths = %v, want %v", gotCheckpointPaths, wantCheckpointPaths)
	}

	// Populate checkpointDir with manifests for both rootfs and durable-dir bundles.
	for _, prefix := range []string{"app_rootfs", "cache", "data"} {
		if err := os.WriteFile(filepath.Join(checkpointDir, prefix+"_fscheckpoint.pb"), []byte("manifest"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		name     string
		fidelity ateompb.SnapshotFidelity
		wantArgs []string
	}{
		{
			name:     "Full (MEMORY): restores rootfs and durable dirs",
			fidelity: ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			wantArgs: []string{
				"--fs-restore-image-path", filepath.Join(checkpointDir, "app_rootfs"),
				"--fs-restore-image-path", filepath.Join(checkpointDir, "cache"),
				"--fs-restore-image-path", filepath.Join(checkpointDir, "data"),
			},
		},
		{
			name:     "Rootfs+DurDir (ROOTFS): restores rootfs and durable dirs",
			fidelity: ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			wantArgs: []string{
				"--fs-restore-image-path", filepath.Join(checkpointDir, "app_rootfs"),
				"--fs-restore-image-path", filepath.Join(checkpointDir, "cache"),
				"--fs-restore-image-path", filepath.Join(checkpointDir, "data"),
			},
		},
		{
			name:     "Only DurDir (VOLUMES): restores only durable dirs, skipping rootfs",
			fidelity: ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			wantArgs: []string{
				"--fs-restore-image-path", filepath.Join(checkpointDir, "cache"),
				"--fs-restore-image-path", filepath.Join(checkpointDir, "data"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefixes := fsRestorePrefixes(tc.fidelity, containers, durableVolumes)
			gotArgs, err := fsRestoreArgs(checkpointDir, prefixes)
			if err != nil {
				t.Fatalf("fsRestoreArgs() = %v", err)
			}
			if !slices.Equal(gotArgs, tc.wantArgs) {
				t.Errorf("fsRestoreArgs(%v) = %v, want %v", tc.fidelity, gotArgs, tc.wantArgs)
			}
		})
	}
}
