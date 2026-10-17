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
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

func TestRootfsFSCheckpointPaths(t *testing.T) {
	containers := []*ateworkerpb.Container{
		{Name: "app"},
		{Name: "sidecar"},
	}

	got := rootfsFSCheckpointPaths(containers)
	want := []string{
		"rootfs/=app:/",
		"rootfs/=sidecar:/",
	}
	if !slices.Equal(got, want) {
		t.Errorf("rootfsFSCheckpointPaths() = %v, want %v", got, want)
	}
}

func TestDurableFSCheckpointPaths(t *testing.T) {
	containers := []*ateworkerpb.Container{
		{
			Name: "app",
			DurableDirVolumeMounts: []*ateworkerpb.VolumeMount{
				{Name: "data", MountPath: "/var/data"},
				{Name: "cache", MountPath: "/var/cache"},
			},
		},
		{
			Name: "sidecar",
			DurableDirVolumeMounts: []*ateworkerpb.VolumeMount{
				{Name: "logs", MountPath: "/var/log/app"},
			},
		},
	}

	got := durableFSCheckpointPaths(containers)
	want := []string{
		"fs/=app:/var/data",
		"fs/=app:/var/cache",
		"fs/=sidecar:/var/log/app",
	}
	if !slices.Equal(got, want) {
		t.Errorf("durableFSCheckpointPaths() = %v, want %v", got, want)
	}
}

func TestDurableSnapshotFiles(t *testing.T) {
	snapshotFiles := []string{
		"checkpoint.img",
		"fs/fscheckpoint.pb",
		"fs/multitar.img",
		"fs/pages.img",
		"fs/pages_meta.img",
		"pages.img",
		"pages_meta.img",
		"rootfs/fscheckpoint.pb",
		"rootfs/multitar.img",
		"rootfs/pages.img",
		"rootfs/pages_meta.img",
	}

	got := durableSnapshotFiles(snapshotFiles)
	want := []string{
		"fs/fscheckpoint.pb",
		"fs/multitar.img",
		"fs/pages.img",
		"fs/pages_meta.img",
	}
	if !slices.Equal(got, want) {
		t.Errorf("durableSnapshotFiles() = %v, want %v", got, want)
	}
}

func TestListSnapshotFilesWithFSSubdir(t *testing.T) {
	checkpointDir := t.TempDir()
	for _, sub := range []string{"fs", "rootfs"} {
		if err := os.MkdirAll(filepath.Join(checkpointDir, sub), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, rel := range []string{
		"checkpoint.img",
		"pages.img",
		"pages_meta.img",
		"rootfs/fscheckpoint.pb",
		"rootfs/multitar.img",
		"rootfs/pages.img",
		"rootfs/pages_meta.img",
		"fs/fscheckpoint.pb",
		"fs/multitar.img",
		"fs/pages.img",
		"fs/pages_meta.img",
	} {
		if err := os.WriteFile(filepath.Join(checkpointDir, rel), []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	got, err := listSnapshotFiles(checkpointDir)
	if err != nil {
		t.Fatalf("listSnapshotFiles() = %v", err)
	}
	want := []string{
		"checkpoint.img",
		"fs/fscheckpoint.pb",
		"fs/multitar.img",
		"fs/pages.img",
		"fs/pages_meta.img",
		"pages.img",
		"pages_meta.img",
		"rootfs/fscheckpoint.pb",
		"rootfs/multitar.img",
		"rootfs/pages.img",
		"rootfs/pages_meta.img",
	}
	if !slices.Equal(got, want) {
		t.Errorf("listSnapshotFiles() = %v, want %v", got, want)
	}
}

func TestFSRestoreArgs(t *testing.T) {
	t.Run("no filesystem checkpoint present", func(t *testing.T) {
		checkpointDir := t.TempDir()
		got, err := fsRestoreArgs(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES)
		if err != nil {
			t.Fatalf("fsRestoreArgs(VOLUMES) = %v", err)
		}
		if len(got) != 0 {
			t.Errorf("fsRestoreArgs(VOLUMES) = %v, want empty", got)
		}
		for _, fidelity := range []ateworkerpb.SnapshotFidelity{
			ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		} {
			if got, err := fsRestoreArgs(checkpointDir, fidelity); err == nil {
				t.Errorf("fsRestoreArgs(%v) = (%v, nil), want error when rootfs/ is missing", fidelity, got)
			}
		}
	})

	t.Run("only rootfs checkpoint present", func(t *testing.T) {
		checkpointDir := t.TempDir()
		rootfsDir := filepath.Join(checkpointDir, "rootfs")
		if err := os.MkdirAll(rootfsDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(rootfsDir, "fscheckpoint.pb"), []byte("manifest"), 0o600); err != nil {
			t.Fatal(err)
		}

		for _, fidelity := range []ateworkerpb.SnapshotFidelity{
			ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		} {
			got, err := fsRestoreArgs(checkpointDir, fidelity)
			if err != nil {
				t.Fatalf("fsRestoreArgs(%v) = %v", fidelity, err)
			}
			want := []string{
				"--fs-restore-image-path", rootfsDir,
			}
			if !slices.Equal(got, want) {
				t.Errorf("fsRestoreArgs(%v) = %v, want %v", fidelity, got, want)
			}
		}

		gotVolumes, err := fsRestoreArgs(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES)
		if err != nil {
			t.Fatalf("fsRestoreArgs(VOLUMES) = %v", err)
		}
		if len(gotVolumes) != 0 {
			t.Errorf("fsRestoreArgs(VOLUMES) = %v, want empty", gotVolumes)
		}
	})

	t.Run("ROOTFS and MEMORY with only fs checkpoint present fail", func(t *testing.T) {
		checkpointDir := t.TempDir()
		fsDir := filepath.Join(checkpointDir, "fs")
		if err := os.MkdirAll(fsDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(fsDir, "fscheckpoint.pb"), []byte("manifest"), 0o600); err != nil {
			t.Fatal(err)
		}

		for _, fidelity := range []ateworkerpb.SnapshotFidelity{
			ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		} {
			got, err := fsRestoreArgs(checkpointDir, fidelity)
			if err == nil {
				t.Errorf("fsRestoreArgs(%v) = (%v, nil), want error when rootfs/ is missing", fidelity, got)
			} else if !strings.Contains(err.Error(), "rootfs") {
				t.Errorf("fsRestoreArgs(%v) error = %q, want it to name the rootfs checkpoint", fidelity, err)
			}
		}

		gotVolumes, err := fsRestoreArgs(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES)
		if err != nil {
			t.Fatalf("fsRestoreArgs(VOLUMES) = %v", err)
		}
		if want := []string{"--fs-restore-image-path", fsDir}; !slices.Equal(gotVolumes, want) {
			t.Errorf("fsRestoreArgs(VOLUMES) = %v, want %v", gotVolumes, want)
		}
	})

	t.Run("both rootfs and fs subdir checkpoints present", func(t *testing.T) {
		checkpointDir := t.TempDir()
		rootfsDir := filepath.Join(checkpointDir, "rootfs")
		if err := os.MkdirAll(rootfsDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(rootfsDir, "fscheckpoint.pb"), []byte("manifest"), 0o600); err != nil {
			t.Fatal(err)
		}
		fsDir := filepath.Join(checkpointDir, "fs")
		if err := os.MkdirAll(fsDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(fsDir, "fscheckpoint.pb"), []byte("manifest"), 0o600); err != nil {
			t.Fatal(err)
		}

		for _, fidelity := range []ateworkerpb.SnapshotFidelity{
			ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		} {
			got, err := fsRestoreArgs(checkpointDir, fidelity)
			if err != nil {
				t.Fatalf("fsRestoreArgs(%v) = %v", fidelity, err)
			}
			want := []string{
				"--fs-restore-image-path", rootfsDir,
				"--fs-restore-image-path", fsDir,
			}
			if !slices.Equal(got, want) {
				t.Errorf("fsRestoreArgs(%v) = %v, want %v", fidelity, got, want)
			}
		}

		gotVolumes, err := fsRestoreArgs(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES)
		if err != nil {
			t.Fatalf("fsRestoreArgs(VOLUMES) = %v", err)
		}
		wantVolumes := []string{
			"--fs-restore-image-path", fsDir,
		}
		if !slices.Equal(gotVolumes, wantVolumes) {
			t.Errorf("fsRestoreArgs(VOLUMES) = %v, want %v", gotVolumes, wantVolumes)
		}
	})
}

func TestFSRestoreContainers(t *testing.T) {
	containers := []*ateworkerpb.Container{
		{
			Name: "init",
		},
		{
			Name: "app",
			DurableDirVolumeMounts: []*ateworkerpb.VolumeMount{
				{Name: "data", MountPath: "/var/data"},
			},
		},
		{
			Name: "sidecar-shared-only",
			DurableDirVolumeMounts: []*ateworkerpb.VolumeMount{
				{Name: "data", MountPath: "/mnt/data"},
			},
		},
		{
			Name: "sidecar-with-own-vol",
			DurableDirVolumeMounts: []*ateworkerpb.VolumeMount{
				{Name: "data", MountPath: "/mnt/data"},
				{Name: "logs", MountPath: "/var/log"},
			},
		},
	}

	for _, tc := range []struct {
		fidelity ateworkerpb.SnapshotFidelity
		want     []string
	}{
		// Only fs/ is restored, so only durable-dir mount owners wait.
		{ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, []string{"app", "sidecar-with-own-vol"}},
		// rootfs/ is restored for every container, so every container waits.
		{ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, []string{"init", "app", "sidecar-shared-only", "sidecar-with-own-vol"}},
		{ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, []string{"init", "app", "sidecar-shared-only", "sidecar-with-own-vol"}},
	} {
		if got := fsRestoreContainers(tc.fidelity, containers); !slices.Equal(got, tc.want) {
			t.Errorf("fsRestoreContainers(%v) = %v, want %v", tc.fidelity, got, tc.want)
		}
	}
}
