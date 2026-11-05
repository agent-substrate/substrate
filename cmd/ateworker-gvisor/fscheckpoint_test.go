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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/protobuf/testing/protocmp"
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
		"fs/data/=app:/var/data",
		"fs/cache/=app:/var/cache",
		"fs/logs/=sidecar:/var/log/app",
	}
	if !slices.Equal(got, want) {
		t.Errorf("durableFSCheckpointPaths() = %v, want %v", got, want)
	}
}

func TestDurableSnapshotFiles(t *testing.T) {
	for _, tc := range []struct {
		name string
		// durable are the durable-dir bundle files in the layout.
		durable []string
	}{
		{name: "one bundle", durable: []string{"fs/fscheckpoint.pb", "fs/multitar.img", "fs/pages.img", "fs/pages_meta.img"}},
		{name: "a bundle per volume", durable: []string{
			"fs/cache/fscheckpoint.pb", "fs/cache/multitar.img",
			"fs/data/fscheckpoint.pb", "fs/data/multitar.img", "fs/data/pages.img",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshotFiles := slices.Concat([]string{"checkpoint.img"}, tc.durable, []string{
				"pages.img",
				"pages_meta.img",
				"rootfs/fscheckpoint.pb",
				"rootfs/multitar.img",
				"rootfs/pages.img",
				"rootfs/pages_meta.img",
			})
			if got := durableSnapshotFiles(snapshotFiles); !slices.Equal(got, tc.durable) {
				t.Errorf("durableSnapshotFiles() = %v, want %v", got, tc.durable)
			}
		})
	}
}

// A checkpoint that wrote per-volume bundles carries the layout with a rule
// that keeps it off workers without it. One with no durable-dir bundle, or an
// old one bundle, carries nothing.
func TestSnapshotAttributes(t *testing.T) {
	perVolume := []*ateworkerpb.AttributeEntry{{
		Key:              "worker.durable_fs_layout",
		Value:            "per_volume",
		CompatibleValues: []*ateworkerpb.CompatibleValue{{Value: "", Fidelity: ateworkerpb.CompatibleFidelity_COMPATIBLE_FIDELITY_NONE}},
	}}
	for _, tc := range []struct {
		name  string
		files []string
		want  []*ateworkerpb.AttributeEntry
	}{
		{name: "memory only, no durable dirs", files: []string{"checkpoint.img", "pages.img", "rootfs/fscheckpoint.pb", "rootfs/pages.img"}},
		{name: "one bundle written by an older worker", files: []string{"checkpoint.img", "fs/fscheckpoint.pb", "fs/pages.img"}},
		{name: "a bundle per volume", files: []string{"checkpoint.img", "fs/data/fscheckpoint.pb", "fs/data/pages.img", "rootfs/fscheckpoint.pb"}, want: perVolume},
		{name: "a VOLUMES checkpoint per volume", files: []string{"fs/data/fscheckpoint.pb", "fs/logs/fscheckpoint.pb"}, want: perVolume},
		{name: "nothing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, snapshotAttributes(tc.files), protocmp.Transform()); diff != "" {
				t.Errorf("snapshotAttributes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
	// The worker reads both layouts, so it restores an old snapshot in full.
	wantRegistered := []*ateletpb.AttributeEntry{{
		Key:              "worker.durable_fs_layout",
		Value:            "per_volume",
		CompatibleValues: []*ateletpb.CompatibleValue{{Value: "", Fidelity: ateletpb.CompatibleFidelity_COMPATIBLE_FIDELITY_MEMORY}},
	}}
	if diff := cmp.Diff(wantRegistered, registeredAttributes(), protocmp.Transform()); diff != "" {
		t.Errorf("registeredAttributes() mismatch (-want +got):\n%s", diff)
	}
}

func TestListSnapshotFilesWithFSSubdir(t *testing.T) {
	for _, tc := range []struct {
		name string
		// fs are the durable-dir bundle files the layout writes, sorted.
		fs []string
	}{
		{name: "one bundle", fs: []string{"fs/fscheckpoint.pb", "fs/multitar.img", "fs/pages.img", "fs/pages_meta.img"}},
		{name: "a bundle per volume", fs: []string{
			"fs/cache/fscheckpoint.pb", "fs/cache/multitar.img",
			"fs/data/fscheckpoint.pb", "fs/data/multitar.img", "fs/data/pages.img", "fs/data/pages_meta.img",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkpointDir := t.TempDir()
			others := []string{
				"checkpoint.img",
				"pages.img",
				"pages_meta.img",
				"rootfs/fscheckpoint.pb",
				"rootfs/multitar.img",
				"rootfs/pages.img",
				"rootfs/pages_meta.img",
			}
			for _, rel := range slices.Concat(others, tc.fs) {
				if err := os.MkdirAll(filepath.Dir(filepath.Join(checkpointDir, rel)), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(checkpointDir, rel), []byte("data"), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			got, err := listSnapshotFiles(checkpointDir)
			if err != nil {
				t.Fatalf("listSnapshotFiles() = %v", err)
			}
			want := slices.Sorted(slices.Values(slices.Concat(others, tc.fs)))
			if !slices.Equal(got, want) {
				t.Errorf("listSnapshotFiles() = %v, want %v", got, want)
			}
		})
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

// A worker that writes a bundle per volume restores both layouts at every
// fidelity.
func TestFSRestoreArgsLayouts(t *testing.T) {
	const (
		volumes = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
		rootfs  = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		memory  = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	)
	for _, tc := range []struct {
		name string
		// manifests are the bundles in the checkpoint, by directory, and
		// emptyDirs directories under fs/ that hold no bundle.
		manifests []string
		emptyDirs []string
		// wantDurable are the durable-dir bundle dirs restored, after rootfs/
		// at ROOTFS and MEMORY.
		wantDurable []string
	}{
		{name: "one bundle", manifests: []string{"rootfs", "fs"}, wantDurable: []string{"fs"}},
		{name: "a bundle per volume", manifests: []string{"rootfs", "fs/data", "fs/cache"}, wantDurable: []string{"fs/cache", "fs/data"}},
		{name: "a volume dir without a bundle is skipped", manifests: []string{"rootfs", "fs/data"}, emptyDirs: []string{"fs/cache"}, wantDurable: []string{"fs/data"}},
		{name: "no durable dirs", manifests: []string{"rootfs"}},
	} {
		for _, fidelity := range []ateworkerpb.SnapshotFidelity{volumes, rootfs, memory} {
			t.Run(tc.name+"/"+fidelity.String(), func(t *testing.T) {
				checkpointDir := t.TempDir()
				for _, dir := range tc.manifests {
					if err := os.MkdirAll(filepath.Join(checkpointDir, dir), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(checkpointDir, dir, "fscheckpoint.pb"), []byte("manifest"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				for _, dir := range tc.emptyDirs {
					if err := os.MkdirAll(filepath.Join(checkpointDir, dir), 0o700); err != nil {
						t.Fatal(err)
					}
				}
				var want []string
				if fidelity != volumes {
					want = append(want, "--fs-restore-image-path", filepath.Join(checkpointDir, "rootfs"))
				}
				for _, dir := range tc.wantDurable {
					want = append(want, "--fs-restore-image-path", filepath.Join(checkpointDir, dir))
				}
				got, err := fsRestoreArgs(checkpointDir, fidelity)
				if err != nil {
					t.Fatalf("fsRestoreArgs() = %v", err)
				}
				if !slices.Equal(got, want) {
					t.Errorf("fsRestoreArgs() = %v, want %v", got, want)
				}
			})
		}
	}
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
