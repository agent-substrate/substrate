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
		"app:/var/data",
		"app:/var/cache",
		"sidecar:/var/log/app",
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
	fsDir := filepath.Join(checkpointDir, "fs")
	if err := os.MkdirAll(fsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{
		"checkpoint.img",
		"pages.img",
		"pages_meta.img",
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
	}
	if !slices.Equal(got, want) {
		t.Errorf("listSnapshotFiles() = %v, want %v", got, want)
	}
}

func TestFSRestoreArgs(t *testing.T) {
	t.Run("no fs checkpoint present", func(t *testing.T) {
		checkpointDir := t.TempDir()
		got, err := fsRestoreArgs(checkpointDir)
		if err != nil {
			t.Fatalf("fsRestoreArgs() = %v", err)
		}
		if len(got) != 0 {
			t.Errorf("fsRestoreArgs() = %v, want empty", got)
		}
	})

	t.Run("fs checkpoint present in fs subdir", func(t *testing.T) {
		checkpointDir := t.TempDir()
		fsDir := filepath.Join(checkpointDir, "fs")
		if err := os.MkdirAll(fsDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(fsDir, "fscheckpoint.pb"), []byte("manifest"), 0o600); err != nil {
			t.Fatal(err)
		}

		got, err := fsRestoreArgs(checkpointDir)
		if err != nil {
			t.Fatalf("fsRestoreArgs() = %v", err)
		}
		want := []string{
			"--fs-restore-image-path", fsDir,
		}
		if !slices.Equal(got, want) {
			t.Errorf("fsRestoreArgs() = %v, want %v", got, want)
		}
	})
}
