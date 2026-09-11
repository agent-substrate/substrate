// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/google/go-cmp/cmp"
)

type mountCall struct {
	volumeID   string
	targetPath string
	attributes map[string]string
}

type fakeWorkerPlugin struct {
	mountErr    error
	mountErrs   map[string]error
	unmountErr  error
	unmountErrs map[string]error
	unmounted   []string
	mountCalls  []mountCall
}

func (f *fakeWorkerPlugin) MountVolume(ctx context.Context, volumeID string, targetPath string, attributes map[string]string) error {
	f.mountCalls = append(f.mountCalls, mountCall{
		volumeID:   volumeID,
		targetPath: targetPath,
		attributes: attributes,
	})
	if f.mountErrs != nil {
		if err, ok := f.mountErrs[volumeID]; ok {
			return err
		}
	}
	return f.mountErr
}

func (f *fakeWorkerPlugin) UnmountVolume(ctx context.Context, volumeID string, targetPath string) error {
	f.unmounted = append(f.unmounted, volumeID)
	if f.unmountErrs != nil {
		if err, ok := f.unmountErrs[volumeID]; ok {
			return err
		}
	}
	return f.unmountErr
}

var _ volume.VolumePluginWorkerPlane = (*fakeWorkerPlugin)(nil)

func TestUnmountExternalVolumes(t *testing.T) {
	ctx := context.Background()
	actorUID := "test-actor-123"

	extVol1 := &ateletpb.Volume{
		Name: "vol-1",
		Source: &ateletpb.Volume_External{
			External: &ateletpb.ExternalVolumeSource{
				StorageVolumeId: "mock-vol-1",
				VolumeType:      "mock-driver",
			},
		},
	}
	extVol2 := &ateletpb.Volume{
		Name: "vol-2",
		Source: &ateletpb.Volume_External{
			External: &ateletpb.ExternalVolumeSource{
				StorageVolumeId: "mock-vol-2",
				VolumeType:      "mock-driver",
			},
		},
	}
	durableVol := &ateletpb.Volume{
		Name: "durable-1",
		Source: &ateletpb.Volume_DurableDir{
			DurableDir: &ateletpb.DurableDirVolume{},
		},
	}

	t.Run("success", func(t *testing.T) {
		fake := &fakeWorkerPlugin{}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, durableVol, extVol2})
		if err != nil {
			t.Fatalf("unmountExternalVolumes failed unexpectedly: %v", err)
		}
		if len(fake.unmounted) != 2 || fake.unmounted[0] != "mock-vol-1" || fake.unmounted[1] != "mock-vol-2" {
			t.Errorf("unmounted volumes = %v, want [mock-vol-1, mock-vol-2]", fake.unmounted)
		}
	})

	t.Run("unmount failure is blocking", func(t *testing.T) {
		fake := &fakeWorkerPlugin{
			unmountErr: errors.New("device or resource busy"),
		}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1})
		if err == nil {
			t.Fatal("unmountExternalVolumes returned nil, want blocking error")
		}
		if !errors.Is(err, fake.unmountErr) {
			t.Errorf("error = %v, want to contain unmount error", err)
		}
	})

	t.Run("multiple unmount failures return joined error", func(t *testing.T) {
		fake := &fakeWorkerPlugin{
			unmountErr: fmt.Errorf("unmount failed"),
		}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2})
		if err == nil {
			t.Fatal("unmountExternalVolumes returned nil, want blocking error")
		}
		// Both volumes should have attempt made
		if len(fake.unmounted) != 2 {
			t.Errorf("attempted unmount count = %d, want 2", len(fake.unmounted))
		}
	})
}

func TestMountExternalVolumes(t *testing.T) {
	ctx := context.Background()
	actorUID := "test-actor-456"

	extVol1 := &ateletpb.Volume{
		Name: "vol-1",
		Source: &ateletpb.Volume_External{
			External: &ateletpb.ExternalVolumeSource{
				StorageVolumeId: "mock-vol-1",
				VolumeType:      "mock-driver",
				VolumeContext:   map[string]string{"key": "val1"},
			},
		},
	}
	extVol2 := &ateletpb.Volume{
		Name: "vol-2",
		Source: &ateletpb.Volume_External{
			External: &ateletpb.ExternalVolumeSource{
				StorageVolumeId: "mock-vol-2",
				VolumeType:      "mock-driver",
				VolumeContext:   map[string]string{"key": "val2"},
			},
		},
	}
	durableVol := &ateletpb.Volume{
		Name: "durable-1",
		Source: &ateletpb.Volume_DurableDir{
			DurableDir: &ateletpb.DurableDirVolume{},
		},
	}

	t.Run("success mounts external volumes and creates mount directory", func(t *testing.T) {
		tempDir := t.TempDir()
		origVolumeHostPath := volumeHostPath
		volumeHostPath = func(uid, name string) string {
			return filepath.Join(tempDir, uid, name)
		}
		t.Cleanup(func() { volumeHostPath = origVolumeHostPath })

		fake := &fakeWorkerPlugin{}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, durableVol, extVol2})
		if err != nil {
			t.Fatalf("mountExternalVolumes failed unexpectedly: %v", err)
		}

		if len(fake.mountCalls) != 2 {
			t.Fatalf("mountCalls count = %d, want 2", len(fake.mountCalls))
		}

		expectedPath1 := filepath.Join(tempDir, actorUID, "vol-1")
		expectedPath2 := filepath.Join(tempDir, actorUID, "vol-2")

		for _, path := range []string{expectedPath1, expectedPath2} {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat mount path %q: %v", path, err)
			}
			if !info.IsDir() {
				t.Errorf("%q is not a directory", path)
			}
			if perm := info.Mode().Perm(); perm != 0o750 {
				t.Errorf("perm for %q = %o, want 750", path, perm)
			}
		}

		wantCalls := []mountCall{
			{volumeID: "mock-vol-1", targetPath: expectedPath1, attributes: map[string]string{"key": "val1"}},
			{volumeID: "mock-vol-2", targetPath: expectedPath2, attributes: map[string]string{"key": "val2"}},
		}
		if diff := cmp.Diff(wantCalls, fake.mountCalls, cmp.AllowUnexported(mountCall{})); diff != "" {
			t.Errorf("mountCalls mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("target directory already exists is handled cleanly", func(t *testing.T) {
		tempDir := t.TempDir()
		origVolumeHostPath := volumeHostPath
		volumeHostPath = func(uid, name string) string {
			return filepath.Join(tempDir, uid, name)
		}
		t.Cleanup(func() { volumeHostPath = origVolumeHostPath })

		expectedPath := filepath.Join(tempDir, actorUID, "vol-1")
		if err := os.MkdirAll(expectedPath, 0o750); err != nil {
			t.Fatalf("pre-creating mount point: %v", err)
		}

		fake := &fakeWorkerPlugin{}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1})
		if err != nil {
			t.Fatalf("mountExternalVolumes with existing directory failed: %v", err)
		}
		if len(fake.mountCalls) != 1 || fake.mountCalls[0].targetPath != expectedPath {
			t.Errorf("mountCalls = %v, want 1 call for %q", fake.mountCalls, expectedPath)
		}
	})

	t.Run("plugin lookup failure returns error", func(t *testing.T) {
		tempDir := t.TempDir()
		origVolumeHostPath := volumeHostPath
		volumeHostPath = func(uid, name string) string {
			return filepath.Join(tempDir, uid, name)
		}
		t.Cleanup(func() { volumeHostPath = origVolumeHostPath })

		unknownVol := &ateletpb.Volume{
			Name: "vol-unknown",
			Source: &ateletpb.Volume_External{
				External: &ateletpb.ExternalVolumeSource{
					StorageVolumeId: "mock-vol-unknown",
					VolumeType:      "unknown-driver",
				},
			},
		}

		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{unknownVol})
		if err == nil {
			t.Fatal("expected mountExternalVolumes to fail with unknown plugin, got nil")
		}
		if !strings.Contains(err.Error(), "unknown-driver") {
			t.Errorf("error = %v, want to contain driver name %q", err, "unknown-driver")
		}
	})

	t.Run("plugin mount failure returns error", func(t *testing.T) {
		tempDir := t.TempDir()
		origVolumeHostPath := volumeHostPath
		volumeHostPath = func(uid, name string) string {
			return filepath.Join(tempDir, uid, name)
		}
		t.Cleanup(func() { volumeHostPath = origVolumeHostPath })

		fake := &fakeWorkerPlugin{
			mountErr: errors.New("mount operation failed: device or resource busy"),
		}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1})
		if err == nil {
			t.Fatal("expected mountExternalVolumes to fail, got nil")
		}
		if !strings.Contains(err.Error(), "device or resource busy") {
			t.Errorf("error = %v, want to contain underlying mount error", err)
		}
	})

	t.Run("multi-volume partial failure aborts on first failure", func(t *testing.T) {
		tempDir := t.TempDir()
		origVolumeHostPath := volumeHostPath
		volumeHostPath = func(uid, name string) string {
			return filepath.Join(tempDir, uid, name)
		}
		t.Cleanup(func() { volumeHostPath = origVolumeHostPath })

		fake := &fakeWorkerPlugin{
			mountErr: errors.New("cannot mount volume"),
		}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2})
		if err == nil {
			t.Fatal("expected mountExternalVolumes to fail, got nil")
		}
		if len(fake.mountCalls) != 1 {
			t.Errorf("mountCalls count = %d, want 1 (stopped after first failure)", len(fake.mountCalls))
		}
	})

	t.Run("multi-volume partial failure does not roll back already mounted volume", func(t *testing.T) {
		tempDir := t.TempDir()
		origVolumeHostPath := volumeHostPath
		volumeHostPath = func(uid, name string) string {
			return filepath.Join(tempDir, uid, name)
		}
		t.Cleanup(func() { volumeHostPath = origVolumeHostPath })

		expectedPath1 := filepath.Join(tempDir, actorUID, "vol-1")
		expectedPath2 := filepath.Join(tempDir, actorUID, "vol-2")

		fake := &fakeWorkerPlugin{
			mountErrs: map[string]error{
				"mock-vol-2": errors.New("simulated mount error on vol-2"),
			},
		}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2})
		if err == nil {
			t.Fatal("expected mountExternalVolumes to fail when vol-2 fails, got nil")
		}
		if !strings.Contains(err.Error(), "simulated mount error on vol-2") {
			t.Errorf("error = %v, want error to contain %q", err, "simulated mount error on vol-2")
		}

		// Both volumes had MountVolume called: vol-1 succeeded, vol-2 failed.
		if len(fake.mountCalls) != 2 {
			t.Fatalf("mountCalls count = %d, want 2", len(fake.mountCalls))
		}
		if fake.mountCalls[0].volumeID != "mock-vol-1" || fake.mountCalls[1].volumeID != "mock-vol-2" {
			t.Errorf("mountCalls = %v, want calls for mock-vol-1 followed by mock-vol-2", fake.mountCalls)
		}

		// Verify vol-1 was not rolled back or unmounted when vol-2 failed.
		if len(fake.unmounted) != 0 {
			t.Errorf("unmounted count = %d, want 0 (vol-1 was not rolled back)", len(fake.unmounted))
		}

		// Verify host mount directory for vol-1 exists and remains on disk.
		if _, err := os.Stat(expectedPath1); err != nil {
			t.Errorf("stat vol-1 mount path %q: %v", expectedPath1, err)
		}

		// Verify host mount directory for vol-2 was also created prior to MountVolume call.
		if _, err := os.Stat(expectedPath2); err != nil {
			t.Errorf("stat vol-2 mount path %q: %v", expectedPath2, err)
		}
	})
}

func TestVolumeHostDirectoryCleanup(t *testing.T) {
	ctx := context.Background()
	actorUID := "test-actor-cleanup"

	extVol1 := &ateletpb.Volume{
		Name: "vol-1",
		Source: &ateletpb.Volume_External{
			External: &ateletpb.ExternalVolumeSource{
				StorageVolumeId: "mock-vol-1",
				VolumeType:      "mock-driver",
			},
		},
	}
	extVol2 := &ateletpb.Volume{
		Name: "vol-2",
		Source: &ateletpb.Volume_External{
			External: &ateletpb.ExternalVolumeSource{
				StorageVolumeId: "mock-vol-2",
				VolumeType:      "mock-driver",
			},
		},
	}

	t.Run("unmountExternalVolumes preserves host mount directories", func(t *testing.T) {
		tempDir := t.TempDir()
		origVolumeHostPath := volumeHostPath
		volumeHostPath = func(uid, name string) string {
			return filepath.Join(tempDir, uid, name)
		}
		t.Cleanup(func() { volumeHostPath = origVolumeHostPath })

		fake := &fakeWorkerPlugin{}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		if err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2}); err != nil {
			t.Fatalf("mountExternalVolumes failed: %v", err)
		}

		path1 := volumeHostPath(actorUID, "vol-1")
		path2 := volumeHostPath(actorUID, "vol-2")
		for _, p := range []string{path1, path2} {
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("expected mount path %q to exist before unmount: %v", p, err)
			}
		}

		if err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2}); err != nil {
			t.Fatalf("unmountExternalVolumes failed: %v", err)
		}

		// unmountExternalVolumes unmounts the CSI volumes, but preserves the host directories.
		for _, p := range []string{path1, path2} {
			if _, err := os.Stat(p); err != nil {
				t.Errorf("mount path %q was unexpectedly deleted by unmountExternalVolumes: %v", p, err)
			}
		}
	})

	t.Run("resetActorDirs removes empty unmounted volume host directories", func(t *testing.T) {
		origActorsDir := ateompath.ActorsDir
		tempDir := t.TempDir()
		ateompath.ActorsDir = filepath.Join(tempDir, "actors")
		t.Cleanup(func() { ateompath.ActorsDir = origActorsDir })

		fake := &fakeWorkerPlugin{}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		// Mount and then unmount external volumes.
		if err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2}); err != nil {
			t.Fatalf("mountExternalVolumes failed: %v", err)
		}
		if err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2}); err != nil {
			t.Fatalf("unmountExternalVolumes failed: %v", err)
		}

		path1 := ateompath.VolumeHostPath(actorUID, "vol-1")
		path2 := ateompath.VolumeHostPath(actorUID, "vol-2")
		for _, p := range []string{path1, path2} {
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("expected mount path %q to exist before reset: %v", p, err)
			}
		}

		// resetActorDirs cleans up empty volume directories left after unmount.
		if err := resetActorDirs(actorUID); err != nil {
			t.Fatalf("resetActorDirs failed: %v", err)
		}

		for _, p := range []string{path1, path2} {
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Errorf("expected mount path %q to be deleted by resetActorDirs, stat err = %v", p, err)
			}
		}

		// The parent volumes directory is preserved and empty.
		volumesDir := ateompath.VolumesDir(actorUID)
		entries, err := os.ReadDir(volumesDir)
		if err != nil {
			t.Fatalf("ReadDir(%q) failed: %v", volumesDir, err)
		}
		if len(entries) != 0 {
			t.Errorf("expected volumes dir to be empty, found %d entries", len(entries))
		}
	})

	t.Run("resetActorDirs preserves non-empty volume directories and returns error", func(t *testing.T) {
		origActorsDir := ateompath.ActorsDir
		tempDir := t.TempDir()
		ateompath.ActorsDir = filepath.Join(tempDir, "actors")
		t.Cleanup(func() { ateompath.ActorsDir = origActorsDir })

		fake := &fakeWorkerPlugin{}
		s := &AteomHerder{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		if err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1}); err != nil {
			t.Fatalf("mountExternalVolumes failed: %v", err)
		}

		// Simulate lingering content in the volume mount point (e.g. unmount failed or data present).
		path1 := ateompath.VolumeHostPath(actorUID, "vol-1")
		filePath := filepath.Join(path1, "lingering.txt")
		if err := os.WriteFile(filePath, []byte("volume data"), 0o600); err != nil {
			t.Fatalf("writing test file: %v", err)
		}

		err := resetActorDirs(actorUID)
		if err == nil {
			t.Fatal("expected resetActorDirs to fail on non-empty volume dir, got nil")
		}
		if !strings.Contains(err.Error(), "while removing volume dir") {
			t.Errorf("error = %v, want error containing 'while removing volume dir'", err)
		}

		// Verify data is preserved and not deleted.
		if _, err := os.Stat(filePath); err != nil {
			t.Errorf("stat %q: %v (data was unexpectedly deleted)", filePath, err)
		}
	})

	t.Run("resetActorDirs succeeds cleanly when volumes directory does not exist", func(t *testing.T) {
		origActorsDir := ateompath.ActorsDir
		tempDir := t.TempDir()
		ateompath.ActorsDir = filepath.Join(tempDir, "actors")
		t.Cleanup(func() { ateompath.ActorsDir = origActorsDir })

		if err := resetActorDirs(actorUID); err != nil {
			t.Fatalf("resetActorDirs failed when volumes dir does not exist: %v", err)
		}

		volumesDir := ateompath.VolumesDir(actorUID)
		if info, err := os.Stat(volumesDir); err != nil || !info.IsDir() {
			t.Errorf("expected volumes dir %q to be created, stat err = %v", volumesDir, err)
		}
	})
}
