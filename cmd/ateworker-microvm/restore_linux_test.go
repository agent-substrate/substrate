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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/roottest"
)

// failedRestoreRootfs lays out what a memory restore has assembled on the host
// by the time buildActorContainers returns: a rootfs upper dir re-materialized
// from the snapshot and each container's bundle rootfs. With overlays, the
// bundle rootfs is an overlay mount, as in production.
func failedRestoreRootfs(t *testing.T, actorUID string, overlays bool) *ateworkerpb.ActorDirs {
	t.Helper()
	t.Cleanup(func() { kata.CleanupSandboxState(context.Background(), actorUID) })
	dirs := &ateworkerpb.ActorDirs{RootDir: t.TempDir(), OciBundleDir: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(rootfsUpperDir(dirs), "app", "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootfsUpperDir(dirs), "app", "data", "state"), []byte("restored"), 0o644); err != nil {
		t.Fatal(err)
	}
	containers := []*ateworkerpb.Container{{Name: "app", ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app"}}}}
	bundle := ociBundlePath(dirs, "app")
	if err := os.MkdirAll(filepath.Join(bundle, "rootfs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if overlays {
		layer := t.TempDir()
		if err := os.MkdirAll(filepath.Join(layer, "fs", "etc"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := imagecache.WriteSpec(bundle, &imagecache.OverlaySpec{Layers: []string{layer}}); err != nil {
			t.Fatalf("WriteSpec: %v", err)
		}
		// Detach the overlays even if the cleanup under test leaves them, so
		// t.TempDir can remove the bundle.
		t.Cleanup(func() { _ = imagecache.UnmountAllUnder(dirs.GetOciBundleDir()) })
	}
	if _, err := (&AteWorkerService{}).buildActorContainers(actorUID, dirs, containers); err != nil {
		t.Fatalf("buildActorContainers: %v", err)
	}
	return dirs
}

// mountsUnder lists the mount points at or below dir.
func mountsUnder(t *testing.T, dir string) []string {
	t.Helper()
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	var mounts []string
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		if mp := fields[4]; mp == dir || strings.HasPrefix(mp, dir+"/") {
			mounts = append(mounts, mp)
		}
	}
	return mounts
}

// cleanupFailedRestoreRootfs, which a memory restore that fails after
// buildActorContainers runs, leaves neither the rootfs upper dir nor any bundle
// rootfs overlay behind: a retried restore re-materializes the upper into the
// same dir and remounts the bundles.
func TestCleanupFailedRestoreRootfs(t *testing.T) {
	t.Run("removes the rootfs upper dir", func(t *testing.T) {
		actorUID := uuid.NewString()
		dirs := failedRestoreRootfs(t, actorUID, false)
		(&AteWorkerService{}).cleanupFailedRestoreRootfs(t.Context(), actorUID, dirs)
		if _, err := os.Stat(rootfsUpperDir(dirs)); !os.IsNotExist(err) {
			t.Errorf("rootfs upper dir after cleanup: stat = %v, want it removed", err)
		}
	})

	t.Run("detaches the bundle rootfs overlays", func(t *testing.T) {
		roottest.Require(t, "mounts the bundle rootfs overlay")
		actorUID := uuid.NewString()
		dirs := failedRestoreRootfs(t, actorUID, true)
		if len(mountsUnder(t, dirs.GetOciBundleDir())) == 0 {
			t.Fatal("buildActorContainers mounted no bundle rootfs overlay")
		}
		(&AteWorkerService{}).cleanupFailedRestoreRootfs(t.Context(), actorUID, dirs)
		if got := mountsUnder(t, dirs.GetOciBundleDir()); len(got) != 0 {
			t.Errorf("mounts under the bundle dir after cleanup = %v, want none", got)
		}
		if _, err := os.Stat(rootfsUpperDir(dirs)); !os.IsNotExist(err) {
			t.Errorf("rootfs upper dir after cleanup: stat = %v, want it removed", err)
		}
	})
}
