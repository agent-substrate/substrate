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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/tarutil"
)

// durableTarFile is the snapshot file holding the tar of one durable-dir
// volume: the contents of <volumeName> under
// ActorDirs.durable_dir_volume_mounts_dir, plus a root entry for the volume
// directory's own metadata. The volume directory itself comes from atelet,
// never from the snapshot: runsc bind-mounts it by path, so a symlink planted
// there would expose whatever it points at to the sandbox.
func durableTarFile(volumeName string) (string, error) {
	if volumeName == "" || volumeName == "." || strings.Contains(volumeName, "/") || !filepath.IsLocal(volumeName) {
		return "", fmt.Errorf("invalid durable-dir volume name %q", volumeName)
	}
	return "durable-dir-" + volumeName + ".tar", nil
}

// hasDurableVolumes reports whether any container mounts a durable-dir volume.
func hasDurableVolumes(containers []*ateworkerpb.Container) bool {
	for _, c := range containers {
		if len(c.GetDurableDirVolumeMounts()) > 0 {
			return true
		}
	}
	return false
}

// tarDurableVolumes archives each durable-dir volume under dir into the
// checkpoint directory, one tar per volume, and returns the file names. The
// caller must have paused the guest first.
//
// Sockets the workload left behind and gVisor internal files (.gvisor.*) are
// skipped rather than archived.
func tarDurableVolumes(ctx context.Context, dir, checkpointDir string, volumes []string) ([]string, error) {
	skip := func(rel string) bool {
		base := filepath.Base(rel)
		return strings.HasPrefix(base, ".gvisor.")
	}
	var files []string
	for _, vol := range volumes {
		name, err := durableTarFile(vol)
		if err != nil {
			return nil, err
		}
		if err := tarutil.CreateFilteredWithRoot(ctx, filepath.Join(checkpointDir, name), filepath.Join(dir, vol), skip); err != nil {
			return nil, fmt.Errorf("while archiving durable-dir volume %q: %w", vol, err)
		}
		files = append(files, name)
	}
	return files, nil
}

// untarDurableVolumes restores each durable-dir volume from the snapshot into
// its directory under dir, which atelet has already created, empty. A volume
// with no tar in the snapshot (added to the template since) stays empty.
func untarDurableVolumes(dir, snapshotDir string, volumes []string) error {
	for _, vol := range volumes {
		name, err := durableTarFile(vol)
		if err != nil {
			return err
		}
		tarPath := filepath.Join(snapshotDir, name)
		if _, err := os.Stat(tarPath); errors.Is(err, fs.ErrNotExist) {
			continue
		}
		volDir := filepath.Join(dir, vol)
		if err := os.MkdirAll(volDir, 0o700); err != nil {
			return fmt.Errorf("while creating durable-dir volume dir %q: %w", volDir, err)
		}
		if err := tarutil.Extract(tarPath, volDir); err != nil {
			return fmt.Errorf("while restoring durable-dir volume %q: %w", vol, err)
		}
		removeGVisorFiles(volDir)
	}
	return nil
}

// removeGVisorFiles deletes gVisor internal files (.gvisor.*) under dir,
// best-effort, through an os.Root so the restored tree's symlinks are never
// followed.
func removeGVisorFiles(dir string) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return
	}
	defer root.Close()
	_ = fs.WalkDir(root.FS(), ".", func(rel string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasPrefix(d.Name(), ".gvisor.") {
			_ = root.Remove(rel)
		}
		return nil
	})
}
