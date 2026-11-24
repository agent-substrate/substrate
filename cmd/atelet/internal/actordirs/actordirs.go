// Copyright 2026 The Agent Substrate Authors
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

// Package actordirs manages each actor's on-node directory tree.
package actordirs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/trash"
	"go.ate.dev/substrate/internal/imagecache"
)

// Manager resets and removes per-actor directories, retiring them through
// trash when configured or removing them synchronously when nil.
type Manager struct {
	trash *trash.Reaper
}

// New returns a Manager that retires directories through r, or removes them
// synchronously when r is nil.
func New(r *trash.Reaper) *Manager {
	return &Manager{trash: r}
}

// ResetDir empties dir with remove and recreates it with mode. what names the
// directory in errors.
func ResetDir(what, dir string, remove func(string) error, mode os.FileMode) error {
	if err := remove(dir); err != nil {
		return fmt.Errorf("while deleting %s: %w", what, err)
	}
	if err := os.MkdirAll(dir, mode); err != nil {
		return fmt.Errorf("while creating %s: %w", what, err)
	}
	return nil
}

func (m *Manager) resetDir(ctx context.Context, actorUID, what, trashDir, dir string, mode os.FileMode) error {
	var r *trash.Reaper
	if m != nil {
		r = m.trash
	}
	return ResetDir(what, dir, func(p string) error {
		return r.Retire(ctx, trashDir, p, trash.RetireOptions{
			ActorUID: actorUID,
			Kind:     trash.KindActorDir,
		})
	}, mode)
}

// removeEmptyVolumeDirs removes each empty mountpoint directory under the
// actor's external volumes directory, refusing to delete through a populated
// directory in case an unmount failed.
func removeEmptyVolumeDirs(actorUID string) error {
	volumesDir := ateletpath.ExternalVolumeMountsDir(actorUID)
	entries, err := os.ReadDir(volumesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("while reading volumes dir: %w", err)
	}
	for _, entry := range entries {
		volPath := filepath.Join(volumesDir, entry.Name())
		if err := os.Remove(volPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("while removing volume dir: %w", err)
		}
	}
	return nil
}

// ResetActorDirs empties the actor's directories and leaves them in place for
// its next activation. Use RemoveActorDirs when the actor will not come back.
func (m *Manager) ResetActorDirs(ctx context.Context, actorUID string) error {
	if err := m.ResetActorDirsKeepingVolumes(ctx, actorUID); err != nil {
		return err
	}
	// Do not call RemoveAll on volume directories in case the unmount failed.
	// We do not want to delete mount content.
	return removeEmptyVolumeDirs(actorUID)
}

// ResetActorDirsKeepingVolumes is ResetActorDirs for Run and Restore, which
// leave the external volume mount points alone: a failed earlier attempt can
// leave a volume published there, and publishing it again is a no-op.
func (m *Manager) ResetActorDirsKeepingVolumes(ctx context.Context, actorUID string) error {
	// Explicitly leave runsc logs dir untouched.
	actorsTrash := ateletpath.ActorsTrashDir()
	snapshotsTrash := ateletpath.SnapshotsTrashDir()

	if err := m.resetDir(ctx, actorUID, "bundle dir", actorsTrash, ateletpath.OCIBundleDir(actorUID), 0o700); err != nil {
		return err
	}
	if err := m.resetDir(ctx, actorUID, "checkpoint-state dir", snapshotsTrash, ateletpath.CheckpointStateDir(actorUID), 0o700); err != nil {
		return err
	}
	if err := m.resetDir(ctx, actorUID, "restore-state dir", snapshotsTrash, ateletpath.RestoreStateDir(actorUID), 0o700); err != nil {
		return err
	}
	if err := m.resetDir(ctx, actorUID, "durable-dir volumes mount dir", actorsTrash, ateletpath.DurableDirVolumeMountsDir(actorUID), 0o755); err != nil {
		return err
	}
	// World-readable (0o755): bind-mounted read-only into the actor, whose
	// workload reads it through the gofer.
	if err := m.resetDir(ctx, actorUID, "system-info volume roots dir", actorsTrash, ateletpath.SystemInfoVolumeRootsDir(actorUID), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(ateletpath.ExternalVolumeMountsDir(actorUID), 0o755); err != nil {
		return fmt.Errorf("while creating volumes dir: %w", err)
	}
	return nil
}

// RemoveActorFilesExceptState removes the actor's snapshot directory tree and
// all files and subdirectories under the actor's directory except
// snapshot-state.json, refusing to proceed while an external volume mount
// directory is still populated.
func (m *Manager) RemoveActorFilesExceptState(ctx context.Context, actorUID string) error {
	if err := removeEmptyVolumeDirs(actorUID); err != nil {
		return err
	}
	volumesDir := ateletpath.ExternalVolumeMountsDir(actorUID)
	if err := os.Remove(volumesDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("while removing volumes dir: %w", err)
	}
	var r *trash.Reaper
	if m != nil {
		r = m.trash
	}
	if r != nil {
		if err := r.Retire(ctx, ateletpath.SnapshotsTrashDir(), ateletpath.ActorSnapshotPath(actorUID), trash.RetireOptions{
			ActorUID: actorUID,
			Kind:     trash.KindLocalSnapshot,
		}); err != nil {
			return fmt.Errorf("while deleting actor snapshot dir: %w", err)
		}
	} else if err := os.RemoveAll(ateletpath.ActorSnapshotPath(actorUID)); err != nil {
		return fmt.Errorf("while deleting actor snapshot dir: %w", err)
	}
	actorDir := ateletpath.ActorPath(actorUID)
	stateFile := filepath.Base(ateletpath.ActorSnapshotStateFile(actorUID))
	entries, err := os.ReadDir(actorDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("while reading actor dir: %w", err)
	}
	actorsTrash := ateletpath.ActorsTrashDir()
	for _, entry := range entries {
		if entry.Name() == stateFile {
			continue
		}
		entryPath := filepath.Join(actorDir, entry.Name())
		if r != nil {
			if err := r.Retire(ctx, actorsTrash, entryPath, trash.RetireOptions{
				ActorUID: actorUID,
				Kind:     trash.KindActorDir,
			}); err != nil {
				return fmt.Errorf("while deleting actor dir: %w", err)
			}
			continue
		}
		if err := imagecache.RemoveAllWritable(entryPath); err != nil {
			return fmt.Errorf("while deleting actor dir: %w", err)
		}
	}
	return nil
}

// RemoveActorDirs reclaims both of the actor's directory trees, roots
// included: nothing else on the node deletes them, and no later activation
// will look there.
//
// It refuses to proceed while a volume directory is still populated, so a
// failed unmount cannot become a deletion of the mount's contents, and removes
// a bundle upper dir carrying an image's read-only modes without recreating
// directories. Terminate calls Forget first, which has already deleted
// snapshot-state.json; after a crash here, Start finds no state file, so only a
// retried Terminate finishes the cleanup.
func (m *Manager) RemoveActorDirs(ctx context.Context, actorUID string) error {
	if err := m.RemoveActorFilesExceptState(ctx, actorUID); err != nil {
		return err
	}
	if err := os.Remove(ateletpath.ActorSnapshotStateFile(actorUID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("while deleting the actor's snapshot state: %w", err)
	}
	if err := os.Remove(ateletpath.ActorPath(actorUID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("while deleting actor dir: %w", err)
	}
	return nil
}

// ResetActorDirs empties the actor's directories synchronously.
func ResetActorDirs(actorUID string) error {
	return (*Manager)(nil).ResetActorDirs(context.Background(), actorUID)
}

// RemoveActorDirs reclaims both of the actor's directory trees synchronously.
func RemoveActorDirs(actorUID string) error {
	return (*Manager)(nil).RemoveActorDirs(context.Background(), actorUID)
}
