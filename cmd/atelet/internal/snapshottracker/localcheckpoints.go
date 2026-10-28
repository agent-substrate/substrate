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

package snapshottracker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/sparsefile"
)

// PruneLocalCheckpoints removes every local snapshot of the actor except those
// named in keep. A missing directory is not an error, so retries are safe.
func PruneLocalCheckpoints(ctx context.Context, actorUID string, keep ...string) error {
	return pruneLocalCheckpointDir(ctx, ateletpath.LocalCheckpointsDir(actorUID), keep...)
}

func pruneLocalCheckpointDir(ctx context.Context, dir string, keep ...string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("while listing local checkpoints in %s: %w", dir, err)
	}
	// Every entry is attempted: one undeletable snapshot must not strand the
	// others on disk.
	var errs []error
	kept := false
	for _, entry := range entries {
		if slices.Contains(keep, entry.Name()) {
			kept = true
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			errs = append(errs, fmt.Errorf("while pruning local checkpoint %s: %w", path, err))
			continue
		}
		slog.InfoContext(ctx, "pruned local checkpoint", slog.String("path", path))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if !kept {
		if err := os.Remove(dir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("while removing local checkpoints dir %s: %w", dir, err)
		}
	}
	return nil
}

// removeLocalCheckpoint removes one local snapshot of the actor. A missing
// snapshot is not an error.
func removeLocalCheckpoint(ctx context.Context, actorUID, name string) error {
	path := ateletpath.LocalSnapshotDir(actorUID, name)
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("while removing local checkpoint %s: %w", path, err)
	}
	slog.InfoContext(ctx, "removed local checkpoint", slog.String("path", path))
	return nil
}

// localSnapshotSize returns the disk space allocated to the files of one local
// snapshot of the actor, nested ones included. Allocated blocks track the data
// an upload of the snapshot sends, where apparent sizes would count a sparse
// memory image up to the guest RAM ceiling. A missing snapshot, or a file that
// vanishes during the walk, counts nothing. On error it returns what it
// measured so far.
func localSnapshotSize(actorUID, name string) (int64, error) {
	var size int64
	err := filepath.WalkDir(ateletpath.LocalSnapshotDir(actorUID, name), func(_ string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			var info fs.FileInfo
			if info, err = d.Info(); err == nil {
				size += sparsefile.AllocatedBytes(info)
			}
		}
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	})
	return size, err
}
