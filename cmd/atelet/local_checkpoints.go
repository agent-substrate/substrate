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
	"log/slog"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

// pruneLocalCheckpoints removes the actor's local snapshots, except the one
// named by keep (pass "" to remove them all).
//
// keep is the destination of a checkpoint currently being written. An earlier
// attempt at that same checkpoint may already have moved files into it, and
// those files are the only copy: the rename took them out of the checkpoint
// dir, and the snapshot is not committed until its manifest lands, so nothing
// would re-create them. Pruning the destination would therefore destroy a
// half-moved snapshot that the move is about to finish.
//
// Best-effort: failures are logged, never fatal.
func pruneLocalCheckpoints(ctx context.Context, actorUID, keep string) {
	pruneLocalCheckpointDir(ctx, ateompath.LocalCheckpointsDir(actorUID), keep)
}

func pruneLocalCheckpointDir(ctx context.Context, dir, keep string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.WarnContext(ctx, "failed to list local checkpoints for pruning", slog.String("dir", dir), slog.Any("err", err))
		}
		return
	}
	for _, entry := range entries {
		if keep != "" && entry.Name() == keep {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			slog.WarnContext(ctx, "failed to prune local checkpoint", slog.String("path", path), slog.Any("err", err))
			continue
		}
		slog.InfoContext(ctx, "pruned local checkpoint", slog.String("path", path))
	}
	// Only removes the directory when it is empty, so a kept snapshot stays.
	_ = os.Remove(dir)
}
