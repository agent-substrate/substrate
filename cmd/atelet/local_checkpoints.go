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

// pruneLocalCheckpoints removes every local snapshot of the actor.
// Best-effort: failures are logged, never fatal.
func pruneLocalCheckpoints(ctx context.Context, actorUID string) {
	pruneLocalCheckpointDir(ctx, ateompath.LocalCheckpointsDir(actorUID))
}

func pruneLocalCheckpointDir(ctx context.Context, dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.WarnContext(ctx, "failed to list local checkpoints for pruning", slog.String("dir", dir), slog.Any("err", err))
		}
		return
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			slog.WarnContext(ctx, "failed to prune local checkpoint", slog.String("path", path), slog.Any("err", err))
			continue
		}
		slog.InfoContext(ctx, "pruned local checkpoint", slog.String("path", path))
	}
	_ = os.Remove(dir)
}
