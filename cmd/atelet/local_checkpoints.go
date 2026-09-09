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
	"log/slog"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

// pruneLocalCheckpoints removes every local snapshot of the actor. A missing
// directory is not an error, so retries are safe.
func pruneLocalCheckpoints(ctx context.Context, actorUID string) error {
	return pruneLocalCheckpointDir(ctx, ateompath.LocalCheckpointsDir(actorUID))
}

func pruneLocalCheckpointDir(ctx context.Context, dir string) error {
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
	for _, entry := range entries {
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
	if err := os.Remove(dir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("while removing local checkpoints dir %s: %w", dir, err)
	}
	return nil
}
