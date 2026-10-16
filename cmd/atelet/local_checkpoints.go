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
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
)

// localCheckpointPrune is a planned removal of an actor's local snapshots.
// Listing and deleting are split so a caller can list on its request path
// and delete after its response: a prune then removes only snapshots that
// existed when it was planned, never one a later pause writes, however long
// the deletions take.
type localCheckpointPrune struct {
	dir   string
	paths []string
	// keep is the snapshot left in place, or "" when the directory itself goes.
	keep string
	// exists records whether the directory was there to list.
	exists bool
}

// planLocalCheckpointPrune lists the actor's local snapshots other than keep.
// A missing directory plans nothing, so retries are safe.
func planLocalCheckpointPrune(actorUID, keep string) (*localCheckpointPrune, error) {
	return planLocalCheckpointPruneDir(ateletpath.LocalCheckpointsDir(actorUID), keep)
}

func planLocalCheckpointPruneDir(dir, keep string) (*localCheckpointPrune, error) {
	plan := &localCheckpointPrune{dir: dir, keep: keep}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return plan, nil
		}
		return nil, fmt.Errorf("while listing local checkpoints in %s: %w", dir, err)
	}
	plan.exists = true
	for _, entry := range entries {
		if entry.Name() == keep {
			continue
		}
		plan.paths = append(plan.paths, filepath.Join(dir, entry.Name()))
	}
	return plan, nil
}

// empty reports whether run has nothing to remove.
func (p *localCheckpointPrune) empty() bool {
	return len(p.paths) == 0 && (p.keep != "" || !p.exists)
}

// run removes the planned snapshots, then the directory unless a snapshot was
// kept. Removing a snapshot written one pause earlier can take seconds when
// the node's disk is behind on writeback, so the duration is logged.
func (p *localCheckpointPrune) run(ctx context.Context) error {
	// Every entry is attempted: one undeletable snapshot must not strand the
	// others on disk.
	var errs []error
	for _, path := range p.paths {
		start := time.Now()
		if err := removeLocalCheckpoint(path); err != nil {
			errs = append(errs, fmt.Errorf("while pruning local checkpoint %s: %w", path, err))
			continue
		}
		slog.InfoContext(ctx, "pruned local checkpoint", slog.String("path", path), slog.Duration("took", time.Since(start)))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if p.keep != "" {
		return nil
	}
	if err := os.Remove(p.dir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("while removing local checkpoints dir %s: %w", p.dir, err)
	}
	return nil
}

// removeLocalCheckpoint deletes one snapshot directory; tests swap it for a
// slow one to exercise the back-pressure path.
var removeLocalCheckpoint = os.RemoveAll

// pruneLocalCheckpoints removes every local snapshot of the actor. A missing
// directory is not an error, so retries are safe.
func pruneLocalCheckpoints(ctx context.Context, actorUID string) error {
	return pruneLocalCheckpointDir(ctx, ateletpath.LocalCheckpointsDir(actorUID))
}

func pruneLocalCheckpointDir(ctx context.Context, dir string) error {
	plan, err := planLocalCheckpointPruneDir(dir, "")
	if err != nil {
		return err
	}
	return plan.run(ctx)
}
