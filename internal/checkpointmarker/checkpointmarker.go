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

// Package checkpointmarker reads and writes the per-actor checkpoint
// completion marker both ateom runtimes use for idempotent replays.
//
// Because checkpoints are destructive (taking the sandbox down), the marker
// records completed snapshot files so lost responses or retried calls can be
// replayed without re-running against a destroyed sandbox.
package checkpointmarker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

// Record represents the marker written on checkpoint completion.
type Record struct {
	SnapshotFiles []string `json:"snapshotFiles"`
	// Scope is the checkpoint scope (e.g. FULL, DATA), ensuring a marker is only
	// replayed for matching checkpoint requests.
	Scope string `json:"scope"`
}

// Write records the completed checkpoint for actorUID atomically (temp file
// plus rename and sync). An empty snapshotFiles slice is valid (e.g. CSI
// volumes snapshotted out-of-band) and still recorded.
func Write(actorUID, scope string, snapshotFiles []string) error {
	if scope == "" {
		return fmt.Errorf("refusing to record a checkpoint marker for actor %q with no scope", actorUID)
	}

	data, err := json.Marshal(&Record{SnapshotFiles: snapshotFiles, Scope: scope})
	if err != nil {
		return fmt.Errorf("while marshaling checkpoint marker: %w", err)
	}

	path := ateompath.CheckpointDoneFile(actorUID)
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+ateompath.CheckpointDoneFileName+".tmp-")
	if err != nil {
		return fmt.Errorf("while creating checkpoint marker temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName) // no-op once the rename below succeeds
	}()

	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("while writing checkpoint marker: %w", err)
	}
	// Flush bytes before rename to ensure file content is durable on disk.
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("while syncing checkpoint marker: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("while closing checkpoint marker: %w", err)
	}

	// Atomically publish the marker so readers never observe partial data.
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("while renaming checkpoint marker into place: %w", err)
	}

	// Sync parent directory to persist the directory entry across crashes.
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("while opening checkpoint marker directory to sync: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("while syncing checkpoint marker directory: %w", err)
	}
	return nil
}

// Read returns the marker recorded for actorUID matching the requested scope.
// ok is false if no marker exists, the scope mismatches, or the marker is invalid.
func Read(actorUID, scope string) (_ *Record, ok bool, _ error) {
	path := ateompath.CheckpointDoneFile(actorUID)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("while reading checkpoint marker: %w", err)
	}
	rec := &Record{}
	if err := json.Unmarshal(data, rec); err != nil {
		discardUnusable(path, actorUID, fmt.Errorf("while parsing checkpoint marker: %w", err))
		return nil, false, nil
	}
	if rec.Scope != scope {
		slog.Info("Checkpoint marker records a different checkpoint; not replaying it",
			slog.String("actor_uid", actorUID), slog.String("marker_scope", rec.Scope), slog.String("requested_scope", scope))
		return nil, false, nil
	}
	return rec, true, nil
}

// discardUnusable removes corrupted marker files to prevent retry loops from
// repeatedly failing on unparseable state.
func discardUnusable(path, actorUID string, reason error) {
	slog.Warn("Discarding unusable checkpoint marker; the checkpoint will be re-attempted",
		slog.String("actor_uid", actorUID), slog.String("path", path), slog.Any("err", reason))
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		// Not fatal: a later successful checkpoint overwrites the marker by rename regardless.
		slog.Warn("Failed to remove unusable checkpoint marker",
			slog.String("actor_uid", actorUID), slog.String("path", path), slog.Any("err", err))
	}
}
