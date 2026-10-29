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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"syscall"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/atomicfile"
	"go.ate.dev/substrate/internal/nodepath"
)

// saveLocked writes what atelet tracks of the actor to its snapshot state
// file. An actor whose directory is gone was terminated on this node, so it is
// not written, and requeueLocked drops its entry as it does once the file is
// written. Best-effort: if writing fails, the entry stays in memory and atelet
// carries on from it, and a restart loses what changed.
func (t *Tracker) saveLocked(a *actorSnapshots) {
	if err := t.writeStateLocked(a); err != nil {
		slog.Warn("Failed to save the actor's snapshot state", slog.String("actorUID", a.actorUID), slog.Any("err", err))
	}
}

// writeStateLocked writes the actor's snapshot state file as saveLocked does,
// and returns the error if writing fails.
func (t *Tracker) writeStateLocked(a *actorSnapshots) error {
	if _, err := os.Stat(ateletpath.ActorPath(a.actorUID)); errors.Is(err, os.ErrNotExist) {
		slog.Warn("Dropping the snapshot state of an actor whose directory is gone", slog.String("actorUID", a.actorUID))
		a.unsaved = false
		t.undurableActorsTally.forget(a.actorUID)
		return nil
	}
	// The tally follows the entry, which atelet carries on from even if the
	// write fails.
	t.undurableActorsTally.observe(a)
	data, err := json.Marshal(a.actorSnapshotsRecord)
	if err == nil {
		err = atomicfile.Write(ateletpath.ActorSnapshotStateFile(a.actorUID), data, 0o600)
	}
	a.unsaved = err != nil
	return err
}

// readSnapshotState reads what atelet tracked of the actor from its snapshot
// state file, or returns nil when the file does not exist or cannot be read.
func readSnapshotState(actorUID string) *actorSnapshots {
	data, err := os.ReadFile(ateletpath.ActorSnapshotStateFile(actorUID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	var rec actorSnapshotsRecord
	if err == nil {
		err = json.Unmarshal(data, &rec)
	}
	if err != nil {
		slog.Warn("Skipping an unreadable snapshot state file", slog.String("actorUID", actorUID), slog.Any("err", err))
		return nil
	}
	return &actorSnapshots{actorUID: actorUID, actorSnapshotsRecord: rec}
}

// removeSnapshotState deletes the actor's snapshot state file. A missing file
// is not an error.
func removeSnapshotState(actorUID string) error {
	if err := os.Remove(ateletpath.ActorSnapshotStateFile(actorUID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("while deleting the actor's snapshot state: %w", err)
	}
	return nil
}

// removeDetachedActorState deletes a detached actor's snapshot state file and
// its now-empty directory once its detach report is no longer due.
func removeDetachedActorState(actorUID string) {
	if err := removeSnapshotState(actorUID); err != nil {
		slog.Warn("Failed to remove the detached actor's snapshot state", slog.String("actorUID", actorUID), slog.Any("err", err))
		return
	}
	if err := os.Remove(ateletpath.ActorPath(actorUID)); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ENOTEMPTY) && !errors.Is(err, syscall.EEXIST) {
		slog.Warn("Failed to remove the detached actor's directory", slog.String("actorUID", actorUID), slog.Any("err", err))
	}
}

// savedActorUIDs returns the UIDs of the actors with a snapshot state file on
// this node. A node with no actor directories yet has none. An actor whose
// file cannot be checked is included, so reading the file reports why.
func savedActorUIDs() ([]string, error) {
	entries, err := os.ReadDir(nodepath.ActorsDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("while listing actor directories: %w", err)
	}
	var actorUIDs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(entry.Name())); errors.Is(err, os.ErrNotExist) {
			continue
		}
		actorUIDs = append(actorUIDs, entry.Name())
	}
	return actorUIDs, nil
}
