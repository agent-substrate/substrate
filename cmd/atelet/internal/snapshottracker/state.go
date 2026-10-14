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

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/atomicfile"
	"github.com/agent-substrate/substrate/internal/nodepath"
)

// saveLocked writes what atelet tracks of the actor to its snapshot state
// file. An actor whose directory is gone was terminated on this node, so it is
// not written, and requeueLocked drops its entry as it does once the file is
// written. Best-effort: if writing fails, the entry stays in memory and atelet
// carries on from it, and a restart loses what changed.
func (t *Tracker) saveLocked(a *actorSnapshots) {
	if _, err := os.Stat(ateletpath.ActorPath(a.actorUID)); errors.Is(err, os.ErrNotExist) {
		slog.Warn("Dropping the snapshot state of an actor whose directory is gone", slog.String("actorUID", a.actorUID))
		a.unsaved = false
		return
	}
	data, err := json.Marshal(a.actorSnapshotsRecord)
	if err == nil {
		err = atomicfile.Write(ateletpath.ActorSnapshotStateFile(a.actorUID), data, 0o600)
	}
	a.unsaved = err != nil
	if err != nil {
		slog.Warn("Failed to save the actor's snapshot state", slog.String("actorUID", a.actorUID), slog.Any("err", err))
	}
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
