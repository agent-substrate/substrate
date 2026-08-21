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
// completion marker both ateom runtimes leave beside a finished checkpoint.
//
// A checkpoint is destructive: it takes the sandbox down. That makes it the
// one workload operation whose response cannot simply be re-derived by trying
// again — a caller that loses the response (an atelet restart, a deadline
// exceeded mid-call) has no way to tell "never started" from "finished, and
// the answer went missing". Replaying it drove the sandbox runtime against
// state the first attempt had already destroyed.
//
// The marker is what makes the second attempt answerable: ateom writes it once
// the snapshot files are all on disk, recording exactly the file list it is
// about to report, and consults it before touching the runtime. Writes are
// atomic, so a marker that exists is always complete.
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

// Record is the marker's content: the snapshot files ateom wrote, in the same
// order it reported them, so a replayed response is identical to the original,
// and the scope they were written for, which says which checkpoint they belong
// to. The file list may be empty — see Write.
type Record struct {
	SnapshotFiles []string `json:"snapshotFiles"`
	// Scope is the CheckpointWorkloadRequest's scope, stringified. A marker
	// records one particular checkpoint, not merely that the actor has had
	// one, so a request asking for different content is not answerable from it
	// — see Read.
	Scope string `json:"scope"`
}

// Write records the completed checkpoint for actorUID. It writes atomically
// (temp file plus rename) so a crash mid-write leaves no marker rather than a
// truncated one that would be read as a complete checkpoint.
//
// An empty snapshotFiles is recorded, not refused. A Data-scope checkpoint of
// an actor whose only volumes are CSI ones legitimately writes no files — the
// volumes are snapshotted out of band — and that is a checkpoint that
// completed and took the guest down like any other. Refusing to mark it would
// leave the one result that most needs replaying unrecorded: the retry would
// find no marker, find no guest either, and report an actor whose state is
// unrecoverable when in truth its checkpoint had succeeded. Whether an empty
// set is acceptable for a given request is atelet's call (shouldHaveSnapshots),
// and it reaches the same verdict on the replay as on the original.
func Write(actorUID, scope string, snapshotFiles []string) error {
	// A marker that does not say which checkpoint it records is unusable: Read
	// can only match it against a request's scope, so an unscoped one could
	// never be replayed.
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
	// Flush the bytes before the rename publishes the name: a rename over
	// unsynced content can survive a node crash as an empty file.
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("while syncing checkpoint marker: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("while closing checkpoint marker: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("while renaming checkpoint marker into place: %w", err)
	}
	// Sync the parent directory as well. The rename is a directory-metadata
	// change, and syncing the file's contents does not commit the name that
	// makes them findable: without this a node crash can lose the marker
	// entirely, which is precisely the crash the marker exists to survive.
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

// Read returns the marker recorded for actorUID by a checkpoint of the given
// scope. ok is false when no such checkpoint has completed, which is the
// ordinary case on a first attempt and is not an error.
//
// The scope match is what makes the marker answer "did THIS checkpoint
// finish?" rather than "has this actor been checkpointed?". The two differ
// because the marker is keyed on the actor: it names a per-actor path, and it
// outlives the attempt that wrote it until resetActorDirs clears it. A DATA
// marker replayed against a FULL request would have atelet commit a manifest
// claiming a full snapshot whose file set is only the durable-dir tar, leaving
// nothing to resume the guest from.
//
// A mismatch reports "no completed checkpoint" and leaves the marker alone:
// the request falls through to the ordinary path, where the runtime finds no
// sandbox to checkpoint and says so as unrecoverable. That is the honest
// answer for a differently-scoped checkpoint of an actor whose sandbox an
// earlier one already destroyed.
//
// Leaving it alone is about what Read may do, not about how long the marker
// survives: a record Read cannot use is still not Read's to delete, unlike the
// damaged one below that nobody can use. The fall-through usually removes it
// moments later anyway — both runtimes clear the checkpoint dir before taking
// a checkpoint — so this is not a promise that a mismatched marker outlives
// the call.
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
	// A marker naming no files is a real result, not a damaged one: see Write.
	// Scope is what separates the two — it is required, so a marker that parsed
	// but carries no scope is the damaged case, and the mismatch below catches
	// it.
	//
	// An unscoped marker was written by an ateom from before the scope was
	// recorded. It cannot be matched, so it cannot be replayed; treat it like
	// any other mismatch rather than assuming it meant the scope now asked
	// for.
	if rec.Scope != scope {
		slog.Info("Checkpoint marker records a different checkpoint; not replaying it",
			slog.String("actor_uid", actorUID), slog.String("marker_scope", rec.Scope), slog.String("requested_scope", scope))
		return nil, false, nil
	}
	return rec, true, nil
}

// discardUnusable removes a marker that cannot be used, so the caller can
// report "no completed checkpoint" and re-run one.
//
// A damaged marker is not evidence of anything: it can neither replay a result
// nor prove one was produced. Keeping it would wedge the actor permanently —
// nothing else ever deletes the marker, so every retry would fail here
// identically while the control plane re-drove a pause/suspend that could
// never progress. Re-running the checkpoint is the recoverable answer: if the
// sandbox is still there the checkpoint simply succeeds, and if an earlier
// attempt already tore it down, the runtime's own failure classification
// reports that as unrecoverable rather than as a retriable error.
func discardUnusable(path, actorUID string, reason error) {
	slog.Warn("Discarding unusable checkpoint marker; the checkpoint will be re-attempted",
		slog.String("actor_uid", actorUID), slog.String("path", path), slog.Any("err", reason))
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		// Not fatal: a later successful checkpoint overwrites the marker by
		// rename regardless.
		slog.Warn("Failed to remove unusable checkpoint marker",
			slog.String("actor_uid", actorUID), slog.String("path", path), slog.Any("err", err))
	}
}
