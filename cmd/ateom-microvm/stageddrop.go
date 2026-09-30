//go:build linux

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
)

// dropSuffix marks a path this module has taken ownership of for removal, so a
// later sweep over the parent can tell an in-flight drop from a live file.
const dropSuffix = ".ateom-drop"

// stagedDrop is a removal of a staged snapshot file that has been moved off the
// caller's path and is being unlinked in the background.
type stagedDrop struct {
	// path is the renamed file or directory, which is what the unlink targets.
	path string
	// done is closed once the unlink has finished (successfully or not).
	done chan struct{}
}

// wait blocks until the drop has finished.
func (d *stagedDrop) wait() {
	<-d.done
}

// startStagedDrop takes ownership of path and unlinks it in the background.
//
// The rename is what makes this safe to hand off: it is a single directory
// operation, while the unlink that follows is proportional to the size of what
// the snapshot wrote (measured at 0.4-4.3s for a 1 GiB image on a
// network-backed worker dir). Moving the file out of the way synchronously
// means every reader that goes looking for the staged name finds it already
// gone, so nothing has to wait for the bytes to be freed. Because the unlink
// targets the renamed path rather than the original one, a caller that stages a
// fresh file under the original name is never raced by this drop.
//
// A path that does not exist is not an error: the whole point of the rename is
// that the staged file is dead weight the caller has already decided to lose,
// and a concurrent teardown having removed it first is the outcome we wanted.
//
// A crash between the rename and the unlink strands the renamed file until
// whatever owns the parent directory resets it. That is acceptable only because
// the callers here drop paths the owner already re-creates wholesale, so the
// staged image was never going to outlive the actor either way.
func startStagedDrop(ctx context.Context, path string) *stagedDrop {
	dropped := path + dropSuffix
	if err := os.Rename(path, dropped); err != nil {
		// ENOENT is the expected race with a teardown that got there first;
		// anything else leaves the original in place, which is the old
		// behavior, so this stays non-fatal.
		slog.DebugContext(ctx, "nothing to drop", slog.String("path", path), slog.Any("err", err))
		return nil
	}
	drop := &stagedDrop{path: dropped, done: make(chan struct{})}
	// Detached: the unlink outlives the caller's RPC, whose context is cancelled
	// the moment restore returns.
	unlinkCtx := context.WithoutCancel(ctx)
	go func() {
		defer close(drop.done)
		if err := os.RemoveAll(dropped); err != nil {
			// Not fatal: at worst the space is held until the actor is torn
			// down, which is exactly what a failed drop cost before.
			slog.WarnContext(unlinkCtx, "could not drop staged snapshot file", slog.String("path", dropped), slog.Any("err", err))
		}
	}()
	return drop
}
