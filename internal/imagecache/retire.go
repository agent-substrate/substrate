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

package imagecache

// Two-phase layer deletion: eviction renames a layer dir aside (one
// rename(2) under the layer interlock — the only step that contends
// with the pull path) and the slow RemoveAll of the renamed-aside tree
// happens afterwards, outside all locks. A crash in between leaves a
// ".rm-*" dir for the startup sweep. Nothing here needs privileges:
// retirement is rename/chmod/unlink, which plain root can do even on
// read-only trees.

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/semaphore"
)

// retiredPrefix marks a layer dir that eviction has renamed aside and that
// no longer exists by diffid. It shares the dot-hidden namespace with
// ".tmp-" so diffid-named dirs can never collide with it.
const retiredPrefix = ".rm-"

// logMsgLayerRetireVetoed is a fixed message so an e2e can grep for the
// exact race being exercised.
const logMsgLayerRetireVetoed = "Image cache layer retirement vetoed: recently used"

// retireStatus reports what retireLayer did with a layer.
type retireStatus int

const (
	// retireGone: no dir under the layer's final name — already removed, or
	// mid-unpack (only the commit rename creates the final name). Nothing
	// stranded.
	retireGone retireStatus = iota
	// retireVetoed: the layer stays — fresh mtime, in-flight reuse, or a
	// failed rename.
	retireVetoed
	// retireRetired: renamed to a ".rm-*" name, gone from the pool; the
	// caller removes the renamed dir afterwards.
	retireRetired
)

// layerLock returns one layer's interlock, held by ensureLayer across its
// stat and refresh-or-unpack and by retireLayer across its stat and rename,
// so a reuse and a retirement never interleave. A lock rather than the
// shared flight key, because a caller that joins a flight cannot tell
// whether it joined a reuse or a retirement that renamed the dir away.
// Never share one between layers: it is held for the length of an unpack,
// so sharing would serialize unrelated downloads and let a retirement veto
// a layer it could have taken.
func (s *Store) layerLock(hex string) *semaphore.Weighted {
	if lk, ok := s.layerLocks.Load(hex); ok {
		return lk.(*semaphore.Weighted)
	}
	lk, _ := s.layerLocks.LoadOrStore(hex, semaphore.NewWeighted(1))
	return lk.(*semaphore.Weighted)
}

// isLayerDirName reports whether name is a well-formed sha256 layer
// directory name. Callers enumerate directories and read hexes out of
// records, so they can encounter anything an operator (or a corrupt
// record) left there; only conforming names are treated as layers.
func isLayerDirName(name string) bool {
	if len(name) != 64 {
		return false
	}
	for i := 0; i < len(name); i++ {
		if c := name[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// retireLayer evicts a layer by renaming its dir to a ".rm-*" name and
// returns the renamed path; the caller deletes it afterwards. A layer
// with an mtime after cutoff is vetoed and left in place.
//
// The mtime check and the rename run under the layer's interlock — the same
// lock ensureLayer holds while it refreshes the mtime — so a retirement and
// a reuse cannot interleave: whichever runs second sees the first.
func (s *Store) retireLayer(hex string, cutoff time.Time) (string, retireStatus, error) {
	if !isLayerDirName(hex) {
		return "", retireVetoed, fmt.Errorf("not a layer dir name: %q", hex)
	}
	dir := filepath.Join(s.layersDir(), hex)

	// Pre-flight, outside the interlock: a missing dir or a fresh mtime means
	// nothing to do and no reason to contend with the pull path at all. Both
	// checks are re-run under the lock, so this is a fast path, not the
	// correctness path.
	if fi, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return "", retireGone, nil
	} else if err != nil {
		return "", retireVetoed, err
	} else if fi.ModTime().After(cutoff) {
		slog.Info(logMsgLayerRetireVetoed, slog.String("diffid", hex), slog.Time("last_used", fi.ModTime()))
		return "", retireVetoed, nil
	}

	// If we cannot acquire the lock it's being held by ensureLayer, which is
	// using the layer.
	lock := s.layerLock(hex)
	if !lock.TryAcquire(1) {
		return "", retireVetoed, nil
	}
	defer lock.Release(1)

	fi, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return "", retireGone, nil
	} else if err != nil {
		return "", retireVetoed, err
	}
	// A concurrent ensureLayer touched the dir if it reused this layer since
	// the pre-flight stat.
	if fi.ModTime().After(cutoff) {
		slog.Info(logMsgLayerRetireVetoed, slog.String("diffid", hex), slog.Time("last_used", fi.ModTime()))
		return "", retireVetoed, nil
	}

	dst := filepath.Join(s.layersDir(), fmt.Sprintf("%s%s-%d", retiredPrefix, hex[:12], time.Now().UnixNano()))
	if err := os.Rename(dir, dst); err != nil {
		return "", retireVetoed, fmt.Errorf("while retiring layer %s: %w", hex, err)
	}
	return dst, retireRetired, nil
}
