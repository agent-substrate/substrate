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
// rename(2) under the pin lock — the only step that contends with the
// pull path) and the slow RemoveAll of the renamed-aside tree
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
)

// retiredPrefix marks a layer dir that eviction has renamed aside and that
// no longer exists by diffid. It shares the dot-hidden namespace with
// ".tmp-" so diffid-named dirs can never collide with it.
const retiredPrefix = ".rm-"

// logMsgLayerRetireVetoed is a fixed message so an e2e can grep for the
// exact race being exercised.
const logMsgLayerRetireVetoed = "Image cache layer retirement vetoed: recently used"

// logMsgLayerRetirePinned is its counterpart for a layer an in-flight pull
// is using, which can hold a layer for up to the pull timeout.
const logMsgLayerRetirePinned = "Image cache layer retirement vetoed: in use by a pull"

// retireStatus reports what retireLayer did with a layer.
type retireStatus int

const (
	// retireGone: no dir under the layer's final name — already removed, or
	// mid-unpack (only the commit rename creates the final name). Nothing
	// stranded.
	retireGone retireStatus = iota
	// retireVetoed: the layer stays — pinned by an in-flight pull, fresh
	// mtime, or a failed rename.
	retireVetoed
	// retireRetired: renamed to a ".rm-*" name, gone from the pool; the
	// caller removes the renamed dir afterwards.
	retireRetired
)

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
// pinned by an in-flight pull, or with an mtime after cutoff, is vetoed
// and left in place.
//
// The pin check, the mtime check and the rename all run under pinMu, the
// lock a pull takes to pin: either the pin lands first and the retirement
// is vetoed, or the rename lands first and the pull finds the dir missing
// and unpacks it again. The hold is one stat and one rename.
func (s *Store) retireLayer(hex string, cutoff time.Time) (string, retireStatus, error) {
	if !isLayerDirName(hex) {
		return "", retireVetoed, fmt.Errorf("not a layer dir name: %q", hex)
	}

	var (
		dst string
		st  retireStatus
		err error
	)
	if n := s.whileUnpinned(hex, func() { dst, st, err = s.renameAside(hex, cutoff) }); n > 0 {
		slog.Info(logMsgLayerRetirePinned, slog.String("diffid", hex), slog.Int("pulls", n))
		return "", retireVetoed, nil
	}
	return dst, st, err
}

// whileUnpinned runs fn with pinMu held, unless pulls have hex pinned, in
// which case it returns how many without running fn. No pull can pin the
// layer while fn runs, so a pull either vetoes fn or sees what fn left.
func (s *Store) whileUnpinned(hex string, fn func()) (pinnedBy int) {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	if n := s.pins[hex]; n > 0 {
		return n
	}
	fn()
	return 0
}

// renameAside renames an unpinned layer dir to a ".rm-*" name unless its
// mtime is after cutoff. Run only under whileUnpinned.
func (s *Store) renameAside(hex string, cutoff time.Time) (string, retireStatus, error) {
	dir := filepath.Join(s.layersDir(), hex)
	fi, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return "", retireGone, nil
	} else if err != nil {
		return "", retireVetoed, err
	}
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
