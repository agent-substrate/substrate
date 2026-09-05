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

package filecache

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
	"time"
)

// EvictStats reports what an EvictUnused pass did (or, dry-run, would do).
type EvictStats struct {
	// Retired counts entries renamed out of the cache's namespace: from
	// that rename on, lookups miss and refetch, whether or not the later
	// physical removal succeeded (FreedBytes tracks that part). In a dry
	// run, the entries a real pass would have retired.
	Retired int
	// FreedBytes counts bytes actually returned to the filesystem: the sizes
	// of retired entries whose data had no consumer hard links left and
	// whose physical removal completed. An entry retired but not removed (a
	// RemoveAll failure, reported in the returned error) is excluded; its
	// bytes sit in a .rm-* dir until SweepDebris. In a dry run this is the
	// would-free estimate.
	FreedBytes int64
	// PendingBytes counts bytes of retired entries whose data is still
	// hard-linked by a consumer: the cache's claim is gone, but the kernel
	// frees the space only when the last consumer link is removed.
	PendingBytes int64
	// SkippedYoung counts entries vetoed by the store's min age.
	SkippedYoung int
	// SkippedBusy counts entries vetoed at retire time: a hit or a fresh
	// fetch moved their last-use clock after the pass listed them.
	SkippedBusy int
}

// evictCandidate is one listed entry, snapshotted lock-free at the start of
// a pass; retireEntry re-verifies it under the locks before touching it.
type evictCandidate struct {
	dir    string // entry dir name (the key hash)
	mtime  time.Time
	size   int64
	linked bool // data is a regular file some consumer still hard-links
}

// EvictUnused frees cache space until targetBytes of actually-freeable
// bytes are reclaimed or no eligible entries remain, least-recently-used
// first. It never removes an entry younger than the store's min age, never
// races a fetch or a hit (both veto at retire time), and never breaks a
// consumer: an entry whose data is still hard-linked can be retired — its
// bytes count as pending, freed by the kernel when the last consumer link
// goes — so the worst outcome for any caller is a re-download.
//
// With dryRun, nothing is touched and the stats report what a real pass
// would have chosen. Passes are pressure-driven: the caller decides when
// and how much; a non-positive target is a no-op.
func (s *Store) EvictUnused(ctx context.Context, targetBytes int64, dryRun bool) (EvictStats, error) {
	var stats EvictStats
	if targetBytes <= 0 {
		return stats, nil
	}
	s.evictMu.Lock()
	defer s.evictMu.Unlock()

	candidates, err := s.listCandidates(ctx)
	if err != nil {
		return stats, err
	}

	// Age gate, then order: entries nobody links first (evicting a linked
	// entry frees nothing now), least recently used within each group.
	now := time.Now()
	eligible := candidates[:0]
	for _, c := range candidates {
		if now.Sub(c.mtime) < s.minAge {
			stats.SkippedYoung++
			continue
		}
		eligible = append(eligible, c)
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].linked != eligible[j].linked {
			return !eligible[i].linked
		}
		return eligible[i].mtime.Before(eligible[j].mtime)
	})

	// Victim selection runs against selectedBytes, not stats.FreedBytes:
	// freed is only credited once physical removal succeeds below.
	type retiredEntry struct {
		path  string
		freed int64 // c.size for unlinked victims, 0 for linked ones
	}
	var errs []error
	var retired []retiredEntry
	var selectedBytes int64
	for _, c := range eligible {
		if selectedBytes >= targetBytes {
			break
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		var freed int64
		if !c.linked {
			freed = c.size
		}
		if !dryRun {
			rmPath, ok, err := s.retireEntry(c)
			if err != nil {
				errs = append(errs, fmt.Errorf("while retiring entry %s: %w", c.dir, err))
				continue
			}
			if !ok {
				stats.SkippedBusy++
				continue
			}
			retired = append(retired, retiredEntry{path: rmPath, freed: freed})
		} else {
			stats.FreedBytes += freed
		}
		stats.Retired++
		stats.PendingBytes += c.size - freed
		selectedBytes += freed
	}

	// The slow physical deletion happens after all retires, outside hitMu
	// and the singleflight, so hits and fetches never wait on it (evictMu
	// stays held: only a concurrent pass would wait, and serializing passes
	// is its job). A crash before it finishes leaves .rm-* dirs for
	// SweepDebris, as does a removal failure here — those bytes are not
	// counted freed.
	for _, r := range retired {
		if err := os.RemoveAll(r.path); err != nil {
			errs = append(errs, fmt.Errorf("while removing retired entry %s: %w", filepath.Base(r.path), err))
			continue
		}
		stats.FreedBytes += r.freed
	}
	return stats, errors.Join(errs...)
}

// listCandidates snapshots the published entries. Deliberately lock-free
// and therefore stale: retireEntry re-verifies every victim before acting,
// so racing hits and fetches only make a candidate disappear, never a
// wrong eviction.
func (s *Store) listCandidates(ctx context.Context) ([]evictCandidate, error) {
	children, err := os.ReadDir(s.entriesDir())
	if err != nil {
		return nil, fmt.Errorf("while listing entries: %w", err)
	}
	candidates := make([]evictCandidate, 0, len(children))
	for _, child := range children {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !child.IsDir() {
			continue
		}
		entryDir := filepath.Join(s.entriesDir(), child.Name())
		fi, err := os.Stat(entryDir)
		if err != nil {
			continue // retired mid-listing
		}
		size, linked, err := s.sizeEntry(entryDir)
		if err != nil {
			continue // ditto
		}
		candidates = append(candidates, evictCandidate{
			dir:    child.Name(),
			mtime:  fi.ModTime(),
			size:   size,
			linked: linked,
		})
	}
	return candidates, nil
}

// sizeEntry sums an entry's regular-file bytes and reports whether its data
// file carries consumer hard links (a directory-tree entry cannot, so it
// reports unlinked and relies on the min age and, later, a root set).
func (s *Store) sizeEntry(entryDir string) (size int64, linked bool, err error) {
	err = filepath.WalkDir(entryDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		size += info.Size()
		if path == filepath.Join(entryDir, dataName) {
			if st, ok := info.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
				linked = true
			}
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return size, linked, nil
}

// retireEntry removes c from the cache's namespace if it is still exactly
// the entry the pass listed, returning the renamed .rm-* path and whether
// it retired. It runs inside the key's singleflight — joining an in-flight
// fetch instead of racing it (the join's shared result leaves retired
// false) — and takes hitMu exclusively for the final re-check and rename,
// so a hit can never link out of an entry being retired. A moved last-use
// clock is a veto, not an error.
func (s *Store) retireEntry(c evictCandidate) (string, bool, error) {
	var rmPath string
	var retired bool
	_, err, _ := s.sf.Do(c.dir, func() (any, error) {
		s.hitMu.Lock()
		defer s.hitMu.Unlock()

		entryDir := filepath.Join(s.entriesDir(), c.dir)
		fi, err := os.Stat(entryDir)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil // already gone
		}
		if err != nil {
			return nil, err
		}
		if !fi.ModTime().Equal(c.mtime) {
			return nil, nil // hit or refetched since the listing: veto
		}
		// Unique suffix: a crashed pass may have left .rm-<dir>-* behind and
		// the key may have been refetched and retired again before a sweep.
		p := filepath.Join(s.root, rmPrefix+c.dir+"-"+strconv.FormatInt(time.Now().UnixNano(), 36))
		if err := os.Rename(entryDir, p); err != nil {
			return nil, err
		}
		rmPath, retired = p, true
		return nil, nil
	})
	return rmPath, retired, err
}
