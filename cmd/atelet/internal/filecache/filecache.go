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

// Package filecache is a node-local disk cache of downloaded artifacts,
// keyed by opaque identities (see Key). Its contract: an artifact wanted by
// N concurrent callers is fetched once, publication into the cache is atomic
// and crash-safe, and cached bytes are evicted under byte-budget pressure
// without ever breaking a consumer.
//
// On-disk layout, under a store's root (which the store owns exclusively):
//
//	entries/<sha256(key)>/
//	  data       # the cached file (or directory tree)
//	  meta.json  # canonical key + creation time; debugging only
//	tmp/         # in-flight fetches; same filesystem as entries/, so
//	             # publication is one atomic rename
//	.rm-*        # retired entries awaiting removal
//
// An entry directory's mtime is its last-use clock. Nothing on a correctness
// path reads meta.json: entries are matched to keys by hashing the keys.
//
// A crash can leave debris in tmp/ (a fetch that never finished) or .rm-*
// dirs (an eviction that renamed but never removed); SweepDebris reaps both
// and runs once at startup, before the store serves requests. Everything
// under entries/ is a complete, published entry.
package filecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	entriesDirName = "entries"
	tmpDirName     = "tmp"
	// rmPrefix marks a retired entry awaiting removal, at the store root (not
	// under entries/, so a retired entry is invisible to lookups and GC
	// listings the moment it is renamed).
	rmPrefix = ".rm-"

	dataName = "data"
	metaName = "meta.json"

	// defaultMinAge is the default eviction minimum age (see WithMinAge).
	defaultMinAge = 10 * time.Minute
	// defaultFetchTimeout is the default per-fetch bound (see
	// WithFetchTimeout). Generous enough for multi-GiB artifacts on a busy
	// node.
	defaultFetchTimeout = 10 * time.Minute
)

// Store is one on-disk cache. It is safe for concurrent use and assumes it
// is the only writer under its root (one atelet per node).
type Store struct {
	root string

	// minAge vetoes eviction of any entry younger than this, covering the
	// window between publication and the consumer's use becoming visible to
	// GC (a hardlink's Nlink, or a root-set record).
	minAge time.Duration

	// fetchTimeout bounds each fetch. Fetches run detached from the contexts
	// of the callers waiting on them, so this is the only bound on how long
	// one can run.
	fetchTimeout time.Duration
}

// Option configures a Store.
type Option func(*Store)

// WithMinAge sets the eviction minimum age.
func WithMinAge(d time.Duration) Option {
	return func(s *Store) { s.minAge = d }
}

// WithFetchTimeout sets the per-fetch bound.
func WithFetchTimeout(d time.Duration) Option {
	return func(s *Store) { s.fetchTimeout = d }
}

// New opens (creating if needed) the store rooted at root.
func New(root string, opts ...Option) (*Store, error) {
	s := &Store{
		root:         root,
		minAge:       defaultMinAge,
		fetchTimeout: defaultFetchTimeout,
	}
	for _, opt := range opts {
		opt(s)
	}
	// Entries are world-readable (their files get hard-linked into consumer
	// dirs and, later, consumed in place); tmp holds unpublished fetches and
	// stays private.
	if err := os.MkdirAll(s.entriesDir(), 0o755); err != nil {
		return nil, fmt.Errorf("while creating entries dir: %w", err)
	}
	if err := os.MkdirAll(s.tmpDir(), 0o700); err != nil {
		return nil, fmt.Errorf("while creating tmp dir: %w", err)
	}
	return s, nil
}

func (s *Store) entriesDir() string { return filepath.Join(s.root, entriesDirName) }
func (s *Store) tmpDir() string     { return filepath.Join(s.root, tmpDirName) }

// entryDir is the published location of k's entry.
func (s *Store) entryDir(k Key) string { return filepath.Join(s.entriesDir(), k.dir) }

// dataPath is the published location of k's cached file (or tree).
func (s *Store) dataPath(k Key) string { return filepath.Join(s.entryDir(k), dataName) }

// entryMeta is the debugging sidecar written next to an entry's data. It is
// never read on a correctness path; a missing or corrupt one affects
// nothing.
type entryMeta struct {
	// Key is the canonical key string, so an operator staring at du output
	// can tell what an entry holds.
	Key       string    `json:"key"`
	CreatedAt time.Time `json:"createdAt"`
}

// writeEntryMeta writes the meta.json sidecar into an (unpublished) entry
// dir.
func writeEntryMeta(entryDir string, k Key, createdAt time.Time) error {
	data, err := json.Marshal(entryMeta{Key: k.String(), CreatedAt: createdAt})
	if err != nil {
		return fmt.Errorf("while marshaling entry meta: %w", err)
	}
	if err := os.WriteFile(filepath.Join(entryDir, metaName), data, 0o644); err != nil {
		return fmt.Errorf("while writing entry meta: %w", err)
	}
	return nil
}

// readEntryMeta loads an entry dir's meta.json sidecar.
func readEntryMeta(entryDir string) (entryMeta, error) {
	data, err := os.ReadFile(filepath.Join(entryDir, metaName))
	if err != nil {
		return entryMeta{}, fmt.Errorf("while reading entry meta: %w", err)
	}
	var m entryMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return entryMeta{}, fmt.Errorf("while parsing entry meta: %w", err)
	}
	return m, nil
}

// SweepStats reports what a SweepDebris pass removed.
type SweepStats struct {
	// TmpRemoved counts removed tmp/ children (unfinished fetches).
	TmpRemoved int
	// RetiredRemoved counts removed .rm-* dirs (interrupted evictions).
	RetiredRemoved int
}

// SweepDebris removes crash debris: everything under tmp/ and every .rm-*
// dir at the store root. It runs once at startup, before the store serves
// requests; published entries are never touched. Removal failures are
// joined and reported after the sweep visits everything, so one bad path
// does not shadow the rest.
func (s *Store) SweepDebris(ctx context.Context) (SweepStats, error) {
	var stats SweepStats
	var errs []error

	tmpChildren, err := os.ReadDir(s.tmpDir())
	if err != nil {
		errs = append(errs, fmt.Errorf("while listing tmp dir: %w", err))
	}
	for _, child := range tmpChildren {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if err := os.RemoveAll(filepath.Join(s.tmpDir(), child.Name())); err != nil {
			errs = append(errs, fmt.Errorf("while removing tmp debris %q: %w", child.Name(), err))
			continue
		}
		stats.TmpRemoved++
	}

	rootChildren, err := os.ReadDir(s.root)
	if err != nil {
		errs = append(errs, fmt.Errorf("while listing store root: %w", err))
	}
	for _, child := range rootChildren {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !strings.HasPrefix(child.Name(), rmPrefix) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(s.root, child.Name())); err != nil {
			errs = append(errs, fmt.Errorf("while removing retired entry %q: %w", child.Name(), err))
			continue
		}
		stats.RetiredRemoved++
	}

	return stats, errors.Join(errs...)
}

// TotalBytes sums the sizes of all published entries' regular files. It is
// the GC driver's usage measure against the store's byte budget.
func (s *Store) TotalBytes(ctx context.Context) (int64, error) {
	var total int64
	err := filepath.WalkDir(s.entriesDir(), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// An entry retired mid-walk is not an error; skip what vanished.
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("while sizing entries: %w", err)
	}
	return total, nil
}
