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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
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

	// sf collapses concurrent fetches of the same key into one flight.
	// Eviction retires entries inside the same flight, so a retire can
	// never race a fetch of the key it is removing.
	sf singleflight.Group

	// hitMu closes the hit-vs-evict window: held shared while serving a hit
	// (stat, link or open, and last-use touch), exclusive by eviction's
	// final re-check and retire rename. An entry therefore cannot vanish
	// mid-hit. Uncontended except during an eviction pass.
	hitMu sync.RWMutex

	// evictMu serializes EvictUnused passes (concurrent passes would fight
	// over the same candidates for no benefit).
	evictMu sync.Mutex
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
