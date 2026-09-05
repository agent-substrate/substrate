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
	"syscall"
	"time"
)

// FileFetcher produces a single-file artifact at dstPath (creating the file
// itself, so it can seek and truncate for sparse output). It runs at most
// once per key across concurrent GetFileTo callers, detached from their
// contexts and bounded by the store's fetch timeout. Its error is delivered
// to every caller waiting on the flight, wrapped with %w so error
// classification (errors.Is) sees through the store.
type FileFetcher func(ctx context.Context, dstPath string) error

// linkRetries bounds the publish-then-link loop in GetFileTo. An entry can
// be evicted between a flight completing and this caller's link only if it
// sat unlinked past the store's min age, so a single retry is already an
// anomaly; more than a few means something is deleting entries out from
// under the store.
const linkRetries = 3

// GetFileTo materializes the artifact identified by key at dst, fetching it
// with fetch if it is not cached. dst must be an absolute path that does not
// exist yet and lives on the cache's filesystem (the same mount, not just
// the same disk): on success it is a hard link to the read-only cache
// copy, so the caller keeps a valid file regardless of later eviction, and
// the entry is published mode 0444 so an in-place write fails loudly rather
// than corrupting the shared bytes.
//
// Concurrent calls for one key share a single fetch. A caller whose ctx is
// canceled returns early with ctx.Err() while the fetch keeps running for
// the others; there is no negative caching, so after a failed fetch the
// next call starts fresh.
func (s *Store) GetFileTo(ctx context.Context, key Key, dst string, fetch FileFetcher) error {
	return s.getTo(ctx, key, dst, fetch, s.linkOut)
}

// getTo is the read-through loop shared by GetFileTo and GetFileCopyTo:
// serve a hit via out (link or copy), else run the singleflight fetch and
// retry.
func (s *Store) getTo(ctx context.Context, key Key, dst string, fetch FileFetcher, out func(Key, string) (bool, error)) error {
	if key.isZero() {
		return errors.New("filecache: zero Key (use a Key constructor)")
	}
	if !filepath.IsAbs(dst) {
		return fmt.Errorf("filecache: destination %q is not an absolute path", dst)
	}
	if fetch == nil {
		return errors.New("filecache: nil FileFetcher")
	}
	for attempt := 0; ; attempt++ {
		served, err := out(key, dst)
		if err != nil {
			return err
		}
		if served {
			return nil
		}
		if attempt >= linkRetries {
			return fmt.Errorf("entry for %v vanished after %d fetches; is something else deleting under the store root?", key, attempt)
		}
		if err := s.fetchFlight(ctx, key, fetch); err != nil {
			return err
		}
	}
}

// linkOut links key's published data file to dst and touches the entry's
// last-use clock, reporting false (and no error) on a cache miss. The
// shared hitMu holds eviction's final re-check out of the stat-to-link
// window, so a published entry cannot be retired mid-hit.
func (s *Store) linkOut(key Key, dst string) (bool, error) {
	s.hitMu.RLock()
	defer s.hitMu.RUnlock()

	if _, err := os.Stat(s.dataPath(key)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("while checking cache for %v: %w", key, err)
	}
	if err := os.Link(s.dataPath(key), dst); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, fmt.Errorf("destination %s already exists: %w", dst, err)
		}
		if errors.Is(err, syscall.EXDEV) {
			return false, fmt.Errorf("destination %s is not on the cache's filesystem (link-out requires one filesystem): %w", dst, err)
		}
		return false, fmt.Errorf("while linking %v to %s: %w", key, dst, err)
	}
	// Last-use touch for eviction's LRU ordering; best-effort, a miss only
	// ages the entry.
	now := time.Now()
	_ = os.Chtimes(s.entryDir(key), now, now)
	return true, nil
}

// fetchFlight runs (or joins) the singleflight fetch for key and waits for
// it or for ctx. The flight itself runs on a context detached from the
// callers' — bounded only by the store's fetch timeout — so one canceled
// caller never aborts a download other callers are waiting on.
func (s *Store) fetchFlight(ctx context.Context, key Key, fetch FileFetcher) error {
	ch := s.sf.DoChan(key.dir, func() (any, error) {
		return nil, s.fetchAndPublish(context.WithoutCancel(ctx), key, fetch)
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return fmt.Errorf("while fetching %v: %w", key, res.Err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// fetchAndPublish runs one fetch into tmp/ and atomically publishes the
// result under entries/. On any failure the temp dir is removed, so a bad
// fetch is never visible in the cache; a crash instead leaves it for
// SweepDebris.
func (s *Store) fetchAndPublish(ctx context.Context, key Key, fetch FileFetcher) error {
	ctx, cancel := context.WithTimeout(ctx, s.fetchTimeout)
	defer cancel()

	// A flight that completed between this caller's miss and this flight
	// starting may have published already.
	if _, err := os.Stat(s.dataPath(key)); err == nil {
		return nil
	}

	tmpDir, err := os.MkdirTemp(s.tmpDir(), key.dir+"-")
	if err != nil {
		return fmt.Errorf("while creating fetch temp dir: %w", err)
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(tmpDir)
		}
	}()

	dataPath := filepath.Join(tmpDir, dataName)
	if err := fetch(ctx, dataPath); err != nil {
		return err
	}
	fi, err := os.Stat(dataPath)
	if err != nil {
		return fmt.Errorf("fetcher succeeded but produced no file: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("fetcher produced %v, want a regular file", fi.Mode())
	}

	// Read-only before publication: cached bytes are shared through hard
	// links, so an in-place write by any consumer must fail rather than
	// poison the copy every later caller links.
	if err := os.Chmod(dataPath, 0o444); err != nil {
		return fmt.Errorf("while making fetched file read-only: %w", err)
	}
	if err := writeEntryMeta(tmpDir, key, time.Now()); err != nil {
		return err
	}
	if err := os.Chmod(tmpDir, 0o755); err != nil { // MkdirTemp created it 0700
		return fmt.Errorf("while setting entry dir mode: %w", err)
	}
	if err := os.Rename(tmpDir, s.entryDir(key)); err != nil {
		// Within one process the singleflight makes a rename race
		// unreachable; tolerating a loss anyway keeps overlap with a
		// crashed predecessor's published entry safe.
		if errors.Is(err, syscall.EEXIST) || errors.Is(err, syscall.ENOTEMPTY) {
			return nil
		}
		return fmt.Errorf("while publishing entry: %w", err)
	}
	published = true
	return nil
}
