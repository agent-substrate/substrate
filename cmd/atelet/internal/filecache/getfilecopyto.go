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
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/sparsefile"
)

// GetFileCopyTo materializes the artifact identified by key at dst as a
// private copy, fetching it with fetch if it is not cached. Unlike
// GetFileTo's hard link, the result is an inode the caller owns outright
// (mode 0600): consumers that mutate the staged file in place get one they
// can safely write, at the cost of data-extent I/O per call — the copy
// preserves the cached file's holes. dst must be an absolute path that does
// not exist yet; it may be on any filesystem, a copy is not bound to the
// cache's mount.
//
// Everything else matches GetFileTo: concurrent calls for one key share a
// single fetch, and a served copy is immune to eviction — the data is read
// through a held-open handle, whose bytes the kernel keeps until the copy
// completes.
func (s *Store) GetFileCopyTo(ctx context.Context, key Key, dst string, fetch FileFetcher) error {
	return s.getTo(ctx, key, dst, fetch, s.copyOut)
}

// copyOut copies key's published data file to dst, reporting false (and no
// error) on a cache miss. Only the open runs under the hit lock; the copy
// itself proceeds outside it, safe against a concurrent retire because it
// reads the held-open handle, not the name.
func (s *Store) copyOut(key Key, dst string) (bool, error) {
	src, err := s.openData(key)
	if err != nil || src == nil {
		return false, err
	}
	defer src.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, fmt.Errorf("destination %s already exists: %w", dst, err)
		}
		return false, fmt.Errorf("while creating %s: %w", dst, err)
	}
	if _, err := sparsefile.Copy(src, out); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return false, fmt.Errorf("while copying %v to %s: %w", key, dst, err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(dst)
		return false, fmt.Errorf("while finishing copy of %v to %s: %w", key, dst, err)
	}
	return true, nil
}

// openData opens key's published data under the hit lock and touches the
// entry's last-use clock, returning (nil, nil) on a miss. Holding the open
// handle is what protects the caller afterwards: eviction may retire the
// entry the moment the lock is released, but the inode's bytes outlive its
// name for as long as the handle stays open.
func (s *Store) openData(key Key) (*os.File, error) {
	s.hitMu.RLock()
	defer s.hitMu.RUnlock()

	f, err := os.Open(s.dataPath(key))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("while opening cached %v: %w", key, err)
	}
	// Last-use touch for eviction's LRU ordering; best-effort, a miss only
	// ages the entry.
	now := time.Now()
	_ = os.Chtimes(s.entryDir(key), now, now)
	return f, nil
}
