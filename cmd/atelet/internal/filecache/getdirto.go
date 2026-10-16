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

// DirFetcher downloads a directory-tree artifact (an extracted release
// archive, say) into dstDir, which the store has created empty. The tree
// may hold only directories and regular files. As with FileFetcher, the
// store runs it at most once per key across concurrent callers, detached
// from their contexts and bounded by the fetch timeout, and its error
// reaches every waiting caller wrapped with %w.
type DirFetcher func(ctx context.Context, dstDir string) error

// GetDirTo materializes the tree identified by key at dst, fetching it with
// fetch if it is not cached. dst must be an absolute path that does not
// exist yet and lives on the cache's filesystem. On success it is a
// directory the caller owns, mirroring the cached tree, in which every
// regular file is a hard link to the read-only cache copy. The caller keeps
// a valid tree regardless of later eviction, and may add files of its own
// without touching the cache; writing through a linked file fails loudly.
//
// dst is built beside its final name and renamed into place, so it never
// exists half-linked: an existing dst, reported as fs.ErrExist, is a
// complete tree. Concurrent calls for one key share a single fetch, as in
// GetFileTo.
func (s *Store) GetDirTo(ctx context.Context, key Key, dst string, fetch DirFetcher) error {
	if fetch == nil {
		return errors.New("filecache: nil DirFetcher")
	}
	return s.getTo(ctx, key, dst, fetchFunc(fetch), kindTree, s.linkTreeOut)
}

// linkTreeOut mirrors key's published tree at dst, hard-linking every
// regular file, and touches the entry's last-use clock. It reports false
// (and no error) on a cache miss. The tree is built beside dst and renamed
// into place, so dst appears complete or not at all: a caller that finds it
// already there may use it. hitMu is held shared for the whole walk, so
// eviction cannot retire the entry while its files are being linked.
func (s *Store) linkTreeOut(key Key, dst string) (bool, error) {
	s.hitMu.RLock()
	defer s.hitMu.RUnlock()

	src := s.dataPath(key)
	fi, err := os.Stat(src)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("while checking cache for %v: %w", key, err)
	}
	if !fi.IsDir() {
		return false, fmt.Errorf("entry for %v is a file, not a directory tree (use GetFileTo)", key)
	}
	if _, err := os.Lstat(dst); err == nil {
		return false, fmt.Errorf("destination %s already exists: %w", dst, fs.ErrExist)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return false, fmt.Errorf("while checking destination %s: %w", dst, err)
	}
	tmp, err := os.MkdirTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".linking-")
	if err != nil {
		return false, fmt.Errorf("while creating %s: %w", dst, err)
	}
	if err := os.Chmod(tmp, fi.Mode().Perm()); err != nil {
		_ = os.RemoveAll(tmp)
		return false, fmt.Errorf("while creating %s: %w", dst, err)
	}
	if err := linkTree(src, tmp); err != nil {
		_ = os.RemoveAll(tmp)
		return false, fmt.Errorf("while linking %v to %s: %w", key, dst, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.RemoveAll(tmp)
		// A concurrent caller for the same destination won the rename; its
		// tree is complete, which is what ErrExist tells the caller.
		if errors.Is(err, fs.ErrExist) || errors.Is(err, syscall.ENOTEMPTY) {
			return false, fmt.Errorf("destination %s already exists: %w", dst, fs.ErrExist)
		}
		return false, fmt.Errorf("while placing %s: %w", dst, err)
	}
	// Last-use touch for eviction's LRU ordering; best-effort, a miss only
	// ages the entry.
	now := time.Now()
	_ = os.Chtimes(s.entryDir(key), now, now)
	return true, nil
}

// linkTree recreates src's directories under dst (which exists) and hard
// links its regular files. protectTree admitted nothing else into a
// published tree, so any other entry type is corruption, not a case.
func linkTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dst, rel)
		switch {
		case d.IsDir():
			// protectTree published every directory as 0700; the mirror
			// gets the same without a stat per directory.
			return os.Mkdir(target, 0o700)
		case d.Type().IsRegular():
			if err := os.Link(path, target); err != nil {
				if errors.Is(err, syscall.EXDEV) {
					return fmt.Errorf("destination is not on the cache's filesystem (link-out requires one filesystem): %w", err)
				}
				return err
			}
			return nil
		default:
			return fmt.Errorf("cached tree holds %q of type %v, want a directory or regular file", rel, d.Type())
		}
	})
}

// protectTree checks that a tree fetcher left dataPath a directory of
// directories and regular files, with at least one file, and makes every
// file read-only before publication (see protectFile). Directories become
// the store owner's alone: only the store walks a cached tree, to link it
// out or retire it, and a consumer reaches the files through its own
// mirrored directories, which linkTreeOut gives the same mode.
func protectTree(dataPath string) error {
	files := 0
	err := filepath.WalkDir(dataPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			return os.Chmod(path, 0o700)
		case d.Type().IsRegular():
			files++
			info, err := d.Info()
			if err != nil {
				return err
			}
			return os.Chmod(path, readOnlyMode(info.Mode().Perm()))
		default:
			rel, _ := filepath.Rel(dataPath, path)
			return fmt.Errorf("fetcher produced %q of type %v, want only directories and regular files", rel, d.Type())
		}
	})
	if err != nil {
		return fmt.Errorf("while protecting fetched tree: %w", err)
	}
	if files == 0 {
		return errors.New("fetcher succeeded but produced an empty tree")
	}
	return nil
}
