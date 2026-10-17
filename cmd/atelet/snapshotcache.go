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

// The snapshot file cache.
//
// A shared snapshot's files, the snapshot a tag holds and every actor
// created from it restores, are immutable once published, yet every restore
// that needs them downloads them again into its own per-actor dir. This
// cache (a filecache.Store) makes those files a node-level resource:
// concurrent restores of one snapshot share a single download, and later
// restores are served from disk instead of object storage.
//
// Whether a snapshot is cacheable is the control plane's call, carried on
// the request as ObjectStorage.sharing; atelet only maps that property to a
// serving mode per sandbox class. This file owns the cache's lifecycle
// (flags, validation, opening with the startup debris sweep) and the
// restore path's cached reads. The eviction loop follows separately.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	atev1alpha1 "go.ate.dev/substrate/pkg/api/v1alpha1"
)

var (
	snapshotCacheDir    = pflag.String("snapshot-cache-dir", ateletpath.SnapshotCacheDir, "Directory for the node-local shared snapshot file cache. Empty disables caching (every restore downloads its snapshot files). Must be on the same filesystem mount as the actor state dirs: cache hits are served as hard links into the per-actor restore dirs.")
	snapshotCacheMinAge = pflag.Duration("snapshot-cache-min-age", 10*time.Minute, "Cached snapshot files younger than this are never evicted, protecting files fetched but not yet linked into a restore dir.")
)

func validateSnapshotCacheFlags() error {
	if *snapshotCacheMinAge < 0 {
		// A negative min-age inverts the veto (the cutoff lands in the
		// future), making just-fetched files evictable mid-restore.
		return fmt.Errorf("--snapshot-cache-min-age %v must be >= 0", *snapshotCacheMinAge)
	}
	if *snapshotCacheDir != "" && !nodepath.UnderBasePath(*snapshotCacheDir) {
		slog.Warn("Snapshot cache dir is outside the worker base path; hits cannot be hard-linked into restore dirs across mounts, so every hit degrades to a copy",
			slog.String("snapshot_cache_dir", *snapshotCacheDir),
			slog.String("actors_dir", nodepath.ActorsDir))
	}
	return nil
}

// openSnapshotCache opens the snapshot cache rooted at dir and clears crash
// debris before the store serves any restore. An empty dir disables
// caching: the returned store is nil and restores download their snapshot
// files directly.
func openSnapshotCache(ctx context.Context, dir string, minAge time.Duration) (*filecache.Store, error) {
	if dir == "" {
		slog.InfoContext(ctx, "Snapshot cache disabled; every restore downloads its snapshot files")
		return nil, nil
	}
	store, err := filecache.New(dir, filecache.WithMinAge(minAge))
	if err != nil {
		return nil, fmt.Errorf("while opening snapshot cache at %s: %w", dir, err)
	}
	stats, err := store.SweepDebris(ctx)
	if err != nil {
		// Leftover debris wastes space but affects no lookup, so the store
		// is fully usable: log and carry on rather than failing startup.
		slog.WarnContext(ctx, "Snapshot cache debris sweep incomplete", slog.Any("err", err))
	}
	slog.InfoContext(ctx, "Snapshot cache open",
		slog.String("dir", dir),
		slog.Duration("min_age", minAge),
		slog.Int("tmp_removed", stats.TmpRemoved),
		slog.Int("retired_removed", stats.RetiredRemoved))
	return store, nil
}

// cacheMode is how a restore may materialize snapshot files from the
// cache. The mode exists because the fast path, a hard link, shares the
// cached inode with the consumer, which is only safe when the consumer never
// writes the staged file in place.
type cacheMode int

const (
	// cacheModeOff downloads fresh, bypassing the cache.
	cacheModeOff cacheMode = iota
	// cacheModeLink hard-links the cached copy: zero-cost hits, but the
	// consumer must treat the staged file as read-only.
	cacheModeLink
	// cacheModeCopy stages a private, hole-preserving copy: costlier per hit
	// than a link, but the consumer owns the inode and may mutate it in
	// place, and a copy can cross mounts.
	cacheModeCopy
)

// cacheModeFor returns how this sandbox class's restores may use the
// snapshot cache. ateworker-gvisor consumes restore state strictly
// read-only, so it gets hard links. ateworker-microvm rewrites config.json
// in place at restore and merges checkpoint deltas into memory-ranges'
// inode at suspend, either of which would corrupt a shared inode, so it
// gets private copies: still one download per snapshot per node, and its
// mutations stay its own.
func cacheModeFor(sandboxClass string) cacheMode {
	switch atev1alpha1.SandboxClass(sandboxClass) {
	case atev1alpha1.SandboxClassGvisor:
		return cacheModeLink
	case atev1alpha1.SandboxClassMicroVM:
		return cacheModeCopy
	default:
		return cacheModeOff
	}
}

// snapshotCacheMode returns the cache mode for a restore's durable
// snapshot: classMode when the control plane declared the snapshot shared,
// cacheModeOff otherwise, which covers private snapshots and a caller that
// did not say. atelet never infers cacheability from the URI.
func snapshotCacheMode(object *ateletpb.ObjectStorage, classMode cacheMode) cacheMode {
	if object.GetSharing() == ateletpb.SnapshotSharing_SNAPSHOT_SHARING_SHARED {
		return classMode
	}
	return cacheModeOff
}

// downloadSnapshotFilesCached stages the named files of the snapshot at
// snapshotURI into dstDir through the snapshot cache, each per mode. The
// caller asserts the snapshot is immutable at its URI. Files sit directly
// in dstDir or in its one checkpoint subdirectory, as the plugin's own
// download allows; a destination that already exists, a symlink included,
// fails rather than being followed.
func (s *Atelet) downloadSnapshotFilesCached(ctx context.Context, snapshotURI, dstDir string, files []string, mode cacheMode) error {
	if err := resources.ValidateSnapshotFileNames(files); err != nil {
		return err
	}
	// Subdirectories first, once each: the files are fetched in parallel,
	// and several of them share the checkpoint subdirectory.
	made := map[string]bool{}
	for _, fileName := range files {
		dir := filepath.Dir(fileName)
		if dir == "." || made[dir] {
			continue
		}
		if err := mkdirNoFollow(filepath.Join(dstDir, dir)); err != nil {
			return fmt.Errorf("while creating %s in restore directory: %w", dir, err)
		}
		made[dir] = true
	}
	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range files {
		g.Go(func() error {
			if err := s.fetchSnapshotFile(gCtx, snapshotURI, fileName, filepath.Join(dstDir, fileName), mode); err != nil {
				return fmt.Errorf("while downloading %s: %w", fileName, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// mkdirNoFollow creates dir unless it already is a directory. Anything else
// in its place, a symlink above all, is an error: the restore directory is
// written by path here, outside the os.Root the plugin's own download uses.
func mkdirNoFollow(dir string) error {
	fi, err := os.Lstat(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return os.Mkdir(dir, 0o700)
	case err != nil:
		return err
	case !fi.IsDir():
		return fmt.Errorf("%s exists and is not a directory", dir)
	}
	return nil
}

// fetchSnapshotFile stages one file of the snapshot at snapshotURI at
// local, through the snapshot cache per mode. A cacheModeLink hit is a hard
// link, so local stays valid regardless of later eviction; a cacheModeCopy
// hit is a private copy. Fetch errors pass through the cache wrapped, so
// the plugin's code classification survives.
func (s *Atelet) fetchSnapshotFile(ctx context.Context, snapshotURI, fileName, local string, mode cacheMode) error {
	key := filecache.URIKey(snapshotURI, fileName)
	fetch := func(ctx context.Context, dst string) error {
		return s.fetchSnapshotFileTo(ctx, snapshotURI, fileName, dst)
	}
	if mode == cacheModeCopy {
		return s.snapshotCache.GetFileCopyTo(ctx, key, local, fetch)
	}
	err := s.snapshotCache.GetFileTo(ctx, key, local, fetch)
	if err == nil || !errors.Is(err, syscall.EXDEV) {
		return err
	}
	// The cache sits on a different mount than the restore dir, so a link
	// cannot work, but a copy can, and the failed link's fetch already
	// published the entry, so this is a local read rather than a second
	// download. The warning points at the misconfiguration: a cache under
	// --snapshot-cache-dir off the base-path mount serves every hit the
	// slow way.
	slog.WarnContext(ctx, "Snapshot cache is on a different filesystem than the restore dir; serving a copy instead of a hard link",
		slog.String("snapshot_uri", snapshotURI), slog.String("file", fileName))
	return s.snapshotCache.GetFileCopyTo(ctx, key, local, fetch)
}

// fetchSnapshotFileTo downloads one file of a snapshot to dst through the
// object-store plugin, which writes a directory of files: it fetches into
// a scratch dir beside dst, on the cache's filesystem, and moves the file
// into place. The plugin accepts a write path under the node state dir,
// which is where the cache lives.
func (s *Atelet) fetchSnapshotFileTo(ctx context.Context, snapshotURI, fileName, dst string) error {
	scratch, err := os.MkdirTemp(filepath.Dir(dst), ".fetch-*")
	if err != nil {
		return fmt.Errorf("while creating a snapshot fetch dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(scratch) }()
	if err := s.fetchSnapshotFiles(ctx, snapshotURI, scratch, []string{fileName}); err != nil {
		return err
	}
	return os.Rename(filepath.Join(scratch, fileName), dst)
}
