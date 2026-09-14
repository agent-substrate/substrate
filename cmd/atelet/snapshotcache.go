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
// A shared snapshot's files (a template's golden snapshot today, tag
// snapshots later) are immutable once published, yet every restore that
// needs them downloads them again into its own per-actor dir. This cache (a
// filecache.Store) makes those files a node-level resource: concurrent
// restores of one snapshot share a single download, and later restores are
// served from disk instead of object storage.
//
// Whether a snapshot is cacheable is the control plane's call, carried on
// the request as ExternalRestoreConfiguration.sharing; atelet only maps
// that property to a serving mode per sandbox class. This file owns the
// cache's lifecycle (flags, validation, opening with the startup debris
// sweep) and the restore path's cached reads. The eviction loop is wired
// separately.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ategcs"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/filecache"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/spf13/pflag"
)

var (
	snapshotCacheDir    = pflag.String("snapshot-cache-dir", ateompath.SnapshotCacheDir, "Directory for the node-local shared snapshot file cache. Empty disables caching (every restore downloads its snapshot files). Must be on the same filesystem mount as the actor state dirs: cache hits are served as hard links into the per-actor restore dirs.")
	snapshotCacheMinAge = pflag.Duration("snapshot-cache-min-age", 10*time.Minute, "Cached snapshot files younger than this are never evicted, protecting files fetched but not yet linked into a restore dir.")
)

func validateSnapshotCacheFlags() error {
	if *snapshotCacheMinAge < 0 {
		// A negative min-age inverts the veto (the cutoff lands in the
		// future), making just-fetched files evictable mid-restore.
		return fmt.Errorf("--snapshot-cache-min-age %v must be >= 0", *snapshotCacheMinAge)
	}
	if *snapshotCacheDir != "" && !ateompath.UnderBasePath(*snapshotCacheDir) {
		slog.Warn("Snapshot cache dir is outside the ateom base path; hits cannot be hard-linked into restore dirs across mounts, so every hit degrades to a copy",
			slog.String("snapshot_cache_dir", *snapshotCacheDir),
			slog.String("actors_dir", ateompath.ActorsDir))
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
		slog.Int("tmp_removed", stats.TmpRemoved),
		slog.Int("retired_removed", stats.RetiredRemoved))
	return store, nil
}

// cacheMode is how a restore may materialize snapshot files from the
// cache. The mode exists because the fast path — a hard link — shares the
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
// snapshot cache. ateom-gvisor consumes restore-state strictly read-only,
// so it gets hard links. ateom-microvm rewrites config.json in place at
// restore and merges checkpoint deltas into memory-ranges' inode at suspend
// — either would corrupt a shared inode — so it gets private copies: still
// one download per snapshot per node, and its mutations stay its own.
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

// externalConfigCacheMode returns the cache mode for the request's external
// snapshot: classMode when the control plane declared the snapshot shared,
// cacheModeOff otherwise (private snapshots, and callers that predate the
// sharing field).
func externalConfigCacheMode(req *ateletpb.RestoreRequest, classMode cacheMode) cacheMode {
	if req.GetExternalConfig().GetSharing() == ateletpb.SnapshotSharing_SNAPSHOT_SHARING_SHARED {
		return classMode
	}
	return cacheModeOff
}

// fetchSnapshotObject stages one snapshot object at local — through the
// snapshot cache per mode when the cache is enabled, directly from object
// storage otherwise. A cacheModeLink hit is a hard link, so local stays
// valid regardless of later eviction; a cacheModeCopy hit is a private
// copy. Fetch errors pass through the cache wrapped, keeping ateerrors
// classification intact.
func (s *AteomHerder) fetchSnapshotObject(ctx context.Context, objectURI, local string, mode cacheMode) error {
	if mode == cacheModeOff || s.snapshotCache == nil {
		return ategcs.FetchLocalFileFromGCSWithZstd(ctx, s.gcsClient, objectURI, local)
	}
	key := filecache.URIKey("gcs-zstd", objectURI)
	fetch := func(ctx context.Context, dst string) error {
		return ategcs.FetchLocalFileFromGCSWithZstd(ctx, s.gcsClient, objectURI, dst)
	}
	if mode == cacheModeCopy {
		return s.snapshotCache.GetFileCopyTo(ctx, key, local, fetch)
	}
	err := s.snapshotCache.GetFileTo(ctx, key, local, fetch)
	if err == nil || !errors.Is(err, syscall.EXDEV) {
		return err
	}
	// The cache sits on a different mount than the restore dir, so link-out
	// cannot work — but a copy can, and the failed link's flight already
	// published the entry, so this is a local read rather than a second
	// download. The warning points at the misconfiguration (a cache under
	// --snapshot-cache-dir off the base-path mount serves every hit the slow
	// way).
	slog.WarnContext(ctx, "Snapshot cache is on a different filesystem than the restore dir; serving a copy instead of a hard link",
		slog.String("object", objectURI))
	return s.snapshotCache.GetFileCopyTo(ctx, key, local, fetch)
}
