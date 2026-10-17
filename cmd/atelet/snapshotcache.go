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
// (flags, validation, opening with the startup debris sweep). The restore
// path's cached reads and the eviction loop follow separately.

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/pflag"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/cmd/atelet/internal/filecachegc"
	"go.ate.dev/substrate/internal/nodepath"
)

var (
	snapshotCacheDir          = pflag.String("snapshot-cache-dir", ateletpath.SnapshotCacheDir, "Directory for the node-local shared snapshot file cache. Empty disables caching: every restore downloads its snapshot files, and the other --snapshot-cache-* flags are validated but not read. Must be under the node state dir, "+nodepath.BasePath+": the object-store plugin writes only there, and cache hits are served as hard links into the per-actor restore dirs on the same mount.")
	snapshotCacheMinAge       = pflag.Duration("snapshot-cache-min-age", 10*time.Minute, "Cached snapshot files younger than this are never evicted, protecting files fetched but not yet linked into a restore dir. Read only while the cache is enabled.")
	snapshotCacheFetchTimeout = pflag.Duration("snapshot-cache-fetch-timeout", 10*time.Minute, "Bound on one cached snapshot file download. Downloads run detached from the restores waiting on them, so this is the only bound on how long one can run; a restore arriving while a download hangs waits on it until then. Sized for multi-GiB memory images on a busy node. Read only while the cache is enabled.")
	// snapshotCacheGCFlags are --snapshot-cache-gc-period,
	// --snapshot-cache-max-bytes and --snapshot-cache-gc-dry-run.
	snapshotCacheGCFlags = filecachegc.NewFlags(pflag.CommandLine, "snapshot-cache", "snapshot cache")
)

// validateSnapshotCacheFlags checks every flag of the cache, enabled or
// not: a value outside its domain is a mistake whatever else is set, and
// startup is the cheapest moment to say so.
func validateSnapshotCacheFlags() error {
	if *snapshotCacheMinAge < 0 {
		// A negative min-age inverts the veto (the cutoff lands in the
		// future), making just-fetched files evictable mid-restore.
		return fmt.Errorf("--snapshot-cache-min-age %v must be >= 0", *snapshotCacheMinAge)
	}
	if *snapshotCacheFetchTimeout <= 0 {
		return fmt.Errorf("--snapshot-cache-fetch-timeout %v must be > 0", *snapshotCacheFetchTimeout)
	}
	if err := snapshotCacheGCFlags.Validate(); err != nil {
		return err
	}
	if *snapshotCacheDir != "" && !nodepath.UnderBasePath(*snapshotCacheDir) {
		// The object-store plugin confines its writes to the base path, so
		// a cache elsewhere could never be filled: every shared restore
		// would fail at the fetch.
		return fmt.Errorf("--snapshot-cache-dir %q must be under %s, the only tree the object-store plugin writes to", *snapshotCacheDir, nodepath.BasePath)
	}
	return nil
}

// openSnapshotCache opens the snapshot cache rooted at dir and clears crash
// debris before the store serves any restore. An empty dir disables
// caching: the returned store is nil and restores download their snapshot
// files directly.
func openSnapshotCache(ctx context.Context, dir string, minAge, fetchTimeout time.Duration) (*filecache.Store, error) {
	if dir == "" {
		slog.InfoContext(ctx, "Snapshot cache disabled; every restore downloads its snapshot files")
		return nil, nil
	}
	store, err := filecache.New(dir, filecache.WithMinAge(minAge), filecache.WithFetchTimeout(fetchTimeout))
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
		slog.Duration("fetch_timeout", fetchTimeout),
		slog.Int("tmp_removed", stats.TmpRemoved),
		slog.Int("retired_removed", stats.RetiredRemoved))
	return store, nil
}
