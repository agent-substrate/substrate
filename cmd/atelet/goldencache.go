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

// The golden snapshot file cache.
//
// A golden snapshot's files are immutable once published (a changed golden
// gets a new URI), yet every restore that needs them — fresh-from-golden
// starts and DATA_ON_GOLDEN resumes — downloads them again into its own
// per-actor dir. This cache (a filecache.Store) makes those files a
// node-level resource: concurrent restores of one golden share a single
// download, and later restores hard-link the cached copy instead of
// fetching it.
//
// This file owns the cache's lifecycle: flags, validation, and opening
// (with the startup debris sweep). The restore path's cached reads and the
// eviction loop are wired separately.

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/filecache"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/spf13/pflag"
)

var (
	goldenCacheDir    = pflag.String("golden-cache-dir", ateompath.GoldenCacheDir, "Directory for the node-local golden snapshot file cache. Empty disables caching (every restore downloads its golden files). Must be on the same filesystem mount as the actor state dirs: cache hits are served as hard links into the per-actor restore dirs.")
	goldenCacheMinAge = pflag.Duration("golden-cache-min-age", 10*time.Minute, "Cached golden files younger than this are never evicted, protecting files fetched but not yet linked into a restore dir.")
)

func validateGoldenCacheFlags() error {
	if *goldenCacheMinAge < 0 {
		// A negative min-age inverts the veto (the cutoff lands in the
		// future), making just-fetched files evictable mid-restore.
		return fmt.Errorf("--golden-cache-min-age %v must be >= 0", *goldenCacheMinAge)
	}
	if *goldenCacheDir != "" && !ateompath.UnderBasePath(*goldenCacheDir) {
		slog.Warn("Golden cache dir is outside the ateom base path; hits cannot be hard-linked into restore dirs across mounts, so every restore will fall back to downloading",
			slog.String("golden_cache_dir", *goldenCacheDir),
			slog.String("actors_dir", ateompath.ActorsDir))
	}
	return nil
}

// openGoldenCache opens the golden snapshot cache rooted at dir and clears
// crash debris before the store serves any restore. An empty dir disables
// caching: the returned store is nil and restores download their golden
// files directly.
func openGoldenCache(ctx context.Context, dir string, minAge time.Duration) (*filecache.Store, error) {
	if dir == "" {
		slog.InfoContext(ctx, "Golden snapshot cache disabled; every restore downloads its golden files")
		return nil, nil
	}
	store, err := filecache.New(dir, filecache.WithMinAge(minAge))
	if err != nil {
		return nil, fmt.Errorf("while opening golden cache at %s: %w", dir, err)
	}
	stats, err := store.SweepDebris(ctx)
	if err != nil {
		// Leftover debris wastes space but affects no lookup, so the store
		// is fully usable: log and carry on rather than failing startup.
		slog.WarnContext(ctx, "Golden snapshot cache debris sweep incomplete", slog.Any("err", err))
	}
	slog.InfoContext(ctx, "Golden snapshot cache open",
		slog.String("dir", dir),
		slog.Int("tmp_removed", stats.TmpRemoved),
		slog.Int("retired_removed", stats.RetiredRemoved))
	return store, nil
}
