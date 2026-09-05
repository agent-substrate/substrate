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
// This file owns the cache's lifecycle (flags, validation, opening with the
// startup debris sweep) and the restore path's cached reads. The eviction
// loop is wired separately.

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
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
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

// isGoldenSnapshotURI reports whether snapshotURI names a golden snapshot:
// one owned by an actor in the reserved golden atespace. A fresh-from-golden
// start arrives as an ordinary external FULL restore whose snapshot URI is
// the template's golden, so this is how the download path recognizes an
// immutable, cache-safe source. Per-actor snapshot URIs are never cached:
// each is used by one actor and deleted on its next suspend, so caching
// them buys nothing.
func isGoldenSnapshotURI(snapshotURI string) bool {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	return err == nil && uri.Atespace() == resources.GoldenActorAtespace
}

// goldenCacheMode is how a restore may materialize golden files from the
// cache. The mode exists because the fast path — a hard link — shares the
// cached inode with the consumer, which is only safe when the consumer never
// writes the staged file in place.
type goldenCacheMode int

const (
	// cacheModeOff downloads fresh, bypassing the cache.
	cacheModeOff goldenCacheMode = iota
	// cacheModeLink hard-links the cached copy: zero-cost hits, but the
	// consumer must treat the staged file as read-only.
	cacheModeLink
)

// goldenCacheModeFor returns how this sandbox class's restores may use the
// golden cache. ateom-gvisor consumes restore-state strictly read-only, so
// it gets hard links. ateom-microvm rewrites config.json in place at restore
// and merges checkpoint deltas into memory-ranges' inode at suspend — either
// would corrupt a shared inode — so its class stays off the cache until a
// private-copy mode serves it.
func goldenCacheModeFor(sandboxClass string) goldenCacheMode {
	if atev1alpha1.SandboxClass(sandboxClass) == atev1alpha1.SandboxClassGvisor {
		return cacheModeLink
	}
	return cacheModeOff
}

// fetchSnapshotObject stages one snapshot object at local — through the
// golden cache per mode when the cache is enabled, directly from object
// storage otherwise. A cacheModeLink hit is a hard link, so local stays
// valid regardless of later eviction; fetch errors pass through the cache
// wrapped, keeping ateerrors classification intact.
func (s *AteomHerder) fetchSnapshotObject(ctx context.Context, objectURI, local string, mode goldenCacheMode) error {
	if mode == cacheModeOff || s.goldenCache == nil {
		return ategcs.FetchLocalFileFromGCSWithZstd(ctx, s.gcsClient, objectURI, local)
	}
	err := s.goldenCache.GetFileTo(ctx, filecache.URIKey("gcs-zstd", objectURI), local, func(ctx context.Context, dst string) error {
		return ategcs.FetchLocalFileFromGCSWithZstd(ctx, s.gcsClient, objectURI, dst)
	})
	if err == nil || !errors.Is(err, syscall.EXDEV) {
		return err
	}
	// The cache sits on a different mount than the restore dir, so link-out
	// cannot work. Downloading fresh keeps restores correct; the warning
	// points at the misconfiguration (see --golden-cache-dir).
	slog.WarnContext(ctx, "Golden cache is on a different filesystem than the restore dir; downloading without the cache",
		slog.String("object", objectURI))
	return ategcs.FetchLocalFileFromGCSWithZstd(ctx, s.gcsClient, objectURI, local)
}
