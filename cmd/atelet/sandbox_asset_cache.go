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

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/cmd/atelet/internal/filecachegc"
)

var (
	sandboxAssetCacheDir    = pflag.String("sandbox-asset-cache-dir", ateletpath.SandboxAssetCacheDir, "Directory for the node-local sandbox asset cache (gVisor releases, micro-VM kernels, VMMs and guest images). Must be on the same filesystem mount as the actor state dirs and the worker dirs: an actor's assets, and a worker's default runtime, are hard links from the cache into their own directories.")
	sandboxAssetCacheMinAge = pflag.Duration("sandbox-asset-cache-min-age", time.Hour, "Cached sandbox assets younger than this are never evicted. A running actor's assets, and the default runtime of every local worker, are protected by their links regardless; this covers an asset between its fetch and its first link.")
	// sandboxAssetCacheGCFlags are --sandbox-asset-cache-gc-period,
	// --sandbox-asset-cache-max-bytes and --sandbox-asset-cache-gc-dry-run.
	sandboxAssetCacheGCFlags = filecachegc.NewFlags(pflag.CommandLine, "sandbox-asset-cache", "sandbox asset cache")
	sandboxAssetFetchTimeout = pflag.Duration("sandbox-asset-fetch-timeout", 5*time.Minute, "Bound on one sandbox asset download (and, for a gVisor release, its extraction). Downloads run detached from the requests waiting on them, so this is the only bound on how long one can run; a Run arriving while a download hangs waits on it until then. Sized for multi-hundred-MiB micro-VM guest images on a busy node.")
)

func validateSandboxAssetCacheFlags() error {
	if *sandboxAssetCacheMinAge < 0 {
		// A negative min-age inverts the veto (the cutoff lands in the
		// future), making just-fetched assets evictable.
		return fmt.Errorf("--sandbox-asset-cache-min-age %v must be >= 0", *sandboxAssetCacheMinAge)
	}
	if *sandboxAssetFetchTimeout <= 0 {
		return fmt.Errorf("--sandbox-asset-fetch-timeout %v must be > 0", *sandboxAssetFetchTimeout)
	}
	return nil
}

// openSandboxAssetCache opens the asset cache at dir, sweeps the debris a
// crash may have left, and empties the scratch dir asset fetches without
// an actor link into. It refuses a dir from which any of linkDirs cannot be
// hard-linked: the actors dir, where every hit is such a link, and the
// workers dir, where each worker's default runtime is kept by them.
func openSandboxAssetCache(ctx context.Context, dir string, minAge, fetchTimeout time.Duration, linkDirs ...string) (*filecache.Store, error) {
	store, err := filecache.New(dir, filecache.WithMinAge(minAge), filecache.WithFetchTimeout(fetchTimeout))
	if err != nil {
		return nil, fmt.Errorf("while opening sandbox asset cache at %s: %w", dir, err)
	}
	for _, linkDir := range linkDirs {
		if err := canHardLink(dir, linkDir); err != nil {
			return nil, fmt.Errorf("sandbox asset cache: %w", err)
		}
	}
	stats, err := store.SweepDebris(ctx)
	if err != nil {
		// Leftover debris wastes space but affects no lookup, so the store
		// is fully usable: log and carry on rather than failing startup.
		slog.WarnContext(ctx, "Sandbox asset cache debris sweep incomplete", slog.Any("err", err))
	}
	if err := resetDir("sandbox asset scratch dir", ateletpath.SandboxAssetScratchDir, os.RemoveAll, 0o700); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Sandbox asset cache open",
		slog.String("dir", dir),
		slog.Duration("min_age", minAge),
		slog.Duration("fetch_timeout", fetchTimeout),
		slog.Int("tmp_removed", stats.TmpRemoved),
		slog.Int("retired_removed", stats.RetiredRemoved))
	return store, nil
}

// canHardLink fails unless a file under cacheDir can be hard-linked into
// linkDir, which every cache hit into it relies on. It links a probe file
// and removes it again. Nothing short of the probe is exact: Linux refuses
// a link across two mounts even when they are bind mounts of one
// filesystem, so equal device numbers do not settle it, and a path check
// cannot see a mount at all. linkDir is created if it does not exist yet.
func canHardLink(cacheDir, linkDir string) error {
	if err := os.MkdirAll(linkDir, 0o700); err != nil {
		return fmt.Errorf("while creating %s: %w", linkDir, err)
	}
	probe, err := os.CreateTemp(cacheDir, ".linkprobe-")
	if err != nil {
		return fmt.Errorf("while creating a link probe in %s: %w", cacheDir, err)
	}
	defer os.Remove(probe.Name())
	if err := probe.Close(); err != nil {
		return fmt.Errorf("while closing the link probe: %w", err)
	}
	link := filepath.Join(linkDir, filepath.Base(probe.Name()))
	if err := os.Link(probe.Name(), link); err != nil {
		if errors.Is(err, syscall.EXDEV) {
			return fmt.Errorf("%s and %s are on different mounts, so assets cannot be hard-linked under %s; put the cache on the mount that holds it: %w", cacheDir, linkDir, linkDir, err)
		}
		return fmt.Errorf("while probing a hard link from %s into %s: %w", cacheDir, linkDir, err)
	}
	return os.Remove(link)
}
