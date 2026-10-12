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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/nodepath"
)

// The suspend cache keeps the files of an actor's last external checkpoint on
// the node after they are uploaded. Resuming the actor on the same node from
// that snapshot then moves the files into place instead of fetching and
// decompressing them from object storage, which is most of a resume's
// node-side time.
//
// An entry is keyed by actor and snapshot URI. A snapshot URI names one
// immutable snapshot, so an entry is exactly the bytes the upload read; and an
// actor's newer snapshots get new URIs, so a stale entry can never be restored
// in place of a newer snapshot. Entries are best-effort throughout: any
// failure to write or read one falls back to object storage. Total size is
// bounded by --suspend-cache-max-bytes, oldest entries evicted first.

// suspendCacheEntryDir is the cache entry for snapshotURI of actorUID.
func suspendCacheEntryDir(actorUID, snapshotURI string) string {
	sum := sha256.Sum256([]byte(snapshotURI))
	return filepath.Join(ateletpath.SuspendCacheDir(actorUID), hex.EncodeToString(sum[:16]))
}

// cacheSuspendedCheckpoint keeps checkpointDir, just uploaded to snapshotURI,
// as the actor's suspend cache entry, replacing any earlier entry. It moves
// the directory, so it must run after the upload and before resetActorDirs
// (which recreates checkpointDir empty).
func (s *AteomHerder) cacheSuspendedCheckpoint(ctx context.Context, actorUID, snapshotURI, checkpointDir string, rec *sandboxAssetsRecord) {
	if s.suspendCacheMaxBytes <= 0 {
		return
	}
	entry := suspendCacheEntryDir(actorUID, snapshotURI)
	if err := putSuspendCacheEntry(ateletpath.SuspendCacheDir(actorUID), entry, checkpointDir, rec); err != nil {
		slog.WarnContext(ctx, "Not caching the suspended checkpoint on this node", slog.Any("err", err))
		return
	}
	// Off the suspend's critical path; at most one sweep at a time.
	actorsDir := nodepath.ActorsDir
	go func() {
		if !s.suspendCacheGC.TryLock() {
			return
		}
		defer s.suspendCacheGC.Unlock()
		gcSuspendCache(context.WithoutCancel(ctx), actorsDir, s.suspendCacheMaxBytes)
	}()
}

// putSuspendCacheEntry replaces everything under cacheDir with entry, made by
// moving checkpointDir and writing rec beside its files. The manifest is
// written last: an entry without one is never read.
func putSuspendCacheEntry(cacheDir, entry, checkpointDir string, rec *sandboxAssetsRecord) error {
	if err := os.RemoveAll(cacheDir); err != nil {
		return fmt.Errorf("while removing the previous entry: %w", err)
	}
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return err
	}
	if err := os.Rename(checkpointDir, entry); err != nil {
		return fmt.Errorf("while moving the checkpoint into the cache: %w", err)
	}
	manifest, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tmp := filepath.Join(entry, "."+sandboxManifestName+".tmp")
	if err := os.WriteFile(tmp, manifest, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(entry, sandboxManifestName))
}

// cachedSuspendedCheckpoint returns the manifest of the actor's cached
// checkpoint of snapshotURI and its directory, or ok=false if this node has
// none.
func cachedSuspendedCheckpoint(actorUID, snapshotURI string) (rec *sandboxAssetsRecord, entry string, ok bool) {
	entry = suspendCacheEntryDir(actorUID, snapshotURI)
	manifest, err := readSnapshotManifest(entry)
	if err != nil {
		return nil, "", false
	}
	rec, err = unmarshalSandboxRecord(manifest)
	if err != nil {
		return nil, "", false
	}
	return rec, entry, true
}

// takeSuspendCacheEntry moves files from the cache entry into dstDir, then
// drops the entry whether or not that succeeded: a restore consumes it.
func takeSuspendCacheEntry(entry, dstDir string, files []string) (err error) {
	defer func() {
		if rerr := os.RemoveAll(entry); rerr != nil && err == nil {
			err = rerr
		}
	}()
	for _, name := range files {
		src := filepath.Join(entry, name)
		info, err := os.Lstat(src)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("cached checkpoint file %s is not a regular file", name)
		}
		if err := os.Rename(src, filepath.Join(dstDir, name)); err != nil {
			return err
		}
	}
	return nil
}

// dropSuspendCache removes the actor's suspend cache entries.
func dropSuspendCache(ctx context.Context, actorUID string) {
	if err := os.RemoveAll(ateletpath.SuspendCacheDir(actorUID)); err != nil {
		slog.WarnContext(ctx, "Failed to remove the actor's suspend cache", slog.Any("err", err))
	}
}

type suspendCacheEntry struct {
	dir   string
	bytes int64
	mtime time.Time
}

// gcSuspendCache evicts suspend cache entries under actorsDir, oldest first,
// until the rest fit in maxBytes of allocated disk.
func gcSuspendCache(ctx context.Context, actorsDir string, maxBytes int64) {
	dirs, err := filepath.Glob(filepath.Join(actorsDir, "*", ateletpath.SuspendCacheDirName, "*"))
	if err != nil {
		return
	}
	var entries []suspendCacheEntry
	for _, dir := range dirs {
		e := suspendCacheEntry{dir: dir}
		werr := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			e.bytes += allocatedBytes(info)
			if info.ModTime().After(e.mtime) {
				e.mtime = info.ModTime()
			}
			return nil
		})
		if werr != nil && !errors.Is(werr, fs.ErrNotExist) {
			slog.WarnContext(ctx, "Failed to size a suspend cache entry", slog.String("path", dir), slog.Any("err", werr))
			continue
		}
		entries = append(entries, e)
	}
	slices.SortFunc(entries, func(a, b suspendCacheEntry) int { return b.mtime.Compare(a.mtime) })
	var kept int64
	for _, e := range entries {
		if kept+e.bytes <= maxBytes {
			kept += e.bytes
			continue
		}
		if err := os.RemoveAll(e.dir); err != nil {
			slog.WarnContext(ctx, "Failed to evict a suspend cache entry", slog.String("path", e.dir), slog.Any("err", err))
			continue
		}
		slog.InfoContext(ctx, "Evicted a suspend cache entry", slog.String("path", e.dir), slog.Int64("bytes", e.bytes))
	}
}
