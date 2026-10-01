//go:build linux

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
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const memoryRangesFile = "memory-ranges"

var cachestatUnsupportedOnce sync.Once

type cachedSnapshotFile struct {
	path string
	info fs.FileInfo
}

// dropStagedSnapshotFile truncates a staged snapshot file in restoreDir to 0
// before unlinking so the kernel immediately discards both dirty and clean
// pages without disk writeback.
func dropStagedSnapshotFile(ctx context.Context, path string) {
	if err := os.Truncate(path, 0); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		slog.DebugContext(ctx, "could not truncate staged snapshot file", "path", path, "error", err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.WarnContext(ctx, "could not drop staged snapshot file", "path", path, "error", err)
	}
}

// dropStagedMemoryImage drops the staged memory-ranges file and already-extracted
// tar archives in a scratch restoreDir once an eager ("Copy") restore has
// finished reading guest memory.
func dropStagedMemoryImage(ctx context.Context, restoreDir string) {
	entries, err := os.ReadDir(restoreDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.DebugContext(ctx, "could not read restore dir to drop staged files", "dir", restoreDir, "error", err)
		}
		return
	}
	for _, e := range entries {
		if e.Type().IsRegular() && isSnapshotDataFile(e.Name(), true) {
			dropStagedSnapshotFile(ctx, filepath.Join(restoreDir, e.Name()))
		}
	}
}

// dropActorSnapshotCacheAsync evicts the actor's snapshot files in restoreDir
// from the host page cache in the background after Restore completes, pinned by
// inode so delayed post-writeback passes cannot evict newer snapshots.
func dropActorSnapshotCacheAsync(restoreDir string, includeMemory bool) {
	go func() {
		targets := findSnapshotFilesInDir(restoreDir, includeMemory)
		if len(targets) == 0 {
			return
		}
		if !evictSnapshotFiles(targets) {
			return
		}
		for _, delay := range []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond, 3000 * time.Millisecond} {
			time.Sleep(delay)
			if !evictSnapshotFiles(targets) {
				return
			}
		}
	}()
}

func isSnapshotDataFile(name string, includeMemory bool) bool {
	if includeMemory && name == memoryRangesFile {
		return true
	}
	return strings.HasSuffix(name, ".tar")
}

func findSnapshotFilesInDir(dir string, includeMemory bool) []cachedSnapshotFile {
	if dir == "" {
		return nil
	}
	var targets []cachedSnapshotFile
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() || !isSnapshotDataFile(d.Name(), includeMemory) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		targets = append(targets, cachedSnapshotFile{
			path: path,
			info: info,
		})
		return nil
	})
	return targets
}

// evictSnapshotFiles evicts clean cached pages of targets and returns true if
// any target still exists and has pages (clean or dirty) remaining in cache.
// Files with dirty or writeback pages are skipped so FADV_DONTNEED never
// triggers disk writeback I/O.
func evictSnapshotFiles(targets []cachedSnapshotFile) bool {
	anyRemaining := false
	for _, t := range targets {
		f, err := os.Open(t.path)
		if err != nil {
			continue
		}
		info, err := f.Stat()
		if err == nil && os.SameFile(info, t.info) && info.Size() == t.info.Size() {
			var cstat unix.Cachestat_t
			if err := unix.Cachestat(uint(f.Fd()), &unix.CachestatRange{}, &cstat, 0); err != nil {
				if errors.Is(err, unix.ENOSYS) {
					cachestatUnsupportedOnce.Do(func() {
						slog.Warn("cachestat(2) syscall not supported by kernel; skipping preserved snapshot page-cache eviction", "error", err)
					})
				}
				_ = f.Close()
				continue
			}
			if cstat.Cache == 0 {
				_ = f.Close()
				continue
			}
			anyRemaining = true
			if cstat.Dirty > 0 || cstat.Writeback > 0 {
				_ = f.Close()
				continue
			}
			// length zero extends the Fadvise to the whole file.
			_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
		}
		_ = f.Close()
	}
	return anyRemaining
}
