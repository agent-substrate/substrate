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
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"golang.org/x/sys/unix"
)

type cachedSnapshotFile struct {
	path string
	info fs.FileInfo
}

// minImmediateEvictAge is the fallback age threshold when the kernel does not
// support the cachestat(2) syscall (Linux < 6.5).
const minImmediateEvictAge = 5 * time.Second

// evictCleanFilePageCache evicts path's resident pages from the host page cache
// via POSIX_FADV_DONTNEED only if the file currently has no dirty or writeback
// pages (verified via the Linux 6.5+ cachestat(2) syscall, with a ModTime
// fallback on older kernels). This guarantees FADV_DONTNEED never triggers a
// synchronous dirty-page disk flush (__filemap_fdatawrite_range).
func evictCleanFilePageCache(path string, info fs.FileInfo) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	var cstat unix.Cachestat_t
	if err := unix.Cachestat(uint(f.Fd()), &unix.CachestatRange{}, &cstat, 0); err == nil {
		if cstat.Cache == 0 || cstat.Dirty > 0 || cstat.Writeback > 0 {
			return
		}
	} else if time.Since(info.ModTime()) < minImmediateEvictAge {
		return
	}
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}

// dropStagedSnapshotFile drops a staged snapshot file in restoreDir once
// Restore has finished consuming it:
//
//   - If restoreDir is not the expendable RestoreStateDir (e.g. a direct local
//     restore from LocalSnapshotDir), the file on disk is preserved and its
//     page cache is evicted immediately if its pages are already clean.
//   - If the file in restoreDir is hard-linked from local-checkpoint/
//     (Nlink > 1), its page cache is evicted before unlinking if its pages are
//     already clean (Dirty == 0 && Writeback == 0), avoiding a synchronous
//     dirty-page flush on freshly-written pause snapshots.
//   - If the file in restoreDir has a single link (Nlink == 1), it is
//     truncated to 0 before unlinking so the kernel immediately discards both
//     dirty and clean pages without disk writeback.
func dropStagedSnapshotFile(ctx context.Context, actorDirs *ateompb.ActorDirs, restoreDir, path string) {
	st, err := os.Stat(path)
	if err != nil {
		return
	}
	if restoreDir != actorDirs.GetRestoreDir() && filepath.Base(restoreDir) != "restore-state" {
		evictCleanFilePageCache(path, st)
		return
	}
	if sys, ok := st.Sys().(*syscall.Stat_t); ok && sys.Nlink > 1 {
		evictCleanFilePageCache(path, st)
	} else {
		_ = os.Truncate(path, 0)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.WarnContext(ctx, "could not drop staged snapshot file", "path", path, "error", err)
	}
}

// dropStagedMemoryImage drops the staged memory-ranges file and already-extracted
// tar archives in restoreDir once an eager ("Copy") restore has finished reading
// guest memory.
func dropStagedMemoryImage(ctx context.Context, actorDirs *ateompb.ActorDirs, restoreDir string) {
	for _, name := range []string{"memory-ranges", rootfsUpperTarFile, durableTarFile} {
		dropStagedSnapshotFile(ctx, actorDirs, restoreDir, filepath.Join(restoreDir, name))
	}
}

// dropActorSnapshotCacheAsync evicts the actor's snapshot files from the host
// page cache in the background after Restore completes, pinned by inode so
// delayed post-writeback passes cannot evict newer snapshots.
func dropActorSnapshotCacheAsync(actorDirs *ateompb.ActorDirs, includeMemory bool, extraDirs ...string) {
	dirs := append([]string{actorDirs.GetRestoreDir(), localCheckpointsDir(actorDirs)}, extraDirs...)

	targets := findSnapshotFilesInDirs(includeMemory, dirs...)
	if len(targets) == 0 {
		return
	}
	go func() {
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
	if includeMemory && name == "memory-ranges" {
		return true
	}
	return strings.HasSuffix(name, ".tar")
}

func findSnapshotFilesInDirs(includeMemory bool, dirs ...string) []cachedSnapshotFile {
	var targets []cachedSnapshotFile
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || !d.Type().IsRegular() || !isSnapshotDataFile(d.Name(), includeMemory) {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return nil
			}
			if slices.ContainsFunc(targets, func(t cachedSnapshotFile) bool { return os.SameFile(t.info, info) }) {
				return nil
			}
			targets = append(targets, cachedSnapshotFile{
				path: path,
				info: info,
			})
			return nil
		})
	}
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
		if err == nil && os.SameFile(info, t.info) && info.ModTime().Equal(t.info.ModTime()) && info.Size() == t.info.Size() {
			var cstat unix.Cachestat_t
			if err := unix.Cachestat(uint(f.Fd()), &unix.CachestatRange{}, &cstat, 0); err == nil {
				if cstat.Cache == 0 {
					_ = f.Close()
					continue
				}
				anyRemaining = true
				if cstat.Dirty > 0 || cstat.Writeback > 0 {
					_ = f.Close()
					continue
				}
			} else {
				anyRemaining = true
			}
			_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
		}
		_ = f.Close()
	}
	return anyRemaining
}
