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

// Package snapshotcache disposes of staged restore files and evicts preserved
// snapshot files from the host page cache after an actor restore completes.
package snapshotcache

import (
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// File identifies a snapshot data file pinned by its FileInfo at discovery time
// so delayed post-writeback passes cannot evict or unlink a newer file written
// at the same path.
type File struct {
	Path string
	Info fs.FileInfo
}

var (
	cachestatWarnOnce sync.Once

	// cachestatFn and fadviseFn are seams for unit tests so eviction logic can
	// be verified deterministically regardless of the backing filesystem (ext4,
	// tmpfs, overlayfs) or kernel version.
	cachestatFn = unix.Cachestat
	fadviseFn   = unix.Fadvise

	// RetryDelays are the backoff intervals between post-restore eviction
	// passes when dirty pages are waiting for background kernel writeback.
	RetryDelays = []time.Duration{
		500 * time.Millisecond,
		1500 * time.Millisecond,
		3000 * time.Millisecond,
	}
)

// FindFiles walks dir and returns regular files whose base name satisfies
// match, pinned by their FileInfo at discovery time.
func FindFiles(dir string, match func(name string) bool) []File {
	if dir == "" {
		return nil
	}
	var targets []File
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() || !match(d.Name()) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		targets = append(targets, File{Path: path, Info: info})
		return nil
	})
	return targets
}

// DropFile unlinks t.Path if it still refers to the same inode and size, and
// reports whether the file was removed.
func DropFile(t File) bool {
	st, err := os.Stat(t.Path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Debug("could not stat staged snapshot file", slog.String("path", t.Path), slog.Any("err", err))
		}
		return false
	}
	if !os.SameFile(st, t.Info) || st.Size() != t.Info.Size() {
		return false
	}
	if err := os.Remove(t.Path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("could not drop staged snapshot file", slog.String("path", t.Path), slog.Any("err", err))
		}
		return false
	}
	return true
}

// DropFiles calls DropFile for each file in files and reports whether any file
// was removed.
func DropFiles(files []File) bool {
	dropped := false
	for _, t := range files {
		if DropFile(t) {
			dropped = true
		}
	}
	return dropped
}

// EvictFiles evicts clean cached pages of targets via POSIX_FADV_DONTNEED and
// returns true if any target still has dirty or writeback pages remaining in
// cache (or if fadvise failed). Files with dirty or writeback pages are skipped
// so FADV_DONTNEED never forces synchronous disk writeback I/O.
func EvictFiles(targets []File) bool {
	anyRemaining := false
	for _, t := range targets {
		f, err := os.Open(t.Path)
		if err != nil {
			continue
		}
		info, err := f.Stat()
		if err == nil && os.SameFile(info, t.Info) && info.Size() == t.Info.Size() {
			var cstat unix.Cachestat_t
			if err := cachestatFn(uint(f.Fd()), &unix.CachestatRange{}, &cstat, 0); err != nil {
				loggedWarn := false
				cachestatWarnOnce.Do(func() {
					loggedWarn = true
					slog.Warn("cachestat(2) syscall failed; skipping preserved snapshot page-cache eviction", slog.String("path", t.Path), slog.Any("err", err))
				})
				if !loggedWarn {
					slog.Debug("cachestat(2) syscall failed; skipping preserved snapshot page-cache eviction", slog.String("path", t.Path), slog.Any("err", err))
				}
				_ = f.Close()
				continue
			}
			if cstat.Cache == 0 {
				_ = f.Close()
				continue
			}
			if cstat.Dirty > 0 || cstat.Writeback > 0 {
				anyRemaining = true
				_ = f.Close()
				continue
			}
			// length zero extends the Fadvise to the whole file.
			if err := fadviseFn(int(f.Fd()), 0, 0, unix.FADV_DONTNEED); err != nil {
				anyRemaining = true
			}
		}
		_ = f.Close()
	}
	return anyRemaining
}

// EvictWithRetries runs EvictFiles immediately and, while pages remain in
// cache, retries after each interval in RetryDelays to evict pages once
// background kernel writeback cleans them.
func EvictWithRetries(targets []File) {
	if !EvictFiles(targets) {
		return
	}
	for _, delay := range RetryDelays {
		time.Sleep(delay)
		if !EvictFiles(targets) {
			return
		}
	}
}
