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

package snapshotcache

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestFindAndDropFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "restore-state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"checkpoint.img", "pages.img", "durable-dir.tar", "manifest.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("staged data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	files := FindFiles(dir, func(name string) bool {
		ext := filepath.Ext(name)
		return ext == ".img" || ext == ".tar"
	})
	if len(files) != 3 {
		t.Fatalf("FindFiles() = %d files, want 3", len(files))
	}

	if !DropFiles(files) {
		t.Fatalf("DropFiles() = false, want true")
	}

	for _, name := range []string{"checkpoint.img", "pages.img", "durable-dir.tar"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("staged %s still exists after DropFiles: err=%v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatalf("manifest.json should be preserved: %v", err)
	}
}

func TestDropFileSkipsReplacedInode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pages.img")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	files := FindFiles(dir, func(name string) bool { return name == "pages.img" })
	if len(files) != 1 {
		t.Fatalf("FindFiles() = %d, want 1", len(files))
	}

	replacement := filepath.Join(dir, "pages.img.new")
	if err := os.WriteFile(replacement, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}

	if DropFiles(files) {
		t.Fatalf("DropFiles() = true after inode replacement, want false")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("replaced file was deleted: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("replaced file content = %q, want %q", string(got), "new")
	}
}

func TestEvictFiles(t *testing.T) {
	origCachestat := cachestatFn
	origFadvise := fadviseFn
	t.Cleanup(func() {
		cachestatFn = origCachestat
		fadviseFn = origFadvise
	})

	dir := t.TempDir()
	img := filepath.Join(dir, "pages.img")
	if err := os.WriteFile(img, []byte("checkpoint pages"), 0o600); err != nil {
		t.Fatal(err)
	}
	targets := FindFiles(dir, func(name string) bool { return name == "pages.img" })
	if len(targets) != 1 {
		t.Fatalf("FindFiles() = %d, want 1", len(targets))
	}

	t.Run("evicts clean cached pages", func(t *testing.T) {
		var fadviseCalls int
		cachestatFn = func(_ uint, _ *unix.CachestatRange, cstat *unix.Cachestat_t, _ uint) error {
			*cstat = unix.Cachestat_t{Cache: 16, Dirty: 0, Writeback: 0}
			return nil
		}
		fadviseFn = func(_ int, _ int64, _ int64, advice int) error {
			if advice != unix.FADV_DONTNEED {
				t.Errorf("fadvise advice = %d, want FADV_DONTNEED", advice)
			}
			fadviseCalls++
			return nil
		}
		if EvictFiles(targets) {
			t.Errorf("EvictFiles() = true, want false when clean pages were evicted")
		}
		if fadviseCalls != 1 {
			t.Errorf("fadviseCalls = %d, want 1", fadviseCalls)
		}
	})

	t.Run("skips dirty or writeback pages without calling fadvise", func(t *testing.T) {
		var fadviseCalls int
		cachestatFn = func(_ uint, _ *unix.CachestatRange, cstat *unix.Cachestat_t, _ uint) error {
			*cstat = unix.Cachestat_t{Cache: 16, Dirty: 4, Writeback: 0}
			return nil
		}
		fadviseFn = func(int, int64, int64, int) error {
			fadviseCalls++
			return nil
		}
		if !EvictFiles(targets) {
			t.Errorf("EvictFiles() = false, want true so retry passes run after writeback")
		}
		if fadviseCalls != 0 {
			t.Errorf("fadviseCalls = %d, want 0 for dirty pages", fadviseCalls)
		}
	})

	t.Run("returns false when no pages are cached", func(t *testing.T) {
		var fadviseCalls int
		cachestatFn = func(_ uint, _ *unix.CachestatRange, cstat *unix.Cachestat_t, _ uint) error {
			*cstat = unix.Cachestat_t{Cache: 0}
			return nil
		}
		fadviseFn = func(int, int64, int64, int) error {
			fadviseCalls++
			return nil
		}
		if EvictFiles(targets) {
			t.Errorf("EvictFiles() = true, want false when Cache == 0")
		}
		if fadviseCalls != 0 {
			t.Errorf("fadviseCalls = %d, want 0", fadviseCalls)
		}
	})

	t.Run("returns false and skips fadvise on ENOSYS", func(t *testing.T) {
		var fadviseCalls int
		cachestatFn = func(uint, *unix.CachestatRange, *unix.Cachestat_t, uint) error {
			return unix.ENOSYS
		}
		fadviseFn = func(int, int64, int64, int) error {
			fadviseCalls++
			return nil
		}
		if EvictFiles(targets) {
			t.Errorf("EvictFiles() = true on ENOSYS, want false")
		}
		if fadviseCalls != 0 {
			t.Errorf("fadviseCalls = %d, want 0", fadviseCalls)
		}
	})

	t.Run("stops eviction when inode is replaced", func(t *testing.T) {
		cachestatFn = func(_ uint, _ *unix.CachestatRange, cstat *unix.Cachestat_t, _ uint) error {
			*cstat = unix.Cachestat_t{Cache: 16}
			return nil
		}
		replacement := filepath.Join(dir, "pages.img.new")
		if err := os.WriteFile(replacement, []byte("new checkpoint pages"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(replacement, img); err != nil {
			t.Fatal(err)
		}
		if EvictFiles(targets) {
			t.Errorf("EvictFiles() = true after inode replacement, want false")
		}
	})
}
