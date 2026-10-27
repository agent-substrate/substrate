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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/pkg/objectstorage"
)

func TestValidateSnapshotCacheFlags(t *testing.T) {
	origDir, origMinAge, origPeriod, origFetch := *snapshotCacheDir, *snapshotCacheMinAge, *snapshotCacheGCFlags.Period, *snapshotCacheFetchTimeout
	t.Cleanup(func() {
		*snapshotCacheDir, *snapshotCacheMinAge, *snapshotCacheGCFlags.Period, *snapshotCacheFetchTimeout = origDir, origMinAge, origPeriod, origFetch
	})

	cases := []struct {
		name    string
		dir     string
		minAge  time.Duration
		wantErr bool
	}{
		{"defaults", origDir, 10 * time.Minute, false},
		{"disabled", "", 10 * time.Minute, false},
		{"disabled still rejects a bad min-age", "", -time.Second, true},
		{"zero min-age", origDir, 0, false},
		{"outside base path: the plugin could not fill it", "/var/lib/elsewhere/snapshot-cache", 10 * time.Minute, true},
		{"negative min-age inverts the veto", origDir, -time.Second, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			*snapshotCacheDir, *snapshotCacheMinAge = tc.dir, tc.minAge
			err := validateSnapshotCacheFlags()
			if (err != nil) != tc.wantErr {
				t.Errorf("dir=%q minAge=%v: err=%v, wantErr=%v", tc.dir, tc.minAge, err, tc.wantErr)
			}
		})
	}
	// The eviction flags are validated here too, enabled or not.
	for _, dir := range []string{origDir, ""} {
		*snapshotCacheDir, *snapshotCacheMinAge, *snapshotCacheGCFlags.Period = dir, 10*time.Minute, -time.Second
		if err := validateSnapshotCacheFlags(); err == nil {
			t.Errorf("validateSnapshotCacheFlags() = nil with a negative GC period and dir %q, want an error", dir)
		}
	}
	// So is the fetch timeout: a zero or negative bound would end every
	// download at once.
	*snapshotCacheGCFlags.Period = origPeriod
	for _, timeout := range []time.Duration{0, -time.Second} {
		*snapshotCacheFetchTimeout = timeout
		if err := validateSnapshotCacheFlags(); err == nil {
			t.Errorf("validateSnapshotCacheFlags() = nil with fetch timeout %v, want an error", timeout)
		}
	}
}

func TestOpenSnapshotCacheDisabled(t *testing.T) {
	store, err := openSnapshotCache(context.Background(), "", 10*time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("openSnapshotCache(\"\") error: %v", err)
	}
	if store != nil {
		t.Errorf("openSnapshotCache(\"\") = %v, want nil (disabled)", store)
	}
}

func TestOpenSnapshotCacheCreatesRoot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "snapshot-cache")
	store, err := openSnapshotCache(context.Background(), dir, 10*time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("openSnapshotCache(%q) error: %v", dir, err)
	}
	if store == nil {
		t.Fatal("openSnapshotCache returned a nil store for a non-empty dir")
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Errorf("cache root %s not created: %v", dir, err)
	}
}

// TestOpenSnapshotCacheSweepsDebris pins the startup ordering contract: crash
// debris (unfinished fetches in tmp/, interrupted evictions as .rm-*) is gone
// by the time openSnapshotCache returns, while published entries survive.
func TestOpenSnapshotCacheSweepsDebris(t *testing.T) {
	dir := t.TempDir()
	// A prior life's layout: one published entry, plus debris of both kinds.
	entryDir := filepath.Join(dir, "entries", "aaaa")
	for _, d := range []string{
		filepath.Join(dir, "tmp", "aaaa-12345"),
		filepath.Join(dir, ".rm-bbbb-xyz"),
		entryDir,
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "data"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	store, err := openSnapshotCache(context.Background(), dir, 10*time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("openSnapshotCache error: %v", err)
	}
	if store == nil {
		t.Fatal("openSnapshotCache returned a nil store for a non-empty dir")
	}

	if children, err := os.ReadDir(filepath.Join(dir, "tmp")); err != nil || len(children) != 0 {
		t.Errorf("tmp debris not swept: %d children, %v", len(children), err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".rm-bbbb-xyz")); !os.IsNotExist(err) {
		t.Errorf("retired debris not swept: %v", err)
	}
	if _, err := os.Stat(filepath.Join(entryDir, "data")); err != nil {
		t.Errorf("published entry did not survive the sweep: %v", err)
	}
}

// TestSnapshotCacheMode pins the caching gate: only a snapshot the control
// plane declared shared is served from the cache. Private, unstated, and
// absent sources all download fresh; atelet never infers cacheability from
// the URI.
func TestSnapshotCacheMode(t *testing.T) {
	withSharing := func(sh ateletpb.SnapshotSharing) *ateletpb.ObjectStorage {
		return &ateletpb.ObjectStorage{SnapshotUri: testSnapshotURI, Sharing: sh}
	}
	cases := []struct {
		name   string
		object *ateletpb.ObjectStorage
		want   cacheMode
	}{
		{"shared uses the class mode", withSharing(ateletpb.SnapshotSharing_SNAPSHOT_SHARING_SHARED), cacheModeLink},
		{"private is never cached", withSharing(ateletpb.SnapshotSharing_SNAPSHOT_SHARING_PRIVATE), cacheModeOff},
		{"unstated is never cached", withSharing(ateletpb.SnapshotSharing_SNAPSHOT_SHARING_UNSPECIFIED), cacheModeOff},
		{"no object", nil, cacheModeOff},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := snapshotCacheMode(tc.object, cacheModeLink); got != tc.want {
				t.Errorf("snapshotCacheMode() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCacheModeFor(t *testing.T) {
	if got := cacheModeFor(string(atev1.SandboxClassGvisor)); got != cacheModeLink {
		t.Errorf("gVisor consumes restore state read-only; want cacheModeLink, got %v", got)
	}
	// ateworker-microvm keeps memory-ranges open for the VM's life, merges
	// deltas into it, and drops it from the page cache; through a link each
	// of those would reach the cache's entry, so it gets private copies.
	if got := cacheModeFor(string(atev1.SandboxClassMicroVM)); got != cacheModeCopy {
		t.Errorf("micro-VM keeps and reworks its restore files; want cacheModeCopy, got %v", got)
	}
	if got := cacheModeFor(""); got != cacheModeOff {
		t.Errorf("unknown sandbox class: want cacheModeOff, got %v", got)
	}
}

// countingObjectStorage counts reads, so a test can pin how many downloads
// a sequence of restores performed.
type countingObjectStorage struct {
	objectstorage.ObjectStorage
	gets atomic.Int32
}

func (c *countingObjectStorage) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	c.gets.Add(1)
	return c.ObjectStorage.GetObject(ctx, bucket, object)
}

// newSnapshotCacheHerder returns an atelet whose plugin serves a snapshot
// holding the named files with the given contents, with a snapshot cache
// open under base, plus base itself, which is on the cache's filesystem,
// for restore directories.
func newSnapshotCacheHerder(t *testing.T, files map[string]string) (*Atelet, *countingObjectStorage, string) {
	t.Helper()
	base := t.TempDir()
	store := &countingObjectStorage{ObjectStorage: &recordingObjectStorage{}}
	for name, content := range files {
		payload := filepath.Join(base, "payload-"+filepath.Base(name))
		if err := os.WriteFile(payload, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := objectstorage.SendLocalFileToGCSWithZstd(context.Background(), store.ObjectStorage, testSnapshotURI+"/"+name+".zstd", payload); err != nil {
			t.Fatal(err)
		}
	}
	s := newPluginHerder(t, store)
	cache, err := filecache.New(filepath.Join(base, "cache"))
	if err != nil {
		t.Fatal(err)
	}
	s.snapshotCache = cache
	return s, store, base
}

// restoreDir makes a restore directory under base, as Restore does before
// it downloads.
func restoreDir(t *testing.T, base, name string) string {
	t.Helper()
	dir := filepath.Join(base, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestFetchManifestCached pins that a shared snapshot's manifest is read
// through the cache, one download for any number of restores, with the
// scratch link dropped again, while a private one is read fresh each time.
func TestFetchManifestCached(t *testing.T) {
	const manifest = `{"sandboxClass":"gvisor"}`
	s, store, _ := newSnapshotCacheHerder(t, nil)
	ctx := context.Background()
	// The plugin stores the manifest as is, not zstd-compressed like the
	// data files the herder's helper uploads.
	if err := store.ObjectStorage.PutObject(ctx, "bucket", strings.TrimPrefix(testSnapshotPath, "bucket/")+"/"+sandboxManifestName, strings.NewReader(manifest)); err != nil {
		t.Fatal(err)
	}

	for i := range 2 {
		got, err := s.fetchManifestCached(ctx, testActor, testSnapshotURI, cacheModeLink)
		if err != nil {
			t.Fatalf("fetchManifestCached #%d: %v", i+1, err)
		}
		if string(got) != manifest {
			t.Fatalf("manifest #%d = %q, want %q", i+1, got, manifest)
		}
	}
	if n := store.gets.Load(); n != 1 {
		t.Errorf("two cached manifest reads downloaded %d times, want 1", n)
	}
	if left, err := os.ReadDir(s.snapshotScratchDir); err != nil || len(left) != 0 {
		t.Errorf("scratch dir holds %v (%v) after cached manifest reads, want nothing", left, err)
	}

	// Not shared: read from object storage, as before the cache existed.
	if _, err := s.fetchManifestCached(ctx, testActor, testSnapshotURI, cacheModeOff); err != nil {
		t.Fatal(err)
	}
	if n := store.gets.Load(); n != 2 {
		t.Errorf("a private manifest read downloaded %d times in total, want 2", n)
	}
}

func TestDownloadExternalCheckpointServesFromCache(t *testing.T) {
	s, store, base := newSnapshotCacheHerder(t, map[string]string{"memory-ranges": "golden memory"})
	ctx := context.Background()
	files := []string{"memory-ranges"}

	// Two cached restores: one download, both destinations share the inode.
	first, second := restoreDir(t, base, "first"), restoreDir(t, base, "second")
	for _, dir := range []string{first, second} {
		if err := s.downloadExternalCheckpoint(ctx, testActor, testSnapshotURI, dir, files, cacheModeLink); err != nil {
			t.Fatalf("downloadExternalCheckpoint into %s: %v", dir, err)
		}
		if got, err := os.ReadFile(filepath.Join(dir, "memory-ranges")); err != nil || string(got) != "golden memory" {
			t.Fatalf("staged in %s = %q, %v", dir, got, err)
		}
	}
	if n := store.gets.Load(); n != 1 {
		t.Errorf("two cached restores performed %d downloads, want 1", n)
	}
	fi1, err1 := os.Stat(filepath.Join(first, "memory-ranges"))
	fi2, err2 := os.Stat(filepath.Join(second, "memory-ranges"))
	if err1 != nil || err2 != nil || !os.SameFile(fi1, fi2) {
		t.Errorf("cached destinations do not share an inode (%v, %v)", err1, err2)
	}
	if fi1.Mode().Perm()&0o222 != 0 {
		t.Errorf("a linked restore file is writable (%v); the shared bytes must be read-only", fi1.Mode())
	}

	// A non-cacheable restore downloads fresh and shares nothing.
	direct := restoreDir(t, base, "direct")
	if err := s.downloadExternalCheckpoint(ctx, testActor, testSnapshotURI, direct, files, cacheModeOff); err != nil {
		t.Fatalf("direct downloadExternalCheckpoint: %v", err)
	}
	if n := store.gets.Load(); n != 2 {
		t.Errorf("direct restore did not download (gets=%d, want 2)", n)
	}
	if fi3, err := os.Stat(filepath.Join(direct, "memory-ranges")); err != nil || os.SameFile(fi1, fi3) {
		t.Errorf("direct destination shares the cache inode (%v)", err)
	}
}

// TestDownloadExternalCheckpointCopyMode pins the micro-VM serving mode: one
// download per snapshot file, every destination a private writable inode,
// and a consumer's in-place mutation never reaches the next consumer.
func TestDownloadExternalCheckpointCopyMode(t *testing.T) {
	s, store, base := newSnapshotCacheHerder(t, map[string]string{"memory-ranges": "golden memory"})
	ctx := context.Background()
	files := []string{"memory-ranges"}

	first, second := restoreDir(t, base, "first"), restoreDir(t, base, "second")
	for _, dir := range []string{first, second} {
		if err := s.downloadExternalCheckpoint(ctx, testActor, testSnapshotURI, dir, files, cacheModeCopy); err != nil {
			t.Fatalf("downloadExternalCheckpoint into %s: %v", dir, err)
		}
	}
	if n := store.gets.Load(); n != 1 {
		t.Errorf("two copy-mode restores performed %d downloads, want 1", n)
	}
	fi1, err1 := os.Stat(filepath.Join(first, "memory-ranges"))
	fi2, err2 := os.Stat(filepath.Join(second, "memory-ranges"))
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if os.SameFile(fi1, fi2) {
		t.Error("copy-mode destinations share an inode; each consumer must own its own")
	}

	// The consumer may rewrite its staged file (ateworker-microvm does, to
	// config.json) without poisoning what the next restore receives.
	if err := os.WriteFile(filepath.Join(first, "memory-ranges"), []byte("rewritten by the worker"), 0o600); err != nil {
		t.Fatalf("consumer write to its copy: %v", err)
	}
	third := restoreDir(t, base, "third")
	if err := s.downloadExternalCheckpoint(ctx, testActor, testSnapshotURI, third, files, cacheModeCopy); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(third, "memory-ranges")); err != nil || string(got) != "golden memory" {
		t.Errorf("copy after consumer mutation = %q, %v; the cached bytes were poisoned", got, err)
	}
	if n := store.gets.Load(); n != 1 {
		t.Errorf("gets=%d, want 1 (mutation must not invalidate the cache)", n)
	}
}

func TestDownloadExternalCheckpointWithoutCacheDownloads(t *testing.T) {
	s, store, base := newSnapshotCacheHerder(t, map[string]string{"memory-ranges": "golden memory"})
	s.snapshotCache = nil // caching disabled (--snapshot-cache-dir="")

	dir := restoreDir(t, base, "out")
	if err := s.downloadExternalCheckpoint(context.Background(), testActor, testSnapshotURI, dir, []string{"memory-ranges"}, cacheModeLink); err != nil {
		t.Fatalf("downloadExternalCheckpoint with nil cache: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "memory-ranges")); err != nil || string(got) != "golden memory" {
		t.Fatalf("staged content = %q, %v", got, err)
	}
	if n := store.gets.Load(); n != 1 {
		t.Errorf("gets=%d, want 1", n)
	}
}

func TestDownloadExternalCheckpointCachedFetchErrorReachesCaller(t *testing.T) {
	s, _, base := newSnapshotCacheHerder(t, map[string]string{"memory-ranges": "golden memory"})
	dir := restoreDir(t, base, "out")
	err := s.downloadExternalCheckpoint(context.Background(), testActor, testSnapshotURI, dir, []string{"missing"}, cacheModeLink)
	if err == nil {
		t.Fatal("downloadExternalCheckpoint succeeded for a missing file")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "missing")); !os.IsNotExist(statErr) {
		t.Errorf("failed fetch left a destination file: %v", statErr)
	}
}

// TestDownloadExternalCheckpointSharesOneDownload pins the exit criterion at
// the unit level: concurrent restores staging the same shared snapshot
// perform its downloads once.
func TestDownloadExternalCheckpointSharesOneDownload(t *testing.T) {
	s, store, base := newSnapshotCacheHerder(t, map[string]string{"memory-ranges": "golden memory"})
	files := []string{"memory-ranges"}

	const restores = 4
	errs := make([]error, restores)
	var wg sync.WaitGroup
	for i := range restores {
		dir := restoreDir(t, base, fmt.Sprintf("actor-%d", i))
		wg.Go(func() {
			errs[i] = s.downloadExternalCheckpoint(context.Background(), testActor, testSnapshotURI, dir, files, cacheModeLink)
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("restore %d: %v", i, err)
		}
		got, err := os.ReadFile(filepath.Join(base, fmt.Sprintf("actor-%d", i), "memory-ranges"))
		if err != nil || string(got) != "golden memory" {
			t.Fatalf("restore %d staged %q, %v", i, got, err)
		}
	}
	if n := store.gets.Load(); n != 1 {
		t.Errorf("%d concurrent restores performed %d downloads, want 1", restores, n)
	}
}

// TestDownloadExternalCheckpointCachedSubdirAndSymlinks covers the two ways
// the cached path differs from the plugin writing through an os.Root: a
// file in the checkpoint subdirectory is placed there, and a symlink waiting
// in the restore directory, as a directory or as the file itself, fails the
// restore instead of being followed outside it.
func TestDownloadExternalCheckpointCachedSubdirAndSymlinks(t *testing.T) {
	s, _, base := newSnapshotCacheHerder(t, map[string]string{"memory-ranges": "golden memory", "fs/data.tar": "fs delta", "fs/rootfs-upper-app.tar": "rootfs delta"})
	ctx := context.Background()

	// Two files share the subdirectory and are fetched in parallel: the
	// directory is made once, before either, not raced into by both.
	dir := restoreDir(t, base, "subdir")
	if err := s.downloadExternalCheckpoint(ctx, testActor, testSnapshotURI, dir, []string{"memory-ranges", "fs/data.tar", "fs/rootfs-upper-app.tar"}, cacheModeLink); err != nil {
		t.Fatalf("downloadExternalCheckpoint with subdirectory files: %v", err)
	}
	for name, want := range map[string]string{"fs/data.tar": "fs delta", "fs/rootfs-upper-app.tar": "rootfs delta"} {
		if got, err := os.ReadFile(filepath.Join(dir, name)); err != nil || string(got) != want {
			t.Fatalf("subdirectory file %s = %q, %v", name, got, err)
		}
	}

	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(outside, "memory-ranges")
	if err := os.WriteFile(keep, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}
	fileLink := restoreDir(t, base, "file-link")
	if err := os.Symlink(keep, filepath.Join(fileLink, "memory-ranges")); err != nil {
		t.Fatal(err)
	}
	if err := s.downloadExternalCheckpoint(ctx, testActor, testSnapshotURI, fileLink, []string{"memory-ranges"}, cacheModeLink); err == nil {
		t.Error("a symlink in place of the restore file was followed")
	}
	dirLink := restoreDir(t, base, "dir-link")
	if err := os.Symlink(outside, filepath.Join(dirLink, "fs")); err != nil {
		t.Fatal(err)
	}
	if err := s.downloadExternalCheckpoint(ctx, testActor, testSnapshotURI, dirLink, []string{"fs/data.tar"}, cacheModeCopy); err == nil {
		t.Error("a symlink in place of the checkpoint subdirectory was followed")
	}
	if got, err := os.ReadFile(keep); err != nil || string(got) != "keep me" {
		t.Errorf("file outside the restore directory = %q, %v; want untouched", got, err)
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 1 {
		t.Errorf("the directory outside the restore directory holds %d entries, want only the original file", len(entries))
	}
}
