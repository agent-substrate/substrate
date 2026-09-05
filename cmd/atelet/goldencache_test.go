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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/filecache"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func TestValidateGoldenCacheFlags(t *testing.T) {
	origDir, origMinAge := *goldenCacheDir, *goldenCacheMinAge
	t.Cleanup(func() { *goldenCacheDir, *goldenCacheMinAge = origDir, origMinAge })

	cases := []struct {
		name    string
		dir     string
		minAge  time.Duration
		wantErr bool
	}{
		{"defaults", origDir, 10 * time.Minute, false},
		{"disabled", "", 10 * time.Minute, false},
		{"zero min-age", origDir, 0, false},
		{"outside base path warns but is valid", "/var/lib/elsewhere/golden-cache", 10 * time.Minute, false},
		{"negative min-age inverts the veto", origDir, -time.Second, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			*goldenCacheDir, *goldenCacheMinAge = tc.dir, tc.minAge
			err := validateGoldenCacheFlags()
			if (err != nil) != tc.wantErr {
				t.Errorf("dir=%q minAge=%v: err=%v, wantErr=%v", tc.dir, tc.minAge, err, tc.wantErr)
			}
		})
	}
}

func TestOpenGoldenCacheDisabled(t *testing.T) {
	store, err := openGoldenCache(context.Background(), "", 10*time.Minute)
	if err != nil {
		t.Fatalf("openGoldenCache(\"\") error: %v", err)
	}
	if store != nil {
		t.Errorf("openGoldenCache(\"\") = %v, want nil (disabled)", store)
	}
}

func TestOpenGoldenCacheCreatesRoot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "golden-cache")
	store, err := openGoldenCache(context.Background(), dir, 10*time.Minute)
	if err != nil {
		t.Fatalf("openGoldenCache(%q) error: %v", dir, err)
	}
	if store == nil {
		t.Fatal("openGoldenCache returned a nil store for a non-empty dir")
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Errorf("cache root %s not created: %v", dir, err)
	}
}

func TestIsGoldenSnapshotURI(t *testing.T) {
	cases := []struct {
		name string
		uri  string
		want bool
	}{
		{"golden-atespace actor snapshot", goldenSnapshotURI, true},
		{"customer-atespace actor snapshot", testSnapshotURI, false},
		{"not a snapshot URI", "gs://bucket/some/random/object", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isGoldenSnapshotURI(tc.uri); got != tc.want {
				t.Errorf("isGoldenSnapshotURI(%q) = %v, want %v", tc.uri, got, tc.want)
			}
		})
	}
}

func TestGoldenCacheModeFor(t *testing.T) {
	if got := goldenCacheModeFor(string(atev1alpha1.SandboxClassGvisor)); got != cacheModeLink {
		t.Errorf("gVisor consumes restore-state read-only; want cacheModeLink, got %v", got)
	}
	// ateom-microvm rewrites config.json and memory-ranges in place; linking
	// its restore files from the cache would corrupt shared copies.
	if got := goldenCacheModeFor(string(atev1alpha1.SandboxClassMicroVM)); got != cacheModeOff {
		t.Errorf("micro-VM mutates restore-state files in place; want cacheModeOff, got %v", got)
	}
	if got := goldenCacheModeFor(""); got != cacheModeOff {
		t.Errorf("unknown sandbox class: want cacheModeOff, got %v", got)
	}
}

// countingObjectStorage counts GetObject calls, so tests can pin how many
// downloads a fetch sequence actually performed.
type countingObjectStorage struct {
	mapObjectStorage
	gets atomic.Int32
}

func (c *countingObjectStorage) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	c.gets.Add(1)
	return c.mapObjectStorage.GetObject(ctx, bucket, object)
}

// newGoldenCacheHerder returns a herder whose gcsClient serves the given
// content for one golden object, plus the object's URI and a directory (on
// the same filesystem as the cache) for link destinations.
func newGoldenCacheHerder(t *testing.T, content string) (*AteomHerder, *countingObjectStorage, string, string) {
	t.Helper()
	base := t.TempDir()
	store, err := filecache.New(filepath.Join(base, "cache"))
	if err != nil {
		t.Fatal(err)
	}
	dstDir := filepath.Join(base, "restore")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatal(err)
	}
	gcs := &countingObjectStorage{mapObjectStorage: mapObjectStorage{objects: map[string][]byte{
		goldenSnapshotPath + "/memory-ranges.zstd": zstdBytes(t, content),
	}}}
	s := &AteomHerder{gcsClient: gcs, goldenCache: store}
	return s, gcs, goldenSnapshotURI + "/memory-ranges.zstd", dstDir
}

func TestFetchSnapshotObjectServesFromGoldenCache(t *testing.T) {
	s, gcs, objectURI, dstDir := newGoldenCacheHerder(t, "golden memory")
	ctx := context.Background()

	// Two cached fetches: one download, both destinations share the inode.
	first := filepath.Join(dstDir, "first")
	second := filepath.Join(dstDir, "second")
	for _, dst := range []string{first, second} {
		if err := s.fetchSnapshotObject(ctx, objectURI, dst, cacheModeLink); err != nil {
			t.Fatalf("fetchSnapshotObject(%s): %v", dst, err)
		}
		if got, err := os.ReadFile(dst); err != nil || string(got) != "golden memory" {
			t.Fatalf("staged %s = %q, %v", dst, got, err)
		}
	}
	if n := gcs.gets.Load(); n != 1 {
		t.Errorf("two cached fetches performed %d downloads, want 1", n)
	}
	fi1, err1 := os.Stat(first)
	fi2, err2 := os.Stat(second)
	if err1 != nil || err2 != nil || !os.SameFile(fi1, fi2) {
		t.Errorf("cached destinations do not share an inode (%v, %v)", err1, err2)
	}

	// A non-cacheable fetch downloads fresh and shares nothing.
	direct := filepath.Join(dstDir, "direct")
	if err := s.fetchSnapshotObject(ctx, objectURI, direct, cacheModeOff); err != nil {
		t.Fatalf("direct fetchSnapshotObject: %v", err)
	}
	if n := gcs.gets.Load(); n != 2 {
		t.Errorf("direct fetch did not download (gets=%d, want 2)", n)
	}
	if fi3, err := os.Stat(direct); err != nil || os.SameFile(fi1, fi3) {
		t.Errorf("direct destination shares the cache inode (%v)", err)
	}
}

func TestFetchSnapshotObjectWithoutCacheDownloads(t *testing.T) {
	s, gcs, objectURI, dstDir := newGoldenCacheHerder(t, "golden memory")
	s.goldenCache = nil // caching disabled (--golden-cache-dir="")

	dst := filepath.Join(dstDir, "out")
	if err := s.fetchSnapshotObject(context.Background(), objectURI, dst, cacheModeLink); err != nil {
		t.Fatalf("fetchSnapshotObject with nil cache: %v", err)
	}
	if got, err := os.ReadFile(dst); err != nil || string(got) != "golden memory" {
		t.Fatalf("staged content = %q, %v", got, err)
	}
	if n := gcs.gets.Load(); n != 1 {
		t.Errorf("gets=%d, want 1", n)
	}
}

func TestFetchSnapshotObjectFetchErrorReachesCaller(t *testing.T) {
	s, _, _, dstDir := newGoldenCacheHerder(t, "golden memory")
	dst := filepath.Join(dstDir, "out")
	err := s.fetchSnapshotObject(context.Background(), goldenSnapshotURI+"/missing.zstd", dst, cacheModeLink)
	if err == nil {
		t.Fatal("fetchSnapshotObject succeeded for a missing object")
	}
	if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
		t.Errorf("failed fetch left a destination file: %v", statErr)
	}
}

// TestDownloadExternalCheckpointSharesOneGoldenDownload pins M2's exit
// criterion at the unit level: concurrent restores staging the same golden
// snapshot perform its downloads once.
func TestDownloadExternalCheckpointSharesOneGoldenDownload(t *testing.T) {
	s, gcs, _, dstDir := newGoldenCacheHerder(t, "golden memory")
	files := []string{"memory-ranges"}

	const restores = 4
	errs := make([]error, restores)
	var wg sync.WaitGroup
	for i := range restores {
		dir := filepath.Join(dstDir, fmt.Sprintf("actor-%d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			errs[i] = s.downloadExternalCheckpoint(context.Background(), goldenSnapshotURI, dir, files, cacheModeLink)
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("restore %d: %v", i, err)
		}
		got, err := os.ReadFile(filepath.Join(dstDir, fmt.Sprintf("actor-%d", i), "memory-ranges"))
		if err != nil || string(got) != "golden memory" {
			t.Fatalf("restore %d staged %q, %v", i, got, err)
		}
	}
	if n := gcs.gets.Load(); n != 1 {
		t.Errorf("%d concurrent restores performed %d downloads, want 1", restores, n)
	}
}

// TestOpenGoldenCacheSweepsDebris pins the startup ordering contract: crash
// debris (unfinished fetches in tmp/, interrupted evictions as .rm-*) is gone
// by the time openGoldenCache returns, while published entries survive.
func TestOpenGoldenCacheSweepsDebris(t *testing.T) {
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

	store, err := openGoldenCache(context.Background(), dir, 10*time.Minute)
	if err != nil {
		t.Fatalf("openGoldenCache error: %v", err)
	}
	if store == nil {
		t.Fatal("openGoldenCache returned a nil store for a non-empty dir")
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
