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

package rootfscache

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const testDigest = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

func buildTar(t *testing.T, entries []struct {
	name, body string
	typeflag   byte
	mode       int64
}) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		mode := e.mode
		if mode == 0 {
			if e.typeflag == tar.TypeDir {
				mode = 0o755
			} else {
				mode = 0o644
			}
		}
		hdr := &tar.Header{
			Name:     e.name,
			Typeflag: e.typeflag,
			Mode:     mode,
			Size:     int64(len(e.body)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("tar.WriteHeader: %v", err)
		}
		if e.body != "" {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("tar.Write: %v", err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	return buf.Bytes()
}

// tarProviderFor returns an EnsureRootfs tar provider that yields a fresh
// reader over data on each call.
func tarProviderFor(data []byte) func() (io.ReadCloser, error) {
	return func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
}

func TestEnsureRootfs_CacheMiss(t *testing.T) {
	base := t.TempDir()
	c, err := New(context.Background(), base, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tarData := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
		{name: "etc/", typeflag: tar.TypeDir},
		{name: "etc/hostname", typeflag: tar.TypeReg, body: "test-host\n"},
	})

	lowerDir, cached, err := c.EnsureRootfs(context.Background(), testDigest, tarProviderFor(tarData))
	if err != nil {
		t.Fatalf("EnsureRootfs: %v", err)
	}
	if cached {
		t.Errorf("expected cache miss, got hit")
	}
	if lowerDir == "" {
		t.Fatalf("lowerDir is empty")
	}

	// Verify the rootfs was extracted correctly.
	data, err := os.ReadFile(filepath.Join(lowerDir, "etc/hostname"))
	if err != nil {
		t.Fatalf("read etc/hostname: %v", err)
	}
	if string(data) != "test-host\n" {
		t.Errorf("etc/hostname = %q, want %q", data, "test-host\n")
	}

	// Verify sentinel file exists.
	readyPath := filepath.Join(base, testDigest, readySentinel)
	if _, err := os.Stat(readyPath); err != nil {
		t.Fatalf("ready sentinel missing: %v", err)
	}

	if c.Count() != 1 {
		t.Errorf("count = %d, want 1", c.Count())
	}
}

func TestEnsureRootfs_CacheHit(t *testing.T) {
	base := t.TempDir()
	c, err := New(context.Background(), base, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tarData := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
		{name: "hello", typeflag: tar.TypeReg, body: "world"},
	})

	// First call: cache miss.
	if _, _, err := c.EnsureRootfs(context.Background(), testDigest, tarProviderFor(tarData)); err != nil {
		t.Fatalf("EnsureRootfs (miss): %v", err)
	}

	// Second call: cache hit.  Pass nil reader — it must not be read.
	lowerDir, cached, err := c.EnsureRootfs(context.Background(), testDigest, nil)
	if err != nil {
		t.Fatalf("EnsureRootfs (hit): %v", err)
	}
	if !cached {
		t.Errorf("expected cache hit, got miss")
	}
	if lowerDir == "" {
		t.Fatalf("lowerDir is empty on hit")
	}

	// Verify content still accessible.
	data, err := os.ReadFile(filepath.Join(lowerDir, "hello"))
	if err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if string(data) != "world" {
		t.Errorf("hello = %q, want %q", data, "world")
	}
}

func TestEnsureRootfs_ConcurrentMisses(t *testing.T) {
	base := t.TempDir()
	c, err := New(context.Background(), base, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tarData := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
		{name: "concurrent", typeflag: tar.TypeReg, body: "ok"},
	})

	const goroutines = 10
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	lowerDirs := make([]string, goroutines)
	cachedFlags := make([]bool, goroutines)

	for i := 0; i < goroutines; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine gets its own reader over the same data.
			lowerDirs[i], cachedFlags[i], errs[i] = c.EnsureRootfs(
				context.Background(), testDigest, tarProviderFor(tarData),
			)
		}()
	}
	wg.Wait()

	// At least one goroutine should have done the extraction (miss).
	// All should succeed with the same lowerDir.
	anyMiss := false
	for i := 0; i < goroutines; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: %v", i, errs[i])
			continue
		}
		if !cachedFlags[i] {
			anyMiss = true
		}
		if lowerDirs[i] == "" {
			t.Errorf("goroutine %d: empty lowerDir", i)
		}
		if i > 0 && lowerDirs[i] != lowerDirs[0] {
			t.Errorf("goroutine %d: lowerDir %q != goroutine 0 lowerDir %q", i, lowerDirs[i], lowerDirs[0])
		}
	}
	if !anyMiss {
		t.Errorf("expected at least one cache miss among %d goroutines", goroutines)
	}

	// Only one cache entry should exist.
	if c.Count() != 1 {
		t.Errorf("count = %d, want 1", c.Count())
	}
}

func TestEnsureRootfs_PartialEntryCleanup(t *testing.T) {
	base := t.TempDir()
	// Simulate a crash: create the digest directory but no .ready sentinel.
	partialDir := filepath.Join(base, testDigest, "lower")
	if err := os.MkdirAll(partialDir, 0o700); err != nil {
		t.Fatalf("mkdir partial: %v", err)
	}
	if err := os.WriteFile(filepath.Join(partialDir, "stale"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write stale: %v", err)
	}

	c, err := New(context.Background(), base, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// The partial entry should have been cleaned up during loadIndex.
	if c.Count() != 0 {
		t.Errorf("count = %d, want 0 (partial entry should be removed)", c.Count())
	}

	// Now a fresh extraction should succeed.
	tarData := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
		{name: "fresh", typeflag: tar.TypeReg, body: "data"},
	})
	lowerDir, cached, err := c.EnsureRootfs(context.Background(), testDigest, tarProviderFor(tarData))
	if err != nil {
		t.Fatalf("EnsureRootfs: %v", err)
	}
	if cached {
		t.Errorf("expected miss after cleanup, got hit")
	}
	if _, err := os.Stat(filepath.Join(lowerDir, "fresh")); err != nil {
		t.Errorf("fresh file missing: %v", err)
	}
}

func TestEvictLRU(t *testing.T) {
	base := t.TempDir()
	c, err := New(context.Background(), base, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	digest1 := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	digest2 := "sha256:2222222222222222222222222222222222222222222222222222222222222222"

	tarData1 := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
		{name: "d1", typeflag: tar.TypeReg, body: "data1"},
	})
	tarData2 := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
		{name: "d2", typeflag: tar.TypeReg, body: "data2"},
	})

	if _, _, err := c.EnsureRootfs(context.Background(), digest1, tarProviderFor(tarData1)); err != nil {
		t.Fatalf("EnsureRootfs d1: %v", err)
	}
	if _, _, err := c.EnsureRootfs(context.Background(), digest2, tarProviderFor(tarData2)); err != nil {
		t.Fatalf("EnsureRootfs d2: %v", err)
	}

	if c.Count() != 2 {
		t.Fatalf("count = %d, want 2", c.Count())
	}

	// Evict the oldest (digest1, loaded first).
	evicted, size := c.EvictLRU()
	if evicted != digest1 {
		t.Errorf("evicted = %q, want %q", evicted, digest1)
	}
	if size <= 0 {
		t.Errorf("evicted size = %d, want > 0", size)
	}
	if c.Count() != 1 {
		t.Errorf("count = %d, want 1 after eviction", c.Count())
	}

	// The evicted directory should be gone.
	if _, err := os.Stat(filepath.Join(base, digest1)); !os.IsNotExist(err) {
		t.Errorf("evicted dir still exists: %v", err)
	}
}

// seedTwoEntries populates the cache with digest1 (older) and digest2 (newer)
// and returns their lowerDirs. digest1 is guaranteed to sort as the LRU victim.
func seedTwoEntries(t *testing.T, c *Cache) (digest1, digest2, lower1, lower2 string) {
	t.Helper()
	digest1, digest2 = evictDigest1, evictDigest2

	tarData1 := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
		{name: "d1", typeflag: tar.TypeReg, body: "data1"},
	})
	tarData2 := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
		{name: "d2", typeflag: tar.TypeReg, body: "data2"},
	})

	l1, _, err := c.EnsureRootfs(context.Background(), digest1, tarProviderFor(tarData1))
	if err != nil {
		t.Fatalf("EnsureRootfs d1: %v", err)
	}
	l2, _, err := c.EnsureRootfs(context.Background(), digest2, tarProviderFor(tarData2))
	if err != nil {
		t.Fatalf("EnsureRootfs d2: %v", err)
	}
	return digest1, digest2, l1, l2
}

const (
	evictDigest1 = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	evictDigest2 = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

// lowerPathFor returns the deterministic lowerDir a cache rooted at base would
// use for digest. Tests use it to pre-build an in-use set before seeding, so the
// injected provider never mutates shared state concurrently with the background
// eviction goroutine (which would otherwise trip the race detector).
func lowerPathFor(base, digest string) string {
	return filepath.Join(base, digest, "lower")
}

// TestEvictLRU_SkipsInUse verifies that an in-use lowerDir (as reported by the
// injected provider) is never selected as the eviction victim, even when it is
// the least-recently-used entry — deleting a mounted lowerdir would corrupt a
// live actor.
func TestEvictLRU_SkipsInUse(t *testing.T) {
	base := t.TempDir()

	// Pin the older entry (digest1) up front. Eviction must skip it and take
	// digest2. The set is immutable after New, so the background eviction
	// goroutine can read it race-free.
	pinned := map[string]bool{lowerPathFor(base, evictDigest1): true}
	c, err := New(context.Background(), base, 0, WithInUseFunc(func() (map[string]bool, error) {
		return pinned, nil
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	digest1, digest2, _, _ := seedTwoEntries(t, c)

	evicted, size := c.EvictLRU()
	if evicted != digest2 {
		t.Errorf("evicted = %q, want %q (the pinned LRU entry must be skipped)", evicted, digest2)
	}
	if size <= 0 {
		t.Errorf("evicted size = %d, want > 0", size)
	}
	if c.LowerDir(digest1) == "" {
		t.Errorf("pinned digest1 was evicted; it must remain")
	}
	if _, err := os.Stat(filepath.Join(base, digest1)); err != nil {
		t.Errorf("pinned digest1 dir missing: %v", err)
	}
}

// TestEvictLRU_AllPinned verifies that when every entry is in use, EvictLRU
// evicts nothing rather than deleting a live lowerdir.
func TestEvictLRU_AllPinned(t *testing.T) {
	base := t.TempDir()

	pinned := map[string]bool{
		lowerPathFor(base, evictDigest1): true,
		lowerPathFor(base, evictDigest2): true,
	}
	c, err := New(context.Background(), base, 0, WithInUseFunc(func() (map[string]bool, error) {
		return pinned, nil
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	seedTwoEntries(t, c)

	evicted, size := c.EvictLRU()
	if evicted != "" || size != 0 {
		t.Errorf("EvictLRU = (%q, %d), want (\"\", 0) when all entries pinned", evicted, size)
	}
	if c.Count() != 2 {
		t.Errorf("count = %d, want 2 (nothing should be evicted)", c.Count())
	}
}

// TestEvictLRU_ProviderError verifies that a failing in-use provider makes
// eviction a no-op (conservative: exceeding the budget beats corrupting a
// possibly-live lowerdir).
func TestEvictLRU_ProviderError(t *testing.T) {
	base := t.TempDir()

	c, err := New(context.Background(), base, 0, WithInUseFunc(func() (map[string]bool, error) {
		return nil, io.ErrUnexpectedEOF
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	seedTwoEntries(t, c)

	evicted, size := c.EvictLRU()
	if evicted != "" || size != 0 {
		t.Errorf("EvictLRU = (%q, %d), want (\"\", 0) on provider error", evicted, size)
	}
	if c.Count() != 2 {
		t.Errorf("count = %d, want 2 (nothing should be evicted on provider error)", c.Count())
	}
}

func TestLowerDir(t *testing.T) {
	base := t.TempDir()
	c, err := New(context.Background(), base, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Before caching.
	if got := c.LowerDir(testDigest); got != "" {
		t.Errorf("LowerDir before cache = %q, want empty", got)
	}

	tarData := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
	})
	if _, _, err := c.EnsureRootfs(context.Background(), testDigest, tarProviderFor(tarData)); err != nil {
		t.Fatalf("EnsureRootfs: %v", err)
	}

	// After caching.
	got := c.LowerDir(testDigest)
	if got == "" {
		t.Fatalf("LowerDir after cache is empty")
	}
	if got != filepath.Join(base, testDigest, "lower") {
		t.Errorf("LowerDir = %q, want %q", got, filepath.Join(base, testDigest, "lower"))
	}
}

// seedReadyEntryOnDisk writes a completed cache entry (lower/ + .ready +
// .last_access) directly to disk so a freshly constructed Cache picks it up via
// loadIndex. bodySize bytes of content are written so the entry has nonzero
// size; accessedAt sets the recorded last-access time (older sorts as the LRU
// victim).
func seedReadyEntryOnDisk(t *testing.T, base, digest string, bodySize int, accessedAt time.Time) {
	t.Helper()
	lower := filepath.Join(base, digest, "lower")
	if err := os.MkdirAll(lower, 0o700); err != nil {
		t.Fatalf("mkdir lower: %v", err)
	}
	if err := os.WriteFile(filepath.Join(lower, "blob"), make([]byte, bodySize), 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	if err := os.WriteFile(filepath.Join(base, digest, readySentinel), []byte(accessedAt.Format(time.RFC3339)), 0o444); err != nil {
		t.Fatalf("write ready: %v", err)
	}
	if err := os.WriteFile(filepath.Join(base, digest, lastAccessFile), []byte(accessedAt.Format(time.RFC3339Nano)), 0o644); err != nil {
		t.Fatalf("write last_access: %v", err)
	}
}

// TestUntar_ReturnsByteCount verifies Untar reports the total regular-file
// content bytes it wrote, so the cache can size an entry without a second walk.
func TestUntar_ReturnsByteCount(t *testing.T) {
	tarData := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
		{name: "a", typeflag: tar.TypeReg, body: "hello"}, // 5
		{name: "sub/", typeflag: tar.TypeDir},
		{name: "sub/b", typeflag: tar.TypeReg, body: "world!!"}, // 7
	})
	dir := t.TempDir()
	n, err := Untar(context.Background(), bytes.NewReader(tarData), dir)
	if err != nil {
		t.Fatalf("Untar: %v", err)
	}
	if n != 12 {
		t.Errorf("Untar bytes = %d, want 12", n)
	}
}

// TestEnsureRootfs_RecordsSizeFromUntar verifies the byte count Untar returns is
// threaded into the cache entry's size (rather than recomputed via a tree walk).
func TestEnsureRootfs_RecordsSizeFromUntar(t *testing.T) {
	base := t.TempDir()
	c, err := New(context.Background(), base, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tarData := buildTar(t, []struct {
		name, body string
		typeflag   byte
		mode       int64
	}{
		{name: ".", typeflag: tar.TypeDir},
		{name: "f", typeflag: tar.TypeReg, body: "0123456789"}, // 10
	})
	if _, _, err := c.EnsureRootfs(context.Background(), testDigest, tarProviderFor(tarData)); err != nil {
		t.Fatalf("EnsureRootfs: %v", err)
	}
	if got := c.Size(); got != 10 {
		t.Errorf("Size = %d, want 10", got)
	}
}

// TestNew_EvictsWhenBootingOverBudget verifies that a Cache which loads an
// already-over-budget set of entries from disk reclaims space at startup,
// evicting the least-recently-used entry — not only on the next miss.
func TestNew_EvictsWhenBootingOverBudget(t *testing.T) {
	base := t.TempDir()
	older := time.Now().Add(-2 * time.Hour)
	newer := time.Now().Add(-1 * time.Hour)
	// Two ~4 KiB entries; a 6000-byte budget fits one but not both, so the
	// older entry (evictDigest1) must be evicted at startup.
	seedReadyEntryOnDisk(t, base, evictDigest1, 4096, older)
	seedReadyEntryOnDisk(t, base, evictDigest2, 4096, newer)

	c, err := New(context.Background(), base, 6000)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Startup eviction runs asynchronously; poll until it converges.
	deadline := time.Now().Add(2 * time.Second)
	for c.Size() > 6000 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := c.Size(); got > 6000 {
		t.Fatalf("still over budget after startup eviction: size=%d", got)
	}
	if got := c.Count(); got != 1 {
		t.Fatalf("count = %d, want 1 after startup eviction", got)
	}
	if c.LowerDir(evictDigest1) != "" {
		t.Errorf("older digest1 still present; expected it evicted")
	}
	if c.LowerDir(evictDigest2) == "" {
		t.Errorf("newer digest2 missing; expected it retained")
	}
}

func TestValidateDigest(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"sha256:abc123", false},
		{"", true},
		{"../escape", true},
		{"sha256:abc/def", true},
		{"sha256:abc..def", true},
	}
	for _, tc := range tests {
		err := validateDigest(tc.input)
		if (err != nil) != tc.want {
			t.Errorf("validateDigest(%q) err=%v, wantErr=%v", tc.input, err, tc.want)
		}
	}
}
