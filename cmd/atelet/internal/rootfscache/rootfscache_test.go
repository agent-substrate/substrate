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
	"os"
	"path/filepath"
	"sync"
	"testing"
)

const testDigest = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

func buildTar(t *testing.T, entries []struct{ name, body string; typeflag byte; mode int64 }) []byte {
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

func TestEnsureRootfs_CacheMiss(t *testing.T) {
	base := t.TempDir()
	c, err := New(context.Background(), base, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tarData := buildTar(t, []struct{ name, body string; typeflag byte; mode int64 }{
		{name: ".", typeflag: tar.TypeDir},
		{name: "etc/", typeflag: tar.TypeDir},
		{name: "etc/hostname", typeflag: tar.TypeReg, body: "test-host\n"},
	})

	lowerDir, cached, err := c.EnsureRootfs(context.Background(), testDigest, bytes.NewReader(tarData))
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

	tarData := buildTar(t, []struct{ name, body string; typeflag byte; mode int64 }{
		{name: ".", typeflag: tar.TypeDir},
		{name: "hello", typeflag: tar.TypeReg, body: "world"},
	})

	// First call: cache miss.
	if _, _, err := c.EnsureRootfs(context.Background(), testDigest, bytes.NewReader(tarData)); err != nil {
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

	tarData := buildTar(t, []struct{ name, body string; typeflag byte; mode int64 }{
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
				context.Background(), testDigest, bytes.NewReader(tarData),
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
	tarData := buildTar(t, []struct{ name, body string; typeflag byte; mode int64 }{
		{name: ".", typeflag: tar.TypeDir},
		{name: "fresh", typeflag: tar.TypeReg, body: "data"},
	})
	lowerDir, cached, err := c.EnsureRootfs(context.Background(), testDigest, bytes.NewReader(tarData))
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

	tarData1 := buildTar(t, []struct{ name, body string; typeflag byte; mode int64 }{
		{name: ".", typeflag: tar.TypeDir},
		{name: "d1", typeflag: tar.TypeReg, body: "data1"},
	})
	tarData2 := buildTar(t, []struct{ name, body string; typeflag byte; mode int64 }{
		{name: ".", typeflag: tar.TypeDir},
		{name: "d2", typeflag: tar.TypeReg, body: "data2"},
	})

	if _, _, err := c.EnsureRootfs(context.Background(), digest1, bytes.NewReader(tarData1)); err != nil {
		t.Fatalf("EnsureRootfs d1: %v", err)
	}
	if _, _, err := c.EnsureRootfs(context.Background(), digest2, bytes.NewReader(tarData2)); err != nil {
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

	tarData := buildTar(t, []struct{ name, body string; typeflag byte; mode int64 }{
		{name: ".", typeflag: tar.TypeDir},
	})
	if _, _, err := c.EnsureRootfs(context.Background(), testDigest, bytes.NewReader(tarData)); err != nil {
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
