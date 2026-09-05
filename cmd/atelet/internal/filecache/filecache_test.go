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

package filecache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// readEntryMeta loads an entry dir's meta.json sidecar. Test-only: nothing
// on a production path reads the sidecar back.
func readEntryMeta(entryDir string) (entryMeta, error) {
	data, err := os.ReadFile(filepath.Join(entryDir, metaName))
	if err != nil {
		return entryMeta{}, fmt.Errorf("while reading entry meta: %w", err)
	}
	var m entryMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return entryMeta{}, fmt.Errorf("while parsing entry meta: %w", err)
	}
	return m, nil
}

// allocatedBytes reports how much disk a file actually occupies, which is
// less than its logical size when it has holes.
func allocatedBytes(t *testing.T, path string) int64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	return st.Blocks * 512
}

// sparseTestFetcher returns a FileFetcher producing a mostly-hole file:
// size logical bytes with one 4 KiB data extent. Tests that depend on the
// result being genuinely sparse must skip when the filesystem materialized
// it (see allocatedBytes).
func sparseTestFetcher(size int64) FileFetcher {
	return func(ctx context.Context, dstPath string) error {
		f, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		if err := f.Truncate(size); err != nil {
			return err
		}
		if _, err := f.WriteAt(make([]byte, 4<<10), size/2); err != nil {
			return err
		}
		return f.Close()
	}
}

func newTestStore(t *testing.T, opts ...Option) *Store {
	t.Helper()
	s, err := New(filepath.Join(t.TempDir(), "cache"), opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// publishTestEntry plants a published entry with the given data files
// (relative name -> content), bypassing retrieval, and returns its key.
func publishTestEntry(t *testing.T, s *Store, name string, files map[string]string) Key {
	t.Helper()
	k := URIKey("test://" + name)
	if len(files) == 0 {
		if err := os.MkdirAll(s.entryDir(k), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for rel, content := range files {
		path := filepath.Join(s.entryDir(k), rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o444); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeEntryMeta(s.entryDir(k), k, time.Now()); err != nil {
		t.Fatalf("writeEntryMeta: %v", err)
	}
	return k
}

func TestNewCreatesLayout(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cache")
	s, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, dir := range []string{s.entriesDir(), s.tmpDir()} {
		fi, err := os.Stat(dir)
		if err != nil || !fi.IsDir() {
			t.Errorf("stat %q: err=%v, isDir=%v", dir, err, err == nil && fi.IsDir())
		}
	}

	// Reopening an existing root keeps published entries.
	k := publishTestEntry(t, s, "survivor", map[string]string{dataName: "x"})
	if _, err := New(root); err != nil {
		t.Fatalf("New (reopen): %v", err)
	}
	if _, err := os.Stat(s.dataPath(k)); err != nil {
		t.Errorf("entry lost across reopen: %v", err)
	}
}

func TestNewDefaultsAndOptions(t *testing.T) {
	s := newTestStore(t)
	if s.minAge != defaultMinAge {
		t.Errorf("minAge = %v, want default %v", s.minAge, defaultMinAge)
	}
	if s.fetchTimeout != defaultFetchTimeout {
		t.Errorf("fetchTimeout = %v, want default %v", s.fetchTimeout, defaultFetchTimeout)
	}

	s = newTestStore(t, WithMinAge(time.Second), WithFetchTimeout(time.Minute))
	if s.minAge != time.Second {
		t.Errorf("minAge = %v, want %v", s.minAge, time.Second)
	}
	if s.fetchTimeout != time.Minute {
		t.Errorf("fetchTimeout = %v, want %v", s.fetchTimeout, time.Minute)
	}
}

func TestEntryMetaRoundTrip(t *testing.T) {
	s := newTestStore(t)
	k := publishTestEntry(t, s, "meta", map[string]string{dataName: "x"})

	m, err := readEntryMeta(s.entryDir(k))
	if err != nil {
		t.Fatalf("readEntryMeta: %v", err)
	}
	if m.Key != k.String() {
		t.Errorf("meta key = %q, want %q", m.Key, k.String())
	}
	if m.CreatedAt.IsZero() {
		t.Error("meta createdAt is zero")
	}
}

func TestSweepDebris(t *testing.T) {
	s := newTestStore(t)
	kept := publishTestEntry(t, s, "kept", map[string]string{dataName: "x"})

	// Unfinished fetches: a bare temp file and a temp extraction dir.
	if err := os.WriteFile(filepath.Join(s.tmpDir(), "dl-1"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(s.tmpDir(), "dl-2", "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	// An eviction that renamed but never removed.
	if err := os.MkdirAll(filepath.Join(s.root, rmPrefix+"deadbeef", "nested"), 0o755); err != nil {
		t.Fatal(err)
	}

	stats, err := s.SweepDebris(context.Background())
	if err != nil {
		t.Fatalf("SweepDebris: %v", err)
	}
	if stats.TmpRemoved != 2 || stats.RetiredRemoved != 1 {
		t.Errorf("stats = %+v, want TmpRemoved=2 RetiredRemoved=1", stats)
	}

	tmpChildren, err := os.ReadDir(s.tmpDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(tmpChildren) != 0 {
		t.Errorf("tmp dir not empty after sweep: %d children", len(tmpChildren))
	}
	if _, err := os.Stat(filepath.Join(s.root, rmPrefix+"deadbeef")); !os.IsNotExist(err) {
		t.Errorf("retired dir survived sweep: err=%v", err)
	}
	if _, err := os.Stat(s.dataPath(kept)); err != nil {
		t.Errorf("published entry removed by sweep: %v", err)
	}

	// A clean store sweeps to zero.
	stats, err = s.SweepDebris(context.Background())
	if err != nil {
		t.Fatalf("SweepDebris (clean): %v", err)
	}
	if stats != (SweepStats{}) {
		t.Errorf("stats on clean store = %+v, want zero", stats)
	}
}

func TestSweepDebrisCanceled(t *testing.T) {
	s := newTestStore(t)
	if err := os.WriteFile(filepath.Join(s.tmpDir(), "dl-1"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SweepDebris(ctx); err == nil {
		t.Error("SweepDebris with canceled ctx succeeded, want error")
	}
}

func TestTotalBytes(t *testing.T) {
	s := newTestStore(t)

	total, err := s.TotalBytes(context.Background())
	if err != nil {
		t.Fatalf("TotalBytes (empty): %v", err)
	}
	if total != 0 {
		t.Errorf("TotalBytes on empty store = %d, want 0", total)
	}

	publishTestEntry(t, s, "file", map[string]string{dataName: "12345"})
	publishTestEntry(t, s, "tree", map[string]string{
		filepath.Join(dataName, "runsc"):              "1234567890",
		filepath.Join(dataName, "gvisor-bin", "help"): "123",
	})
	// Unpublished bytes in tmp/ must not count.
	if err := os.WriteFile(filepath.Join(s.tmpDir(), "dl-1"), []byte("zzzz"), 0o600); err != nil {
		t.Fatal(err)
	}

	total, err = s.TotalBytes(context.Background())
	if err != nil {
		t.Fatalf("TotalBytes: %v", err)
	}
	// The measure is allocated (on-disk) bytes: the sum of the entries'
	// regular files' footprints — three data files plus two meta.json
	// sidecars — and nothing else.
	var want int64
	for _, rel := range []string{
		filepath.Join(s.entryDir(URIKey("test://file")), dataName),
		filepath.Join(s.entryDir(URIKey("test://file")), metaName),
		filepath.Join(s.entryDir(URIKey("test://tree")), dataName, "runsc"),
		filepath.Join(s.entryDir(URIKey("test://tree")), dataName, "gvisor-bin", "help"),
		filepath.Join(s.entryDir(URIKey("test://tree")), metaName),
	} {
		want += allocatedBytes(t, rel)
	}
	if total != want {
		t.Errorf("TotalBytes = %d, want %d (allocated bytes of the published files)", total, want)
	}
}

// TestTotalBytesCountsAllocatedNotLogical pins the accounting unit against
// the cache's most important artifact shape: a sparse guest memory image.
// Counting logical length would report a near-empty cache as huge, driving
// eviction that frees nothing.
func TestTotalBytesCountsAllocatedNotLogical(t *testing.T) {
	s := newTestStore(t)
	const logical = 64 << 20
	key := URIKey("test://sparse")
	if err := s.GetFileTo(context.Background(), key, dstPath(t, s, "sparse"), sparseTestFetcher(logical)); err != nil {
		t.Fatal(err)
	}
	if alloc := allocatedBytes(t, s.dataPath(key)); alloc >= logical/2 {
		t.Skipf("cached file did not end up sparse (%d of %d bytes allocated); this filesystem cannot report holes", alloc, int64(logical))
	}

	total, err := s.TotalBytes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if total >= logical/2 {
		t.Errorf("TotalBytes = %d for a sparse entry with %d allocated bytes; logical sizes are being counted", total, allocatedBytes(t, s.dataPath(key)))
	}
}
