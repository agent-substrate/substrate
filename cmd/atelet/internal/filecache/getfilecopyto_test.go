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
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
)

func TestGetFileCopyToMissThenHit(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("golden", "memory")
	fetch, calls := countingFetcher("golden memory")
	ctx := context.Background()

	first := dstPath(t, s, "first")
	second := dstPath(t, s, "second")
	for _, dst := range []string{first, second} {
		if err := s.GetFileCopyTo(ctx, key, dst, fetch); err != nil {
			t.Fatalf("GetFileCopyTo(%s): %v", dst, err)
		}
		if got, err := os.ReadFile(dst); err != nil || string(got) != "golden memory" {
			t.Fatalf("copy %s = %q, %v", dst, got, err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("two copies fetched %d times, want 1", n)
	}

	// Private inodes: the copies share nothing with each other or the cache,
	// and the caller may write them.
	fi1, err1 := os.Stat(first)
	fi2, err2 := os.Stat(second)
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if os.SameFile(fi1, fi2) {
		t.Error("copies share an inode; each caller must own its own")
	}
	if st, ok := fi1.Sys().(*syscall.Stat_t); ok && st.Nlink != 1 {
		t.Errorf("copy has %d links; a copy must not be linked to the cache", st.Nlink)
	}
	if fi1.Mode().Perm()&0o200 == 0 {
		t.Errorf("copy mode %v is not owner-writable; copy mode exists for mutating consumers", fi1.Mode())
	}
}

func TestGetFileCopyToMutationIsolation(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("golden", "config")
	fetch, calls := countingFetcher("original")
	ctx := context.Background()

	first := dstPath(t, s, "first")
	if err := s.GetFileCopyTo(ctx, key, first, fetch); err != nil {
		t.Fatal(err)
	}
	// The consumer rewrites its staged file in place — the whole reason copy
	// mode exists (ateom-microvm does this to config.json).
	if err := os.WriteFile(first, []byte("mutated by consumer"), 0o600); err != nil {
		t.Fatalf("consumer write to its copy: %v", err)
	}

	second := dstPath(t, s, "second")
	if err := s.GetFileCopyTo(ctx, key, second, fetch); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(second); err != nil || string(got) != "original" {
		t.Errorf("copy after consumer mutation = %q, %v; the cache copy was poisoned", got, err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("fetches = %d, want 1 (mutation must not invalidate the entry)", n)
	}
}

func TestGetFileCopyToConcurrentSingleFlight(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("golden", "memory")
	fetch, calls := countingFetcher("golden memory")

	const copiers = 8
	errs := make([]error, copiers)
	var wg sync.WaitGroup
	for i := range copiers {
		dst := dstPath(t, s, fmt.Sprintf("copy-%d", i))
		wg.Go(func() {
			errs[i] = s.GetFileCopyTo(context.Background(), key, dst, fetch)
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("copier %d: %v", i, err)
		}
		got, err := os.ReadFile(dstPath(t, s, fmt.Sprintf("copy-%d", i)))
		if err != nil || string(got) != "golden memory" {
			t.Fatalf("copier %d content %q, %v", i, got, err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("%d concurrent copiers fetched %d times, want 1", copiers, n)
	}
}

func TestGetFileCopyToPreservesSparseness(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("golden", "sparse")
	const size = 16 << 20
	sparseFetcher := func(ctx context.Context, dstPath string) error {
		f, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		if err := f.Truncate(size); err != nil {
			return err
		}
		if _, err := f.WriteAt(make([]byte, 4<<10), 1<<20); err != nil {
			return err
		}
		return f.Close()
	}

	dst := dstPath(t, s, "sparse-copy")
	if err := s.GetFileCopyTo(context.Background(), key, dst, sparseFetcher); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != size {
		t.Errorf("copy logical size %d, want %d", fi.Size(), int64(size))
	}

	srcAlloc := allocatedBytes(t, s.dataPath(key))
	if srcAlloc >= size/2 {
		t.Skipf("cached entry did not end up sparse (%d of %d bytes allocated); this filesystem cannot report holes", srcAlloc, int64(size))
	}
	if dstAlloc := allocatedBytes(t, dst); dstAlloc >= size/2 {
		t.Errorf("copy allocated %d of %d bytes: holes were filled in", dstAlloc, int64(size))
	}
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

func TestGetFileCopyToRejectsBadArguments(t *testing.T) {
	s := newTestStore(t)
	fetch, _ := countingFetcher("x")
	ctx := context.Background()

	if err := s.GetFileCopyTo(ctx, Key{}, dstPath(t, s, "zero"), fetch); err == nil {
		t.Error("zero key accepted")
	}
	if err := s.GetFileCopyTo(ctx, URIKey("k"), "relative/path", fetch); err == nil {
		t.Error("relative destination accepted")
	}
	if err := s.GetFileCopyTo(ctx, URIKey("k"), dstPath(t, s, "nilfetch"), nil); err == nil {
		t.Error("nil fetcher accepted")
	}

	exists := dstPath(t, s, "exists")
	if err := os.WriteFile(exists, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.GetFileCopyTo(ctx, URIKey("k"), exists, fetch); err == nil {
		t.Error("existing destination accepted")
	}
	if got, _ := os.ReadFile(exists); string(got) != "occupied" {
		t.Errorf("existing destination was overwritten: %q", got)
	}
}
