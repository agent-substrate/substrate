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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingFetcher returns a FileFetcher writing content, and a counter of
// how many times it ran.
func countingFetcher(content string) (FileFetcher, *atomic.Int32) {
	var calls atomic.Int32
	return func(ctx context.Context, dstPath string) error {
		calls.Add(1)
		return os.WriteFile(dstPath, []byte(content), 0o600)
	}, &calls
}

// dstPath returns a fresh destination path (which must not exist yet) in a
// per-test consumer dir on the same filesystem as the store.
func dstPath(t *testing.T, s *Store, name string) string {
	t.Helper()
	dir := filepath.Join(s.root, "..", "consumer")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, name)
}

func TestGetFileToMissThenHit(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("gs://bucket/golden", "mem.img")
	fetch, calls := countingFetcher("golden bytes")

	dst1 := dstPath(t, s, "d1")
	if err := s.GetFileTo(context.Background(), key, dst1, fetch); err != nil {
		t.Fatalf("GetFileTo (miss): %v", err)
	}
	got, err := os.ReadFile(dst1)
	if err != nil || string(got) != "golden bytes" {
		t.Fatalf("dst content = %q, %v; want %q", got, err, "golden bytes")
	}
	fi, err := os.Stat(dst1)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o444 {
		t.Errorf("dst mode = %v, want 0444", fi.Mode().Perm())
	}

	dst2 := dstPath(t, s, "d2")
	if err := s.GetFileTo(context.Background(), key, dst2, fetch); err != nil {
		t.Fatalf("GetFileTo (hit): %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("fetch ran %d times, want 1", calls.Load())
	}

	// Both destinations and the cache copy share one inode.
	fi2, err := os.Stat(dst2)
	if err != nil {
		t.Fatal(err)
	}
	cfi, err := os.Stat(s.dataPath(key))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(fi, fi2) || !os.SameFile(fi, cfi) {
		t.Error("dst1, dst2, and cache copy are not one inode")
	}

	// Nothing left in flight.
	tmpChildren, err := os.ReadDir(s.tmpDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(tmpChildren) != 0 {
		t.Errorf("tmp dir has %d leftover children", len(tmpChildren))
	}
}

func TestGetFileToConcurrentCallersShareOneFetch(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("gs://bucket/golden", "mem.img")

	var calls atomic.Int32
	release := make(chan struct{})
	fetch := func(ctx context.Context, dstPath string) error {
		calls.Add(1)
		<-release // hold every caller in one flight
		return os.WriteFile(dstPath, []byte("x"), 0o600)
	}

	const n = 16
	errs := make([]error, n)
	var started, done sync.WaitGroup
	for i := range n {
		started.Add(1)
		done.Go(func() {
			started.Done()
			errs[i] = s.GetFileTo(context.Background(), key, dstPath(t, s, fmt.Sprintf("d%d", i)), fetch)
		})
	}
	started.Wait()
	close(release)
	done.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: %v", i, err)
		}
	}
	if calls.Load() != 1 {
		t.Errorf("fetch ran %d times, want 1", calls.Load())
	}
}

func TestGetFileToCanceledWaiterDoesNotAbortFlight(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("gs://bucket/golden", "mem.img")

	var calls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	fetch := func(ctx context.Context, dstPath string) error {
		calls.Add(1)
		close(entered)
		select {
		case <-release:
		case <-ctx.Done(): // must NOT fire on the caller's cancel
			return ctx.Err()
		}
		return os.WriteFile(dstPath, []byte("x"), 0o600)
	}

	ctx, cancel := context.WithCancel(context.Background())
	callerErr := make(chan error, 1)
	go func() {
		callerErr <- s.GetFileTo(ctx, key, dstPath(t, s, "canceled"), fetch)
	}()
	<-entered
	cancel()
	if err := <-callerErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller returned %v, want context.Canceled", err)
	}

	// The flight is still running detached; releasing it publishes the
	// entry, and a later caller hits the cache with no second fetch.
	close(release)
	if err := s.GetFileTo(context.Background(), key, dstPath(t, s, "later"), fetch); err != nil {
		t.Fatalf("GetFileTo after canceled waiter: %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("fetch ran %d times, want 1", calls.Load())
	}
}

func TestGetFileToFetchErrorReachesAllWaitersThenRetries(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("gs://bucket/golden", "mem.img")
	fetchErr := errors.New("bucket unreachable")

	var calls atomic.Int32
	release := make(chan struct{})
	failing := func(ctx context.Context, dstPath string) error {
		calls.Add(1)
		<-release
		return fetchErr
	}

	const n = 4
	errs := make([]error, n)
	var started, done sync.WaitGroup
	for i := range n {
		started.Add(1)
		done.Go(func() {
			started.Done()
			errs[i] = s.GetFileTo(context.Background(), key, dstPath(t, s, fmt.Sprintf("f%d", i)), failing)
		})
	}
	started.Wait()
	close(release)
	done.Wait()

	for i, err := range errs {
		if !errors.Is(err, fetchErr) {
			t.Errorf("caller %d: %v, want the fetch error", i, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("failing fetch ran %d times, want 1", calls.Load())
	}

	// No debris and no published entry after the failure.
	tmpChildren, err := os.ReadDir(s.tmpDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(tmpChildren) != 0 {
		t.Errorf("tmp dir has %d children after failed fetch", len(tmpChildren))
	}
	if _, err := os.Stat(s.entryDir(key)); !os.IsNotExist(err) {
		t.Errorf("entry published despite failed fetch: err=%v", err)
	}

	// No negative caching: the next call fetches fresh and succeeds.
	ok, okCalls := countingFetcher("recovered")
	if err := s.GetFileTo(context.Background(), key, dstPath(t, s, "retry"), ok); err != nil {
		t.Fatalf("GetFileTo (retry): %v", err)
	}
	if okCalls.Load() != 1 {
		t.Errorf("retry fetch ran %d times, want 1", okCalls.Load())
	}
}

func TestGetFileToRejectsExistingDst(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("gs://bucket/golden", "mem.img")
	fetch, _ := countingFetcher("x")

	dst := dstPath(t, s, "occupied")
	if err := os.WriteFile(dst, []byte("previous"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.GetFileTo(context.Background(), key, dst, fetch); err == nil {
		t.Fatal("GetFileTo to existing dst succeeded, want error")
	}
	got, err := os.ReadFile(dst)
	if err != nil || string(got) != "previous" {
		t.Errorf("existing dst clobbered: %q, %v", got, err)
	}
}

func TestGetFileToRejectsBadArguments(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("gs://bucket/golden", "mem.img")
	fetch, calls := countingFetcher("x")

	if err := s.GetFileTo(context.Background(), Key{}, dstPath(t, s, "zero"), fetch); err == nil {
		t.Error("GetFileTo with zero key succeeded, want error")
	}
	if err := s.GetFileTo(context.Background(), key, "", fetch); err == nil {
		t.Error("GetFileTo with empty dst succeeded, want error")
	}
	if err := s.GetFileTo(context.Background(), key, "relative/dst", fetch); err == nil {
		t.Error("GetFileTo with relative dst succeeded, want error")
	}
	if err := s.GetFileTo(context.Background(), key, dstPath(t, s, "nilfetch"), nil); err == nil {
		t.Error("GetFileTo with nil fetcher succeeded, want error")
	}
	if calls.Load() != 0 {
		t.Errorf("fetch ran %d times for rejected arguments, want 0", calls.Load())
	}
}

func TestGetFileToFetcherProducingNoFileFails(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("gs://bucket/golden", "mem.img")
	noop := func(ctx context.Context, dstPath string) error { return nil }

	if err := s.GetFileTo(context.Background(), key, dstPath(t, s, "empty"), noop); err == nil {
		t.Fatal("GetFileTo with file-less fetcher succeeded, want error")
	}
	if _, err := os.Stat(s.entryDir(key)); !os.IsNotExist(err) {
		t.Errorf("entry published despite file-less fetcher: err=%v", err)
	}
}

func TestGetFileToHitTouchesLastUse(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("gs://bucket/golden", "mem.img")
	fetch, _ := countingFetcher("x")

	if err := s.GetFileTo(context.Background(), key, dstPath(t, s, "first"), fetch); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-time.Hour)
	if err := os.Chtimes(s.entryDir(key), stale, stale); err != nil {
		t.Fatal(err)
	}

	if err := s.GetFileTo(context.Background(), key, dstPath(t, s, "second"), fetch); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(s.entryDir(key))
	if err != nil {
		t.Fatal(err)
	}
	if !fi.ModTime().After(stale.Add(time.Minute)) {
		t.Errorf("entry mtime %v not refreshed by hit (stale mark %v)", fi.ModTime(), stale)
	}
}

func TestGetFileToPublishedCopyIsWriteProtected(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("gs://bucket/golden", "mem.img")
	fetch, _ := countingFetcher("precious")

	dst := dstPath(t, s, "d")
	if err := s.GetFileTo(context.Background(), key, dst, fetch); err != nil {
		t.Fatal(err)
	}
	// The mutation tripwire: writing through the consumer's link must fail,
	// not silently poison the shared copy.
	if err := os.WriteFile(dst, []byte("mutated"), 0o444); err == nil {
		t.Error("write through consumer link succeeded, want permission error")
	}
}
