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
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStressGetsAgainstEviction pins the package's core invariant under the
// race detector: with getters and evictors hammering the same keys, every
// GetFileTo succeeds with correct content — eviction may force refetches,
// but can never fail a caller or corrupt what one received. The store's min
// age must exceed a get's publish-to-link window (here: generous versus
// microseconds), which is the same sizing contract production relies on.
func TestStressGetsAgainstEviction(t *testing.T) {
	s := newTestStore(t, WithMinAge(200*time.Millisecond))
	const keys = 8
	const getters = 4
	duration := 700 * time.Millisecond
	if testing.Short() {
		duration = 200 * time.Millisecond
	}

	content := func(k int) string { return fmt.Sprintf("content-%d", k) }
	var fetches atomic.Int32
	fetcherFor := func(k int) FileFetcher {
		return func(ctx context.Context, dstPath string) error {
			fetches.Add(1)
			return os.WriteFile(dstPath, []byte(content(k)), 0o600)
		}
	}

	// Pre-populate every key and age the entries past minAge so evictors
	// have real work from the start; drop the seed links so entries are
	// evictable as unlinked.
	for k := range keys {
		seed := dstPath(t, s, fmt.Sprintf("seed-%d", k))
		if err := s.GetFileTo(context.Background(), URIKey("stress", fmt.Sprint(k)), seed, fetcherFor(k)); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(seed); err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-time.Hour)
		if err := os.Chtimes(s.entryDir(URIKey("stress", fmt.Sprint(k))), old, old); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	errCh := make(chan error, getters+2)
	var wg sync.WaitGroup

	for g := range getters {
		wg.Go(func() {
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := rng.Intn(keys)
				dst := dstPath(t, s, fmt.Sprintf("g%d-i%d", g, i))
				if err := s.GetFileTo(context.Background(), URIKey("stress", fmt.Sprint(k)), dst, fetcherFor(k)); err != nil {
					errCh <- fmt.Errorf("getter %d: %w", g, err)
					return
				}
				got, err := os.ReadFile(dst)
				if err != nil || string(got) != content(k) {
					errCh <- fmt.Errorf("getter %d key %d: content %q, err %v", g, k, got, err)
					return
				}
				// Dropping some links keeps a supply of unlinked entries so
				// evictors exercise the freeing path, not just pending.
				if i%2 == 0 {
					if err := os.Remove(dst); err != nil {
						errCh <- err
						return
					}
				}
			}
		})
	}
	for range 2 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := s.EvictUnused(context.Background(), evictAll, false); err != nil {
					errCh <- fmt.Errorf("evictor: %w", err)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	// The storm must not leave half-states behind: tmp/ is empty (every
	// fetch published or cleaned up) and any .rm-* residue is sweepable.
	tmpChildren, err := os.ReadDir(s.tmpDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(tmpChildren) != 0 {
		t.Errorf("tmp dir has %d children after storm", len(tmpChildren))
	}
	if _, err := s.SweepDebris(context.Background()); err != nil {
		t.Errorf("SweepDebris after storm: %v", err)
	}
	if total, err := s.TotalBytes(context.Background()); err != nil || total < 0 {
		t.Errorf("TotalBytes after storm: %d, %v", total, err)
	}
	t.Logf("storm: %d fetches across %d keys", fetches.Load(), keys)

	// Surviving consumer links must read back intact even if their entries
	// were evicted during the storm.
	consumer := filepath.Join(s.root, "..", "consumer")
	kept, err := filepath.Glob(filepath.Join(consumer, "g*-i*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range kept {
		if got, err := os.ReadFile(p); err != nil || len(got) == 0 {
			t.Errorf("surviving link %s unreadable: %q, %v", filepath.Base(p), got, err)
		}
	}
}
