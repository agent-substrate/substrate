// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectstorage

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestChunkFor(t *testing.T) {
	cases := []struct {
		name string
		in   int64
		want int
	}{
		{"empty", 0, parZstdMinChunk},
		{"tiny metadata file", 36, parZstdMinChunk},
		{"just under the floor", parZstdMinChunk - parZstdChunkSlack - 1, parZstdMinChunk},
		{"above the floor keeps slack", 1 << 20, (1 << 20) + parZstdChunkSlack},
		{"just under a full chunk", parZstdChunk - parZstdChunkSlack - 1, parZstdChunk - 1},
		{"exactly a full chunk", parZstdChunk, parZstdChunk},
		{"larger than a full chunk", 64 << 20, parZstdChunk},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := chunkFor(tc.in); got != tc.want {
				t.Errorf("chunkFor(%d) = %d, want %d", tc.in, got, tc.want)
			}
			if got := chunkFor(tc.in); got > parZstdChunk {
				t.Errorf("chunkFor(%d) = %d exceeds parZstdChunk %d", tc.in, got, parZstdChunk)
			}
		})
	}
}

// A small input must not reserve a full-size frame buffer. Before #2553 the pool
// was filled eagerly with workers*parZstdQueue buffers of parZstdChunk each, so a
// 36-byte file reserved 16 MiB.
func TestSmallInputDoesNotReserveFullChunk(t *testing.T) {
	p := newParZstd(io.Discard, 1, chunkFor(36))
	defer p.Close()
	if got := cap(p.buf); got != parZstdMinChunk {
		t.Errorf("first buffer cap = %d, want %d", got, parZstdMinChunk)
	}
	// Only the one buffer in hand has been created; the rest of the pool's
	// allowance is still unspent.
	if got, want := len(p.quota), 1*parZstdQueue-1; got != want {
		t.Errorf("unspent buffer allowance = %d, want %d", got, want)
	}
}

// The pool still refuses to grow past its cap, so the lazy fill did not turn
// the bound into an unbounded allocator. Exercising getBuf() rather than
// counting tokens: a test that only reads len(p.quota) still passes if getBuf()
// allocates past an exhausted quota, which is the regression worth guarding.
func TestBufferPoolStillCapped(t *testing.T) {
	p := newParZstd(io.Discard, 2, parZstdMinChunk)
	defer p.Close()

	// newParZstd clamps workers to GOMAXPROCS, so derive the allowance from the
	// worker count it actually settled on rather than the number requested.
	allowance := p.workers * parZstdQueue
	held := make([][]byte, 0, allowance)
	for len(held)+1 < allowance { // p.buf already holds one of them
		held = append(held, p.getBuf())
	}

	// Every buffer the pool may create is now out, so the next request has to
	// wait for one to come back rather than making another.
	waiting := make(chan []byte, 1)
	go func() { waiting <- p.getBuf() }()
	select {
	case <-waiting:
		t.Fatal("getBuf() handed out a buffer past the pool's cap")
	case <-time.After(100 * time.Millisecond):
	}

	// Returning one releases the waiter, and it gets that buffer back.
	returned := held[0]
	p.free <- returned[:0]
	select {
	case got := <-waiting:
		if cap(got) != cap(returned) {
			t.Errorf("resumed getBuf() returned a buffer of cap %d, want the recycled %d", cap(got), cap(returned))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("getBuf() never resumed after a buffer was returned")
	}
}

// Concurrent compression of tiny files must scale with the input, not with
// parZstdChunk. The reporter measured 397.8 MiB for 24 files of 36 bytes.
func TestConcurrentSmallFileAllocation(t *testing.T) {
	const files = 24
	dir := t.TempDir()
	paths := make([]string, files)
	for i := range paths {
		p := filepath.Join(dir, "f"+strconv.Itoa(i))
		if err := os.WriteFile(p, make([]byte, 36), 0o600); err != nil {
			t.Fatal(err)
		}
		paths[i] = p
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	var wg sync.WaitGroup
	for _, p := range paths {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			f, err := os.Open(p)
			if err != nil {
				t.Error(err)
				return
			}
			defer f.Close()
			if _, err := writeContent(io.Discard, f); err != nil {
				t.Error(err)
			}
		}(p)
	}
	wg.Wait()
	runtime.ReadMemStats(&after)

	got := after.TotalAlloc - before.TotalAlloc
	// Generously above the ~17 MiB observed after the fix and far below the
	// ~398 MiB before it, so this fails on a regression without being brittle.
	const limit = 96 << 20
	t.Logf("%d concurrent 36-byte files allocated %.1f MiB", files, float64(got)/(1<<20))
	if got > limit {
		t.Errorf("allocated %d bytes for %d tiny files, want under %d", got, files, limit)
	}
}
