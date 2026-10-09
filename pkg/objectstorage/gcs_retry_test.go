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

package objectstorage

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
)

// serveGCS points the storage client at a fake GCS that serves uploads,
// composes, copies and deletes. It answers the first fail[kind] requests of
// each kind ("upload", "compose", "copy" or "delete") with 429 and serves the
// rest. It returns a func that reports how many requests of a kind it has seen.
func serveGCS(t *testing.T, fail map[string]int) (attempts func(kind string) int) {
	t.Helper()
	var (
		mu     sync.Mutex
		seen   = map[string]int{}
		srvURL string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var kind string
		switch {
		case strings.Contains(r.URL.Path, "/upload/"):
			kind = "upload"
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/compose"):
			kind = "compose"
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/rewriteTo/"):
			kind = "copy"
		case r.Method == http.MethodDelete:
			kind = "delete"
		}
		if kind != "" {
			mu.Lock()
			seen[kind]++
			throttled := seen[kind] <= fail[kind]
			mu.Unlock()
			if throttled {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Fprint(w, `{"error":{"code":429,"message":"Your request distribution is too uneven across the key-ranges in your bucket.","errors":[{"reason":"rateLimitExceeded"}]}}`)
				return
			}
		}
		_, _ = io.Copy(io.Discard, r.Body)
		switch {
		case kind == "delete":
			w.WriteHeader(http.StatusNoContent)
		case kind == "copy":
			// The whole copy in one rewrite call.
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"done":true,"resource":{"name":"snap/pages.img.zstd","bucket":"snapshots"}}`)
		case kind == "upload" && r.URL.Query().Get("uploadType") == "resumable":
			// Resumable initiation: hand back a session for the chunk PUTs.
			w.Header().Set("Location", srvURL+"/upload-session")
		case kind != "", r.URL.Path == "/upload-session":
			// A single-shot upload, a compose or a session's chunk: each is
			// done in one request.
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"snap/pages.img.zstd","bucket":"snapshots"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	srvURL = srv.URL

	// The storage client routes everything, uploads included, at the emulator.
	t.Setenv("STORAGE_EMULATOR_HOST", srv.URL)
	return func(kind string) int {
		mu.Lock()
		defer mu.Unlock()
		return seen[kind]
	}
}

// newTestGCSClient returns a client from NewGCSClient, closed when t ends.
func newTestGCSClient(t *testing.T) ObjectStorage {
	t.Helper()
	store, err := NewGCSClient(context.Background())
	if err != nil {
		t.Fatalf("storage client: %v", err)
	}
	t.Cleanup(func() { store.(*gcsClient).client.Close() })
	return store
}

// fastRetries makes clients built after it back off 1ms between attempts
// until t ends, so a test can drive many retries quickly.
func fastRetries(t *testing.T) {
	old := retryBackoff
	retryBackoff = gax.Backoff{Initial: time.Millisecond, Max: time.Millisecond}
	t.Cleanup(func() { retryBackoff = old })
}

// A transient 429 on an upload must be retried, not surfaced: GCS sheds write
// bursts with rateLimitExceeded while it scales a bucket's key ranges, and the
// default client policy (RetryIdempotent) would fail the whole snapshot on the
// first one because plain object writes carry no precondition.
func TestPutObjectRetriesTransient429(t *testing.T) {
	tests := []struct {
		name string
		fail int // upload requests answered with 429 before one succeeds
	}{
		{name: "one 429", fail: 1},
		{name: "six 429s", fail: 6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fastRetries(t)
			attempts := serveGCS(t, map[string]int{"upload": tc.fail})
			store := newTestGCSClient(t)
			// Bounded, so an upload that never stops retrying fails the test
			// instead of hanging it.
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()

			if err := store.PutObject(ctx, "snapshots", "snap/pages.img.zstd", strings.NewReader("payload")); err != nil {
				t.Fatalf("PutObject after %d transient 429s: %v", tc.fail, err)
			}
			if got, want := attempts("upload"), tc.fail+1; got != want {
				t.Fatalf("upload attempts = %d, want %d (%d 429s, one success)", got, want, tc.fail)
			}
		})
	}
}

// An upload that only ever gets 429s keeps retrying until its context ends,
// as setRetry's comment says, and then fails.
func TestPutObjectRetriesUntilDeadline(t *testing.T) {
	serveGCS(t, map[string]int{"upload": math.MaxInt})
	store := newTestGCSClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := store.PutObject(ctx, "snapshots", "snap/pages.img.zstd", strings.NewReader("payload"))
	if err == nil || ctx.Err() == nil {
		t.Fatalf("PutObject() = %v before its context ended, want it to retry until then", err)
	}
}

// Compose has no attempt cap either, so it gets through six 429s.
func TestComposeRetriesPast5Attempts(t *testing.T) {
	fastRetries(t)
	attempts := serveGCS(t, map[string]int{"compose": 6})
	bkt := newTestGCSClient(t).(*gcsClient).client.Bucket("snapshots")
	parts := []*storage.ObjectHandle{bkt.Object("snap/part-0"), bkt.Object("snap/part-1")}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	if err := composeAll(ctx, bkt, "snap/pages.img.zstd", parts, "run"); err != nil {
		t.Fatalf("composeAll() after six 429s: %v", err)
	}
	if got := attempts("compose"); got != 7 {
		t.Errorf("compose attempts = %d, want 7", got)
	}
}

// Part deletes that keep failing stop at cleanupTimeout, even though the
// upload's own context is done by the time its cleanup runs.
func TestDeletePartsStopsAtCleanupTimeout(t *testing.T) {
	fastRetries(t)
	old := cleanupTimeout
	cleanupTimeout = 200 * time.Millisecond
	t.Cleanup(func() { cleanupTimeout = old })
	serveGCS(t, map[string]int{"delete": math.MaxInt})
	bkt := newTestGCSClient(t).(*gcsClient).client.Bucket("snapshots")
	parts := []*storage.ObjectHandle{bkt.Object("snap/part-0"), bkt.Object("snap/part-1")}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		deleteParts(ctx, parts)
	}()
	select {
	case <-done:
		// Returning sooner would mean the deletes ran on the upload's context.
		if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
			t.Fatalf("deleteParts() returned after %v, want it to retry for cleanupTimeout", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("deleteParts() still running after 10s, want it to stop at cleanupTimeout")
	}
}

// A part cleanup that fails does not fail an upload whose object is already
// in place.
func TestPutCompositeSucceedsWhenCleanupFails(t *testing.T) {
	fastRetries(t)
	old := cleanupTimeout
	cleanupTimeout = 200 * time.Millisecond
	t.Cleanup(func() { cleanupTimeout = old })
	attempts := serveGCS(t, map[string]int{"delete": math.MaxInt})
	g := newTestGCSClient(t).(*gcsClient)
	t.Cleanup(func() {
		for _, c := range g.pool {
			c.Close()
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	// One small part: composeAll copies it into place, then every delete of
	// it fails.
	if err := g.putComposite(ctx, "snapshots", "snap/pages.img.zstd", strings.NewReader("payload"), strings.NewReader("")); err != nil {
		t.Fatalf("putComposite() = %v, want nil although its part cleanup failed", err)
	}
	if attempts("copy") == 0 || attempts("delete") == 0 {
		t.Fatalf("copies = %d, deletes = %d, want both to have run", attempts("copy"), attempts("delete"))
	}
}

// deleteParts deletes parts in parallel, at most cleanupConcurrency at once.
// One at a time, a large upload's parts would leave little of cleanupTimeout
// for retries.
func TestDeletePartsRunsInParallel(t *testing.T) {
	var (
		mu                        sync.Mutex
		inFlight, peak, deletions int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		inFlight++
		peak = max(peak, inFlight)
		mu.Unlock()
		// Long enough for parallel deletes to overlap.
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inFlight--
		deletions++
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("STORAGE_EMULATOR_HOST", srv.URL)
	bkt := newTestGCSClient(t).(*gcsClient).client.Bucket("snapshots")
	var parts []*storage.ObjectHandle
	for i := range 4 * cleanupConcurrency {
		parts = append(parts, bkt.Object(fmt.Sprintf("snap/part-%02d", i)))
	}

	deleteParts(t.Context(), parts)

	mu.Lock()
	defer mu.Unlock()
	if deletions != len(parts) {
		t.Errorf("deleteParts() made %d deletes, want %d", deletions, len(parts))
	}
	if peak < 2 || peak > cleanupConcurrency {
		t.Errorf("deleteParts() ran %d deletes at once, want 2 to %d", peak, cleanupConcurrency)
	}
}
