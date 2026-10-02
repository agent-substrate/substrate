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
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// serveUploads points the storage client at a fake GCS that answers the first
// fail upload requests with 429 and serves the rest. It returns a func that
// reports how many upload requests the fake has seen.
func serveUploads(t *testing.T, fail int) (attempts func() int) {
	t.Helper()
	var (
		mu     sync.Mutex
		seen   int
		srvURL string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/upload/"):
			mu.Lock()
			seen++
			throttled := seen <= fail
			mu.Unlock()
			if throttled {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Fprint(w, `{"error":{"code":429,"message":"Your request distribution is too uneven across the key-ranges in your bucket.","errors":[{"reason":"rateLimitExceeded"}]}}`)
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
			if r.URL.Query().Get("uploadType") == "resumable" {
				// Resumable initiation: hand back a session for the chunk PUTs.
				w.Header().Set("Location", srvURL+"/upload-session")
				return
			}
			// Single-shot multipart upload: done in one request.
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"snap/pages.img.zstd","bucket":"snapshots"}`)
		case r.URL.Path == "/upload-session":
			_, _ = io.Copy(io.Discard, r.Body)
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
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		return seen
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
		// This pins storage SDK behavior, not ours: the SDK does not pass
		// WithMaxAttempts to its upload code, so a single-request upload
		// retries until ctx is done. If an SDK upgrade fails this case,
		// uploads now stop at 5 attempts; update setRetry's comment.
		{name: "more 429s than MaxAttempts", fail: 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attempts := serveUploads(t, tc.fail)
			store := newTestGCSClient(t)
			// Bounded, so an upload that never stops retrying fails the test
			// instead of hanging it.
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()

			if err := store.PutObject(ctx, "snapshots", "snap/pages.img.zstd", strings.NewReader("payload")); err != nil {
				t.Fatalf("PutObject after %d transient 429s: %v", tc.fail, err)
			}
			if got, want := attempts(), tc.fail+1; got != want {
				t.Fatalf("upload attempts = %d, want %d (%d 429s, one success)", got, want, tc.fail)
			}
		})
	}
}

// An upload that only ever gets 429s keeps retrying until its context ends,
// as setRetry's comment says, and then fails with the context's error. Like
// the case above, this pins storage SDK behavior.
func TestPutObjectRetriesUntilDeadline(t *testing.T) {
	serveUploads(t, math.MaxInt)
	store := newTestGCSClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := store.PutObject(ctx, "snapshots", "snap/pages.img.zstd", strings.NewReader("payload"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PutObject() = %v, want context.DeadlineExceeded", err)
	}
}
