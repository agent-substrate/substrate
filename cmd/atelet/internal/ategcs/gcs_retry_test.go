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

package ategcs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// A transient 429 on an upload must be retried, not surfaced: GCS sheds write
// bursts with rateLimitExceeded while it scales a bucket's key ranges, and the
// default client policy (RetryIdempotent) would fail the whole snapshot on the
// first one because plain object writes carry no precondition.
func TestPutObjectRetriesTransient429(t *testing.T) {
	var (
		mu       sync.Mutex
		attempts int
		srvURL   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/upload/"):
			mu.Lock()
			attempts++
			n := attempts
			mu.Unlock()
			if n == 1 {
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
	defer srv.Close()
	srvURL = srv.URL

	// The storage client routes everything, uploads included, at the emulator.
	t.Setenv("STORAGE_EMULATOR_HOST", srv.URL)
	ctx := context.Background()
	store, err := NewGCSClient(ctx)
	if err != nil {
		t.Fatalf("storage client: %v", err)
	}
	defer store.(*gcsClient).client.Close()

	if err := store.PutObject(ctx, "snapshots", "snap/pages.img.zstd", strings.NewReader("payload")); err != nil {
		t.Fatalf("PutObject after a transient 429: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("upload attempts = %d, want 2 (one 429, one retry)", attempts)
	}
}
