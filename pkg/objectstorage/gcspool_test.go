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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// newTestGCSClient builds a gcsClient from opts and closes all of its clients when
// the test ends.
func newTestGCSClient(t *testing.T, opts ...option.ClientOption) *gcsClient {
	t.Helper()
	store, err := NewGCSClient(context.Background(), opts...)
	if err != nil {
		t.Fatalf("NewGCSClient: %v", err)
	}
	g := store.(*gcsClient)
	t.Cleanup(func() {
		g.controlClient.Close()
		for _, c := range g.pool {
			c.Close()
		}
	})
	return g
}

// TestPooledClientsAreBuiltLikeTheControlClient checks that pooled connections
// carry the control client's options. A mismatch fails mid-object rather than at
// open: a public bucket without Uniform Bucket Level Access rejects an
// authenticated token with HTTP 412.
//
// Credentials are removed so the two cases separate -- an anonymous client
// still builds and a default one cannot -- which makes a full pool the proof that
// the options were used.
func TestPooledClientsAreBuiltLikeTheControlClient(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/credentials.json")
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")

	g := newTestGCSClient(t, option.WithoutAuthentication())
	if len(g.pool) != poolSize {
		t.Fatalf("pool holds %d clients, want %d", len(g.pool), poolSize)
	}
	seen := map[*storage.Client]bool{}
	for range poolSize {
		c := g.poolClient()
		if c == nil || c == g.controlClient {
			t.Fatal("poolClient did not return a pooled client")
		}
		seen[c] = true
	}
	if len(seen) != poolSize {
		t.Errorf("%d consecutive requests used %d distinct clients, want %d", poolSize, len(seen), poolSize)
	}
}

// TestTransfersSpreadAcrossConnections checks at the ObjectStorage boundary that
// small objects, which go up and come down as single requests, still use the whole
// pool: were every object to share one client, concurrent snapshots on a node would
// split one connection and idle the rest. The objects go one at a time so each
// client reuses its own kept-alive connection, which makes distinct connections
// count distinct clients.
func TestTransfersSpreadAcrossConnections(t *testing.T) {
	const payload = "payload"
	var (
		mu     sync.Mutex
		conns  map[string]bool
		srvURL string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		conns[r.RemoteAddr] = true
		mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		switch {
		case strings.Contains(r.URL.Path, "/upload/"):
			if r.URL.Query().Get("uploadType") == "resumable" {
				w.Header().Set("Location", srvURL+"/upload-session")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"snap/object","bucket":"snapshots"}`)
		case r.URL.Path == "/upload-session":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"snap/object","bucket":"snapshots"}`)
		case r.Method == http.MethodGet:
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	srvURL = srv.URL
	t.Setenv("STORAGE_EMULATOR_HOST", srv.URL)
	g := newTestGCSClient(t)
	ctx := context.Background()

	reset := func() {
		mu.Lock()
		defer mu.Unlock()
		conns = map[string]bool{}
	}
	distinct := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(conns)
	}

	reset()
	for i := range 2 * poolSize {
		if err := g.PutObject(ctx, "snapshots", fmt.Sprintf("snap/object-%d", i), strings.NewReader(payload)); err != nil {
			t.Fatalf("PutObject: %v", err)
		}
	}
	if got := distinct(); got != poolSize {
		t.Errorf("%d small uploads used %d connections, want %d", 2*poolSize, got, poolSize)
	}

	reset()
	for i := range 2 * poolSize {
		rc, err := g.GetObject(ctx, "snapshots", fmt.Sprintf("snap/object-%d", i))
		if err != nil {
			t.Fatalf("GetObject: %v", err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil || string(got) != payload {
			t.Fatalf("GetObject read %q, %v; want %q", got, err, payload)
		}
	}
	if got := distinct(); got != poolSize {
		t.Errorf("%d small downloads used %d connections, want %d", 2*poolSize, got, poolSize)
	}
}
