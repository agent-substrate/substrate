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
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// TestPooledClientsAreBuiltLikeTheClientTheyStandIn checks that pooled
// connections carry the wrapped client's options. The pool serves ranges past
// the first, so a mismatch fails mid-object rather than at open: a public
// bucket without Uniform Bucket Level Access rejects an authenticated token
// with HTTP 412.
//
// Credentials are removed so the two cases separate -- an anonymous client
// still builds and a default one cannot -- which makes distinct pooled
// connections the proof that the options were used.
func TestPooledClientsAreBuiltLikeTheClientTheyStandIn(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/credentials.json")
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")

	ctx := context.Background()
	store, err := NewGCSClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("an anonymous client must build without credentials: %v", err)
	}

	g, ok := store.(*gcsClient)
	if !ok {
		t.Fatal("NewGCSClient did not return a *gcsClient")
	}
	defer g.client.Close()

	pooled := g.uploadClient(0)
	if pooled == nil {
		t.Fatal("uploadClient returned nil")
	}
	if pooled == g.client {
		t.Fatal("uploadClient fell back to the wrapped client: the pool was not given the client's " +
			"options, so ranges past the first would go out authenticated")
	}
	if len(g.pool) != uploadPoolSize {
		t.Errorf("pool holds %d clients, want %d", len(g.pool), uploadPoolSize)
	}
	for i := range uploadPoolSize {
		if c := g.uploadClient(i); c == nil || c == g.client {
			t.Errorf("uploadClient(%d) did not return a pooled connection", i)
		}
	}
}

// recordingClient is a storage.Client whose every request is counted, standing
// in for one pooled connection.
func recordingClient(t *testing.T, hits *atomic.Int32) *storage.Client {
	t.Helper()
	client, err := storage.NewClient(t.Context(), option.WithHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		hits.Add(1)
		if req.Method == http.MethodGet {
			// A whole small object in one range: the reader sees its size and
			// stays a single request.
			return &http.Response{
				StatusCode:    http.StatusPartialContent,
				Header:        http.Header{"Content-Range": {"bytes 0-6/7"}, "Content-Type": {"application/octet-stream"}},
				ContentLength: 7,
				Body:          io.NopCloser(strings.NewReader("payload")),
			}, nil
		}
		_, _ = io.Copy(io.Discard, req.Body)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"name":"object","bucket":"snapshots"}`)),
		}, nil
	})}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// TestSingleRequestTransfersSpreadOverThePool is the point of the pool for a node
// turning over actors: a compressed snapshot is usually below the part and range
// thresholds, so it is one request, and without this every snapshot the node moves
// at once would multiplex onto the wrapped client's one connection.
func TestSingleRequestTransfersSpreadOverThePool(t *testing.T) {
	var wrapped atomic.Int32
	g := &gcsClient{client: recordingClient(t, &wrapped)}
	hits := make([]atomic.Int32, 3)
	for i := range hits {
		g.pool = append(g.pool, recordingClient(t, &hits[i]))
	}

	ctx := t.Context()
	for i := range 2 * len(hits) {
		if i%2 == 0 {
			rc, err := g.GetObject(ctx, "snapshots", "object")
			if err != nil {
				t.Fatalf("GetObject %d: %v", i, err)
			}
			if _, err := io.Copy(io.Discard, rc); err != nil {
				t.Fatalf("reading object %d: %v", i, err)
			}
			rc.Close()
		} else if err := g.PutObject(ctx, "snapshots", "object", strings.NewReader("payload")); err != nil {
			t.Fatalf("PutObject %d: %v", i, err)
		}
	}

	if n := wrapped.Load(); n != 0 {
		t.Errorf("the wrapped client served %d requests, want 0: small transfers bypassed the pool", n)
	}
	for i := range hits {
		if n := hits[i].Load(); n != 2 {
			t.Errorf("pooled client %d served %d requests, want 2: transfers were not spread round-robin", i, n)
		}
	}
}
