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
	anon, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("an anonymous client must build without credentials: %v", err)
	}
	defer anon.Close()

	g, ok := NewGCSClient(anon, option.WithoutAuthentication()).(*gcsClient)
	if !ok {
		t.Fatal("NewGCSClient did not return a *gcsClient")
	}

	pooled := g.uploadClient(ctx, 0)
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
		if c := g.uploadClient(ctx, i); c == nil || c == g.client {
			t.Errorf("uploadClient(%d) did not return a pooled connection", i)
		}
	}
}
