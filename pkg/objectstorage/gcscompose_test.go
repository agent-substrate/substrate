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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// emulatorClient returns a storage client bound to a GCS emulator, or skips. The Go
// client honors STORAGE_EMULATOR_HOST, so:
//
//	docker run -d -p 4443:4443 fsouza/fake-gcs-server -scheme http -public-host localhost:4443
//	STORAGE_EMULATOR_HOST=localhost:4443 go test ./pkg/objectstorage -run Composite
func emulatorClient(t *testing.T) (*storage.Client, string) {
	t.Helper()
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Skip("set STORAGE_EMULATOR_HOST to run against a GCS emulator")
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("storage client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	bucket := "objectstorage-test"
	if err := client.Bucket(bucket).Create(ctx, "test-project", nil); err != nil &&
		!strings.Contains(err.Error(), "Conflict") && !strings.Contains(err.Error(), "exist") {
		t.Logf("create bucket (ignored if it exists): %v", err)
	}
	return client, bucket
}

// body returns n bytes of deterministic pseudo-random data — incompressible and
// position-sensitive, so a part uploaded out of order or dropped shows up as a hash
// mismatch rather than passing by luck.
func body(n int) []byte {
	r := rand.New(rand.NewPCG(42, 7))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// A composed upload must be byte-identical to what a single request would have
// written: the parts are ranges of one stream, and the snapshot format on top of this
// has no idea it was split.
func TestPutObjectCompositeRoundTrip(t *testing.T) {
	client, bucket := emulatorClient(t)
	g := &gcsClient{client: client}
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "single request, under the threshold", size: 3 << 20},
		{name: "exactly the threshold", size: uploadCompositeMin},
		{name: "one part past the threshold", size: uploadCompositeMin + 1},
		{name: "several parts, last one partial", size: uploadCompositeMin + 2*uploadPartSize + 12345},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := body(tc.size)
			object := "snap/" + strings.ReplaceAll(tc.name, " ", "-")
			if err := g.PutObject(ctx, bucket, object, bytes.NewReader(want)); err != nil {
				t.Fatalf("PutObject: %v", err)
			}
			rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
			if err != nil {
				t.Fatalf("reading back: %v", err)
			}
			got, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatalf("reading back: %v", err)
			}
			if len(got) != len(want) {
				t.Fatalf("read back %d bytes, want %d", len(got), len(want))
			}
			if sha256.Sum256(got) != sha256.Sum256(want) {
				t.Fatalf("content differs after round trip (%d bytes)", len(got))
			}
		})
	}

	// Parts are scratch and must not outlive the upload: they would otherwise be
	// billed forever and confuse anything listing a snapshot's files.
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: "snap/"})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("listing: %v", err)
		}
		if strings.Contains(attrs.Name, ".part-") || strings.Contains(attrs.Name, ".compose-") {
			t.Errorf("upload left scratch object %q behind", attrs.Name)
		}
	}
}

// fakeObjects is an in-memory GCS that serves only what composeAll sends: compose
// (concatenation, capped at maxComposeSources like GCS) and delete.
type fakeObjects struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func (f *fakeObjects) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.EscapedPath(), "/storage/v1/b/test/o/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(rest, "/compose"):
		name, _ := url.PathUnescape(strings.TrimSuffix(rest, "/compose"))
		var req struct {
			SourceObjects []struct{ Name string } `json:"sourceObjects"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.SourceObjects) > maxComposeSources {
			http.Error(w, "bad compose", http.StatusBadRequest)
			return
		}
		var data []byte
		for _, src := range req.SourceObjects {
			b, ok := f.objs[src.Name]
			if !ok {
				http.NotFound(w, r)
				return
			}
			data = append(data, b...)
		}
		f.objs[name] = data
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"name": name, "bucket": "test"})
	case r.Method == http.MethodDelete:
		name, _ := url.PathUnescape(rest)
		if _, ok := f.objs[name]; !ok {
			http.NotFound(w, r)
			return
		}
		delete(f.objs, name)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

// Folding more than maxComposeSources parts goes through intermediate objects, and
// none may outlive the compose, including the ones the final compose consumes.
func TestComposeAllRemovesIntermediates(t *testing.T) {
	const partSize = 1 << 10
	want := body((maxComposeSources + 8) * partSize)
	fake := &fakeObjects{objs: map[string][]byte{}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	bkt := fakeGCSClient(t, srv).client.Bucket("test")

	var parts []*storage.ObjectHandle
	for i := 0; i < len(want); i += partSize {
		name := fmt.Sprintf("fold/obj.part-run-%04d", i/partSize)
		fake.objs[name] = want[i : i+partSize]
		parts = append(parts, bkt.Object(name))
	}

	if err := composeAll(context.Background(), bkt, "fold/obj", parts, "run"); err != nil {
		t.Fatalf("composeAll: %v", err)
	}

	if got := fake.objs["fold/obj"]; !bytes.Equal(got, want) {
		t.Errorf("composed %d bytes differ from the %d uploaded", len(got), len(want))
	}
	for name := range fake.objs {
		if strings.Contains(name, ".compose-") {
			t.Errorf("composeAll left intermediate %q behind", name)
		}
	}
}
