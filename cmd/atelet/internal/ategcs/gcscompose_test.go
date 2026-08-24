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
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// emulatorClient returns a storage client bound to a GCS emulator, or skips. The Go
// client honors STORAGE_EMULATOR_HOST, so:
//
//	docker run -d -p 4443:4443 fsouza/fake-gcs-server -scheme http -public-host localhost:4443
//	STORAGE_EMULATOR_HOST=localhost:4443 go test ./cmd/atelet/internal/ategcs -run Composite
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
	bucket := "ategcs-test"
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
