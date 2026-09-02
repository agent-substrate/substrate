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

package objectstore_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/google/go-cmp/cmp"
)

// emulatorStore returns a Store bound to a GCS emulator, or skips. The Go
// client honors STORAGE_EMULATOR_HOST, so:
//
//	docker run -d -p 4443:4443 fsouza/fake-gcs-server -scheme http -public-host localhost:4443
//	STORAGE_EMULATOR_HOST=localhost:4443 go test ./internal/objectstore -run GCS
func emulatorStore(t *testing.T) (objectstore.Store, *storage.Client, string) {
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
	bucket := "objectstore-test"
	if err := client.Bucket(bucket).Create(ctx, "test-project", nil); err != nil &&
		!strings.Contains(err.Error(), "Conflict") && !strings.Contains(err.Error(), "exist") {
		t.Logf("create bucket (ignored if it exists): %v", err)
	}
	return objectstore.NewGCS(client), client, bucket
}

func TestGCSStore(t *testing.T) {
	store, client, bucket := emulatorStore(t)
	ctx := t.Context()

	// A unique prefix per run keeps repeated runs against a long-lived
	// emulator from seeing each other's objects.
	prefix := t.Name() + "/"
	writeGCSObject(t, client, bucket, prefix+"manifest.json", "manifest")
	writeGCSObject(t, client, bucket, prefix+"memory.zst", "memory")

	objects, err := store.List(ctx, bucket, prefix)
	if err != nil {
		t.Fatalf("List() = %v, want nil", err)
	}
	want := []string{prefix + "manifest.json", prefix + "memory.zst"}
	if diff := cmp.Diff(want, objects); diff != "" {
		t.Errorf("List() differs (-want +got):\n%s", diff)
	}

	if err := store.Copy(ctx, bucket, prefix+"memory.zst", bucket, prefix+"copy/memory.zst"); err != nil {
		t.Fatalf("Copy() = %v, want nil", err)
	}
	if got := readGCSObject(t, client, bucket, prefix+"copy/memory.zst"); got != "memory" {
		t.Errorf("copied object content = %q, want %q", got, "memory")
	}

	if err := store.Delete(ctx, bucket, prefix+"memory.zst"); err != nil {
		t.Fatalf("Delete() = %v, want nil", err)
	}
	// A second delete finds nothing left to do, which is what a retry over a
	// partly-collected external snapshot looks like.
	if err := store.Delete(ctx, bucket, prefix+"memory.zst"); err != nil {
		t.Fatalf("Delete() of a missing object = %v, want nil", err)
	}

	objects, err = store.List(ctx, bucket, prefix)
	if err != nil {
		t.Fatalf("List() = %v, want nil", err)
	}
	want = []string{prefix + "copy/memory.zst", prefix + "manifest.json"}
	if diff := cmp.Diff(want, objects); diff != "" {
		t.Errorf("List() after the delete differs (-want +got):\n%s", diff)
	}
}

func writeGCSObject(t *testing.T, client *storage.Client, bucket, object, content string) {
	t.Helper()
	w := client.Bucket(bucket).Object(object).NewWriter(t.Context())
	if _, err := io.WriteString(w, content); err != nil {
		t.Fatalf("writing gs://%s/%s: %v", bucket, object, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing gs://%s/%s: %v", bucket, object, err)
	}
}

func readGCSObject(t *testing.T, client *storage.Client, bucket, object string) string {
	t.Helper()
	r, err := client.Bucket(bucket).Object(object).NewReader(t.Context())
	if err != nil {
		t.Fatalf("reading gs://%s/%s: %v", bucket, object, err)
	}
	defer r.Close()
	content, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading gs://%s/%s: %v", bucket, object, err)
	}
	return string(content)
}
