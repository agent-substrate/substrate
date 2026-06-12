// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// memStore is an in-memory ategcs.ObjectStorage for testing.
type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *memStore) key(bucket, object string) string { return bucket + "/" + object }

func (m *memStore) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[m.key(bucket, object)]
	if !ok {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, object)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *memStore) PutObject(ctx context.Context, bucket, object string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = map[string][]byte{}
	}
	m.data[m.key(bucket, object)] = b
	return nil
}

// TestSnapshotManifestRoundTrip verifies the cloud-hypervisor snapshot ship path:
// uploadSnapshotFiles writes a manifest + each file, and downloadSnapshotManifest
// reconstructs them byte-for-byte at the destination.
func TestSnapshotManifestRoundTrip(t *testing.T) {
	store := &memStore{data: map[string][]byte{}}
	ctx := context.Background()

	src := t.TempDir()
	files := []string{"config.json", "state.json", "memory-ranges", "shared-dir.tar"}
	want := map[string][]byte{}
	for i, f := range files {
		content := []byte(fmt.Sprintf("contents-of-%s-%d\x00\x01\x02", f, i))
		if err := os.WriteFile(filepath.Join(src, f), content, 0o600); err != nil {
			t.Fatal(err)
		}
		want[f] = content
	}

	prefix := "gs://test-bucket/actors/x/snapshots/y"
	if err := uploadSnapshotFiles(ctx, store, prefix, src, files); err != nil {
		t.Fatalf("uploadSnapshotFiles: %v", err)
	}

	dst := t.TempDir()
	ok, err := downloadSnapshotManifest(ctx, store, prefix, dst)
	if err != nil {
		t.Fatalf("downloadSnapshotManifest: %v", err)
	}
	if !ok {
		t.Fatal("downloadSnapshotManifest returned ok=false, want true")
	}
	for _, f := range files {
		got, err := os.ReadFile(filepath.Join(dst, f))
		if err != nil {
			t.Errorf("restored %q: %v", f, err)
			continue
		}
		if !bytes.Equal(got, want[f]) {
			t.Errorf("restored %q = %q, want %q", f, got, want[f])
		}
	}
}

// TestDownloadSnapshotManifestMissing verifies the legacy fallback: with no
// manifest in storage, downloadSnapshotManifest reports ok=false (no error) so
// the caller uses the fixed gVisor file set.
func TestDownloadSnapshotManifestMissing(t *testing.T) {
	store := &memStore{data: map[string][]byte{}}
	ok, err := downloadSnapshotManifest(context.Background(), store, "gs://test-bucket/none", t.TempDir())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("ok = true for missing manifest, want false")
	}
}
