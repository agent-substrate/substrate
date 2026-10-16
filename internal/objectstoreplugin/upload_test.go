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

package objectstoreplugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	objectstorev1 "go.ate.dev/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type slowSnapshotStore struct {
	*memObjects
	release   <-chan struct{}
	active    atomic.Int64
	peak      atomic.Int64
	manifests atomic.Int64
	fail      bool
}

func (s *slowSnapshotStore) PutObject(ctx context.Context, bucket, object string, r io.Reader) error {
	if strings.HasSuffix(object, "/"+manifestFile) {
		s.manifests.Add(1)
		return s.memObjects.PutObject(ctx, bucket, object, r)
	}
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.peak.Load(); n > old && !s.peak.CompareAndSwap(old, n); old = s.peak.Load() {
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.fail {
		return errors.New("upload failed")
	}
	return s.memObjects.PutObject(ctx, bucket, object, r)
}

func snapshotUploadFiles(t *testing.T, dir string, n int) []string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	files := []string{manifestFile}
	for i := range n {
		files = append(files, fmt.Sprintf("file%d", i))
	}
	for _, name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("snapshot payload"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return files
}

func TestUploadBudgetSharedAcrossSnapshots(t *testing.T) {
	root := t.TempDir()
	const snapshots, filesPerSnapshot = 10, 8
	requests := make([]*objectstorev1.UploadSnapshotRequest, snapshots)
	for i := range snapshots {
		dir := filepath.Join(root, fmt.Sprintf("src%d", i))
		requests[i] = &objectstorev1.UploadSnapshotRequest{
			SnapshotUri: fmt.Sprintf("%s%d", testURI, i), LocalPath: dir,
			Files: snapshotUploadFiles(t, dir, filesPerSnapshot),
		}
	}
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		store := &slowSnapshotStore{memObjects: newMemObjects(), release: gate}
		// This backend has no SDK buffers. Two compression reservations fit.
		node, err := NewNodePlugin(store, root, 128<<20)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, snapshots)
		for _, req := range requests {
			go func() { _, err := node.UploadSnapshot(t.Context(), req); done <- err }()
		}
		synctest.Wait()
		if got := store.active.Load(); got != 2 {
			t.Errorf("active uploads = %d, want 2 across all requests", got)
		}
		if got := store.manifests.Load(); got != 0 {
			t.Errorf("committed %d manifests before data finished", got)
		}
		close(gate)
		synctest.Wait()
		for range snapshots {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		if got := store.peak.Load(); got != 2 {
			t.Errorf("peak uploads = %d, want 2", got)
		}
		if got := len(store.keys()); got != snapshots*(filesPerSnapshot+1) {
			t.Fatalf("uploaded %d objects", got)
		}
		// Existing callers also send manifests in a separate RPC. Verify a
		// combined request's stored data still downloads through the same reader.
		dst := filepath.Join(root, "restored")
		if err := os.Mkdir(dst, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := node.FetchSnapshot(t.Context(), &objectstorev1.FetchSnapshotRequest{
			SnapshotUri: requests[0].SnapshotUri, WritePath: dst, Files: requests[0].Files,
		}); err != nil {
			t.Fatal(err)
		}
		for _, name := range requests[0].Files {
			data, err := os.ReadFile(filepath.Join(dst, name))
			if err != nil || string(data) != "snapshot payload" {
				t.Fatalf("%s: %q, %v", name, data, err)
			}
		}
	})
}

func TestUploadFailureDoesNotCommitManifest(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "src")
	files := snapshotUploadFiles(t, dir, 3)
	gate := make(chan struct{})
	close(gate)
	store := &slowSnapshotStore{memObjects: newMemObjects(), release: gate, fail: true}
	node, err := NewNodePlugin(store, root, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.UploadSnapshot(t.Context(), &objectstorev1.UploadSnapshotRequest{
		SnapshotUri: testURI, LocalPath: dir, Files: files,
	}); status.Code(err) != codes.Internal {
		t.Fatalf("got %v, want upload error", err)
	}
	if got := store.manifests.Load(); got != 0 {
		t.Fatalf("committed %d manifests after data failed", got)
	}
	store.fail = false
	if _, err := node.UploadSnapshot(t.Context(), &objectstorev1.UploadSnapshotRequest{
		SnapshotUri: testURI, LocalPath: dir, Files: files,
	}); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
}

func TestUploadCanceledWaiterReturnsCanceled(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "src")
	files := snapshotUploadFiles(t, dir, 1)
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		store := &slowSnapshotStore{memObjects: newMemObjects(), release: gate}
		node, err := NewNodePlugin(store, root, 64<<20)
		if err != nil {
			t.Fatal(err)
		}
		req := &objectstorev1.UploadSnapshotRequest{SnapshotUri: testURI, LocalPath: dir, Files: files}
		first := make(chan error, 1)
		go func() { _, err := node.UploadSnapshot(t.Context(), req); first <- err }()
		synctest.Wait()
		ctx, cancel := context.WithCancel(t.Context())
		waiting := make(chan error, 1)
		go func() { _, err := node.UploadSnapshot(ctx, req); waiting <- err }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		if err := <-waiting; status.Code(err) != codes.Canceled {
			t.Errorf("waiting upload: %v", err)
		}
		close(gate)
		synctest.Wait()
		if err := <-first; err != nil {
			t.Fatal(err)
		}
	})
}
