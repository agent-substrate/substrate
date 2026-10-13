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

package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/plugins/azure-blob/internal/blobstore"
	objectstorev1 "go.ate.dev/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testURI = "azblob://snapshots/actors/a/snapshots/one"

type fakeBlob struct {
	data     []byte
	metadata map[string]*string
}

type fakeStore struct {
	mu sync.Mutex

	blobs       map[string]fakeBlob
	staged      map[string]map[string][]byte
	failStageAt int
	stageCalls  int
	listErr     error
	deleteErr   error
	// readErr, when set, fails every download body after its first byte.
	readErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{blobs: map[string]fakeBlob{}, staged: map[string]map[string][]byte{}}
}

func (f *fakeStore) StageBlock(ctx context.Context, name, blockID string, data []byte, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	f.stageCalls++
	if f.failStageAt > 0 && f.stageCalls == f.failStageAt {
		return blobstore.ErrUnavailable
	}
	if f.staged[name] == nil {
		f.staged[name] = map[string][]byte{}
	}
	f.staged[name][blockID] = slices.Clone(data)
	return nil
}

func (f *fakeStore) CommitBlockList(ctx context.Context, name string, blockIDs []string, metadata map[string]*string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	var data []byte
	for _, blockID := range blockIDs {
		block, ok := f.staged[name][blockID]
		if !ok {
			return errors.New("missing staged block")
		}
		data = append(data, block...)
	}
	f.blobs[name] = fakeBlob{data: data, metadata: maps.Clone(metadata)}
	delete(f.staged, name)
	return nil
}

func (f *fakeStore) Download(ctx context.Context, name string) (blobstore.Download, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return blobstore.Download{}, err
	}
	blob, ok := f.blobs[name]
	if !ok {
		return blobstore.Download{}, blobstore.ErrNotFound
	}
	var body io.Reader = bytes.NewReader(slices.Clone(blob.data))
	if f.readErr != nil {
		body = io.MultiReader(io.LimitReader(body, 1), &errReader{err: f.readErr})
	}
	return blobstore.Download{Body: io.NopCloser(body), Size: int64(len(blob.data)), Metadata: maps.Clone(blob.metadata)}, nil
}

func (f *fakeStore) List(ctx context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.listErr != nil {
		return nil, f.listErr
	}
	var names []string
	for name := range f.blobs {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names, nil
}

func (f *fakeStore) Delete(ctx context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.blobs, name)
	return nil
}

type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }

func newTestServer(t *testing.T, store *fakeStore, root string) *Server {
	t.Helper()
	server, err := New(map[string]blobstore.Store{"snapshots": store}, "tenant", root)
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestUploadAndFetchSnapshot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	dest := filepath.Join(root, "dest")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dest, 0o700); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("snapshot"), blockSize/8+13)
	if err := os.WriteFile(filepath.Join(source, "memory.img"), want, 0o600); err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	server := newTestServer(t, store, root)

	upload := &objectstorev1.UploadSnapshotRequest{SnapshotUri: testURI, LocalPath: source, Files: []string{"memory.img"}}
	if _, err := server.UploadSnapshot(ctx, upload); err != nil {
		t.Fatalf("UploadSnapshot: %v", err)
	}
	if _, err := server.UploadSnapshot(ctx, upload); err != nil {
		t.Fatalf("idempotent UploadSnapshot: %v", err)
	}
	if _, err := server.FetchSnapshot(ctx, &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: testURI,
		WritePath:   dest,
		Files:       []string{"memory.img"},
	}); err != nil {
		t.Fatalf("FetchSnapshot: %v", err)
	}
	if _, err := server.FetchSnapshot(ctx, &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: testURI,
		WritePath:   dest,
		Files:       []string{"memory.img"},
	}); err != nil {
		t.Fatalf("idempotent FetchSnapshot: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "memory.img"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("fetched %d bytes, want %d", len(got), len(want))
	}
}

// TestUploadAndFetchSnapshotSubdirectory checks the fs/ subdirectory gVisor
// checkpoints durable directories into: the file keeps its relative name in
// the blob name, and a restore creates the subdirectory.
func TestUploadAndFetchSnapshotSubdirectory(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	dest := filepath.Join(root, "dest")
	if err := os.MkdirAll(filepath.Join(source, "fs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dest, 0o700); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		"checkpoint.img":     []byte("process state"),
		"fs/fscheckpoint.pb": []byte("fs manifest"),
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(source, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	names := []string{"checkpoint.img", "fs/fscheckpoint.pb"}
	store := newFakeStore()
	server := newTestServer(t, store, root)

	if _, err := server.UploadSnapshot(ctx, &objectstorev1.UploadSnapshotRequest{SnapshotUri: testURI, LocalPath: source, Files: names}); err != nil {
		t.Fatalf("UploadSnapshot: %v", err)
	}
	wantBlobs := []string{
		"tenant/actors/a/snapshots/one/checkpoint.img",
		"tenant/actors/a/snapshots/one/fs/fscheckpoint.pb",
	}
	if got := slices.Sorted(maps.Keys(store.blobs)); !slices.Equal(got, wantBlobs) {
		t.Fatalf("stored blobs = %q, want %q", got, wantBlobs)
	}
	for range 2 {
		if _, err := server.FetchSnapshot(ctx, &objectstorev1.FetchSnapshotRequest{SnapshotUri: testURI, WritePath: dest, Files: names}); err != nil {
			t.Fatalf("FetchSnapshot: %v", err)
		}
	}
	for name, want := range files {
		if got, err := os.ReadFile(filepath.Join(dest, name)); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("restored %s = %q (err %v), want %q", name, got, err, want)
		}
	}
	for dir, want := range map[string]int{dest: 2, filepath.Join(dest, "fs"): 1} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != want {
			t.Fatalf("%s holds %d entries, want %d with no temporary files left", dir, len(entries), want)
		}
	}

	if _, err := server.CleanupSnapshot(ctx, &objectstorev1.CleanupSnapshotRequest{SnapshotUri: testURI}); err != nil {
		t.Fatalf("CleanupSnapshot: %v", err)
	}
	if len(store.blobs) != 0 {
		t.Fatalf("CleanupSnapshot left %q", slices.Sorted(maps.Keys(store.blobs)))
	}
}

func TestUploadSnapshotPartialRetry(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("x"), blockSize+1)
	if err := os.WriteFile(filepath.Join(source, "memory.img"), want, 0o600); err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	store.failStageAt = 2
	server := newTestServer(t, store, root)
	req := &objectstorev1.UploadSnapshotRequest{SnapshotUri: testURI, LocalPath: source, Files: []string{"memory.img"}}

	if _, err := server.UploadSnapshot(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("first UploadSnapshot code = %v, want Unavailable: %v", status.Code(err), err)
	}
	store.failStageAt = 0
	if _, err := server.UploadSnapshot(context.Background(), req); err != nil {
		t.Fatalf("retried UploadSnapshot: %v", err)
	}
	blob := store.blobs["tenant/actors/a/snapshots/one/memory.img"]
	if !bytes.Equal(blob.data, want) {
		t.Fatalf("committed bytes differ after retry")
	}
}

type concurrentStageStore struct {
	*fakeStore
	current atomic.Int32
	max     atomic.Int32
	release <-chan struct{}
	started chan<- struct{}
}

func (s *concurrentStageStore) StageBlock(ctx context.Context, name, blockID string, data, checksum []byte) error {
	current := s.current.Add(1)
	defer s.current.Add(-1)
	for {
		max := s.max.Load()
		if current <= max || s.max.CompareAndSwap(max, current) {
			break
		}
	}
	s.started <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.fakeStore.StageBlock(ctx, name, blockID, data, checksum)
}

func TestUploadSnapshotStagesBlocksConcurrently(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "memory.img"), make([]byte, 2*blockSize), 0o600); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	store := &concurrentStageStore{fakeStore: newFakeStore(), release: release, started: started}
	server, err := New(map[string]blobstore.Store{"snapshots": store}, "tenant", root)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := server.UploadSnapshot(context.Background(), &objectstorev1.UploadSnapshotRequest{
			SnapshotUri: testURI,
			LocalPath:   source,
			Files:       []string{"memory.img"},
		})
		done <- err
	}()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range 2 {
		select {
		case <-started:
		case <-timer.C:
			close(release)
			t.Fatal("two block uploads did not overlap")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("UploadSnapshot: %v", err)
	}
	if got := store.max.Load(); got < 2 {
		t.Fatalf("maximum concurrent StageBlock calls = %d, want at least 2", got)
	}
}

func TestUploadSnapshotEmptyFile(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "empty"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	server := newTestServer(t, store, root)
	req := &objectstorev1.UploadSnapshotRequest{SnapshotUri: testURI, LocalPath: source, Files: []string{"empty"}}

	if _, err := server.UploadSnapshot(context.Background(), req); err != nil {
		t.Fatalf("UploadSnapshot: %v", err)
	}
	blob := store.blobs["tenant/actors/a/snapshots/one/empty"]
	if len(blob.data) != 0 {
		t.Fatalf("empty blob contains %d bytes", len(blob.data))
	}
	if _, err := metadataHash(blob.metadata); err != nil {
		t.Fatalf("empty blob integrity metadata: %v", err)
	}
}

func TestFetchSnapshotErrors(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "dest"), 0o700); err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	server := newTestServer(t, store, root)
	req := &objectstorev1.FetchSnapshotRequest{SnapshotUri: testURI, WritePath: filepath.Join(root, "dest"), Files: []string{"manifest.json"}}

	if _, err := server.FetchSnapshot(context.Background(), req); status.Code(err) != codes.NotFound {
		t.Fatalf("missing FetchSnapshot code = %v, want NotFound", status.Code(err))
	}
	sum := sha256.Sum256([]byte("good"))
	digest := hex.EncodeToString(sum[:])
	store.blobs["tenant/actors/a/snapshots/one/manifest.json"] = fakeBlob{
		data:     []byte("corrupt"),
		metadata: map[string]*string{integrityMetadata: &digest},
	}
	if _, err := server.FetchSnapshot(context.Background(), req); status.Code(err) != codes.DataLoss {
		t.Fatalf("corrupt FetchSnapshot code = %v, want DataLoss: %v", status.Code(err), err)
	}
	if _, err := os.Stat(filepath.Join(root, "dest", "manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt fetch exposed destination: %v", err)
	}

	store.blobs["tenant/actors/a/snapshots/one/manifest.json"] = fakeBlob{
		data:     []byte("good"),
		metadata: map[string]*string{},
	}
	if _, err := server.FetchSnapshot(context.Background(), req); status.Code(err) != codes.DataLoss {
		t.Fatalf("missing metadata FetchSnapshot code = %v, want DataLoss: %v", status.Code(err), err)
	}
}

func TestCleanupSnapshot(t *testing.T) {
	store := newFakeStore()
	store.blobs["tenant/actors/a/snapshots/one/a"] = fakeBlob{}
	store.blobs["tenant/actors/a/snapshots/two/a"] = fakeBlob{}
	store.blobs["tenant/actors/b/snapshots/one/a"] = fakeBlob{}
	server := newTestServer(t, store, t.TempDir())
	req := &objectstorev1.CleanupSnapshotRequest{SnapshotUri: "azblob://snapshots/actors/a"}

	if _, err := server.CleanupSnapshot(context.Background(), req); err != nil {
		t.Fatalf("CleanupSnapshot: %v", err)
	}
	if _, err := server.CleanupSnapshot(context.Background(), req); err != nil {
		t.Fatalf("idempotent CleanupSnapshot: %v", err)
	}
	if _, ok := store.blobs["tenant/actors/b/snapshots/one/a"]; !ok {
		t.Fatal("cleanup removed blob outside prefix")
	}
	if len(store.blobs) != 1 {
		t.Fatalf("remaining blobs = %d, want 1", len(store.blobs))
	}

	store.listErr = blobstore.ErrNotFound
	if _, err := server.CleanupSnapshot(context.Background(), req); err != nil {
		t.Fatalf("absent CleanupSnapshot: %v", err)
	}
	store.listErr = blobstore.ErrUnavailable
	if _, err := server.CleanupSnapshot(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("unavailable CleanupSnapshot code = %v, want Unavailable", status.Code(err))
	}
	store.listErr = blobstore.ErrContainerNotFound
	if _, err := server.CleanupSnapshot(context.Background(), req); err != nil {
		t.Fatalf("missing container at list CleanupSnapshot: %v", err)
	}
	store.listErr = nil
	store.deleteErr = blobstore.ErrContainerNotFound
	store.blobs["tenant/actors/a/snapshots/one/a"] = fakeBlob{}
	if _, err := server.CleanupSnapshot(context.Background(), req); err != nil {
		t.Fatalf("missing container at delete CleanupSnapshot: %v", err)
	}
	store.deleteErr = blobstore.ErrPermission
	store.blobs["tenant/actors/a/snapshots/one/a"] = fakeBlob{}
	if _, err := server.CleanupSnapshot(context.Background(), req); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("denied CleanupSnapshot code = %v, want PermissionDenied", status.Code(err))
	}
}

func TestCopySnapshot(t *testing.T) {
	store := newFakeStore()
	sum := sha256.Sum256([]byte("manifest"))
	digest := hex.EncodeToString(sum[:])
	store.blobs["tenant/actors/a/snapshots/one/manifest.json"] = fakeBlob{
		data:     []byte("manifest"),
		metadata: map[string]*string{integrityMetadata: &digest},
	}
	server := newTestServer(t, store, t.TempDir())
	req := &objectstorev1.CopySnapshotRequest{
		SrcUri: testURI,
		DstUri: "azblob://snapshots/tags/stable",
	}
	if _, err := server.CopySnapshot(context.Background(), req); err != nil {
		t.Fatalf("CopySnapshot: %v", err)
	}
	if _, err := server.CopySnapshot(context.Background(), req); err != nil {
		t.Fatalf("idempotent CopySnapshot: %v", err)
	}
	got := store.blobs["tenant/tags/stable/manifest.json"].data
	if string(got) != "manifest" {
		t.Fatalf("copied data = %q", got)
	}

	_, err := server.CopySnapshot(context.Background(), &objectstorev1.CopySnapshotRequest{
		SrcUri: "azblob://snapshots/actors/missing",
		DstUri: "azblob://snapshots/tags/stable",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("empty CopySnapshot code = %v, want FailedPrecondition", status.Code(err))
	}

	badDigest := strings.Repeat("0", sha256.Size*2)
	store.blobs["tenant/actors/a/snapshots/corrupt/manifest.json"] = fakeBlob{
		data:     []byte("manifest"),
		metadata: map[string]*string{integrityMetadata: &badDigest},
	}
	_, err = server.CopySnapshot(context.Background(), &objectstorev1.CopySnapshotRequest{
		SrcUri: "azblob://snapshots/actors/a/snapshots/corrupt",
		DstUri: "azblob://snapshots/tags/corrupt",
	})
	if status.Code(err) != codes.DataLoss {
		t.Fatalf("corrupt CopySnapshot code = %v, want DataLoss", status.Code(err))
	}
	if _, ok := store.blobs["tenant/tags/corrupt/manifest.json"]; ok {
		t.Fatal("CopySnapshot committed bytes that failed integrity verification")
	}
}

func TestCopySnapshotKeepsListedSuffix(t *testing.T) {
	store := newFakeStore()
	sum := sha256.Sum256([]byte("payload"))
	digest := hex.EncodeToString(sum[:])
	// Blob names are flat, so a listed name may hold a ".." segment.
	store.blobs["tenant/actors/a/snapshots/one/../victim/file"] = fakeBlob{
		data:     []byte("payload"),
		metadata: map[string]*string{integrityMetadata: &digest},
	}
	server := newTestServer(t, store, t.TempDir())
	if _, err := server.CopySnapshot(context.Background(), &objectstorev1.CopySnapshotRequest{
		SrcUri: testURI,
		DstUri: "azblob://snapshots/tags/stable",
	}); err != nil {
		t.Fatalf("CopySnapshot: %v", err)
	}
	if got := string(store.blobs["tenant/tags/stable/../victim/file"].data); got != "payload" {
		t.Fatalf("copied data = %q, want the source suffix kept under the destination", got)
	}
	for name := range store.blobs {
		if strings.HasPrefix(name, "tenant/tags/") && !strings.HasPrefix(name, "tenant/tags/stable/") {
			t.Errorf("CopySnapshot wrote %q outside the destination prefix", name)
		}
	}
}

func TestLongFileNameRoundTrip(t *testing.T) {
	store := newFakeStore()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	dest := filepath.Join(root, "dest")
	for _, dir := range []string{source, dest} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	server := newTestServer(t, store, root)
	for _, size := range []int{230, 250, 255} {
		name := strings.Repeat("n", size)
		if err := os.WriteFile(filepath.Join(source, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := server.UploadSnapshot(context.Background(), &objectstorev1.UploadSnapshotRequest{
			SnapshotUri: testURI, LocalPath: source, Files: []string{name},
		}); err != nil {
			t.Fatalf("UploadSnapshot(%d-byte name): %v", size, err)
		}
		if _, err := server.FetchSnapshot(context.Background(), &objectstorev1.FetchSnapshotRequest{
			SnapshotUri: testURI, WritePath: dest, Files: []string{name},
		}); err != nil {
			t.Fatalf("FetchSnapshot(%d-byte name): %v", size, err)
		}
		if got, err := os.ReadFile(filepath.Join(dest, name)); err != nil || string(got) != name {
			t.Fatalf("restored %d-byte name: %v", size, err)
		}
	}
	entries, err := os.ReadDir(dest)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("restore directory holds %d entries, want 3 with no temporary files left", len(entries))
	}
}

func TestCopySnapshotAcrossContainers(t *testing.T) {
	src, dst := newFakeStore(), newFakeStore()
	sum := sha256.Sum256([]byte("manifest"))
	digest := hex.EncodeToString(sum[:])
	src.blobs["tenant/actors/a/snapshots/one/manifest.json"] = fakeBlob{
		data:     []byte("manifest"),
		metadata: map[string]*string{integrityMetadata: &digest},
	}
	server, err := New(map[string]blobstore.Store{"snapshots": src, "tags": dst}, "tenant", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.CopySnapshot(context.Background(), &objectstorev1.CopySnapshotRequest{
		SrcUri: testURI,
		DstUri: "azblob://tags/stable",
	}); err != nil {
		t.Fatalf("CopySnapshot across containers: %v", err)
	}
	if got := dst.blobs["tenant/stable/manifest.json"].data; string(got) != "manifest" {
		t.Fatalf("destination container data = %q, want manifest", got)
	}
	if _, ok := src.blobs["tenant/stable/manifest.json"]; ok {
		t.Fatal("CopySnapshot wrote the destination into the source container")
	}
	if len(src.blobs) != 1 {
		t.Fatalf("source container has %d blobs, want 1", len(src.blobs))
	}
}

func TestStreamingReadErrorsKeepTheirCode(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "dest"), 0o700); err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	sum := sha256.Sum256([]byte("manifest"))
	digest := hex.EncodeToString(sum[:])
	store.blobs["tenant/actors/a/snapshots/one/manifest.json"] = fakeBlob{
		data:     []byte("manifest"),
		metadata: map[string]*string{integrityMetadata: &digest},
	}
	store.readErr = fmt.Errorf("%w: connection reset", blobstore.ErrUnavailable)
	server := newTestServer(t, store, root)

	_, err := server.FetchSnapshot(context.Background(), &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: testURI, WritePath: filepath.Join(root, "dest"), Files: []string{"manifest.json"},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("FetchSnapshot code = %v, want Unavailable: %v", status.Code(err), err)
	}
	if entries, _ := os.ReadDir(filepath.Join(root, "dest")); len(entries) != 0 {
		t.Fatalf("failed fetch left %v in the destination", entries)
	}
	_, err = server.CopySnapshot(context.Background(), &objectstorev1.CopySnapshotRequest{
		SrcUri: testURI, DstUri: "azblob://snapshots/tags/stable",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CopySnapshot code = %v, want Unavailable: %v", status.Code(err), err)
	}
	if _, ok := store.blobs["tenant/tags/stable/manifest.json"]; ok {
		t.Fatal("CopySnapshot committed a partially read blob")
	}
}

func TestCopySnapshotPartialRetry(t *testing.T) {
	store := newFakeStore()
	data := bytes.Repeat([]byte("copy"), blockSize/4+1)
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	store.blobs["tenant/actors/a/snapshots/one/memory.img"] = fakeBlob{
		data: data, metadata: map[string]*string{integrityMetadata: &digest},
	}
	server := newTestServer(t, store, t.TempDir())
	req := &objectstorev1.CopySnapshotRequest{SrcUri: testURI, DstUri: "azblob://snapshots/tags/stable"}

	store.failStageAt = 2
	if _, err := server.CopySnapshot(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("first CopySnapshot code = %v, want Unavailable: %v", status.Code(err), err)
	}
	store.failStageAt = 0
	if _, err := server.CopySnapshot(context.Background(), req); err != nil {
		t.Fatalf("retried CopySnapshot: %v", err)
	}
	if got := store.blobs["tenant/tags/stable/memory.img"].data; !bytes.Equal(got, data) {
		t.Fatal("copied bytes differ after retry")
	}
}

func TestRPCContextErrors(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	dest := filepath.Join(root, "dest")
	for _, dir := range []string{source, dest} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "memory.img"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := newTestServer(t, newFakeStore(), root)
	calls := map[string]func(context.Context) error{
		"fetch": func(ctx context.Context) error {
			_, err := server.FetchSnapshot(ctx, &objectstorev1.FetchSnapshotRequest{
				SnapshotUri: testURI, WritePath: dest, Files: []string{"memory.img"},
			})
			return err
		},
		"upload": func(ctx context.Context) error {
			_, err := server.UploadSnapshot(ctx, &objectstorev1.UploadSnapshotRequest{
				SnapshotUri: testURI, LocalPath: source, Files: []string{"memory.img"},
			})
			return err
		},
		"cleanup": func(ctx context.Context) error {
			_, err := server.CleanupSnapshot(ctx, &objectstorev1.CleanupSnapshotRequest{SnapshotUri: testURI})
			return err
		},
		"copy": func(ctx context.Context) error {
			_, err := server.CopySnapshot(ctx, &objectstorev1.CopySnapshotRequest{
				SrcUri: testURI, DstUri: "azblob://snapshots/tags/stable",
			})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name+"/canceled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := call(ctx); status.Code(err) != codes.Canceled {
				t.Fatalf("code = %v, want Canceled: %v", status.Code(err), err)
			}
		})
		t.Run(name+"/deadline", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 0)
			defer cancel()
			if err := call(ctx); status.Code(err) != codes.DeadlineExceeded {
				t.Fatalf("code = %v, want DeadlineExceeded: %v", status.Code(err), err)
			}
		})
	}
}

func TestRejectsInvalidRequests(t *testing.T) {
	root := t.TempDir()
	server := newTestServer(t, newFakeStore(), root)
	invalidURIs := []string{
		"azblob://snapshots",
		"azblob://snapshots/",
		"azblob://snapshots/.",
		"azblob://other/prefix",
		"azblob://snapshots/../escape",
		"azblob://snapshots/a?query",
		`azblob://snapshots/a\b`,
		"azblob://snapshots/a/.../b",
		"azblob://snapshots/a/ . /b",
		"azblob://snapshots/a/ ",
		"gs://snapshots/prefix",
	}
	for _, uri := range invalidURIs {
		if _, err := server.CleanupSnapshot(context.Background(), &objectstorev1.CleanupSnapshotRequest{SnapshotUri: uri}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("CleanupSnapshot(%q) code = %v, want InvalidArgument", uri, status.Code(err))
		}
	}

	dotURI := "azblob://snapshots/."
	if _, err := server.CopySnapshot(context.Background(), &objectstorev1.CopySnapshotRequest{SrcUri: dotURI, DstUri: testURI}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("CopySnapshot(src=%q) code = %v, want InvalidArgument", dotURI, status.Code(err))
	}
	if _, err := server.CopySnapshot(context.Background(), &objectstorev1.CopySnapshotRequest{SrcUri: testURI, DstUri: dotURI}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("CopySnapshot(dst=%q) code = %v, want InvalidArgument", dotURI, status.Code(err))
	}
	if _, err := server.FetchSnapshot(context.Background(), &objectstorev1.FetchSnapshotRequest{SnapshotUri: dotURI, WritePath: root, Files: []string{"manifest.json"}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("FetchSnapshot(%q) code = %v, want InvalidArgument", dotURI, status.Code(err))
	}
	if _, err := server.UploadSnapshot(context.Background(), &objectstorev1.UploadSnapshotRequest{SnapshotUri: dotURI, LocalPath: root, Files: []string{"manifest.json"}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("UploadSnapshot(%q) code = %v, want InvalidArgument", dotURI, status.Code(err))
	}

	valid := map[string]blobstore.Store{"snapshots": newFakeStore()}
	for _, tc := range []struct {
		name   string
		stores map[string]blobstore.Store
		prefix string
		root   string
	}{
		{name: "no containers", root: root},
		{name: "nil store", stores: map[string]blobstore.Store{"snapshots": nil}, root: root},
		{name: "empty container", stores: map[string]blobstore.Store{"": newFakeStore()}, root: root},
		{name: "container with slash", stores: map[string]blobstore.Store{"a/b": newFakeStore()}, root: root},
		{name: "relative root", stores: valid, root: "relative"},
		{name: "escaping prefix", stores: valid, prefix: "../escape", root: root},
		{name: "unclean prefix", stores: valid, prefix: "a//b", root: root},
		{name: "backslash prefix", stores: valid, prefix: `a\b`, root: root},
		{name: "dots-only segment prefix", stores: valid, prefix: "a/.../b", root: root},
		{name: "space-only segment prefix", stores: valid, prefix: "a/ /b", root: root},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.stores, tc.prefix, tc.root); err == nil {
				t.Fatal("New accepted invalid configuration")
			}
		})
	}

	_, err := server.UploadSnapshot(context.Background(), &objectstorev1.UploadSnapshotRequest{
		SnapshotUri: testURI,
		LocalPath:   filepath.Join(server.root, "missing"),
		Files:       []string{"manifest.json"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing local path code = %v, want InvalidArgument", status.Code(err))
	}

	for _, files := range [][]string{
		nil,
		{""},
		{"."},
		{".."},
		{"..."},
		{" "},
		{"/a"},
		{"../a"},
		{"a/../b"},
		{"./a"},
		{"a/"},
		{"a//b"},
		{"fs/.."},
		{"fs/ . "},
		{`fs\a`},
		{"a", "a"},
		{"fs/a", "fs/a"},
	} {
		_, err := server.UploadSnapshot(context.Background(), &objectstorev1.UploadSnapshotRequest{
			SnapshotUri: testURI, LocalPath: root, Files: files,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("files %q code = %v, want InvalidArgument", files, status.Code(err))
		}
	}
}

func TestRejectsSymlinksOutsideRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	outside := filepath.Join(parent, "outside")
	for _, dir := range []string{root, outside} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(outside, "memory.img"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	server := newTestServer(t, store, root)
	if _, err := server.UploadSnapshot(context.Background(), &objectstorev1.UploadSnapshotRequest{
		SnapshotUri: testURI, LocalPath: link, Files: []string{"memory.img"},
	}); err == nil {
		t.Fatal("UploadSnapshot read through a directory symlink")
	}
	sum := sha256.Sum256([]byte("replacement"))
	digest := hex.EncodeToString(sum[:])
	store.blobs["tenant/actors/a/snapshots/one/memory.img"] = fakeBlob{
		data: []byte("replacement"), metadata: map[string]*string{integrityMetadata: &digest},
	}
	if _, err := server.FetchSnapshot(context.Background(), &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: testURI, WritePath: link, Files: []string{"memory.img"},
	}); err == nil {
		t.Fatal("FetchSnapshot wrote through a directory symlink")
	}
	got, err := os.ReadFile(filepath.Join(outside, "memory.img"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "secret" {
		t.Fatalf("outside file = %q, want unchanged", got)
	}
}

var _ blobstore.Store = (*fakeStore)(nil)
