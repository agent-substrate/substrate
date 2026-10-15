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
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/plugins/azure-blob/internal/blobstore"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const mib = 1 << 20

// TestAzureBlockLimits pins the SDK constants the block size derives from, so
// an SDK bump that changes them shows up here.
func TestAzureBlockLimits(t *testing.T) {
	if blobstore.MaxBlocks != 50_000 {
		t.Errorf("MaxBlocks = %d, want 50000", blobstore.MaxBlocks)
	}
	if blobstore.MaxBlockSize != 4000*mib {
		t.Errorf("MaxBlockSize = %d, want 4000 MiB", blobstore.MaxBlockSize)
	}
}

func TestBlockSize(t *testing.T) {
	const (
		fitsMinBlocks = 50_000 * 4 * mib    // about 195 GiB
		maxBlobSize   = 50_000 * 4000 * mib // about 190.7 TiB
	)
	for _, tc := range []struct {
		name string
		size int64
		want int64
	}{
		{"unknown", -1, 4 * mib},
		{"empty", 0, 4 * mib},
		{"one byte", 1, 4 * mib},
		{"50000 blocks of 4 MiB", fitsMinBlocks, 4 * mib},
		{"first size past 50000 blocks of 4 MiB", fitsMinBlocks + 1, 5 * mib},
		{"50000 blocks of 5 MiB", 50_000 * 5 * mib, 5 * mib},
		{"first size past 50000 blocks of 5 MiB", 50_000*5*mib + 1, 6 * mib},
		{"one byte below the maximum", maxBlobSize - 1, 4000 * mib},
		{"maximum", maxBlobSize, 4000 * mib},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := azureLimits.blockSize(tc.size)
			if err != nil {
				t.Fatalf("blockSize(%d): %v", tc.size, err)
			}
			if got != tc.want {
				t.Fatalf("blockSize(%d) = %d MiB, want %d MiB", tc.size, got/mib, tc.want/mib)
			}
			if tc.size > 0 && ceilDiv(tc.size, got) > 50_000 {
				t.Fatalf("blockSize(%d) = %d needs %d blocks, more than 50000", tc.size, got, ceilDiv(tc.size, got))
			}
		})
	}
	for _, size := range []int64{maxBlobSize + 1, math.MaxInt64} {
		_, err := azureLimits.blockSize(size)
		if !errors.Is(err, errTooLarge) {
			t.Errorf("blockSize(%d) = %v, want errTooLarge", size, err)
		}
		if err != nil && !strings.Contains(err.Error(), strconv.FormatInt(size, 10)) {
			t.Errorf("blockSize(%d) error %q does not name the size", size, err)
		}
	}
}

// TestBufferSlotsBoundMemory checks that the blocks an upload that buffers
// holds at once never exceed bufferBudget (32 MiB), for every block size it
// buffers.
func TestBufferSlotsBoundMemory(t *testing.T) {
	if bufferBudget != 32*mib || maxStages != 8 {
		t.Fatalf("bufferBudget = %d MiB, maxStages = %d, want 32 MiB and 8", bufferBudget/mib, maxStages)
	}
	for _, tc := range []struct {
		blockSize int64
		want      int
	}{
		{4 * mib, 8},
		{5 * mib, 6},
		{16 * mib, 2},
		{17 * mib, 1},
		{32 * mib, 1},
	} {
		got := bufferSlots(tc.blockSize)
		if got != tc.want {
			t.Errorf("bufferSlots(%d MiB) = %d, want %d", tc.blockSize/mib, got, tc.want)
		}
		if held := int64(got) * tc.blockSize; held > 32*mib {
			t.Errorf("bufferSlots(%d MiB) holds %d MiB", tc.blockSize/mib, held/mib)
		}
	}
}

// bytesSource returns data as a blockSource that rereads it, the way
// fileSource rereads a file.
func bytesSource(data []byte, size int64) blockSource {
	return blockSource{
		r:    bytes.NewReader(data),
		size: size,
		reread: func(_ context.Context, off, n int64) io.ReadSeekCloser {
			return nopCloser{io.NewSectionReader(bytes.NewReader(data), off, n)}
		},
	}
}

// bodySize returns the length of a staging body and leaves it at its start.
func bodySize(body io.ReadSeeker) (int64, error) {
	n, err := body.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	_, err = body.Seek(0, io.SeekStart)
	return n, err
}

// sizeRecordingStore records the length of each staged block by its index.
type sizeRecordingStore struct {
	*fakeStore
	mu     sync.Mutex
	blocks map[int]int
}

func (s *sizeRecordingStore) StageBlock(ctx context.Context, name, blockID string, body io.ReadSeeker, checksum []byte) error {
	raw, err := base64.StdEncoding.DecodeString(blockID)
	if err != nil {
		return err
	}
	index, err := strconv.Atoi(string(raw[:8]))
	if err != nil {
		return err
	}
	size, err := bodySize(body)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.blocks[index] = int(size)
	s.mu.Unlock()
	return s.fakeStore.StageBlock(ctx, name, blockID, body, checksum)
}

// TestUploadSnapshotBlockSizeBoundaries runs UploadSnapshot at each boundary
// of the block size, with limits scaled down to two blocks of at most 6 MiB
// so the files stay small.
func TestUploadSnapshotBlockSizeBoundaries(t *testing.T) {
	limits := blockLimits{maxBlocks: 2, maxBlockSize: 6 * mib}
	for _, tc := range []struct {
		name string
		size int64
		want []int
	}{
		{"fits in minimum blocks", 8 * mib, []int{4 * mib, 4 * mib}},
		{"first size that needs a larger block", 8*mib + 1, []int{5 * mib, 3*mib + 1}},
		{"maximum", 12 * mib, []int{6 * mib, 6 * mib}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			if err := os.Mkdir(source, 0o700); err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte("abcdefg"), int(tc.size/7)+1)[:tc.size]
			if err := os.WriteFile(filepath.Join(source, "memory.img"), want, 0o600); err != nil {
				t.Fatal(err)
			}
			store := &sizeRecordingStore{fakeStore: newFakeStore(), blocks: map[int]int{}}
			server, err := New(map[string]blobstore.Store{"snapshots": store}, "tenant", root)
			if err != nil {
				t.Fatal(err)
			}
			server.limits = limits
			req := &objectstorev1.UploadSnapshotRequest{SnapshotUri: testURI, LocalPath: source, Files: []string{"memory.img"}}
			if _, err := server.UploadSnapshot(context.Background(), req); err != nil {
				t.Fatalf("UploadSnapshot: %v", err)
			}
			if len(store.blocks) != len(tc.want) {
				t.Fatalf("staged %d blocks, want %d", len(store.blocks), len(tc.want))
			}
			for index, size := range tc.want {
				if store.blocks[index] != size {
					t.Errorf("block %d holds %d bytes, want %d", index, store.blocks[index], size)
				}
			}
			if blob := store.blobs["tenant/actors/a/snapshots/one/memory.img"]; !bytes.Equal(blob.data, want) {
				t.Fatal("committed bytes differ from the file")
			}
		})
	}
}

// TestUploadSnapshotRefusesTooLargeFileBeforeTransfer checks that a file over
// the limit fails the request before any file in it is staged.
func TestUploadSnapshotRefusesTooLargeFileBeforeTransfer(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "small"), []byte("small"), 0o600); err != nil {
		t.Fatal(err)
	}
	large, err := os.Create(filepath.Join(source, "memory.img"))
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(large.Truncate(12*mib+1), large.Close()); err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	server := newTestServer(t, store, root)
	server.limits = blockLimits{maxBlocks: 2, maxBlockSize: 6 * mib}
	req := &objectstorev1.UploadSnapshotRequest{SnapshotUri: testURI, LocalPath: source, Files: []string{"small", "memory.img"}}

	_, err = server.UploadSnapshot(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UploadSnapshot code = %v, want InvalidArgument: %v", status.Code(err), err)
	}
	if !strings.Contains(err.Error(), `"memory.img"`) || !strings.Contains(err.Error(), strconv.Itoa(12*mib+1)) {
		t.Errorf("error %q does not name the file and its size", err)
	}
	if store.stageCalls != 0 || len(store.blobs) != 0 {
		t.Fatalf("staged %d blocks and committed %d blobs, want none", store.stageCalls, len(store.blobs))
	}
}

// TestCopySnapshotSizesBlocksFromSource checks that a copy picks its block
// size from the source blob's reported size.
func TestCopySnapshotSizesBlocksFromSource(t *testing.T) {
	store := &sizeRecordingStore{fakeStore: newFakeStore(), blocks: map[int]int{}}
	data := bytes.Repeat([]byte("c"), 8*mib+1)
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	store.blobs["tenant/actors/a/snapshots/one/memory.img"] = fakeBlob{
		data:     data,
		metadata: map[string]*string{integrityMetadata: &digest},
	}
	server, err := New(map[string]blobstore.Store{"snapshots": store}, "tenant", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server.limits = blockLimits{maxBlocks: 2, maxBlockSize: 6 * mib}
	req := &objectstorev1.CopySnapshotRequest{SrcUri: testURI, DstUri: "azblob://snapshots/tags/stable"}
	if _, err := server.CopySnapshot(context.Background(), req); err != nil {
		t.Fatalf("CopySnapshot: %v", err)
	}
	if store.blocks[0] != 5*mib || store.blocks[1] != 3*mib+1 || len(store.blocks) != 2 {
		t.Fatalf("staged blocks %v, want 5 MiB and 3 MiB + 1", store.blocks)
	}
	if !bytes.Equal(store.blobs["tenant/tags/stable/memory.img"].data, data) {
		t.Fatal("copied bytes differ from the source")
	}
}

// TestUploadRefusesUnknownSizeSourceOverBlockLimit checks the guard for a
// source whose size was not known up front, such as a download that reported
// no length.
func TestUploadRefusesUnknownSizeSourceOverBlockLimit(t *testing.T) {
	limits := blockLimits{maxBlocks: 2, maxBlockSize: 6 * mib}
	for _, buffer := range []bool{false, true} {
		t.Run("buffer="+strconv.FormatBool(buffer), func(t *testing.T) {
			store := newFakeStore()
			src := bytesSource(make([]byte, 8*mib+1), -1)
			src.buffer = buffer
			if err := limits.upload(context.Background(), store, "blob", src, ""); !errors.Is(err, errTooLarge) {
				t.Fatalf("upload = %v, want errTooLarge", err)
			}
			if len(store.blobs) != 0 {
				t.Fatal("upload committed a blob")
			}
			src = bytesSource(make([]byte, 8*mib), -1)
			src.buffer = buffer
			if err := limits.upload(context.Background(), store, "blob", src, ""); err != nil {
				t.Fatalf("upload of two minimum blocks: %v", err)
			}
		})
	}
}

// heldBytes tracks bytes read from a source and not yet staged.
type heldBytes struct {
	current atomic.Int64
	max     atomic.Int64
}

func (h *heldBytes) add(n int64) {
	current := h.current.Add(n)
	for {
		max := h.max.Load()
		if current <= max || h.max.CompareAndSwap(max, current) {
			return
		}
	}
}

type heldReader struct {
	io.Reader
	held *heldBytes
}

func (r heldReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.held.add(int64(n))
	return n, err
}

type heldStore struct {
	*fakeStore
	held    *heldBytes
	started chan<- struct{}
	gate    <-chan struct{}
}

func (s heldStore) StageBlock(ctx context.Context, name, blockID string, body io.ReadSeeker, checksum []byte) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.gate
	size, err := bodySize(body)
	if err != nil {
		return err
	}
	err = s.fakeStore.StageBlock(ctx, name, blockID, body, checksum)
	s.held.add(-size)
	return err
}

// TestUploadHoldsAtMostBufferBudget checks that an upload that buffers never
// holds more than bufferBudget bytes read and not yet staged: while every
// slot's stage is in flight, the reader does not read the next block.
func TestUploadHoldsAtMostBufferBudget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limits blockLimits
		size   int64
		want   int64
	}{
		{"minimum blocks", azureLimits, 16 * 4 * mib, 8 * 4 * mib},
		{"largest buffered block", blockLimits{maxBlocks: 2, maxBlockSize: 64 * mib}, 2 * 32 * mib, 32 * mib},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockSize, err := tc.limits.blockSize(tc.size)
			if err != nil {
				t.Fatal(err)
			}
			slots := bufferSlots(blockSize)
			held := &heldBytes{}
			started := make(chan struct{}, slots)
			gate := make(chan struct{})
			store := heldStore{fakeStore: newFakeStore(), held: held, started: started, gate: gate}
			source := blockSource{
				r:      heldReader{Reader: io.LimitReader(zeroReader{}, tc.size), held: held},
				size:   tc.size,
				buffer: true,
				reread: func(context.Context, int64, int64) io.ReadSeekCloser {
					panic("an upload that buffers read a block twice")
				},
			}
			done := make(chan error, 1)
			go func() {
				done <- tc.limits.upload(context.Background(), store, "blob", source, "")
			}()
			// Hold every slot's stage open, then give the reader time to
			// read past the slots if it would.
			for range slots {
				<-started
			}
			time.Sleep(100 * time.Millisecond)
			close(gate)
			if err := <-done; err != nil {
				t.Fatalf("upload: %v", err)
			}
			if got := held.max.Load(); got > tc.want {
				t.Fatalf("upload held %d MiB at once, want at most %d MiB", got/mib, tc.want/mib)
			}
			if got := len(store.blobs["blob"].data); int64(got) != tc.size {
				t.Fatalf("committed %d bytes, want %d", got, tc.size)
			}
		})
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// streamStore is a Store that keeps no staged bytes, so an upload's
// allocations show whether the upload, not the store, held its blocks. Each
// StageBlock reads half its body, seeks back to the start and reads it all,
// as the SDK does when it retries, and checks the MD5 the way Azure does.
// It fails unless overlap stages are in flight at once.
type streamStore struct {
	overlap    int
	started    atomic.Int32
	allStarted chan struct{}
	rangeReads atomic.Int32

	mu     sync.Mutex
	blobs  map[string][]byte
	digest map[string]string
	blocks map[int]int64
	ids    []string
}

func newStreamStore(overlap int) *streamStore {
	return &streamStore{
		overlap:    overlap,
		allStarted: make(chan struct{}),
		blobs:      map[string][]byte{},
		digest:     map[string]string{},
		blocks:     map[int]int64{},
	}
}

func (s *streamStore) StageBlock(ctx context.Context, _, blockID string, body io.ReadSeeker, checksum []byte) error {
	if int(s.started.Add(1)) == s.overlap {
		close(s.allStarted)
	}
	select {
	case <-s.allStarted:
	case <-time.After(10 * time.Second):
		return errors.New("stages did not overlap")
	case <-ctx.Done():
		return ctx.Err()
	}
	size, err := bodySize(body)
	if err != nil {
		return err
	}
	if _, err := io.CopyN(io.Discard, body, size/2); err != nil {
		return err
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	hash := md5.New()
	n, err := io.Copy(hash, body)
	if err != nil {
		return err
	}
	if n != size || !bytes.Equal(hash.Sum(nil), checksum) {
		return fmt.Errorf("block %s does not match its MD5", blockID)
	}
	raw, err := base64.StdEncoding.DecodeString(blockID)
	if err != nil {
		return err
	}
	index, err := strconv.Atoi(string(raw[:8]))
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocks[index] = n
	return nil
}

func (s *streamStore) CommitBlockList(_ context.Context, name string, blockIDs []string, metadata map[string]*string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = blockIDs
	s.digest[name] = *metadata[integrityMetadata]
	return nil
}

func (s *streamStore) Download(_ context.Context, name string) (blobstore.Download, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.blobs[name]
	digest := s.digest[name]
	return blobstore.Download{
		Body:     io.NopCloser(bytes.NewReader(data)),
		Size:     int64(len(data)),
		Metadata: map[string]*string{integrityMetadata: &digest},
	}, nil
}

func (s *streamStore) DownloadRange(_ context.Context, name string, offset, count int64) (io.ReadCloser, error) {
	s.rangeReads.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return io.NopCloser(bytes.NewReader(s.blobs[name][offset : offset+count])), nil
}

func (s *streamStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for name := range s.blobs {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	return names, nil
}

func (s *streamStore) Delete(context.Context, string) error { return nil }

// allocated returns the bytes the process allocates while f runs.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// streamedLimits make an 80 MiB file two 40 MiB blocks, each larger than
// bufferBudget.
var streamedLimits = blockLimits{maxBlocks: 2, maxBlockSize: 64 * mib}

const (
	streamedSize  = 80 * mib
	streamedBlock = 40 * mib
	// streamedAllocBound is far below one 40 MiB block, so an upload that
	// held a block in memory fails it.
	streamedAllocBound = 8 * mib
)

func patterned(size int) []byte {
	data := make([]byte, size)
	for i := 0; i < size; i += 4096 {
		data[i] = byte(i / 4096)
	}
	return data
}

// TestUploadSnapshotStreamsLargeBlocks checks that UploadSnapshot stages
// blocks larger than bufferBudget from the file without holding them in
// memory, with stages still running concurrently, and that a body sought back
// to its start, as the SDK does to retry, sends the block again.
func TestUploadSnapshotStreamsLargeBlocks(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	data := patterned(streamedSize)
	if err := os.WriteFile(filepath.Join(source, "memory.img"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	store := newStreamStore(2)
	server, err := New(map[string]blobstore.Store{"snapshots": store}, "tenant", root)
	if err != nil {
		t.Fatal(err)
	}
	server.limits = streamedLimits
	req := &objectstorev1.UploadSnapshotRequest{SnapshotUri: testURI, LocalPath: source, Files: []string{"memory.img"}}

	var uploadErr error
	alloc := allocated(func() { _, uploadErr = server.UploadSnapshot(context.Background(), req) })
	if uploadErr != nil {
		t.Fatalf("UploadSnapshot: %v", uploadErr)
	}
	t.Logf("UploadSnapshot allocated %.1f MiB", float64(alloc)/mib)
	if alloc > streamedAllocBound {
		t.Errorf("UploadSnapshot allocated %d MiB for %d MiB blocks, want at most %d MiB", alloc/mib, streamedBlock/mib, streamedAllocBound/mib)
	}
	if store.blocks[0] != streamedBlock || store.blocks[1] != streamedBlock || len(store.ids) != 2 {
		t.Fatalf("staged blocks %v and committed %d, want two of %d MiB", store.blocks, len(store.ids), streamedBlock/mib)
	}
	if got := store.digest["tenant/actors/a/snapshots/one/memory.img"]; got != want {
		t.Fatalf("committed sha256 %s, want %s", got, want)
	}
}

// TestCopySnapshotStreamsLargeBlocks checks that CopySnapshot stages blocks
// larger than bufferBudget from ranged downloads of the source, without
// holding them in memory.
func TestCopySnapshotStreamsLargeBlocks(t *testing.T) {
	data := patterned(streamedSize)
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	store := newStreamStore(2)
	store.blobs["tenant/actors/a/snapshots/one/memory.img"] = data
	store.digest["tenant/actors/a/snapshots/one/memory.img"] = want
	server, err := New(map[string]blobstore.Store{"snapshots": store}, "tenant", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server.limits = streamedLimits
	req := &objectstorev1.CopySnapshotRequest{SrcUri: testURI, DstUri: "azblob://snapshots/tags/stable"}

	var copyErr error
	alloc := allocated(func() { _, copyErr = server.CopySnapshot(context.Background(), req) })
	if copyErr != nil {
		t.Fatalf("CopySnapshot: %v", copyErr)
	}
	t.Logf("CopySnapshot allocated %.1f MiB", float64(alloc)/mib)
	if alloc > streamedAllocBound {
		t.Errorf("CopySnapshot allocated %d MiB for %d MiB blocks, want at most %d MiB", alloc/mib, streamedBlock/mib, streamedAllocBound/mib)
	}
	if store.blocks[0] != streamedBlock || store.blocks[1] != streamedBlock || len(store.ids) != 2 {
		t.Fatalf("staged blocks %v and committed %d, want two of %d MiB", store.blocks, len(store.ids), streamedBlock/mib)
	}
	// Each block opens a range, and again after the seek back to its start.
	if got := store.rangeReads.Load(); got != 4 {
		t.Errorf("ranged downloads = %d, want 4", got)
	}
	if got := store.digest["tenant/tags/stable/memory.img"]; got != want {
		t.Fatalf("committed sha256 %s, want %s", got, want)
	}
}

// changingFileStore rewrites the start of a file before it stages the first
// block, as if the file changed between the read that hashed the block and
// the read that stages it.
type changingFileStore struct {
	*fakeStore
	path string
	once sync.Once
}

func (s *changingFileStore) StageBlock(ctx context.Context, name, blockID string, body io.ReadSeeker, checksum []byte) error {
	var err error
	s.once.Do(func() {
		var f *os.File
		if f, err = os.OpenFile(s.path, os.O_WRONLY, 0); err == nil {
			_, err = f.WriteAt([]byte("changed"), 0)
			err = errors.Join(err, f.Close())
		}
	})
	if err != nil {
		return err
	}
	return s.fakeStore.StageBlock(ctx, name, blockID, body, checksum)
}

// TestUploadSnapshotFailsWhenFileChangesWhileStaging checks that a block
// whose bytes changed after they were hashed fails its stage, through the MD5
// the block is staged with, and nothing is committed.
func TestUploadSnapshotFailsWhenFileChangesWhileStaging(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(source, "memory.img")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 2*minBlockSize), 0o600); err != nil {
		t.Fatal(err)
	}
	store := &changingFileStore{fakeStore: newFakeStore(), path: path}
	server, err := New(map[string]blobstore.Store{"snapshots": store}, "tenant", root)
	if err != nil {
		t.Fatal(err)
	}
	req := &objectstorev1.UploadSnapshotRequest{SnapshotUri: testURI, LocalPath: source, Files: []string{"memory.img"}}
	if _, err := server.UploadSnapshot(context.Background(), req); err == nil || !strings.Contains(err.Error(), "MD5") {
		t.Fatalf("UploadSnapshot = %v, want an MD5 mismatch", err)
	}
	if len(store.blobs) != 0 {
		t.Fatal("UploadSnapshot committed a blob")
	}
}

func TestRangeBody(t *testing.T) {
	store := newFakeStore()
	store.blobs["blob"] = fakeBlob{data: []byte("0123456789")}
	ctx := context.Background()

	body := &rangeBody{ctx: ctx, store: store, name: "blob", off: 2, n: 5}
	if size, err := body.Seek(0, io.SeekEnd); err != nil || size != 5 {
		t.Fatalf("Seek(0, End) = %d, %v, want 5", size, err)
	}
	if body.body != nil {
		t.Fatal("Seek opened a download")
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 2)
	if _, err := io.ReadFull(body, head); err != nil || string(head) != "23" {
		t.Fatalf("first read = %q, %v, want 23", head, err)
	}
	if pos, err := body.Seek(-1, io.SeekCurrent); err != nil || pos != 1 {
		t.Fatalf("Seek(-1, Current) = %d, %v, want 1", pos, err)
	}
	if rest, err := io.ReadAll(body); err != nil || string(rest) != "3456" {
		t.Fatalf("read after seek = %q, %v, want 3456", rest, err)
	}
	if _, err := body.Seek(-1, io.SeekStart); err == nil {
		t.Error("Seek before the start succeeded")
	}
	if _, err := body.Seek(0, 7); err == nil {
		t.Error("Seek with an invalid whence succeeded")
	}
	if err := errors.Join(body.Close(), body.Close()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	short := &rangeBody{ctx: ctx, store: store, name: "blob", off: 8, n: 5}
	if _, err := io.ReadAll(short); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("read past the blob's end = %v, want ErrUnexpectedEOF", err)
	}
	missing := &rangeBody{ctx: ctx, store: store, name: "missing", off: 0, n: 1}
	if _, err := io.ReadAll(missing); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("read of a missing blob = %v, want ErrNotFound", err)
	}
}
