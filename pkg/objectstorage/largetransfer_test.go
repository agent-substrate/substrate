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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// withLargeTransfers leaves the test n free large-transfer slots by holding the
// others until it ends.
func withLargeTransfers(t *testing.T, n int) {
	t.Helper()
	if len(largeTransfers) != 0 {
		t.Fatalf("%d large-transfer slots still held when the test started", len(largeTransfers))
	}
	held := cap(largeTransfers) - n
	for range held {
		largeTransfers <- struct{}{}
	}
	t.Cleanup(func() {
		for range held {
			<-largeTransfers
		}
	})
}

// blockingStore is an ObjectStorage whose PutObject reports each upload that
// reaches it and then holds it until the test lets it go.
type blockingStore struct {
	started chan string
	finish  chan struct{}
}

func (s *blockingStore) PutObject(ctx context.Context, _, object string, r io.Reader) error {
	s.started <- object
	select {
	case <-s.finish:
	case <-ctx.Done():
		return ctx.Err()
	}
	_, err := io.Copy(io.Discard, r)
	return err
}

func (s *blockingStore) GetObject(context.Context, string, string) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

// sparseFile creates a file of size bytes that is all hole, so a large object
// costs the test no disk.
func sparseFile(t *testing.T, name string, size int64) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestLargeUploadsWaitForASlot(t *testing.T) {
	withLargeTransfers(t, 2)
	store := &blockingStore{started: make(chan string, 4), finish: make(chan struct{})}
	errs := make(chan error, 4)
	upload := func(name string, size int64) {
		f := sparseFile(t, name, size)
		go func() {
			errs <- SendFileToGCSWithZstd(t.Context(), store, "gs://bucket/"+name, f)
		}()
	}

	for i := range 3 {
		upload(fmt.Sprintf("large-%d", i), uploadPartSize+1)
	}
	started := map[string]bool{}
	for range 2 {
		started[<-store.started] = true
	}
	// A small object never waits behind the large ones.
	upload("small", 4096)
	if got := <-store.started; got != "small" {
		t.Fatalf("upload %q started while two large uploads held both slots", got)
	}
	select {
	case got := <-store.started:
		t.Fatalf("upload %q started while two large uploads held both slots", got)
	case <-time.After(200 * time.Millisecond):
	}

	close(store.finish)
	started[<-store.started] = true
	for i := range 3 {
		if !started[fmt.Sprintf("large-%d", i)] {
			t.Fatalf("large-%d never started; started %v", i, started)
		}
	}
	for range 4 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestLargeUploadWaitEndsWithItsContext(t *testing.T) {
	withLargeTransfers(t, 0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	f := sparseFile(t, "large", uploadPartSize+1)
	err := SendFileToGCSWithZstd(ctx, newMemStore(), "gs://bucket/large", f)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SendFileToGCSWithZstd = %v, want context.Canceled", err)
	}
}

func TestRangedReaderHoldsItsSlotUntilClose(t *testing.T) {
	withLargeTransfers(t, 1)
	fetch := func(context.Context, int, int64, int64, []byte) error { return nil }
	first := newRangedReader(t.Context(), downloadChunkSize+1, nil, fetch)
	// The slot is taken before the first range is handed out.
	if _, err := first.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}

	read := make(chan error, 1)
	second := newRangedReader(t.Context(), downloadChunkSize+1, nil, fetch)
	defer second.Close()
	go func() {
		_, err := second.Read(make([]byte, 1))
		read <- err
	}()
	select {
	case err := <-read:
		t.Fatalf("second download read (%v) while the first held the only slot", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second download never got the slot the first one's Close gave back")
	}
}

func TestRangedReaderClosedWhileWaitingGivesNoSlotAway(t *testing.T) {
	withLargeTransfers(t, 0)
	r := newRangedReader(t.Context(), downloadChunkSize+1, nil, func(context.Context, int, int64, int64, []byte) error { return nil })
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read after Close = %v, want context.Canceled", err)
	}
	if len(largeTransfers) != cap(largeTransfers) {
		t.Fatalf("%d slots taken after the waiting download closed, want only the test's %d", len(largeTransfers), cap(largeTransfers))
	}
}
