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
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
)

func TestUploaderBudgetFitsBothPaths(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "sparse"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(8 << 30); err != nil {
		t.Fatal(err)
	}
	for _, client := range []ObjectStorage{&s3Client{}, &gcsClient{}, newMemStore()} {
		minimum := uploadBufferMemory(client) + parallelWorkerMemory
		if _, err := NewUploader(client, minimum-1); err == nil {
			t.Fatalf("%T accepted a budget smaller than one upload", client)
		}
		for _, capacity := range []int64{minimum, DefaultUploadMemory} {
			u, err := NewUploader(client, capacity)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := u.plan(f)
			if err != nil {
				t.Fatal(err)
			}
			if plan.workers < 1 || plan.workers > runtime.GOMAXPROCS(0) || plan.memory > capacity {
				t.Fatalf("%T: invalid plan %+v for capacity %d", client, plan, capacity)
			}
			if _, ok := client.(sparseFilePutter); ok && plan.parts >= 2 {
				// Only the range-parallel path runs.
				if int64(plan.parts)*sparseUploadMemory > plan.memory {
					t.Fatalf("%T: sparse pipeline under-reserved: %+v", client, plan)
				}
				continue
			}
			// An all-holes file streams a few bytes.
			if plan.memory < streamBufferMemory(client, sparseEncodedBound(0, 0))+int64(plan.workers)*parallelWorkerMemory {
				t.Fatalf("%T: streaming pipeline under-reserved: %+v", client, plan)
			}
		}
	}
}

func TestUploaderPlansFromPopulatedBytes(t *testing.T) {
	previous := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(previous)
	for _, tc := range []struct {
		name         string
		populatedMiB int
		gcs, s3      uploadPlan
	}{
		{"all_holes", 0, uploadPlan{1, 1, 192 << 20}, uploadPlan{1, 1, 224 << 20}},
		{"small", 1, uploadPlan{1, 1, 192 << 20}, uploadPlan{1, 1, 224 << 20}},
		{"two_chunks", 9, uploadPlan{2, 1, 256 << 20}, uploadPlan{2, 1, 288 << 20}},
		{"multiple_ranges", 192, uploadPlan{3, 3, 144 << 20}, uploadPlan{5, 3, 480 << 20}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			offsets := make([]int64, tc.populatedMiB)
			for i := range offsets {
				offsets[i] = int64(i) << 20
			}
			f := sparseFileWith(t, 2<<30, offsets)
			for _, backend := range []struct {
				name   string
				client ObjectStorage
				want   uploadPlan
			}{
				{"gcs", &gcsClient{}, tc.gcs},
				{"s3", &s3Client{}, tc.s3},
			} {
				t.Run(backend.name, func(t *testing.T) {
					u, err := NewUploader(backend.client, DefaultUploadMemory)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.Seek(7, io.SeekStart); err != nil {
						t.Fatal(err)
					}
					got, err := u.plan(f)
					if err != nil {
						t.Fatal(err)
					}
					if got != backend.want {
						t.Errorf("plan = %+v, want %+v", got, backend.want)
					}
					if position, err := f.Seek(0, io.SeekCurrent); err != nil || position != 7 {
						t.Errorf("planning changed file position: position=%d, err=%v", position, err)
					}
				})
			}
		})
	}
}

func TestUploaderReservesOnlyTheChosenPath(t *testing.T) {
	previous := runtime.GOMAXPROCS(192)
	defer runtime.GOMAXPROCS(previous)
	offsets := make([]int64, 292)
	for i := range offsets {
		offsets[i] = int64(i) << 20
	}
	ranged := sparseFileWith(t, 2<<30, offsets)
	small := sparseFileWith(t, 2<<30, []int64{0})
	for _, capacity := range []int64{DefaultUploadMemory, 16 << 30} {
		u, err := NewUploader(&gcsClient{}, capacity)
		if err != nil {
			t.Fatal(err)
		}
		// 292 MiB populated: four ranged parts, whatever the budget or GOMAXPROCS.
		got, err := u.plan(ranged)
		if err != nil {
			t.Fatal(err)
		}
		if got.parts != 4 || got.memory != 4*sparseUploadMemory {
			t.Errorf("capacity %d: ranged plan = %+v, want 4 parts reserving %d", capacity, got, 4*sparseUploadMemory)
		}
		// 1 MiB populated: one request, so PutObject's lookahead, the client's
		// chunk buffer, and one worker.
		got, err = u.plan(small)
		if err != nil {
			t.Fatal(err)
		}
		if want := uploadCompositeMin + uploadChunkSize + parallelWorkerMemory; got.memory != want {
			t.Errorf("capacity %d: small plan = %+v, want %d", capacity, got, want)
		}
	}
}

func TestUploaderSmallSparseFilesShareBudget(t *testing.T) {
	previous := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(previous)
	files := []*os.File{
		sparseFileWith(t, 2<<30, []int64{1 << 30}),
		sparseFileWith(t, 2<<30, []int64{1 << 30}),
	}
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		store := &gatedUploadStore{started: make(chan string, len(files)), release: gate}
		u, err := NewUploader(store, 2*parallelWorkerMemory)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, len(files))
		for _, f := range files {
			go func() { done <- u.SendFile(t.Context(), "gs://bucket/"+filepath.Base(f.Name()), f) }()
		}
		synctest.Wait()
		if got := len(store.started); got != len(files) {
			t.Errorf("started %d uploads, want both small resident sets to fit the budget", got)
		}
		close(gate)
		for range files {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	})
}

type gatedUploadStore struct {
	started chan string
	release <-chan struct{}
	err     error
}

func (*gatedUploadStore) supportsStreamingPut() {}
func (*gatedUploadStore) GetObject(context.Context, string, string) (io.ReadCloser, error) {
	return nil, errors.New("unused")
}
func (s *gatedUploadStore) PutObject(ctx context.Context, _, object string, r io.Reader) error {
	// Drain compression first: the reservation must remain held while the
	// destination is slow even after the producer has finished.
	if _, err := io.Copy(io.Discard, r); err != nil {
		return err
	}
	s.started <- object
	select {
	case <-s.release:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestUploaderWaitCancelAndRelease(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failure], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				gate := make(chan struct{})
				store := &gatedUploadStore{started: make(chan string, 3), release: gate}
				if failure {
					store.err = errors.New("upload failed")
				}
				u, err := NewUploader(store, parallelWorkerMemory)
				if err != nil {
					t.Fatal(err)
				}
				send := func(ctx context.Context, name string) <-chan error {
					done := make(chan error, 1)
					go func() { done <- u.sendZstd(ctx, "gs://bucket/"+name, strings.NewReader("payload")) }()
					return done
				}
				first := send(t.Context(), "first")
				synctest.Wait()
				if got := <-store.started; got != "first" {
					t.Fatal(got)
				}
				ctx, cancel := context.WithCancel(t.Context())
				waiting := send(ctx, "waiting")
				synctest.Wait()
				select {
				case got := <-store.started:
					t.Fatalf("%s bypassed budget", got)
				default:
				}
				cancel()
				synctest.Wait()
				if err := <-waiting; !errors.Is(err, context.Canceled) {
					t.Fatalf("waiting upload: %v", err)
				}
				// Cancellation of a waiter must not release the first upload's reservation.
				next := send(t.Context(), "next")
				synctest.Wait()
				select {
				case got := <-store.started:
					t.Fatalf("%s bypassed held budget", got)
				default:
				}
				close(gate)
				synctest.Wait()
				if err := <-first; !errors.Is(err, store.err) {
					t.Fatalf("first: %v", err)
				}
				if err := <-next; !errors.Is(err, store.err) {
					t.Fatalf("next: %v", err)
				}
				if got := <-store.started; got != "next" {
					t.Fatal(got)
				}
			})
		})
	}
}

func TestUploaderCancellationReleasesActiveReservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		store := &gatedUploadStore{started: make(chan string, 2), release: gate}
		u, err := NewUploader(store, parallelWorkerMemory)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- u.sendZstd(ctx, "gs://bucket/canceled", strings.NewReader("payload")) }()
		synctest.Wait()
		<-store.started
		cancel()
		synctest.Wait()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		close(gate)
		if err := u.sendZstd(t.Context(), "gs://bucket/next", strings.NewReader("payload")); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUploaderReadFailureReleasesReservation(t *testing.T) {
	u, err := NewUploader(newMemStore(), parallelWorkerMemory)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("source failed")
	if err := u.sendZstd(t.Context(), "gs://bucket/bad", failingUploadReader{want}); !errors.Is(err, want) {
		t.Fatalf("got %v, want source error", err)
	}
	if err := u.sendZstd(t.Context(), "gs://bucket/next", strings.NewReader("payload")); err != nil {
		t.Fatal(err)
	}
}

type failingUploadReader struct{ err error }

func (r failingUploadReader) Read([]byte) (int, error) { return 0, r.err }

type earlyUploadFailure struct{ err error }

func (*earlyUploadFailure) supportsStreamingPut() {}
func (*earlyUploadFailure) GetObject(context.Context, string, string) (io.ReadCloser, error) {
	return nil, errors.New("unused")
}
func (s *earlyUploadFailure) PutObject(_ context.Context, _, _ string, r io.Reader) error {
	header := make([]byte, len(sparseMagic)+4)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}
	return s.err
}

func TestUploaderDestinationFailureStopsCompression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	writeSparseSource(t, path, 2<<30, []region{{0, 8 << 20, 0x5a}})
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	want := errors.New("destination failed before payload")
	u, err := NewUploader(&earlyUploadFailure{want}, parallelWorkerMemory)
	if err != nil {
		t.Fatal(err)
	}
	if err := u.SendFile(t.Context(), "gs://bucket/object", f); !errors.Is(err, want) {
		t.Fatalf("got %v, want destination error", err)
	}
	// Compression must stop before reading the full populated extent.
	at, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if at >= 8<<20 {
		t.Fatalf("read the entire extent after destination failure: %d", at)
	}
	if !u.budget.reserved.TryAcquire(u.budget.capacity) {
		t.Fatal("failed upload retained its reservation")
	}
	u.budget.reserved.Release(u.budget.capacity)
}
