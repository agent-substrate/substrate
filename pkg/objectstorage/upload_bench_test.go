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
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type delayedSnapshotStore struct{}

func (delayedSnapshotStore) supportsStreamingPut() {}
func (delayedSnapshotStore) GetObject(context.Context, string, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("unused")
}
func (delayedSnapshotStore) PutObject(ctx context.Context, _, _ string, r io.Reader) error {
	var header [len(sparseMagic) + 4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	// Keep compression buffers live while the destination applies backpressure.
	select {
	case <-time.After(20 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	_, err := io.Copy(io.Discard, r)
	return err
}

// BenchmarkConcurrentSnapshotUploads compares the shared compression budget with
// admitting all 80 uploads at once. It samples peak heap, not cumulative allocation.
// SDK buffering is covered separately by the slow GCS and S3 tests.
func BenchmarkConcurrentSnapshotUploads(b *testing.B) {
	const files = 80
	path := filepath.Join(b.TempDir(), "metadata")
	if err := os.WriteFile(path, make([]byte, 36), 0o600); err != nil {
		b.Fatal(err)
	}
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(logger)
	for _, tc := range []struct {
		name     string
		capacity int64
	}{
		{"shared_512MiB", DefaultUploadMemory},
		{"all_80_admitted", files * parallelWorkerMemory},
	} {
		b.Run(tc.name, func(b *testing.B) {
			u, err := NewUploader(delayedSnapshotStore{}, tc.capacity)
			if err != nil {
				b.Fatal(err)
			}
			runtime.GC()
			var baseline runtime.MemStats
			runtime.ReadMemStats(&baseline)
			var peak atomic.Uint64
			peak.Store(baseline.HeapAlloc)
			stop, stopped := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(stopped)
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-stop:
						return
					case <-tick.C:
						var stats runtime.MemStats
						runtime.ReadMemStats(&stats)
						if stats.HeapAlloc > peak.Load() {
							peak.Store(stats.HeapAlloc)
						}
					}
				}
			}()
			b.ResetTimer()
			for range b.N {
				var wg sync.WaitGroup
				for i := range files {
					wg.Go(func() {
						f, err := os.Open(path)
						if err != nil {
							b.Error(err)
							return
						}
						defer f.Close()
						if err := u.SendFile(b.Context(), fmt.Sprintf("gs://bucket/file%d", i), f); err != nil {
							b.Error(err)
						}
					})
				}
				wg.Wait()
			}
			b.StopTimer()
			close(stop)
			<-stopped
			b.ReportMetric(float64(peak.Load()-baseline.HeapAlloc)/(1<<20), "peak-heap-MiB")
			b.ReportMetric(float64(files*b.N)/b.Elapsed().Seconds(), "files/s")
		})
	}
}
