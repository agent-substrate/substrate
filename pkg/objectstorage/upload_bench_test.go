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
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/api/option"
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

type delayedGCSTransport struct{}

func (delayedGCSTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	reply := func(body string) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    r,
		}
	}
	switch {
	case r.URL.Query().Get("uploadType") == "resumable":
		var metadata struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil {
			return nil, err
		}
		response := reply("")
		response.Header.Set("Location", "https://upload.test/session/"+url.PathEscape(metadata.Name))
		return response, nil
	case strings.HasPrefix(r.URL.Path, "/session/"), r.URL.Query().Get("uploadType") == "multipart":
		name := strings.TrimPrefix(r.URL.Path, "/session/")
		if r.URL.Query().Get("uploadType") == "multipart" {
			name = r.URL.Query().Get("name")
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return reply(fmt.Sprintf(`{"name":%q,"bucket":"bucket"}`, name)), nil
	case strings.HasSuffix(r.URL.Path, "/compose"):
		return reply(`{"name":"composed","bucket":"bucket"}`), nil
	case r.Method == http.MethodDelete:
		return reply(""), nil
	default:
		return nil, fmt.Errorf("unexpected GCS request: %s %s", r.Method, r.URL)
	}
}

// BenchmarkConcurrentSnapshotUploads compares the shared compression budget with
// admitting all 80 uploads at once, and measures the full GCS single-upload path
// under the default 512 MiB budget. It samples peak heap, not cumulative allocation.
func BenchmarkConcurrentSnapshotUploads(b *testing.B) {
	const files = 80
	path := filepath.Join(b.TempDir(), "metadata")
	if err := os.WriteFile(path, make([]byte, 36), 0o600); err != nil {
		b.Fatal(err)
	}
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(logger)

	gcsStore, err := NewGCSClient(b.Context(),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{Transport: delayedGCSTransport{}}))
	if err != nil {
		b.Fatal(err)
	}
	g := gcsStore.(*gcsClient)
	defer func() {
		g.client.Close()
		for _, c := range g.pool {
			c.Close()
		}
	}()

	for _, tc := range []struct {
		name     string
		client   ObjectStorage
		capacity int64
	}{
		{"shared_512MiB", delayedSnapshotStore{}, DefaultUploadMemory},
		{"all_80_admitted", delayedSnapshotStore{}, files * parallelWorkerMemory},
		{"gcs_shared_512MiB", gcsStore, DefaultUploadMemory},
	} {
		b.Run(tc.name, func(b *testing.B) {
			u, err := NewUploader(tc.client, tc.capacity)
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

// BenchmarkLargeSparseUpload verifies that large sparse memory images (both
// multi-chunk streaming and multi-part GCS ranged uploads) keep their full
// chunk sizes and throughput, and measures a full 7-file gVisor snapshot
// (6 small files + 1 large pages.img) under the 512 MiB default budget.
func BenchmarkLargeSparseUpload(b *testing.B) {
	dir := b.TempDir()
	largePath := filepath.Join(dir, "pages.img")
	file, err := os.Create(largePath)
	if err != nil {
		b.Fatal(err)
	}
	block := bytes.Repeat([]byte{0x5a}, 1<<20)
	const populatedMiB = 256
	for range populatedMiB {
		if _, err := file.Write(block); err != nil {
			b.Fatal(err)
		}
	}
	if err := file.Truncate(2 << 30); err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}

	var smallPaths []string
	for i := range 6 {
		p := filepath.Join(dir, fmt.Sprintf("small-%d.img", i))
		if err := os.WriteFile(p, bytes.Repeat([]byte{byte(i + 1)}, 64<<10), 0o600); err != nil {
			b.Fatal(err)
		}
		smallPaths = append(smallPaths, p)
	}

	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(logger)

	gcsStore, err := NewGCSClient(b.Context(),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{Transport: delayedGCSTransport{}}))
	if err != nil {
		b.Fatal(err)
	}
	g := gcsStore.(*gcsClient)
	defer func() {
		g.client.Close()
		for _, c := range g.pool {
			c.Close()
		}
	}()

	b.Run("large_256MiB_only", func(b *testing.B) {
		u, err := NewUploader(gcsStore, DefaultUploadMemory)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(populatedMiB << 20)
		b.ResetTimer()
		for range b.N {
			f, err := os.Open(largePath)
			if err != nil {
				b.Fatal(err)
			}
			err = u.SendFile(b.Context(), "gs://bucket/pages.img", f)
			f.Close()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("4_actors_gvisor_7files_each", func(b *testing.B) {
		u, err := NewUploader(gcsStore, DefaultUploadMemory)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for range b.N {
			var wg sync.WaitGroup
			for actor := range 4 {
				for i, p := range smallPaths {
					wg.Go(func() {
						f, err := os.Open(p)
						if err != nil {
							b.Error(err)
							return
						}
						defer f.Close()
						if err := u.SendFile(b.Context(), fmt.Sprintf("gs://bucket/a%d-small-%d.img", actor, i), f); err != nil {
							b.Error(err)
						}
					})
				}
				wg.Go(func() {
					f, err := os.Open(largePath)
					if err != nil {
						b.Error(err)
						return
					}
					defer f.Close()
					if err := u.SendFile(b.Context(), fmt.Sprintf("gs://bucket/a%d-pages.img", actor), f); err != nil {
						b.Error(err)
					}
				})
			}
			wg.Wait()
		}
	})
}
