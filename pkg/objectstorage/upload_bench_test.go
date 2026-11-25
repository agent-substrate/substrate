// Copyright 2026 The Agent Substrate Authors
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
	"errors"
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
	"testing/synctest"
	"time"

	"golang.org/x/time/rate"
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

type pacedGCSTransportBody struct {
	body    io.ReadCloser
	ctx     context.Context
	limiter *rate.Limiter
}

func (b *pacedGCSTransportBody) Read(p []byte) (int, error) {
	if len(p) > 64<<10 {
		p = p[:64<<10]
	}
	n, err := b.body.Read(p)
	if n > 0 {
		if waitErr := b.limiter.WaitN(b.ctx, n); waitErr != nil {
			return 0, waitErr
		}
	}
	return n, err
}

func (b *pacedGCSTransportBody) Close() error {
	return b.body.Close()
}

func (delayedGCSTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var requestBody io.Reader
	if r.Body != nil {
		defer r.Body.Close()
		requestBody = &pacedGCSTransportBody{
			body:    r.Body,
			ctx:     r.Context(),
			limiter: rate.NewLimiter(rate.Limit(90<<20), 64<<10),
		}
	}
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
	if err := r.Context().Err(); err != nil {
		return nil, err
	}

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
		if requestBody == nil {
			return nil, fmt.Errorf("resumable request has no body")
		}
		var metadata struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(requestBody).Decode(&metadata); err != nil {
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
		if requestBody != nil {
			if _, err := io.Copy(io.Discard, requestBody); err != nil {
				return nil, err
			}
		}
		if strings.HasPrefix(r.URL.Path, "/session/") && strings.HasSuffix(r.Header.Get("Content-Range"), "/*") {
			response := reply("")
			response.Header.Set("X-Http-Status-Code-Override", "308")
			return response, nil
		}
		return reply(fmt.Sprintf(`{"name":%q,"bucket":"bucket"}`, name)), nil
	case strings.HasSuffix(r.URL.Path, "/compose"):
		if requestBody != nil {
			if _, err := io.Copy(io.Discard, requestBody); err != nil {
				return nil, err
			}
		}
		return reply(`{"name":"composed","bucket":"bucket"}`), nil
	case r.Method == http.MethodDelete:
		if requestBody != nil {
			if _, err := io.Copy(io.Discard, requestBody); err != nil {
				return nil, err
			}
		}
		return reply(""), nil
	default:
		return nil, fmt.Errorf("unexpected GCS request: %s %s", r.Method, r.URL)
	}
}

type trackedRequestBody struct {
	*strings.Reader
	readBytes int
	closed    bool
}

func (b *trackedRequestBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.readBytes += n
	return n, err
}

func (b *trackedRequestBody) Close() error {
	b.closed = true
	return nil
}

type notifyAfterPacingBurstBody struct {
	*strings.Reader
	readBytes int
	pastBurst chan struct{}
	notified  bool
	closed    bool
}

func (b *notifyAfterPacingBurstBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if n > 0 {
		b.readBytes += n
		if b.readBytes > 64<<10 && !b.notified {
			close(b.pastBurst)
			b.notified = true
		}
	}
	return n, err
}

func (b *notifyAfterPacingBurstBody) Close() error {
	b.closed = true
	return nil
}

func TestDelayedGCSTransportResumableChunks(t *testing.T) {
	for _, tc := range []struct {
		name          string
		method        string
		target        string
		contentRange  string
		wantOverride  string
		wantMetadata  bool
		wantBodyBytes int
	}{
		{
			name:          "intermediate resumable chunk",
			method:        http.MethodPost,
			target:        "https://upload.test/session/file",
			contentRange:  "bytes 0-3/*",
			wantOverride:  "308",
			wantBodyBytes: 4,
		},
		{
			name:          "final resumable chunk",
			method:        http.MethodPost,
			target:        "https://upload.test/session/file",
			contentRange:  "bytes 4-7/8",
			wantMetadata:  true,
			wantBodyBytes: 4,
		},
		{
			name:          "multipart upload",
			method:        http.MethodPost,
			target:        "https://storage.test/upload?uploadType=multipart&name=file",
			wantMetadata:  true,
			wantBodyBytes: 4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				body := &trackedRequestBody{Reader: strings.NewReader("data")}
				req, err := http.NewRequestWithContext(t.Context(), tc.method, tc.target, body)
				if err != nil {
					t.Fatal(err)
				}
				if tc.contentRange != "" {
					req.Header.Set("Content-Range", tc.contentRange)
					req.Header.Set("X-GUploader-No-308", "yes")
				}

				resp, err := (delayedGCSTransport{}).RoundTrip(req)
				if err != nil {
					t.Fatal(err)
				}
				if body.readBytes != tc.wantBodyBytes {
					t.Errorf("request body bytes = %d, want %d", body.readBytes, tc.wantBodyBytes)
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
				}
				if got := resp.Header.Get("X-Http-Status-Code-Override"); got != tc.wantOverride {
					t.Errorf("status override = %q, want %q", got, tc.wantOverride)
				}
				if tc.wantMetadata {
					var metadata struct {
						Name   string `json:"name"`
						Bucket string `json:"bucket"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
						t.Fatal(err)
					}
					if metadata.Name != "file" || metadata.Bucket != "bucket" {
						t.Errorf("metadata = %+v, want name=file bucket=bucket", metadata)
					}
				}
				if !body.closed {
					t.Error("request body was not closed")
				}
				if req.Body != body {
					t.Error("RoundTrip replaced the request body")
				}
			})
		})
	}

	t.Run("canceled request closes body", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			body := &trackedRequestBody{Reader: strings.NewReader("payload")}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://upload.test/session/file", body)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := (delayedGCSTransport{}).RoundTrip(req)
			if resp != nil {
				resp.Body.Close()
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("error = %v, want context.Canceled", err)
			}
			if !body.closed {
				t.Error("request body was not closed")
			}
			if req.Body != body {
				t.Error("RoundTrip replaced the request body")
			}
		})
	})
}

func TestDelayedGCSTransportRttAndPacing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		body := &trackedRequestBody{Reader: strings.NewReader(strings.Repeat("x", 128<<10))}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://upload.test/session/file", body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Range", "bytes 0-131071/*")

		started := time.Now()
		resp, err := (delayedGCSTransport{}).RoundTrip(req)
		elapsed := time.Since(started)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if elapsed < 20*time.Millisecond+500*time.Microsecond || elapsed > 21*time.Millisecond {
			t.Errorf("128 KiB request took %v, want RTT plus one paced chunk", elapsed)
		}
	})
}

func TestDelayedGCSTransportPacesConcurrentRequestsIndependently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		type result struct {
			elapsed time.Duration
			err     error
		}
		results := make(chan result, 2)
		for range 2 {
			go func() {
				body := &trackedRequestBody{Reader: strings.NewReader(strings.Repeat("x", 128<<10))}
				req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://upload.test/session/file", body)
				if err != nil {
					results <- result{err: err}
					return
				}
				req.Header.Set("Content-Range", "bytes 0-131071/*")
				resp, err := (delayedGCSTransport{}).RoundTrip(req)
				if resp != nil {
					resp.Body.Close()
				}
				results <- result{elapsed: time.Since(started), err: err}
			}()
		}
		synctest.Wait()
		for range 2 {
			got := <-results
			if got.err != nil {
				t.Fatal(got.err)
			}
			if got.elapsed < 20*time.Millisecond+500*time.Microsecond || got.elapsed > 21*time.Millisecond {
				t.Errorf("concurrent request took %v, want independent RTT and pacing", got.elapsed)
			}
		}
	})
}

func TestDelayedGCSTransportCancelsDuringPacing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		body := &notifyAfterPacingBurstBody{
			Reader:    strings.NewReader(strings.Repeat("x", 128<<10)),
			pastBurst: make(chan struct{}),
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://upload.test/session/file", body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Range", "bytes 0-131071/*")

		done := make(chan error, 1)
		go func() {
			resp, err := (delayedGCSTransport{}).RoundTrip(req)
			if resp != nil {
				resp.Body.Close()
			}
			done <- err
		}()

		<-body.pastBurst
		// Wait until RoundTrip blocks in rate.Limiter.WaitN before canceling.
		synctest.Wait()
		cancel()
		synctest.Wait()
		err = <-done
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
		if !body.closed {
			t.Error("request body was not closed after pacing cancellation")
		}
	})
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
			for b.Loop() {
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
	largePaths := make(map[int64]string, 2)
	largePopulatedBytes := make(map[int64]int64, 2)
	for _, populatedBytes := range []int64{80 << 20, 256 << 20} {
		file := writeSnapshotSource(b, b.TempDir(), 2<<30, populatedBytes, 8<<20)
		largePaths[populatedBytes] = file.Name()
		_, actualPopulatedBytes, err := sparseExtents(file, 2<<30)
		if err != nil {
			b.Fatal(err)
		}
		largePopulatedBytes[populatedBytes] = actualPopulatedBytes
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
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

	for _, tc := range []struct {
		name           string
		populatedBytes int64
	}{
		{"large_80MiB_only", 80 << 20},
		{"large_256MiB_only", 256 << 20},
	} {
		b.Run(tc.name, func(b *testing.B) {
			u, err := NewUploader(gcsStore, DefaultUploadMemory)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(largePopulatedBytes[tc.populatedBytes])
			for b.Loop() {
				f, err := os.Open(largePaths[tc.populatedBytes])
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
	}

	b.Run("4_actors_gvisor_7files_each", func(b *testing.B) {
		u, err := NewUploader(gcsStore, DefaultUploadMemory)
		if err != nil {
			b.Fatal(err)
		}
		for b.Loop() {
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
					f, err := os.Open(largePaths[256<<20])
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
