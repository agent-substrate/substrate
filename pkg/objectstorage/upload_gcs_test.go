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
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

type slowGCSTransport struct {
	parts   chan string
	release <-chan struct{}
}

func (s *slowGCSTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	reply := func(body string) *http.Response {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}
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
		s.parts <- name
		select {
		case <-s.release:
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

func TestUploaderSlowGCSComposite(t *testing.T) {
	// Use real GCS writers and the real composite part pool, replacing only
	// HTTP transport so every part can be held without wall-clock sleeps.
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		transport := &slowGCSTransport{parts: make(chan string, 16), release: gate}
		client, err := NewGCSClient(t.Context(), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: transport}))
		if err != nil {
			t.Fatal(err)
		}
		g := client.(*gcsClient)
		defer func() {
			g.client.Close()
			for _, c := range g.pool {
				c.Close()
			}
		}()
		u, err := NewUploader(client, uploadBufferMemory(client)+parallelWorkerMemory)
		if err != nil {
			t.Fatal(err)
		}
		send := func(name string) <-chan error {
			done := make(chan error, 1)
			go func() {
				// A raw body just over the lookahead requires three actual
				// composite parts, each with the SDK's retry buffer.
				body := io.LimitReader(rand.New(rand.NewSource(2553)), uploadCompositeMin+1)
				done <- u.Send(context.Background(), "gs://bucket/"+name, body)
			}()
			return done
		}
		first := send("first")
		synctest.Wait()
		if got := len(transport.parts); got != 3 {
			t.Errorf("started %d GCS parts, want 3", got)
		}
		next := send("next")
		synctest.Wait()
		if got := len(transport.parts); got != 3 {
			t.Errorf("next upload started before the first released its buffers: %d parts", got)
		}
		close(gate)
		synctest.Wait()
		if err := <-first; err != nil {
			t.Error(err)
		}
		if err := <-next; err != nil {
			t.Error(err)
		}
		if got := len(transport.parts); got != 6 {
			t.Errorf("uploaded %d parts, want 6", got)
		}
	})
}

func TestGCSSparseUploadHonorsReservedParts(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "memory"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	block := bytes.Repeat([]byte{0x5a}, 1<<20)
	for range 3 * sparsePartTarget / len(block) {
		if _, err := file.Write(block); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Truncate(2 << 30); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		transport := &slowGCSTransport{parts: make(chan string, 8), release: gate}
		client, err := NewGCSClient(t.Context(), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: transport}))
		if err != nil {
			t.Fatal(err)
		}
		g := client.(*gcsClient)
		defer func() {
			g.client.Close()
			for _, c := range g.pool {
				c.Close()
			}
		}()
		done := make(chan error, 1)
		go func() { _, err := g.PutSparseFile(t.Context(), "bucket", "memory", file, 2); done <- err }()
		synctest.Wait()
		if got := len(transport.parts); got != 2 {
			t.Errorf("started %d sparse parts, want reserved limit 2", got)
		}
		close(gate)
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestUploaderGCSSnapshotBurstFitsDefaultBudget(t *testing.T) {
	dir := t.TempDir()
	var smallFiles []*os.File
	for i := range 6 {
		path := filepath.Join(dir, fmt.Sprintf("small-%d.img", i))
		writeSparseSource(t, path, 2<<30, []region{{0, 64 << 10, byte(i + 1)}})
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		smallFiles = append(smallFiles, f)
	}
	pagesPath := filepath.Join(dir, "pages.img")
	writeSparseSource(t, pagesPath, 2<<30, []region{{0, 2 * sparsePartTarget, 0x7f}})
	pages, err := os.Open(pagesPath)
	if err != nil {
		t.Fatal(err)
	}
	defer pages.Close()

	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		transport := &slowGCSTransport{parts: make(chan string, 16), release: gate}
		client, err := NewGCSClient(t.Context(), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: transport}))
		if err != nil {
			t.Fatal(err)
		}
		g := client.(*gcsClient)
		defer func() {
			g.client.Close()
			for _, c := range g.pool {
				c.Close()
			}
		}()
		u, err := NewUploader(client, DefaultUploadMemory)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, len(smallFiles)+1)
		for i, f := range smallFiles {
			go func() {
				done <- u.SendFile(t.Context(), fmt.Sprintf("gs://bucket/small-%d.img", i), f)
			}()
		}
		go func() {
			done <- u.SendFile(t.Context(), "gs://bucket/pages.img", pages)
		}()
		synctest.Wait()
		// All 6 small files and both ranged parts of pages.img must fit in
		// the 512 MiB default budget simultaneously without queueing.
		if got, want := len(transport.parts), len(smallFiles)+2; got != want {
			t.Errorf("started %d concurrent GCS uploads, want %d", got, want)
		}
		close(gate)
		synctest.Wait()
		for range len(smallFiles) + 1 {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestSingleUploadChunkSize(t *testing.T) {
	const unit = int64(googleapi.MinUploadChunkSize)
	for _, tc := range []struct {
		size int64
		want int64
	}{
		{-1, unit},
		{0, unit},
		{1, unit},
		{unit - 1, unit},
		{unit, 2 * unit},
		{unit + 1, 2 * unit},
		{uploadCompositeMin - 1, uploadCompositeMin},
	} {
		got := singleUploadChunkSize(tc.size)
		if got != tc.want {
			t.Errorf("singleUploadChunkSize(%d) = %d, want %d", tc.size, got, tc.want)
		}
		if got <= max(tc.size, 0) {
			t.Errorf("singleUploadChunkSize(%d) = %d, must be strictly greater than size", tc.size, got)
		}
		if got%unit != 0 {
			t.Errorf("singleUploadChunkSize(%d) = %d, must be a multiple of %d", tc.size, got, unit)
		}
	}

	// An object whose size is an exact multiple of MinUploadChunkSize (256 KiB)
	// must still hit EOF inside PutObject's lookahead and take the single-request
	// path rather than spilling into composite parts.
	closed := make(chan struct{})
	close(closed)
	transport := &slowGCSTransport{parts: make(chan string, 4), release: closed}
	client, err := NewGCSClient(t.Context(), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatal(err)
	}
	g := client.(*gcsClient)
	defer func() {
		g.client.Close()
		for _, c := range g.pool {
			c.Close()
		}
	}()
	payload := bytes.Repeat([]byte{0xab}, int(unit))
	body := boundedReader{Reader: bytes.NewReader(payload), max: unit}
	if err := client.PutObject(t.Context(), "bucket", "exact-256k", body); err != nil {
		t.Fatal(err)
	}
	if got := len(transport.parts); got != 1 {
		t.Errorf("256 KiB object sent %d requests, want 1", got)
	}
}

type wrappedReader struct {
	r io.Reader
}

func (w wrappedReader) Read(p []byte) (int, error) { return w.r.Read(p) }
func (w wrappedReader) Unwrap() io.Reader          { return w.r }

type lookaheadCaptureStore struct {
	lookaheads []int64
}

func (*lookaheadCaptureStore) supportsStreamingPut() {}
func (*lookaheadCaptureStore) GetObject(context.Context, string, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("unused")
}
func (s *lookaheadCaptureStore) PutObject(_ context.Context, _, _ string, r io.Reader) error {
	// Wrap the reader before checking singleUploadLookahead to verify the size
	// bound survives decorator unwrapping through the public upload paths.
	s.lookaheads = append(s.lookaheads, singleUploadLookahead(wrappedReader{r: r}))
	_, err := io.Copy(io.Discard, r)
	return err
}

func TestSingleUploadLookaheadThroughPublicUploaders(t *testing.T) {
	dir := t.TempDir()
	sparsePath := filepath.Join(dir, "small.img")
	writeSparseSource(t, sparsePath, 2<<30, []region{{0, 64 << 10, 0x33}})
	sparseFile, err := os.Open(sparsePath)
	if err != nil {
		t.Fatal(err)
	}
	defer sparseFile.Close()

	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	manifestFile, err := os.Open(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	defer manifestFile.Close()

	store := &lookaheadCaptureStore{}
	u, err := NewUploader(store, DefaultUploadMemory)
	if err != nil {
		t.Fatal(err)
	}
	if err := u.SendFile(t.Context(), "gs://bucket/small.img", sparseFile); err != nil {
		t.Fatal(err)
	}
	if err := u.Send(t.Context(), "gs://bucket/manifest.json", manifestFile); err != nil {
		t.Fatal(err)
	}
	const want = int64(googleapi.MinUploadChunkSize)
	if len(store.lookaheads) != 2 || store.lookaheads[0] != want || store.lookaheads[1] != want {
		t.Errorf("captured lookaheads = %v, want [%d %d]", store.lookaheads, want, want)
	}
}
