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
