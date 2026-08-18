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

package ategcs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	osscredentials "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

const notFoundBody = `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <RequestId>00000000000000000000000000000000</RequestId>
</Error>`

// newTestOSSClient serves a minimal in-memory OSS: PUT /bucket/object stores
// the body, GET returns it. The SDK switches to path-style addressing
// automatically for IP endpoints, so the plain httptest URL works as-is.
func newTestOSSClient(t *testing.T) *oss.Client {
	t.Helper()
	var mu sync.Mutex
	stored := map[string][]byte{"bucket/object": []byte("hello oss")}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		switch {
		case r.Method == http.MethodPut && key == "bucket/object":
			b, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			stored[key] = b
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && key == "bucket/object":
			mu.Lock()
			b := stored[key]
			mu.Unlock()
			_, _ = w.Write(b)
		case r.Method == http.MethodGet && key == "bucket/missing":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(notFoundBody))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	return oss.NewClient(oss.LoadDefaultConfig().
		WithEndpoint(server.URL).
		WithRegion("cn-test").
		WithCredentialsProvider(osscredentials.NewStaticCredentialsProvider("ak", "sk")).
		WithHttpClient(server.Client()))
}

func TestOSSClientGetObject(t *testing.T) {
	client := NewOSSClient(newTestOSSClient(t))

	rc, err := client.GetObject(context.Background(), "bucket", "object")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello oss" {
		t.Errorf("GetObject = %q, want %q", got, "hello oss")
	}
}

func TestOSSClientGetObjectNotFound(t *testing.T) {
	client := NewOSSClient(newTestOSSClient(t))

	_, err := client.GetObject(context.Background(), "bucket", "missing")
	if !errors.Is(err, ateerrors.ReasonFailedGetExternalObject) {
		t.Errorf("GetObject(missing) error = %v, want it to wrap %v", err, ateerrors.ReasonFailedGetExternalObject)
	}
}

func TestOSSClientPutObjectRoundTrip(t *testing.T) {
	client := NewOSSClient(newTestOSSClient(t))

	want := "snapshot bytes"
	if err := client.PutObject(context.Background(), "bucket", "object", strings.NewReader(want)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	rc, err := client.GetObject(context.Background(), "bucket", "object")
	if err != nil {
		t.Fatalf("GetObject after PutObject: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != want {
		t.Errorf("round-trip = %q, want %q", got, want)
	}
}

func TestOSSClientStreamingPut(t *testing.T) {
	client := NewOSSClient(newTestOSSClient(t))

	// The suspend path pipes the zstd compressor straight into PutObject when
	// the backend advertises streaming support; a regression here silently
	// degrades uploads to the buffered temp-file path.
	if _, ok := client.(streamingPutter); !ok {
		t.Fatal("OSS client does not advertise streaming PutObject support")
	}

	// A plain pipe has no Len() and no seeker: a backend that needs a
	// seekable body would hang or buffer here. The OSS SDK must stream it.
	want := "chunked stream"
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte(want))
		_ = pw.Close()
	}()
	if err := client.PutObject(context.Background(), "bucket", "object", pr); err != nil {
		t.Fatalf("PutObject with non-seekable body: %v", err)
	}

	rc, err := client.GetObject(context.Background(), "bucket", "object")
	if err != nil {
		t.Fatalf("GetObject after streaming PutObject: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != want {
		t.Errorf("round-trip = %q, want %q", got, want)
	}
}
