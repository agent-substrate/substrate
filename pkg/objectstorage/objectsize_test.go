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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
)

// s3ObjectClient serves content as every object, honoring Range headers
// unless ignoreRange is set.
func s3ObjectClient(t *testing.T, content []byte, ignoreRange bool) ObjectStorage {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
		if ignoreRange || rng == "" {
			w.Header().Set("Content-Length", strconv.Itoa(len(content)))
			_, _ = w.Write(content)
			return
		}
		from, to, _ := strings.Cut(rng, "-")
		off, _ := strconv.Atoi(from)
		end, _ := strconv.Atoi(to)
		end = min(end, len(content)-1)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, end, len(content)))
		w.Header().Set("Content-Length", strconv.Itoa(end-off+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[off : end+1])
	}))
	t.Cleanup(srv.Close)
	return NewS3Client(s3.New(s3.Options{
		Region:           "us-east-1",
		Credentials:      credentials.NewStaticCredentialsProvider("ak", "sk", ""),
		BaseEndpoint:     aws.String(srv.URL),
		UsePathStyle:     true,
		RetryMaxAttempts: 1,
	}))
}

func TestS3GetObjectReportsSize(t *testing.T) {
	for _, tc := range []struct {
		name        string
		size        int
		ignoreRange bool
	}{
		{"one range", 10, false},
		{"empty object", 0, true},
		{"range ignored", 10, true},
		{"several ranges", downloadChunkSize + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := bytes.Repeat([]byte("a"), tc.size)
			rc, err := s3ObjectClient(t, content, tc.ignoreRange).GetObject(t.Context(), "bucket", "object")
			if err != nil {
				t.Fatal(err)
			}
			defer rc.Close()
			if n, ok := ObjectSize(rc); !ok || n != int64(tc.size) {
				t.Errorf("ObjectSize() = %d, %t; want %d, true", n, ok, tc.size)
			}
			got, err := io.ReadAll(rc)
			if err != nil || !bytes.Equal(got, content) {
				t.Errorf("read %d bytes, %v; want %d bytes", len(got), err, len(content))
			}
		})
	}
}

func TestGCSGetObjectReportsSize(t *testing.T) {
	content := []byte("micro-vm kernel bytes")
	client, err := storage.NewClient(t.Context(), option.WithHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusPartialContent,
			Header: http.Header{
				"Content-Range": {fmt.Sprintf("bytes 0-%d/%d", len(content)-1, len(content))},
			},
			ContentLength: int64(len(content)),
			Body:          io.NopCloser(bytes.NewReader(content)),
		}, nil
	})}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	rc, err := (&gcsClient{client: client}).GetObject(t.Context(), "bucket", "object")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if n, ok := ObjectSize(rc); !ok || n != int64(len(content)) {
		t.Errorf("ObjectSize() = %d, %t; want %d, true", n, ok, len(content))
	}
}

func TestObjectSizeUnknown(t *testing.T) {
	if n, ok := ObjectSize(io.NopCloser(strings.NewReader("x"))); ok {
		t.Errorf("ObjectSize of a plain reader = %d, true; want false", n)
	}
	if n, ok := ObjectSize(&s3Body{ReadCloser: io.NopCloser(strings.NewReader("x")), size: -1}); ok {
		t.Errorf("ObjectSize of a reader reporting -1 = %d, true; want false", n)
	}
}
