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

package objectstore_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
)

// s3Request is one call the SDK made, reduced to what the tests assert on.
type s3Request struct {
	method     string
	path       string
	query      string
	copySource string
	copyRange  string
}

// fakeS3 serves canned responses over HTTP so the SDK does real request
// signing and response deserialization, and records what it was asked for.
type fakeS3 struct {
	// handle, when set, answers a request before the default handling and
	// reports whether it did.
	handle func(w http.ResponseWriter, r *http.Request) bool

	mu       sync.Mutex
	requests []s3Request
}

// newS3 starts a fake S3 and returns it with a Store pointed at it.
func newS3(t *testing.T) (*fakeS3, objectstore.Store) {
	t.Helper()
	fake := &fakeS3{}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	return fake, objectstore.NewS3(s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("ak", "sk", ""),
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		// Disable retry backoff to keep tests fast.
		RetryMaxAttempts: 1,
	}))
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, s3Request{
		method:     r.Method,
		path:       r.URL.Path,
		query:      r.URL.RawQuery,
		copySource: r.Header.Get("x-amz-copy-source"),
		copyRange:  r.Header.Get("x-amz-copy-source-range"),
	})
	f.mu.Unlock()

	if f.handle != nil && f.handle(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	query := r.URL.Query()
	switch {
	case r.Method == http.MethodHead:
		w.Header().Set("Content-Length", "1024")
	case r.Method == http.MethodPost && query.Has("uploads"):
		writeXML(w, `<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == http.MethodPost && query.Has("uploadId"):
		writeXML(w, `<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
	case r.Method == http.MethodPut && query.Has("partNumber"):
		writeXML(w, fmt.Sprintf(`<CopyPartResult><ETag>"part-%s"</ETag></CopyPartResult>`, query.Get("partNumber")))
	case r.Method == http.MethodPut:
		writeXML(w, `<CopyObjectResult><ETag>"copied"</ETag></CopyObjectResult>`)
	case r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

// requests returns what the fake was asked for, in arrival order.
func (f *fakeS3) requestsSeen() []s3Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

func writeXML(w http.ResponseWriter, body string) {
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>` + body))
}

// TestS3List covers a listing that spans pages: every page's keys are returned.
func TestS3List(t *testing.T) {
	fake, store := newS3(t)
	fake.handle = func(w http.ResponseWriter, r *http.Request) bool {
		if !r.URL.Query().Has("list-type") {
			return false
		}
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("continuation-token") == "" {
			writeXML(w, `<ListBucketResult>`+
				`<IsTruncated>true</IsTruncated><NextContinuationToken>page-2</NextContinuationToken>`+
				`<Contents><Key>root/snap/manifest.json</Key></Contents>`+
				`</ListBucketResult>`)
			return true
		}
		writeXML(w, `<ListBucketResult>`+
			`<IsTruncated>false</IsTruncated>`+
			`<Contents><Key>root/snap/memory.zst</Key></Contents>`+
			`</ListBucketResult>`)
		return true
	}

	objects, err := store.List(t.Context(), "bucket", "root/snap/")
	if err != nil {
		t.Fatalf("List() = %v, want nil", err)
	}
	want := []string{"root/snap/manifest.json", "root/snap/memory.zst"}
	if diff := cmp.Diff(want, objects); diff != "" {
		t.Errorf("List() differs (-want +got):\n%s", diff)
	}
}

// TestS3Delete covers a delete of an object that is already gone: the state it
// asks for is the state that holds, so it succeeds.
func TestS3Delete(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr bool
	}{
		{
			name:   "deleted",
			status: http.StatusNoContent,
		},
		{
			name:   "already gone",
			status: http.StatusNotFound,
			body:   `<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`,
		},
		{
			name:    "access denied",
			status:  http.StatusForbidden,
			body:    `<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, store := newS3(t)
			fake.handle = func(w http.ResponseWriter, _ *http.Request) bool {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(tt.status)
				writeXML(w, tt.body)
				return true
			}

			err := store.Delete(t.Context(), "bucket", "root/snap/memory.zst")
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("Delete() = %v, want an error: %v", err, tt.wantErr)
			}
		})
	}
}

// TestS3Copy covers an object small enough for a single CopyObject.
func TestS3Copy(t *testing.T) {
	fake, store := newS3(t)

	if err := store.Copy(t.Context(), "src-bucket", "root/snap/memory.zst", "dst-bucket", "root/tag-v1/memory.zst"); err != nil {
		t.Fatalf("Copy() = %v, want nil", err)
	}
	want := []s3Request{
		{
			method: http.MethodHead,
			path:   "/src-bucket/root/snap/memory.zst",
		},
		{
			method:     http.MethodPut,
			path:       "/dst-bucket/root/tag-v1/memory.zst",
			query:      "x-id=CopyObject",
			copySource: "src-bucket/root/snap/memory.zst",
		},
	}
	if diff := cmp.Diff(want, fake.requestsSeen(), cmp.AllowUnexported(s3Request{})); diff != "" {
		t.Errorf("requests differ (-want +got):\n%s", diff)
	}
}

// TestS3CopyMultipart covers an object too large for CopyObject: a memory image
// over 5 GiB has to move as server-side part copies instead.
func TestS3CopyMultipart(t *testing.T) {
	const size = 5<<30 + 1
	fake, store := newS3(t)
	fake.handle = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodHead {
			return false
		}
		w.Header().Set("Content-Length", strconv.Itoa(size))
		return true
	}

	if err := store.Copy(t.Context(), "bucket", "root/snap/memory.zst", "bucket", "root/tag-v1/memory.zst"); err != nil {
		t.Fatalf("Copy() = %v, want nil", err)
	}

	var ranges []string
	var started, completed, aborted int
	for _, req := range fake.requestsSeen() {
		switch {
		case strings.Contains(req.query, "partNumber"):
			ranges = append(ranges, req.copyRange)
		case strings.Contains(req.query, "uploads"):
			started++
		case req.method == http.MethodPost:
			completed++
		case req.method == http.MethodDelete:
			aborted++
		}
	}
	// The last part is the single byte past the 5 GiB CopyObject limit.
	want := []string{
		"bytes=0-1073741823",
		"bytes=1073741824-2147483647",
		"bytes=2147483648-3221225471",
		"bytes=3221225472-4294967295",
		"bytes=4294967296-5368709119",
		"bytes=5368709120-5368709120",
	}
	slices.Sort(ranges)
	slices.Sort(want)
	if diff := cmp.Diff(want, ranges); diff != "" {
		t.Errorf("copied ranges differ (-want +got):\n%s", diff)
	}
	if started != 1 || completed != 1 || aborted != 0 {
		t.Errorf("multipart copy started %d, completed %d, aborted %d times; want 1, 1, 0", started, completed, aborted)
	}
}

// TestS3CopyMultipartAborts covers a part copy that fails: the upload is
// abandoned rather than left to bill for parts nothing will ever complete.
func TestS3CopyMultipartAborts(t *testing.T) {
	const size = 5<<30 + 1
	fake, store := newS3(t)
	fake.handle = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(size))
			return true
		}
		if r.URL.Query().Get("partNumber") != "3" {
			return false
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusInternalServerError)
		writeXML(w, `<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>`)
		return true
	}

	if err := store.Copy(t.Context(), "bucket", "root/snap/memory.zst", "bucket", "root/tag-v1/memory.zst"); err == nil {
		t.Fatal("Copy() = nil, want an error")
	}

	var aborted bool
	for _, req := range fake.requestsSeen() {
		if req.method == http.MethodDelete && strings.Contains(req.query, "uploadId=upload-1") {
			aborted = true
		}
	}
	if !aborted {
		t.Errorf("requests = %+v, want one aborting the multipart upload", fake.requestsSeen())
	}
}
