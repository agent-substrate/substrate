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

package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// TestStoreForURI verifies the per-URI switch: a restore that reads several
// snapshots resolves each object source independently, so the actor's snapshot
// can use a signed store while an unsigned golden falls back to the node's
// built-in client.
func TestStoreForURI(t *testing.T) {
	s := &AteomHerder{gcsClient: fakeObjectStorage{}}
	signedAccess := map[string]*ateletpb.SignedObjectAccess{
		"s3://bucket/actor": {PrefixUrl: "https://account/container", ReadToken: "tok"},
		"s3://bucket/empty": {PrefixUrl: ""}, // no prefix -> not a usable capability
	}

	isSigned := func(uri string, saMap map[string]*ateletpb.SignedObjectAccess) bool {
		_, ok := s.storeForURI(saMap, uri).(*signedObjectStore)
		return ok
	}

	if !isSigned("s3://bucket/actor", signedAccess) {
		t.Errorf("URI with a capability: want signed store")
	}
	if isSigned("s3://bucket/other", signedAccess) {
		t.Errorf("URI absent from the map: want built-in fallback")
	}
	if isSigned("s3://bucket/empty", signedAccess) {
		t.Errorf("empty-prefix capability: want built-in fallback")
	}
	if isSigned("s3://bucket/actor", nil) {
		t.Errorf("nil capability map: want built-in fallback")
	}
}

// TestSignedObjectStoreGetObjectPerObjectURL covers the S3 read path: a
// per-object presigned GET URL is fetched verbatim.
func TestSignedObjectStoreGetObjectPerObjectURL(t *testing.T) {
	const want = "snapshot-bytes"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/presigned/manifest" {
			t.Errorf("path = %s, want /presigned/manifest", r.URL.Path)
		}
		_, _ = io.WriteString(w, want)
	}))
	defer ts.Close()

	s := &signedObjectStore{
		readObjectURLs: map[string]string{"manifest.json": ts.URL + "/presigned/manifest"},
		httpClient:     ts.Client(),
	}
	rc, err := s.GetObject(context.Background(), "bucket", "manifest.json")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

// TestSignedObjectStoreGetObjectPrefixToken covers the Azure read path: the key
// and read token are appended to the prefix URL.
func TestSignedObjectStoreGetObjectPrefixToken(t *testing.T) {
	const want = "azure-bytes"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/container/dir/file.bin" {
			t.Errorf("path = %s, want /container/dir/file.bin", r.URL.Path)
		}
		if r.URL.RawQuery != "sig=abc123" {
			t.Errorf("query = %s, want sig=abc123", r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, want)
	}))
	defer ts.Close()

	s := &signedObjectStore{
		prefixURL:  ts.URL + "/container",
		readToken:  "sig=abc123",
		httpClient: ts.Client(),
	}
	rc, err := s.GetObject(context.Background(), "bucket", "dir/file.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

// TestSignedObjectStoreGetObjectNoCapability verifies a read with neither a
// per-object URL nor a read token fails instead of fetching something wrong.
func TestSignedObjectStoreGetObjectNoCapability(t *testing.T) {
	s := &signedObjectStore{httpClient: http.DefaultClient}
	if _, err := s.GetObject(context.Background(), "bucket", "missing"); err == nil {
		t.Fatal("GetObject with no read capability: want error, got nil")
	}
}

// TestSignedObjectStoreGetObjectHTTPError verifies a non-200 response surfaces
// as an error carrying the status code.
func TestSignedObjectStoreGetObjectHTTPError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	defer ts.Close()

	s := &signedObjectStore{
		readObjectURLs: map[string]string{"o": ts.URL + "/o"},
		httpClient:     ts.Client(),
	}
	_, err := s.GetObject(context.Background(), "bucket", "o")
	if err == nil {
		t.Fatal("GetObject on 403: want error, got nil")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("error = %v, want it to mention status 403", err)
	}
}

// TestSignedObjectStorePutObjectPOST covers the S3 write path: a multipart form
// POST whose key field is set to the object, overriding the policy placeholder,
// while the other signed fields are forwarded.
func TestSignedObjectStorePutObjectPOST(t *testing.T) {
	const payload = "counter-state"
	var gotKey, gotPolicy, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("ParseMultipartForm: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gotKey = r.FormValue("key")
		gotPolicy = r.FormValue("policy")
		f, _, err := r.FormFile("file")
		if err != nil {
			t.Errorf("FormFile: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer f.Close()
		b, _ := io.ReadAll(f)
		gotBody = string(b)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	s := &signedObjectStore{
		writeMethod: "POST",
		postURL:     ts.URL + "/upload",
		postFields:  map[string]string{"key": "ignored-placeholder", "policy": "signed-policy"},
		httpClient:  ts.Client(),
	}
	if err := s.PutObject(context.Background(), "bucket", "snap/dir/counter.bin", strings.NewReader(payload)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if gotKey != "snap/dir/counter.bin" {
		t.Errorf("form key = %q, want the object key (placeholder overridden)", gotKey)
	}
	if gotPolicy != "signed-policy" {
		t.Errorf("policy field = %q, want it forwarded from postFields", gotPolicy)
	}
	if gotBody != payload {
		t.Errorf("uploaded body = %q, want %q", gotBody, payload)
	}
}

// TestSignedObjectStorePutObjectPUT covers the Azure write path: a PUT to the
// prefixed key with the write token, an explicit Content-Length, and the signed
// write headers.
func TestSignedObjectStorePutObjectPUT(t *testing.T) {
	const payload = "block-blob-bytes"
	var gotPath, gotQuery, gotHeader, gotBody string
	var gotLen int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotHeader = r.Header.Get("x-ms-blob-type")
		gotLen = r.ContentLength
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusCreated)
	}))
	defer ts.Close()

	s := &signedObjectStore{
		prefixURL:    ts.URL + "/container",
		writeMethod:  "PUT",
		writeToken:   "sv=2021&sig=xyz",
		writeHeaders: map[string]string{"x-ms-blob-type": "BlockBlob"},
		httpClient:   ts.Client(),
	}
	if err := s.PutObject(context.Background(), "bucket", "dir/file.bin", bytes.NewReader([]byte(payload))); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if gotPath != "/container/dir/file.bin" {
		t.Errorf("path = %q, want /container/dir/file.bin", gotPath)
	}
	if gotQuery != "sv=2021&sig=xyz" {
		t.Errorf("query = %q, want the write token", gotQuery)
	}
	if gotHeader != "BlockBlob" {
		t.Errorf("x-ms-blob-type = %q, want BlockBlob", gotHeader)
	}
	if gotLen != int64(len(payload)) {
		t.Errorf("Content-Length = %d, want %d", gotLen, len(payload))
	}
	if gotBody != payload {
		t.Errorf("body = %q, want %q", gotBody, payload)
	}
}

// TestSignedObjectStorePutObjectPUTRequiresSeekable documents that the PUT path
// needs a seekable body to set Content-Length, so a streaming (non-seekable)
// reader is rejected before any request is made.
func TestSignedObjectStorePutObjectPUTRequiresSeekable(t *testing.T) {
	s := &signedObjectStore{
		prefixURL:   "http://unused/container",
		writeMethod: "PUT",
		writeToken:  "t",
		httpClient:  http.DefaultClient,
	}
	pr, _ := io.Pipe()
	defer pr.Close()
	if err := s.PutObject(context.Background(), "bucket", "obj", pr); err == nil {
		t.Fatal("PutObject with a non-seekable body: want error, got nil")
	}
}
