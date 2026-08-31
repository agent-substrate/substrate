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
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// signedObjectStore reads and writes snapshot objects using control-plane-minted
// signed access, over plain net/http. It links no cloud SDK and holds no cloud
// credential. It dispatches on the mechanics carried in SignedObjectAccess, not
// on the cloud:
//
//	read:  a per-object URL map (S3 presigned GET) or a token appended to
//	       <prefixURL>/<key> (Azure container SAS).
//	write: a multipart POST to postURL with postFields (S3 presigned POST +
//	       starts-with) or a PUT to <prefixURL>/<key>?<writeToken> (Azure).
//
// It satisfies ategcs.ObjectStorage, so the snapshot manifest and zstd code use
// it unchanged. It deliberately omits the streaming-put fast path so PUT uploads
// take the buffered, seekable, Content-Length route Azure block blob requires.
type signedObjectStore struct {
	prefixURL      string
	readToken      string
	readObjectURLs map[string]string
	writeMethod    string
	writeToken     string
	writeHeaders   map[string]string
	postURL        string
	postFields     map[string]string
	httpClient     *http.Client
}

var _ ategcs.ObjectStorage = (*signedObjectStore)(nil)

func newSignedObjectStore(sa *ateletpb.SignedObjectAccess) *signedObjectStore {
	return &signedObjectStore{
		prefixURL:      strings.TrimRight(sa.GetPrefixUrl(), "/"),
		readToken:      sa.GetReadToken(),
		readObjectURLs: sa.GetReadObjectUrls(),
		writeMethod:    sa.GetWriteMethod(),
		writeToken:     sa.GetWriteToken(),
		writeHeaders:   sa.GetWriteHeaders(),
		postURL:        sa.GetPostUrl(),
		postFields:     sa.GetPostFields(),
		httpClient:     http.DefaultClient,
	}
}

// GetObject fetches object over HTTP. bucket is ignored: the capability already
// names the account/bucket. A per-object URL wins; otherwise the key and read
// token are appended to the prefix URL.
func (s *signedObjectStore) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	url, ok := s.readObjectURLs[object]
	if !ok {
		if s.readToken == "" {
			return nil, fmt.Errorf("no read capability for object %q", object)
		}
		url = s.prefixURL + "/" + strings.TrimPrefix(object, "/") + "?" + s.readToken
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s: status %d: %s", object, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp.Body, nil
}

// PutObject writes object over HTTP: POST for S3 (form policy) or PUT for Azure.
func (s *signedObjectStore) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	if s.writeMethod == "POST" {
		return s.postObject(ctx, object, reader)
	}
	return s.putObject(ctx, object, reader)
}

// putObject does a plain PUT with the object key and write token appended, the
// Azure block-blob path.
func (s *signedObjectStore) putObject(ctx context.Context, object string, reader io.Reader) error {
	length, err := seekableLen(reader)
	if err != nil {
		return fmt.Errorf("signed PUT %s: %w", object, err)
	}
	url := s.prefixURL + "/" + strings.TrimPrefix(object, "/") + "?" + s.writeToken
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, reader)
	if err != nil {
		return err
	}
	req.ContentLength = length
	for k, v := range s.writeHeaders {
		req.Header.Set(k, v)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("PUT %s: status %d: %s", object, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// postObject submits a multipart form to the S3 POST endpoint, reusing the one
// signed starts-with policy and setting the key to this object. The signature
// covers the policy, not the body, so the same fields upload every file under
// the prefix.
func (s *signedObjectStore) postObject(ctx context.Context, object string, reader io.Reader) error {
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for k, v := range s.postFields {
		if k == "key" {
			continue
		}
		_ = form.WriteField(k, v)
	}
	_ = form.WriteField("key", object)
	fw, err := form.CreateFormFile("file", object)
	if err != nil {
		return err
	}
	if _, err := io.Copy(fw, reader); err != nil {
		return fmt.Errorf("buffering %s for POST: %w", object, err)
	}
	if err := form.Close(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.postURL, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", form.FormDataContentType())
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("POST %s: status %d: %s", object, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// seekableLen returns the remaining length of a seekable reader without
// consuming it.
func seekableLen(reader io.Reader) (int64, error) {
	seeker, ok := reader.(io.Seeker)
	if !ok {
		return 0, fmt.Errorf("a seekable body is required to set Content-Length")
	}
	cur, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	end, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if _, err := seeker.Seek(cur, io.SeekStart); err != nil {
		return 0, err
	}
	return end - cur, nil
}

// storeForURI returns the signed HTTP store for the snapshot at snapshotURI when
// the control plane minted a capability for it, otherwise atelet's built-in
// storage client. A restore that reads several snapshots (actor plus golden)
// carries one capability per URI, so the store is resolved per object source.
// This is the single switch that keeps the node cloud-agnostic.
func (s *AteomHerder) storeForURI(signedAccess map[string]*ateletpb.SignedObjectAccess, snapshotURI string) ategcs.ObjectStorage {
	if sa := signedAccess[snapshotURI]; sa != nil && sa.GetPrefixUrl() != "" {
		return newSignedObjectStore(sa)
	}
	return s.gcsClient
}
