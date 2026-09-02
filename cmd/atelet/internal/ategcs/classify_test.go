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
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"strings"
	"syscall"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"google.golang.org/api/googleapi"

	"github.com/agent-substrate/substrate/internal/ateerrors"
)

// isTransientBackendErr matches the AWS SDK's transport error through this
// interface; if the SDK ever drops the method, classification silently
// stops. Pin the contract at compile time.
var _ interface {
	error
	HTTPStatusCode() int
} = &awshttp.ResponseError{}

func TestTagTransient(t *testing.T) {
	tagged := []struct {
		name string
		err  error
	}{
		{"googleapi 503", &googleapi.Error{Code: 503, Message: "backend error"}},
		{"googleapi 429", &googleapi.Error{Code: 429, Message: "rate limited"}},
		{"wrapped googleapi 500", fmt.Errorf("while putting GCS object: %w", &googleapi.Error{Code: 500})},
		{"net error", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}},
		{"dial failure via http client", &url.Error{Op: "Get", URL: "https://storage.googleapis.com/bucket/root/assets/runsc", Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}},
		{"stream cut mid-transfer", fmt.Errorf("while reading object body: %w", io.ErrUnexpectedEOF)},
		{"connection reset", fmt.Errorf("in io.Copy: %w", syscall.ECONNRESET)},
	}
	for _, tt := range tagged {
		t.Run(tt.name+" is tagged", func(t *testing.T) {
			got := tagTransientErr(tt.err)
			if !errors.Is(got, ateerrors.ReasonTransientObjectStorage) {
				t.Errorf("tagTransient(%v) = %v, want tagged %v", tt.err, got, ateerrors.ReasonTransientObjectStorage)
			}
			if !errors.Is(got, tt.err) && !errors.Is(got, errors.Unwrap(tt.err)) {
				t.Errorf("tagTransient(%v) = %v, want the cause kept in the chain", tt.err, got)
			}
		})
	}

	passedThrough := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"googleapi 403", &googleapi.Error{Code: 403, Message: "forbidden"}},
		{"local disk full", &fs.PathError{Op: "write", Path: "/var/lib/ate/snap", Err: syscall.ENOSPC}},
		{"already classified absence", fmt.Errorf("%w: Bucket:%q, Object:%q", ateerrors.ReasonFailedGetExternalObject, "bucket", "root/snapshots/ate-demo/snap-1/manifest.json")},
		{"context canceled", fmt.Errorf("while putting object: %w", context.Canceled)},
		{"context deadline", fmt.Errorf("while getting object: %w", context.DeadlineExceeded)},
		{"url error with non-transient cause", &url.Error{Op: "parse", URL: "gs://bucket/%zz", Err: errors.New("invalid URL escape")}},
		{"unclassified corruption", errors.New("zstd: invalid input: magic number mismatch")},
	}
	for _, tt := range passedThrough {
		t.Run(tt.name+" passes through", func(t *testing.T) {
			if got := tagTransientErr(tt.err); got != tt.err {
				t.Errorf("tagTransient(%v) = %v, want the same error back", tt.err, got)
			}
		})
	}
}

// errObjectStorage fails GetObject/PutObject with the configured error, or
// serves rc on a successful get.
type errObjectStorage struct {
	getErr error
	rc     io.ReadCloser
	putErr error
}

func (f *errObjectStorage) GetObject(context.Context, string, string) (io.ReadCloser, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.rc, nil
}

func (f *errObjectStorage) PutObject(context.Context, string, string, io.Reader) error {
	return f.putErr
}

// cutReadCloser serves data, then fails every further read like a connection
// cut mid-download.
type cutReadCloser struct{ data *strings.Reader }

func (r *cutReadCloser) Read(p []byte) (int, error) {
	if r.data.Len() > 0 {
		return r.data.Read(p)
	}
	return 0, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
}

func (r *cutReadCloser) Close() error { return nil }

func TestSendBytesToGCSTagsBackendFault(t *testing.T) {
	store := &errObjectStorage{putErr: &googleapi.Error{Code: 503, Message: "backend error"}}
	err := SendBytesToGCS(context.Background(), store, "gs://bucket/root/snapshots/ate-demo/snap-1/manifest.json", []byte(`{"sandboxClass":"microvm"}`))
	if !errors.Is(err, ateerrors.ReasonTransientObjectStorage) {
		t.Errorf("SendBytesToGCS = %v, want tagged %v", err, ateerrors.ReasonTransientObjectStorage)
	}
}

func TestOpenTagsMidStreamFault(t *testing.T) {
	store := &errObjectStorage{rc: &cutReadCloser{data: strings.NewReader("partial body")}}
	rc, err := Open(context.Background(), store, "gs://bucket/root/assets/runsc")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	_, err = io.ReadAll(rc)
	if !errors.Is(err, ateerrors.ReasonTransientObjectStorage) {
		t.Errorf("reading a cut stream = %v, want tagged %v", err, ateerrors.ReasonTransientObjectStorage)
	}
}

func TestOpenReaderPassesEOFUntouched(t *testing.T) {
	store := &errObjectStorage{rc: io.NopCloser(strings.NewReader("whole body"))}
	rc, err := Open(context.Background(), store, "gs://bucket/root/assets/runsc")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("io.ReadAll: %v", err)
	}
	if string(got) != "whole body" {
		t.Errorf("io.ReadAll = %q, want %q", got, "whole body")
	}
}
