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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3ErrorClient serves one canned error response, so the real SDK does the
// deserialization the classification depends on. A hand-built error would test
// the assertion rather than the behavior: the whole question here is which
// concrete type the SDK produces for a given wire response.
func s3ErrorClient(t *testing.T, status int, body string) ObjectStorage {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return NewS3Client(s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("ak", "sk", ""),
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		// One attempt: the retryable statuses below are being classified, not
		// retried, and the default backoff would put seconds into the test.
		RetryMaxAttempts: 1,
	}))
}

// TestS3GetObjectClassifiesAbsence pins which GetObject failures mean "the
// object is not there" (ReasonFailedGetExternalObject) and which stay opaque
// errors. Callers act on that difference: the checkpoint fast-forward reads
// the sentinel as "not committed yet" and takes a checkpoint, so a missing
// manifest misread as a hard failure blocks every suspend, and a hard failure
// misread as absence re-runs a destructive checkpoint.
func TestS3GetObjectClassifiesAbsence(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body       string
		wantAbsent bool
	}{
		{
			name:       "NoSuchKey is absence",
			status:     http.StatusNotFound,
			body:       `<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`,
			wantAbsent: true,
		},
		{
			// Not every 404 deserializes into the modeled NoSuchKey: an empty
			// or unrecognized body leaves a generic NotFound, which is still
			// S3 saying the object is not there.
			name:       "bare 404 is absence",
			status:     http.StatusNotFound,
			body:       ``,
			wantAbsent: true,
		},
		{
			// GCS maps ErrBucketNotExist to the same sentinel; the two
			// backends have to agree or the fast-forward means different
			// things depending on where the snapshot lives.
			name:       "NoSuchBucket is absence",
			status:     http.StatusNotFound,
			body:       `<?xml version="1.0"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist.</Message></Error>`,
			wantAbsent: true,
		},
		{
			// S3 answers AccessDenied rather than NoSuchKey for a missing key
			// when the caller lacks s3:ListBucket, so this response is
			// genuinely ambiguous. It stays an error: guessing "absent" here
			// would re-run a checkpoint that may already have committed.
			name:       "AccessDenied is not absence",
			status:     http.StatusForbidden,
			body:       `<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`,
			wantAbsent: false,
		},
		{
			name:       "server error is not absence",
			status:     http.StatusInternalServerError,
			body:       `<?xml version="1.0"?><Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>`,
			wantAbsent: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s3ErrorClient(t, tc.status, tc.body).GetObject(context.Background(), "bkt", "obj")
			if err == nil {
				t.Fatal("GetObject succeeded, want an error")
			}
			if got := errors.Is(err, ateerrors.ReasonFailedGetExternalObject); got != tc.wantAbsent {
				t.Errorf("errors.Is(err, ReasonFailedGetExternalObject) = %v, want %v (err: %v)", got, tc.wantAbsent, err)
			}
		})
	}
}
