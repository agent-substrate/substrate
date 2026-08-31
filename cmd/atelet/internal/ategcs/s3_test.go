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

// s3ErrorClient starts a test HTTP server returning canned responses so the
// SDK performs realistic response deserialization.
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
		// Disable retry backoff to keep tests fast.
		RetryMaxAttempts: 1,
	}))
}

// TestS3GetObjectClassifiesAbsence verifies which S3 GetObject errors are classified
// as object absence (ReasonFailedGetExternalObject) versus opaque errors.
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
			name:       "bare 404 is absence",
			status:     http.StatusNotFound,
			body:       ``,
			wantAbsent: true,
		},
		{
			name:       "NoSuchBucket is absence",
			status:     http.StatusNotFound,
			body:       `<?xml version="1.0"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist.</Message></Error>`,
			wantAbsent: true,
		},
		{
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
