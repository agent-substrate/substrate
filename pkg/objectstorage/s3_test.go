// Copyright 2026 The Agent Substrate Authors
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
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
			if got := errors.Is(err, ErrObjectNotFound); got != tc.wantAbsent {
				t.Errorf("errors.Is(err, ReasonFailedGetExternalObject) = %v, want %v (err: %v)", got, tc.wantAbsent, err)
			}
		})
	}
}

// TestDescribeServiceErrorS3 checks that a failed upload is described by its
// S3 error code and request ID, never by its message or the other elements of
// its body, and that a code or request ID that is not a token, a code the SDK
// made up for a response without a body, and an error from another AWS
// service, such as STS while fetching credentials, are left out.
func TestDescribeServiceErrorS3(t *testing.T) {
	const secret = "SECRET-KEY"
	stsErr := fmt.Errorf("get identity: %w", &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusBadRequest}},
			Err:      &smithy.GenericAPIError{Code: "ExpiredTokenException", Message: secret},
		},
		RequestID: "sts-req-1",
	})
	for _, tc := range []struct {
		name   string
		status int
		body   string
		err    error // used instead of an S3 response when set
		want   ServiceError
	}{
		{
			name:   "error body",
			status: http.StatusForbidden,
			body: `<?xml version="1.0"?><Error><Code>SignatureDoesNotMatch</Code>` +
				`<Message>Signature for AKIDEXAMPLE does not match ` + secret + `</Message>` +
				`<AWSAccessKeyId>AKIDEXAMPLE</AWSAccessKeyId><StringToSign>` + secret + `</StringToSign>` +
				`<RequestId>req-1</RequestId><HostId>host-1</HostId></Error>`,
			want: ServiceError{HTTPStatus: http.StatusForbidden, Code: "SignatureDoesNotMatch", RequestID: "req-1"},
		},
		{
			name:   "code and request ID that are not short tokens",
			status: http.StatusBadRequest,
			body:   `<?xml version="1.0"?><Error><Code>Bad ` + secret + `</Code><RequestId>` + strings.Repeat("a", 129) + `</RequestId></Error>`,
			want:   ServiceError{HTTPStatus: http.StatusBadRequest},
		},
		{
			name:   "no error body",
			status: http.StatusBadRequest,
			want:   ServiceError{HTTPStatus: http.StatusBadRequest},
		},
		{
			name:   "no error body, status without a name",
			status: 599,
			want:   ServiceError{HTTPStatus: 599},
		},
		{
			name: "error from another AWS service",
			err:  stsErr,
			want: ServiceError{HTTPStatus: http.StatusBadRequest},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.err
			if err == nil {
				err = s3ErrorClient(t, tc.status, tc.body).PutObject(context.Background(), "bkt", "obj", strings.NewReader("data"))
			}
			if err == nil {
				t.Fatal("PutObject succeeded, want an error")
			}
			got, ok := DescribeServiceError(err)
			if !ok || got != tc.want {
				t.Errorf("DescribeServiceError = %+v, %v; want %+v, true", got, ok, tc.want)
			}
		})
	}
}
