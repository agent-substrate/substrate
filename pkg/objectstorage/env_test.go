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
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestNewFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name          string
		backend       string
		pathStyle     string
		wantS3        bool
		wantPathStyle bool
	}{
		{name: "unset selects GCS", backend: ""},
		{name: "unknown value selects GCS", backend: "gcs"},
		{name: "s3", backend: "s3", wantS3: true},
		{name: "s3 with path style", backend: "s3", pathStyle: "true", wantS3: true, wantPathStyle: true},
		{name: "s3 path style needs exactly true", backend: "s3", pathStyle: "1", wantS3: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// GCS: the emulator host makes storage.NewClient skip credential lookup.
			t.Setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:1")
			// S3: keep the developer's AWS config out of the test.
			missing := filepath.Join(t.TempDir(), "missing")
			t.Setenv("AWS_CONFIG_FILE", missing)
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", missing)
			t.Setenv("AWS_PROFILE", "")
			t.Setenv("AWS_DEFAULT_PROFILE", "")
			t.Setenv("AWS_REGION", "us-east-1")
			t.Setenv("ATE_STORAGE_BACKEND", tc.backend)
			t.Setenv("AWS_S3_USE_PATH_STYLE", tc.pathStyle)

			got, err := NewFromEnv(context.Background())
			if err != nil {
				t.Fatalf("NewFromEnv: %v", err)
			}
			switch c := got.(type) {
			case *s3Client:
				if !tc.wantS3 {
					t.Fatalf("NewFromEnv returned an S3 client, want GCS")
				}
				if got := c.client.Options().UsePathStyle; got != tc.wantPathStyle {
					t.Errorf("UsePathStyle = %v, want %v", got, tc.wantPathStyle)
				}
			case *gcsClient:
				if tc.wantS3 {
					t.Fatalf("NewFromEnv returned a GCS client, want S3")
				}
			default:
				t.Fatalf("NewFromEnv returned %T", got)
			}
		})
	}
}

func TestNewS3ClientFromEnvExcludesAcceptEncodingFromSignature(t *testing.T) {
	type captured struct {
		auth           string
		acceptEncoding string
		checksumCRC32  string
	}
	var (
		mu  sync.Mutex
		got []captured
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, captured{
			auth:           r.Header.Get("Authorization"),
			acceptEncoding: r.Header.Get("Accept-Encoding"),
			checksumCRC32:  r.Header.Get("X-Amz-Checksum-Crc32"),
		})
		mu.Unlock()
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListBucketResult>`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	missing := filepath.Join(t.TempDir(), "missing")
	t.Setenv("AWS_CONFIG_FILE", missing)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", missing)
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_REGION", "us-central1")
	t.Setenv("AWS_ENDPOINT_URL", srv.URL)
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_S3_USE_PATH_STYLE", "true")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")

	client, err := NewS3ClientFromEnv(context.Background())
	if err != nil {
		t.Fatalf("NewS3ClientFromEnv: %v", err)
	}

	if _, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String("test-bucket"),
	}); err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if _, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("snapshot.bin"),
		Body:   strings.NewReader("payload"),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	srv.Close()

	mu.Lock()
	reqs := slices.Clone(got)
	mu.Unlock()

	if len(reqs) != 2 {
		t.Fatalf("captured %d requests, want 2", len(reqs))
	}
	for i, req := range reqs {
		signedHeaders := parseSigV4SignedHeaders(t, req.auth)
		if slices.Contains(signedHeaders, "accept-encoding") {
			t.Errorf("request %d SignedHeaders = %v, must not include accept-encoding", i, signedHeaders)
		}
		if req.acceptEncoding != "identity" {
			t.Errorf("request %d Accept-Encoding = %q, want %q", i, req.acceptEncoding, "identity")
		}
		if req.checksumCRC32 != "" {
			t.Errorf("request %d X-Amz-Checksum-Crc32 = %q, want empty with AWS_REQUEST_CHECKSUM_CALCULATION=when_required", i, req.checksumCRC32)
		}
	}
}

func parseSigV4SignedHeaders(t *testing.T, auth string) []string {
	t.Helper()
	const prefix = "SignedHeaders="
	_, rest, ok := strings.Cut(auth, prefix)
	if !ok {
		t.Fatalf("Authorization header missing %q: %q", prefix, auth)
	}
	raw, _, _ := strings.Cut(rest, ",")
	return strings.Split(raw, ";")
}
