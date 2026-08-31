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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestS3UploadAgainstRealServer times the snapshot upload path against a real
// S3 server (rustfs is what the kind install runs). Skipped unless S3_BENCH_ENDPOINT
// is set, since it needs a server and a payload:
//
//	S3_BENCH_ENDPOINT=http://localhost:9000 S3_BENCH_BUCKET=bench \
//	S3_BENCH_FILE=/path/to/memory-ranges go test ./cmd/atelet/internal/ategcs \
//	  -run TestS3UploadAgainstRealServer -v -count=3
func TestS3UploadAgainstRealServer(t *testing.T) {
	endpoint, bucket := os.Getenv("S3_BENCH_ENDPOINT"), os.Getenv("S3_BENCH_BUCKET")
	srcPath := os.Getenv("S3_BENCH_FILE")
	if endpoint == "" || bucket == "" || srcPath == "" {
		t.Skip("set S3_BENCH_ENDPOINT, S3_BENCH_BUCKET and S3_BENCH_FILE to run")
	}
	fi, err := os.Stat(srcPath)
	if err != nil {
		t.Fatalf("stat payload: %v", err)
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			os.Getenv("S3_BENCH_KEY"), os.Getenv("S3_BENCH_SECRET"), "")))
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	raw := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	if _, err := raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Logf("create bucket (ignored if it exists): %v", err)
	}
	client := NewS3Client(raw)

	start := time.Now()
	url := fmt.Sprintf("gs://%s/bench/memory-ranges-%d.zstd", bucket, time.Now().UnixNano())
	if err := SendLocalFileToGCSWithZstd(ctx, client, url, srcPath); err != nil {
		t.Fatalf("upload: %v", err)
	}
	d := time.Since(start)
	t.Logf("uploaded %.1f MiB (logical) in %v -> %.0f MiB/s",
		float64(fi.Size())/(1<<20), d, float64(fi.Size())/(1<<20)/d.Seconds())

	// A multipart upload that reassembles wrong is worse than a slow one: pull the
	// object back and compare it to the source byte for byte.
	dst := filepath.Join(t.TempDir(), "restored")
	if err := FetchLocalFileFromGCSWithZstd(ctx, client, url, dst); err != nil {
		t.Fatalf("download: %v", err)
	}
	if got, want := sha256File(t, dst), sha256File(t, srcPath); got != want {
		t.Fatalf("round trip mismatch: got %s want %s", got, want)
	}
	t.Logf("round trip byte-exact (%s)", sha256File(t, dst)[:16])
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}
