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

package storagebroker

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// These benchmarks quantify the cost of running the S3 broker out-of-process
// over a UDS sidecar versus doing the same signing in-process, against the same
// S3 endpoint. The in-process path is replicated here (this is a test file, so
// the AWS SDK it links never enters the ate-api-server binary) so the comparison
// survives the removal of the in-tree broker. They are env-gated; `go test`
// skips them unless an S3 endpoint and a running s3-broker-server socket are set:
//
//	S3_ENDPOINT=http://localhost:9000 S3_BUCKET=ate-snapshots \
//	AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//	BROKER_UDS_PATH=/tmp/s3broker.sock \
//	go test -run '^$' -bench Mint -benchtime 300x ./cmd/ateapi/internal/storagebroker/

const (
	benchSnapshotURI = "s3://ate-snapshots/bench/snap"
	benchObjectCount = 5
)

func benchEnvReady(tb testing.TB) {
	if os.Getenv("S3_BUCKET") == "" || os.Getenv("BROKER_UDS_PATH") == "" {
		tb.Skip("mint benchmarks need S3_BUCKET, S3_ENDPOINT, AWS creds, and BROKER_UDS_PATH (s3-broker-server running)")
	}
}

// inProcS3 replicates the in-process presign the (removed) in-tree broker did,
// so the benchmark can measure it against the same logic behind the UDS sidecar.
type inProcS3 struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
}

func newInProcS3(tb testing.TB) *inProcS3 {
	tb.Helper()
	endpoint := firstBenchEnv("S3_ENDPOINT", "AWS_ENDPOINT_URL")
	region := firstBenchEnv("AWS_REGION", "AWS_DEFAULT_REGION")
	if region == "" {
		region = "us-east-1"
	}
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), ""),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	return &inProcS3{client: client, presign: s3.NewPresignClient(client), bucket: os.Getenv("S3_BUCKET")}
}

func benchKeyPrefix(snapshotURI string) string {
	u, err := url.Parse(snapshotURI)
	if err != nil {
		return strings.Trim(snapshotURI, "/")
	}
	return strings.Trim(u.Path, "/")
}

func (b *inProcS3) mintWrite(ctx context.Context, snapshotURI string, ttl time.Duration) error {
	prefix := benchKeyPrefix(snapshotURI) + "/"
	_, err := b.presign.PresignPostObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(prefix)}, func(o *s3.PresignPostOptions) {
		o.Conditions = []any{[]any{"starts-with", "$key", prefix}}
		o.Expires = ttl
	})
	return err
}

func (b *inProcS3) mintRead(ctx context.Context, snapshotURI string, ttl time.Duration) error {
	prefix := benchKeyPrefix(snapshotURI) + "/"
	out, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(b.bucket), Prefix: aws.String(prefix)})
	if err != nil {
		return err
	}
	for _, obj := range out.Contents {
		if _, err := b.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b.bucket), Key: obj.Key}, s3.WithPresignExpires(ttl)); err != nil {
			return err
		}
	}
	return nil
}

func firstBenchEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// benchSeed writes benchObjectCount tiny objects under the bench prefix so the
// read mint has a realistic object list to enumerate and presign.
func benchSeed(tb testing.TB) {
	tb.Helper()
	b := newInProcS3(tb)
	for i := 0; i < benchObjectCount; i++ {
		key := fmt.Sprintf("bench/snap/file-%d.img.zstd", i)
		if _, err := b.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("x"))}); err != nil {
			tb.Fatalf("seeding %s: %v", key, err)
		}
	}
}

func benchUDS(tb testing.TB) Broker {
	tb.Helper()
	u, err := newUDSBroker(context.Background())
	if err != nil {
		tb.Fatalf("uds broker: %v", err)
	}
	return u
}

func BenchmarkMintWriteInProc(b *testing.B) {
	benchEnvReady(b)
	it := newInProcS3(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := it.mintWrite(context.Background(), benchSnapshotURI, 15*time.Minute); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMintWriteUDS(b *testing.B) {
	benchEnvReady(b)
	u := benchUDS(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := u.MintWrite(context.Background(), benchSnapshotURI, 15*time.Minute); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMintReadInProc(b *testing.B) {
	benchEnvReady(b)
	benchSeed(b)
	it := newInProcS3(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := it.mintRead(context.Background(), benchSnapshotURI, 15*time.Minute); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMintReadUDS(b *testing.B) {
	benchEnvReady(b)
	benchSeed(b)
	u := benchUDS(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := u.MintRead(context.Background(), benchSnapshotURI, 15*time.Minute); err != nil {
			b.Fatal(err)
		}
	}
}
