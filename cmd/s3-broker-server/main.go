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

// Command s3-broker-server is the out-of-process S3 storage broker sidecar. It
// links the AWS SDK and holds the S3 credential, and serves signed-URL mints to
// ate-api-server over a Unix domain socket, so ate-api-server's own binary links
// no cloud SDK: the control-plane core dials this socket and mints nothing
// itself. S3's SDK is already vendored for atelet's object store, so this sidecar
// ships in-repo (unlike a cloud whose SDK is not vendored, which lives in a
// separate module); the point is only to keep it out of the core process.
//
// It mints the two S3 mechanisms:
//   - write: one presigned POST policy with a starts-with condition on the key,
//     so a single capability covers every file atelet writes under the snapshot
//     prefix without the control plane knowing the file names in advance, and
//   - read: a presigned GET per object, enumerated by listing the prefix (the
//     broker holds the credential; the node never does).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// mintRequest and mintReply mirror the wire contract in
// cmd/ateapi/internal/storagebroker/uds.go.
type mintRequest struct {
	Verb        string `json:"verb"`
	SnapshotURI string `json:"snapshotUri"`
	TTLSeconds  int    `json:"ttlSeconds"`
}

type mintReply struct {
	PrefixURL      string            `json:"prefixUrl"`
	ReadToken      string            `json:"readToken,omitempty"`
	ReadObjectURLs map[string]string `json:"readObjectUrls,omitempty"`
	WriteMethod    string            `json:"writeMethod,omitempty"`
	WriteToken     string            `json:"writeToken,omitempty"`
	WriteHeaders   map[string]string `json:"writeHeaders,omitempty"`
	PostURL        string            `json:"postUrl,omitempty"`
	PostFields     map[string]string `json:"postFields,omitempty"`
}

type s3Broker struct {
	client    *s3.Client
	presign   *s3.PresignClient
	bucket    string
	prefixURL string
}

func main() {
	ctx := context.Background()
	socket := getenv("BROKER_UDS_PATH", "/run/broker/broker.sock")
	b, err := newS3Broker()
	if err != nil {
		fatal(fmt.Sprintf("building s3 broker: %v", err))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/mint", func(w http.ResponseWriter, r *http.Request) {
		var req mintRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ttl := time.Duration(req.TTLSeconds) * time.Second
		if ttl <= 0 {
			ttl = 15 * time.Minute
		}
		var (
			reply mintReply
			mErr  error
		)
		if req.Verb == "write" {
			reply, mErr = b.mintWrite(r.Context(), req.SnapshotURI, ttl)
		} else {
			reply, mErr = b.mintRead(r.Context(), req.SnapshotURI, ttl)
		}
		if mErr != nil {
			http.Error(w, mErr.Error(), http.StatusInternalServerError)
			return
		}
		slog.InfoContext(r.Context(), "Minted snapshot capability", slog.String("verb", req.Verb), slog.String("snapshotUri", req.SnapshotURI))
		_ = json.NewEncoder(w).Encode(reply)
	})

	_ = os.Remove(socket)
	if err := os.MkdirAll(dir(socket), 0o755); err != nil {
		fatal(fmt.Sprintf("mkdir socket dir: %v", err))
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		fatal(fmt.Sprintf("listen on %s: %v", socket, err))
	}
	slog.InfoContext(ctx, "s3-broker-server listening", slog.String("socket", socket), slog.String("bucket", b.bucket))
	if err := http.Serve(ln, mux); err != nil {
		fatal(fmt.Sprintf("serve: %v", err))
	}
}

func newS3Broker() (*s3Broker, error) {
	endpoint := firstEnv("S3_ENDPOINT", "AWS_ENDPOINT_URL")
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		return nil, fmt.Errorf("S3_BUCKET is required")
	}
	region := firstEnv("AWS_REGION", "AWS_DEFAULT_REGION")
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
	return &s3Broker{
		client:    client,
		presign:   s3.NewPresignClient(client),
		bucket:    bucket,
		prefixURL: strings.TrimRight(endpoint, "/") + "/" + bucket,
	}, nil
}

// snapshotKeyPrefix turns a snapshot URI into the object-key prefix atelet's
// file keys share, mirroring how atelet derives the object path.
func snapshotKeyPrefix(snapshotURI string) string {
	u, err := url.Parse(snapshotURI)
	if err != nil {
		return strings.Trim(snapshotURI, "/")
	}
	return strings.Trim(u.Path, "/")
}

func (b *s3Broker) mintWrite(ctx context.Context, snapshotURI string, ttl time.Duration) (mintReply, error) {
	prefix := snapshotKeyPrefix(snapshotURI) + "/"
	post, err := b.presign.PresignPostObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(prefix),
	}, func(o *s3.PresignPostOptions) {
		o.Conditions = []any{[]any{"starts-with", "$key", prefix}}
		o.Expires = ttl
	})
	if err != nil {
		return mintReply{}, fmt.Errorf("presigning S3 POST policy: %w", err)
	}
	return mintReply{
		PrefixURL:   b.prefixURL,
		WriteMethod: "POST",
		PostURL:     post.URL,
		PostFields:  post.Values,
	}, nil
}

func (b *s3Broker) mintRead(ctx context.Context, snapshotURI string, ttl time.Duration) (mintReply, error) {
	prefix := snapshotKeyPrefix(snapshotURI) + "/"
	out, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return mintReply{}, fmt.Errorf("listing snapshot prefix %q: %w", prefix, err)
	}
	urls := make(map[string]string, len(out.Contents))
	for _, obj := range out.Contents {
		key := aws.ToString(obj.Key)
		pres, err := b.presign.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(key),
		}, s3.WithPresignExpires(ttl))
		if err != nil {
			return mintReply{}, fmt.Errorf("presigning GET for %q: %w", key, err)
		}
		urls[key] = pres.URL
	}
	return mintReply{PrefixURL: b.prefixURL, ReadObjectURLs: urls}, nil
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func dir(path string) string {
	i := strings.LastIndex(path, "/")
	if i <= 0 {
		return "."
	}
	return path[:i]
}

func fatal(msg string) {
	slog.Error(msg)
	os.Exit(1)
}
