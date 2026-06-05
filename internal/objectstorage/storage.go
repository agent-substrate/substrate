//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package objectstorage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// ObjectStorage is a generic interface for interacting with object storage
// backends such as GCS and S3 (including S3-compatible services like
// Alibaba Cloud OSS, MinIO, and Cloudflare R2).
type ObjectStorage interface {
	GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, object string, reader io.Reader) error
}

// ParseGCSURL parses a gs:// URL and returns the bucket and object key.
func ParseGCSURL(gsURL string) (string, string, error) {
	parsed, err := url.Parse(gsURL)
	if err != nil {
		return "", "", fmt.Errorf("while parsing %q: %w", gsURL, err)
	}

	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}

// ParseObjectURL parses an object storage URL (gs://, s3://, or oss://) and
// returns the bucket and object key. The URL structure is the same across all
// backends: <scheme>://<bucket>/<object>.
func ParseObjectURL(rawURL string) (string, string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("while parsing %q: %w", rawURL, err)
	}
	scheme := parsed.Scheme
	if scheme != "gs" && scheme != "s3" && scheme != "oss" {
		return "", "", fmt.Errorf("unsupported object storage scheme %q (expected gs, s3, or oss)", scheme)
	}
	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}
