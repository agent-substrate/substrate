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
	"strconv"
	"strings"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// GetObject streams the object, fetching it as parallel byte ranges when it spans
// more than one chunk (see rangedget.go). Smaller objects stay a single request.
//
// Unlike the GCS client this needs no pool of clients: the AWS SDK's transport keeps
// a connection pool rather than multiplexing everything onto one HTTP/2 connection,
// so concurrent ranges already get connections of their own.
func (s *s3Client) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	head, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		// The first chunk doubles as the size probe: the response reports the whole
		// object's size in Content-Range, so nothing pays an extra round trip.
		Range: aws.String(byteRange(0, downloadChunkSize)),
	})
	if err != nil {
		if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
			return nil, fmt.Errorf("%w: Failed to get S3 Bucket:%q, Object:%q", ateerrors.ReasonFailedGetExternalObject, bucket, object)
		}
		return nil, err
	}
	size, ok := totalFromContentRange(head.ContentRange)
	if !ok || size <= downloadChunkSize {
		// No Content-Range (a server that ignored the range, so this is the whole
		// object) or small enough to be one request either way.
		return head.Body, nil
	}
	return newRangedReader(ctx, size, head.Body, s.fetchRange(bucket, object)), nil
}

// fetchRange reads one range into buf.
func (s *s3Client) fetchRange(bucket, object string) fetchRangeFunc {
	return func(ctx context.Context, _ int, off, n int64, buf []byte) error {
		out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
			Range:  aws.String(byteRange(off, n)),
		})
		if err != nil {
			return fmt.Errorf("while opening range %d+%d of %q: %w", off, n, object, err)
		}
		defer out.Body.Close()
		if _, err := io.ReadFull(out.Body, buf); err != nil {
			return fmt.Errorf("while reading range %d+%d of %q: %w", off, n, object, err)
		}
		return nil
	}
}

// byteRange formats an HTTP range header for n bytes at off (the end is inclusive).
func byteRange(off, n int64) string {
	return fmt.Sprintf("bytes=%d-%d", off, off+n-1)
}

// totalFromContentRange pulls the object's full size out of a Content-Range header
// ("bytes 0-16777215/450000000"). Reports false when it is absent or unparsable.
func totalFromContentRange(cr *string) (int64, bool) {
	if cr == nil {
		return 0, false
	}
	_, total, found := strings.Cut(*cr, "/")
	if !found {
		return 0, false
	}
	size, err := strconv.ParseInt(strings.TrimSpace(total), 10, 64)
	if err != nil {
		return 0, false
	}
	return size, true
}
