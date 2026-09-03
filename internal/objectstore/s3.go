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

package objectstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"golang.org/x/sync/errgroup"
)

const (
	// copyObjectMaxSize is the largest object CopyObject accepts. A snapshot's
	// memory image routinely exceeds it, so anything this size or above goes
	// through a multipart copy instead.
	copyObjectMaxSize = 5 << 30
	// copyPartSize is how much of a larger object each UploadPartCopy moves. S3
	// allows 10,000 parts, so 1 GiB parts cover objects up to 10 TiB while
	// keeping the part count — and the round trips — low for the sizes actually
	// seen.
	copyPartSize = 1 << 30
	// copyPartConcurrency is how many parts of one object are copied at once.
	// The copy is server-side, so this buys request parallelism only; it stacks
	// with prefixConcurrency, which is why it is small.
	copyPartConcurrency = 4
)

type s3Store struct {
	client *s3.Client
}

// NewS3 returns a Store backed by an S3-compatible object store.
func NewS3(client *s3.Client) Store {
	return &s3Store{client: client}
}

func (s *s3Store) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	var objects []string
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("while listing objects under s3://%s/%s: %w", bucket, prefix, err)
		}
		for _, object := range page.Contents {
			objects = append(objects, aws.ToString(object.Key))
		}
	}
	return objects, nil
}

// Delete removes one object. Objects are deleted one at a time rather than
// batched through DeleteObjects: an external snapshot is a handful of objects,
// and the callers already run these in parallel.
func (s *s3Store) Delete(ctx context.Context, bucket, object string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	// S3 reports a delete of a missing object as success, but an S3-compatible
	// implementation may not, and that is the state this asks for either way.
	if err != nil && !isS3NotFound(err) {
		return fmt.Errorf("while deleting s3://%s/%s: %w", bucket, object, err)
	}
	return nil
}

func (s *s3Store) Copy(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(srcObject),
	})
	if err != nil {
		return fmt.Errorf("while reading metadata of s3://%s/%s: %w", srcBucket, srcObject, err)
	}
	size := aws.ToInt64(head.ContentLength)
	if size >= copyObjectMaxSize {
		return s.copyMultipart(ctx, srcBucket, srcObject, dstBucket, dstObject, size)
	}
	_, err = s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(dstBucket),
		Key:        aws.String(dstObject),
		CopySource: aws.String(copySource(srcBucket, srcObject)),
	})
	if err != nil {
		return fmt.Errorf("while copying s3://%s/%s to s3://%s/%s: %w", srcBucket, srcObject, dstBucket, dstObject, err)
	}
	return nil
}

// copyMultipart copies an object too large for CopyObject as a sequence of
// server-side part copies. A failure aborts the upload, so a retry of the
// caller starts from a clean destination rather than paying for parts nothing
// will ever complete.
func (s *s3Store) copyMultipart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, size int64) error {
	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(dstBucket),
		Key:    aws.String(dstObject),
	})
	if err != nil {
		return fmt.Errorf("while starting multipart copy of s3://%s/%s: %w", srcBucket, srcObject, err)
	}
	uploadID := created.UploadId

	parts, err := s.copyParts(ctx, srcBucket, srcObject, dstBucket, dstObject, uploadID, size)
	if err != nil {
		return s.abortMultipart(ctx, dstBucket, dstObject, uploadID, err)
	}

	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(dstBucket),
		Key:             aws.String(dstObject),
		UploadId:        uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		// A completion that fails leaves the parts uploaded, so this needs the
		// same cleanup a failed part copy gets.
		return s.abortMultipart(ctx, dstBucket, dstObject, uploadID, fmt.Errorf("while completing multipart copy to s3://%s/%s: %w", dstBucket, dstObject, err))
	}
	return nil
}

// abortMultipart discards the parts of the upload that failed with cause, and
// returns cause joined with whatever went wrong discarding them.
func (s *s3Store) abortMultipart(ctx context.Context, dstBucket, dstObject string, uploadID *string, cause error) error {
	// Abort on a context that is still live: the failing context may be the
	// reason this is unwinding, and an abandoned upload keeps billing.
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	if _, err := s.client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(dstBucket),
		Key:      aws.String(dstObject),
		UploadId: uploadID,
	}); err != nil {
		return errors.Join(cause, fmt.Errorf("while aborting multipart copy to s3://%s/%s: %w", dstBucket, dstObject, err))
	}
	return cause
}

// abortTimeout bounds the cleanup of a failed multipart copy, which runs on a
// context detached from the one that failed.
const abortTimeout = 30 * time.Second

// copyParts copies every part of the object. The result is indexed by part
// number, which is the order CompleteMultipartUpload requires.
func (s *s3Store) copyParts(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, uploadID *string, size int64) ([]types.CompletedPart, error) {
	source := aws.String(copySource(srcBucket, srcObject))
	parts := make([]types.CompletedPart, (size+copyPartSize-1)/copyPartSize)
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(copyPartConcurrency)
	for i := range parts {
		partNumber := int32(i + 1)
		start := int64(i) * copyPartSize
		end := min(start+copyPartSize, size) - 1
		group.Go(func() error {
			copied, err := s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
				Bucket:          aws.String(dstBucket),
				Key:             aws.String(dstObject),
				UploadId:        uploadID,
				PartNumber:      aws.Int32(partNumber),
				CopySource:      source,
				CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
			})
			if err != nil {
				return fmt.Errorf("while copying part %d of s3://%s/%s: %w", partNumber, srcBucket, srcObject, err)
			}
			parts[partNumber-1] = types.CompletedPart{
				ETag:       copied.CopyPartResult.ETag,
				PartNumber: aws.Int32(partNumber),
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return parts, nil
}

// copySource renders the x-amz-copy-source value, whose bucket and key are a
// URL path and must be escaped as one.
func copySource(bucket, object string) string {
	source := url.URL{Path: bucket + "/" + object}
	return source.EscapedPath()
}

// isS3NotFound reports whether err says the object or bucket is not there.
func isS3NotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	// S3-compatible implementations do not all model these as typed errors.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		}
	}
	return false
}
