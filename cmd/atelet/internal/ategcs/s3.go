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

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	// uploadPartSize is how much of a streamed object the uploader buffers before it
	// sends. A snapshot under this size goes up as a single PutObject; a larger one is
	// split into parts of this size. 32 MiB covers a typical golden snapshot (an idle
	// micro-VM actor's is ~24 MiB compressed) in one request.
	uploadPartSize = 32 << 20
	// uploadConcurrency is how many parts of a larger object are in flight at once. A
	// single stream to an object store tops out well below what a node can push — on
	// GKE, 300 MiB went at 82-107 MiB/s on one stream against 233-257 MiB/s on four —
	// and the snapshot upload is the largest item in a suspend.
	//
	// Peak buffering per upload is uploadPartSize * uploadConcurrency (128 MiB).
	uploadConcurrency = 4
)

// The upload manager is marked deprecated in favor of feature/s3/transfermanager,
// which is still v0.x and whose current release wants 19 other modules changed with
// it. Take the stable one until the successor is GA; the nolints below are that
// decision, not an oversight.
type s3Client struct {
	client   *s3.Client
	uploader *manager.Uploader //nolint:staticcheck // SA1019: see the note above.
}

func NewS3Client(client *s3.Client) ObjectStorage {
	return &s3Client{
		client: client,
		//nolint:staticcheck // SA1019: see the note on s3Client.
		uploader: manager.NewUploader(client, func(u *manager.Uploader) {
			u.PartSize = uploadPartSize
			u.Concurrency = uploadConcurrency
		}),
	}
}

// supportsStreamingPut is the streamingPutter marker: the upload manager buffers each
// part itself, so it accepts a non-seekable body (plain PutObject cannot — the SDK
// needs to seek to sign and set Content-Length, which is why this path used to stage
// the compressed snapshot to a temp file first). Callers can now pipe the compressor
// straight into the upload, overlapping compression with the network. Never called:
// its presence is the signal.
func (s *s3Client) supportsStreamingPut() {}

func (s *s3Client) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
			return nil, fmt.Errorf("%w: Failed to get S3 Bucket:%q, Object:%q", ateerrors.ReasonFailedGetExternalObject, bucket, object)
		}
		return nil, err
	}
	return output.Body, nil
}

func (s *s3Client) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	//nolint:staticcheck // SA1019: see the note on s3Client.
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Body:   reader,
	})
	return err
}
