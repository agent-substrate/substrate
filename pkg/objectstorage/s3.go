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
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// s3Token matches the error codes and request IDs describeS3Error keeps:
// short tokens, as S3's are.
var s3Token = regexp.MustCompile(`^[A-Za-z0-9._=+-]{1,128}$`)

// describeS3Error describes an S3 response error in err's chain for
// DescribeServiceError by its status, error code and request ID. It leaves out
// the message, which can name the caller and the resource, as AccessDenied's
// does, and a code or request ID that is not a short token. It also leaves out
// the code the SDK makes up when the response has no error body, so such a
// response is described as one.
func describeS3Error(err error) (ServiceError, bool) {
	respErr, ok := errors.AsType[s3.ResponseError](err)
	if !ok {
		return ServiceError{}, false
	}
	var desc ServiceError
	if withStatus, ok := respErr.(interface{ HTTPStatusCode() int }); ok {
		desc.HTTPStatus = withStatus.HTTPStatusCode()
	}
	if id := respErr.ServiceRequestID(); s3Token.MatchString(id) {
		desc.RequestID = id
	}
	apiErr, ok := errors.AsType[smithy.APIError](err)
	if ok && !sdkDefaultError(apiErr, desc.HTTPStatus) && s3Token.MatchString(apiErr.ErrorCode()) {
		desc.Code = apiErr.ErrorCode()
	}
	return desc, true
}

// sdkDefaultError reports whether apiErr carries the code and message the SDK
// fills in for an error response without a body: the status text, or
// "UnknownError" for a status that has none.
func sdkDefaultError(apiErr smithy.APIError, status int) bool {
	text := http.StatusText(status)
	if text == "" {
		text = "UnknownError"
	}
	return apiErr.ErrorCode() == strings.ReplaceAll(text, " ", "") && apiErr.ErrorMessage() == text
}

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

func (s *s3Client) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	//nolint:staticcheck // SA1019: see the note on s3Client.
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Body:   reader,
	})
	return err
}
