// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectstorage

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// describeGCSError describes a GCS API error in err's chain for
// DescribeServiceError. Reads use the XML API, whose error body carries a
// generic Code and Message and puts the caller and resource in Details, which
// is not read. JSON API errors quote the caller and resource in their message,
// so only their reason is kept.
func describeGCSError(err error) (ServiceError, bool) {
	apiErr, ok := errors.AsType[*googleapi.Error](err)
	if !ok {
		return ServiceError{}, false
	}
	desc := ServiceError{HTTPStatus: apiErr.Code}
	if len(apiErr.Errors) > 0 {
		desc.Code = apiErr.Errors[0].Reason
		return desc, true
	}
	var body struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}
	if xml.Unmarshal([]byte(apiErr.Body), &body) == nil {
		desc.Code, desc.Message = body.Code, body.Message
	}
	return desc, true
}

type gcsClient struct {
	client *storage.Client
	// pool holds extra clients so concurrent transfers get their own
	// connections, see uploadClient. next is the round-robin position for
	// single-request transfers, see nextClient.
	pool []*storage.Client
	next atomic.Uint64
}

// NewGCSClient returns a GCS-backed ObjectStorage. It builds its own
// storage.Client from opts (and more from the same opts for the connection
// pool, see newPool) rather than accepting one, because it installs a
// RetryAlways policy (see setRetry) that is only safe for this package's own
// operations and must not leak onto a client shared with other code.
func NewGCSClient(ctx context.Context, opts ...option.ClientOption) (ObjectStorage, error) {
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	setRetry(client)
	return &gcsClient{client: client, pool: newPool(ctx, opts)}, nil
}

// setRetry makes every operation on c retry transient errors (408, 429, 5xx)
// with backoff. The client's default RetryIdempotent policy never retries an
// object write without a precondition, so a single 429 — routine while GCS
// scales a cold bucket's key ranges up to a suspend burst — failed the whole
// snapshot. RetryAlways is safe for this client because every write targets a
// unique name (snapshot UUID directories, runID-suffixed part names) or is
// idempotent (compose and copy from fixed sources, delete).
//
// Nothing caps the attempts: an operation retries until its context ends, so
// every caller must bound its context. Part cleanup uses cleanupTimeout. One
// exception: an upload the SDK sends as resumable (one at least as large as its
// Writer's ChunkSize, rounded up to 256 KiB) also gives up once a chunk has
// been retrying for the Writer's ChunkRetryDeadline, 32s by default.
func setRetry(c *storage.Client) {
	c.SetRetry(
		storage.WithPolicy(storage.RetryAlways),
		storage.WithBackoff(retryBackoff),
	)
}

// retryBackoff is how long setRetry waits between attempts. Suspend is
// latency-sensitive, so the first retry comes within 250ms, and no wait is
// longer than 5s.
var retryBackoff = gax.Backoff{Initial: 250 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2}

// supportsStreamingPut is the streamingPutter marker: the GCS client's PutObject
// accepts a non-seekable streaming body without buffering (it copies the reader
// straight into a storage.Writer — no Content-Length / signing requirement), so
// callers can pipe compression directly into the upload (overlap) instead of
// staging a seekable temp file. (S3's PutObject needs a seekable body, so s3Client
// does NOT implement this — see objects.go sendZstd.) Never called: its presence is
// the signal.
func (g *gcsClient) supportsStreamingPut() {}

// boundedReader wraps an io.Reader whose remaining length is at most max bytes.
type boundedReader struct {
	io.Reader
	max int64
}

func (b boundedReader) sizeBound() int64  { return b.max }
func (b boundedReader) Unwrap() io.Reader { return b.Reader }

// readerSizeBound returns the upper bound on r's remaining length if r or an
// unwrapped inner reader reports one via sizeBound().
func readerSizeBound(r io.Reader) (int64, bool) {
	for r != nil {
		if b, ok := r.(interface{ sizeBound() int64 }); ok {
			return b.sizeBound(), true
		}
		u, ok := r.(interface{ Unwrap() io.Reader })
		if !ok {
			return 0, false
		}
		r = u.Unwrap()
	}
	return 0, false
}

// singleUploadChunkSize returns the Writer.ChunkSize that sends an object of at
// most size bytes in one request. googleapi rounds ChunkSize up to a multiple of
// MinUploadChunkSize (256 KiB), allocates a retry buffer of that size, and sends
// media strictly smaller than ChunkSize in a single request.
func singleUploadChunkSize(size int64) int64 {
	const unit = int64(googleapi.MinUploadChunkSize)
	return ((max(size, 0) + unit) / unit) * unit
}

// singleUploadLookahead returns the PutObject lookahead size for reader.
func singleUploadLookahead(reader io.Reader) int64 {
	if bound, ok := readerSizeBound(reader); ok && bound >= 0 && bound < uploadCompositeMin {
		return singleUploadChunkSize(bound)
	}
	return uploadCompositeMin
}

// PutObject writes reader to the object, in a single request below
// uploadCompositeMin and as parallel parts above it. When reader carries an
// upper bound below uploadCompositeMin, only that request's chunk size is
// allocated for the lookahead instead of the full uploadCompositeMin.
func (g *gcsClient) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	head := make([]byte, singleUploadLookahead(reader))
	n, err := io.ReadFull(reader, head)
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		// The whole object is in hand and fits in one request.
		return g.putSingle(ctx, bucket, object, head[:n])
	case err != nil:
		return fmt.Errorf("while reading object body: %w", err)
	}
	return g.putComposite(ctx, bucket, object, bytes.NewReader(head[:n]), reader)
}

// putSingle writes body in one request, on a pooled client so the small objects
// a node uploads concurrently do not queue on one connection.
func (g *gcsClient) putSingle(ctx context.Context, bucket, object string, body []byte) error {
	wc := g.nextClient().Bucket(bucket).Object(object).NewWriter(ctx)
	wc.ChunkSize = int(singleUploadChunkSize(int64(len(body))))
	// Write reports local pipe errors; wc.Close() reports the actual GCS
	// upload (auth, permissions, transient). Join both so the caller
	// doesn't lose either.
	_, copyErr := wc.Write(body)
	closeErr := wc.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return fmt.Errorf("while putting GCS object: %w", err)
	}
	return nil
}
