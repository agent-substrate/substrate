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
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
)

type gcsClient struct {
	// controlClient serves metadata-only calls (compose, copy, delete). Object bytes
	// never go through it: see poolClient.
	controlClient *storage.Client
	// pool holds the clients that move object bytes, so concurrent transfers get
	// their own connections. All are built from the same options as controlClient:
	// a pooled client that authenticates differently from the one that opened an
	// object fails partway through reading it.
	pool []*storage.Client
	// next is the pool index poolClient hands out next.
	next atomic.Uint32
}

// poolSize is how many storage.Clients object transfers spread over. One client keeps
// a single HTTP/2 connection per host and multiplexes every request onto it, so
// transfers that should be independent share one TCP stream: measured on a GKE worker
// node at 8 streams, 334 MB/s shared against 518 MB/s with a connection each.
const poolSize = 8

// NewGCSClient returns a GCS-backed ObjectStorage. It builds its own
// storage.Clients from opts rather than accepting one, because it installs a
// RetryAlways policy (see setRetry) that is only safe for this package's own
// operations and must not leak onto a client shared with other code. The clients
// are built concurrently, here rather than on first use, so no request waits on
// their setup.
func NewGCSClient(ctx context.Context, opts ...option.ClientOption) (ObjectStorage, error) {
	clients := make([]*storage.Client, 1+poolSize)
	var grp errgroup.Group
	for i := range clients {
		grp.Go(func() error {
			// The clients outlive this call, so they must not hold a context that
			// ends with it.
			c, err := storage.NewClient(context.WithoutCancel(ctx), opts...)
			if err != nil {
				return err
			}
			setRetry(c)
			clients[i] = c
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		for _, c := range clients {
			if c != nil {
				_ = c.Close()
			}
		}
		return nil, err
	}
	return &gcsClient{controlClient: clients[0], pool: clients[1:]}, nil
}

// poolClient returns the client for the next request that moves object bytes. It
// advances on every call, so consecutive parts or ranges of one object land on
// distinct connections and concurrent objects interleave evenly across the pool.
func (g *gcsClient) poolClient() *storage.Client {
	return g.pool[g.next.Add(1)%uint32(len(g.pool))]
}

// setRetry makes every operation on c retry transient errors (408, 429, 5xx)
// with backoff. The client's default RetryIdempotent policy never retries an
// object write without a precondition, so a single 429 — routine while GCS
// scales a cold bucket's key ranges up to a suspend burst — failed the whole
// snapshot. RetryAlways is safe for this client because every write targets a
// unique name (snapshot UUID directories, runID-suffixed part names) or is
// idempotent (compose and copy from fixed sources, delete).
func setRetry(c *storage.Client) {
	c.SetRetry(
		storage.WithPolicy(storage.RetryAlways),
		// Suspend is latency-sensitive, so bound the worst case: at most 5
		// attempts, under 5s of backoff sleep in total (250ms+500ms+1s+2s).
		storage.WithBackoff(gax.Backoff{Initial: 250 * time.Millisecond, Max: 2 * time.Second, Multiplier: 2}),
		storage.WithMaxAttempts(5),
	)
}

// supportsStreamingPut is the streamingPutter marker: the GCS client's PutObject
// accepts a non-seekable streaming body without buffering (it copies the reader
// straight into a storage.Writer — no Content-Length / signing requirement), so
// callers can pipe compression directly into the upload (overlap) instead of
// staging a seekable temp file. (S3's PutObject needs a seekable body, so s3Client
// does NOT implement this — see objects.go sendZstd.) Never called: its presence is
// the signal.
func (g *gcsClient) supportsStreamingPut() {}

// uploadChunkSize is how much of a streamed object the GCS client buffers before
// starting a request. Each chunk costs a round trip, so a snapshot that fits in one
// chunk pays only one: measured on a GKE worker node, a 24 MiB object took 425-530ms
// at the 16 MiB default versus 258-314ms at 64 MiB. The buffer is capped by the object
// size, so small objects still cost only their own bytes.
const uploadChunkSize = 64 << 20

// PutObject writes reader to the object, in a single request below
// uploadCompositeMin and as parallel parts above it. The size is not known up front,
// so this reads that many bytes to find out which case it is, then hands them on.
func (g *gcsClient) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	head := make([]byte, uploadCompositeMin)
	n, err := io.ReadFull(reader, head)
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		// The whole object is in hand and fits in one request.
		return g.putSingle(ctx, bucket, object, bytes.NewReader(head[:n]))
	case err != nil:
		return fmt.Errorf("while reading object body: %w", err)
	}
	return g.putComposite(ctx, bucket, object, bytes.NewReader(head[:n]), reader)
}

// putSingle writes the whole body in one resumable request.
func (g *gcsClient) putSingle(ctx context.Context, bucket, object string, reader io.Reader) error {
	wc := g.poolClient().Bucket(bucket).Object(object).NewWriter(ctx)
	wc.ChunkSize = uploadChunkSize
	// io.Copy reports local read errors; wc.Close() reports the actual
	// GCS upload (auth, permissions, transient). Join both so the caller
	// doesn't lose either.
	_, copyErr := io.Copy(wc, reader)
	closeErr := wc.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return fmt.Errorf("while putting GCS object: %w", err)
	}
	return nil
}
