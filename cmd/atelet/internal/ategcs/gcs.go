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

package ategcs

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/internal/ateerrors"
)

type gcsClient struct {
	client *storage.Client
}

func NewGCSClient(client *storage.Client) ObjectStorage {
	return &gcsClient{client: client}
}

func (g *gcsClient) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	rc, err := g.client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, storage.ErrBucketNotExist) {
			return nil, fmt.Errorf("%w: Bucket:%q, Object:%q", ateerrors.ReasonFailedGetExternalObject, bucket, object)
		}
		return nil, err
	}
	return rc, nil
}

// supportsStreamingPut is the streamingPutter marker: the GCS client's PutObject
// accepts a non-seekable streaming body without buffering (it copies the reader
// straight into a storage.Writer — no Content-Length / signing requirement), so
// callers can pipe compression directly into the upload (overlap) instead of
// staging a seekable temp file. (S3's PutObject needs a seekable body, so s3Client
// does NOT implement this — see objects.go sendZstd.) Never called: its presence is
// the signal.
func (g *gcsClient) supportsStreamingPut() {}

// uploadChunkSize is how much of a streamed object the GCS client buffers before it
// starts a request. A resumable upload sends its chunks one after another, each paying
// a round trip, so an object that spans several chunks pays for several — which for the
// snapshots we upload is most of the time, because they are small.
//
// Measured on a GKE worker node (c3-standard-4, us-central1-f) uploading to the
// snapshot bucket through this exact streaming path, three runs of a 24 MiB object
// (the size of an idle micro-VM golden snapshot):
//
//	16 MiB (the client default)  425-530 ms
//	32 MiB                       331-405 ms
//	64 MiB                       258-314 ms
//	128 MiB                      350-487 ms
//
// 64 MiB covers a typical snapshot in one request and is the floor here; larger only
// costs buffer. The buffer is per in-flight upload and is capped by the object size, so
// a small object still costs only its own bytes.
//
// This does nothing for large snapshots: past ~100 MiB the transfer itself dominates
// and a single stream tops out near 100 MiB/s regardless of chunk size (measured
// 77-107 MiB/s at 300 MiB for every chunk size from 16 to 128 MiB). Getting past that
// needs several streams — 4-way parallel parts plus a compose reached 233-257 MiB/s —
// which is a format change, since each part has to be independently produced.
const uploadChunkSize = 64 << 20

func (g *gcsClient) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	wc := g.client.Bucket(bucket).Object(object).NewWriter(ctx)
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
