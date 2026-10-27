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

package objectstorage

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"

	"golang.org/x/sync/semaphore"
)

// DefaultUploadMemory is the shared reservation budget for snapshot compression
// and upload buffers. It is not a limit on the process's total memory.
const DefaultUploadMemory int64 = 512 << 20

const (
	// Each parallel encoder owns input buffers, queued compressed frames, and
	// zstd state. Include the writer's frame and the producer's pending frame
	// even when there is only one worker.
	parallelWorkerMemory int64 = (parZstdQueue*3 + 2) * parZstdChunk
	// A part's encoder (parZstd inline, workers == 1) holds an input chunk, its
	// output frame, the encoder's history for that chunk, and a copy buffer: about
	// 26 MiB with 8 MiB chunks (BenchmarkWriteSparsePart/part=64MiB).
	serialWorkerMemory int64 = 32 << 20
	sparseUploadMemory       = serialWorkerMemory + partWriterChunk
)

type uploadBudget struct {
	capacity int64
	reserved *semaphore.Weighted
}

var defaultUploadBudget = &uploadBudget{
	capacity: DefaultUploadMemory,
	reserved: semaphore.NewWeighted(DefaultUploadMemory),
}

// Uploader shares a memory reservation budget across concurrent uploads. Reuse
// one Uploader for all snapshot requests served by a node. Reservations cover
// compression and the GCS/S3 clients' buffering, and last until both finish.
// Other ObjectStorage implementations must bound their own upload buffers.
// Runtime overhead, downloads, and memory awaiting garbage collection are not
// covered by the reservation budget.
type Uploader struct {
	client ObjectStorage
	budget *uploadBudget
}

// NewUploader creates an uploader with capacity bytes of reservations. Capacity
// must fit one compression worker plus the backend's upload buffers; larger files
// use fewer workers when necessary rather than waiting for an impossible request.
func NewUploader(client ObjectStorage, capacity int64) (*Uploader, error) {
	minimum := uploadBufferMemory(client) + parallelWorkerMemory
	if capacity < minimum {
		return nil, fmt.Errorf("upload memory budget must be at least %d bytes for this backend", minimum)
	}
	return &Uploader{client: client, budget: &uploadBudget{
		capacity: capacity,
		reserved: semaphore.NewWeighted(capacity),
	}}, nil
}

// uploadBufferMemory includes buffers retained while the compressor is running.
// WithClient returns an Uploader that sends through client and shares u's
// memory budget, for a transfer that runs with its own credentials. client
// must be the same kind of backend as u's, since the budget was sized for it.
func (u *Uploader) WithClient(client ObjectStorage) *Uploader {
	return &Uploader{client: client, budget: u.budget}
}

func uploadBufferMemory(client ObjectStorage) int64 {
	switch client.(type) {
	case *s3Client:
		// The SDK holds one producer buffer in addition to the active parts.
		return (uploadConcurrency + 1) * uploadPartSize
	case *gcsClient:
		// Composite uploads retain the lookahead, our part pool, and the SDK's
		// copy of each part. Single uploads fit within this reservation too.
		return uploadCompositeMin + 2*uploadConcurrency*uploadPartSize
	default:
		return 0
	}
}

type uploadPlan struct {
	workers int
	parts   int
	memory  int64
}

func (u *Uploader) plan(content io.Reader) (uploadPlan, error) {
	plan, _, _, err := u.planStream(content)
	return plan, err
}

func (u *Uploader) planStream(content io.Reader) (uploadPlan, int64, io.Reader, error) {
	buffers := uploadBufferMemory(u.client)
	workers := min(int64(runtime.GOMAXPROCS(0)), (u.budget.capacity-buffers)/parallelWorkerMemory)
	workerMemory := parallelWorkerMemory
	parts := 1
	streamMax := int64(-1)
	if f, ok := content.(*os.File); ok {
		info, err := f.Stat()
		if err != nil {
			return uploadPlan{}, 0, nil, err
		}
		position, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return uploadPlan{}, 0, nil, err
		}
		exts, populated, err := sparseExtents(f, info.Size())
		_, seekErr := f.Seek(position, io.SeekStart)
		if err != nil {
			return uploadPlan{}, 0, nil, err
		}
		if seekErr != nil {
			return uploadPlan{}, 0, nil, seekErr
		}
		// Holes never enter the compressor or range uploads.
		workers = min(workers, populated/parZstdChunk+1)
		parts = min(sparsePartCount(populated), int(u.budget.capacity/sparseUploadMemory))
		if _, ok := u.client.(sparseFilePutter); ok && parts >= 2 {
			// PutSparseFile splits a file into parts whenever it has two or
			// more (see errSparseTooSmall), so the streaming fallback never
			// runs: reserve only what the parts hold.
			return uploadPlan{workers: int(workers), parts: parts, memory: int64(parts) * sparseUploadMemory}, -1, content, nil
		}
		streamMax = sparseEncodedBound(populated, len(exts))
		buffers = streamBufferMemory(u.client, streamMax)
		workerMemory = parZstdWorkerMemory(parZstdChunkSize(populated))
		content = &sparseFile{File: f, size: info.Size(), exts: exts, populated: populated}
	}
	memory := buffers + workers*workerMemory
	return uploadPlan{workers: int(workers), parts: parts, memory: memory}, streamMax, content, nil
}

// sparseEncodedBound bounds the size of the sparse-extent stream for a file
// whose populated bytes lie in the given number of extents: zstd's worst-case
// expansion plus the format's header, per-extent frames, and per-chunk zstd
// frame headers.
func sparseEncodedBound(populated int64, extents int) int64 {
	return populated + populated/128 + int64(extents)*16 + (populated/parZstdChunk+1)*64 + 64
}

// streamBufferMemory is the backend buffering for a streamed object of at
// most size bytes. GCS sends an object smaller than uploadCompositeMin in one
// request: PutObject allocates singleUploadChunkSize(size) for the lookahead
// and putSingle sets the same ChunkSize on the writer, never allocating the
// composite parts.
func streamBufferMemory(client ObjectStorage, size int64) int64 {
	if _, ok := client.(*gcsClient); ok && size < uploadCompositeMin {
		return 2 * singleUploadChunkSize(size)
	}
	return uploadBufferMemory(client)
}

// SendFile compresses and uploads an already-open snapshot file. The caller keeps
// ownership of the file. Waiting for memory is canceled with ctx.
func (u *Uploader) SendFile(ctx context.Context, url string, file *os.File) error {
	ctx, span := tracer.Start(ctx, "sendFileToGCSWithZstd")
	defer span.End()
	return u.sendZstd(ctx, url, file)
}

// Send uploads uncompressed content, using the same budget as SendFile. This is
// used for snapshot manifests; the reader is streamed without retaining its body.
func (u *Uploader) Send(ctx context.Context, url string, content io.Reader) error {
	ctx, span := tracer.Start(ctx, "sendBytesToGCS")
	defer span.End()

	bucket, object, err := parseGCSURL(url)
	if err != nil {
		return err
	}
	memory := uploadBufferMemory(u.client)
	if f, ok := content.(*os.File); ok {
		// A manifest is a few KiB: size its reservation by the file rather
		// than by the largest object the backend could buffer.
		if info, err := f.Stat(); err == nil && info.Mode().IsRegular() {
			size := info.Size()
			memory = streamBufferMemory(u.client, size)
			content = boundedReader{Reader: content, max: size}
		}
	}
	if err := u.budget.reserved.Acquire(ctx, memory); err != nil {
		return err
	}
	defer u.budget.reserved.Release(memory)
	return u.client.PutObject(ctx, bucket, object, content)
}

// contextWriter stops compression between writes even after an asynchronous
// destination has failed and is draining queued frames.
type contextWriter struct {
	ctx context.Context
	dst io.Writer
}

func (w contextWriter) Write(b []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.dst.Write(b)
}
