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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("objectstorage")

// ErrObjectNotFound reports the object or its bucket is absent from the
// storage backend, as opposed to a transient fetch failure.
var ErrObjectNotFound = errors.New("object not found in storage backend")

// ServiceError describes a storage service's failed response in terms its
// backend has judged safe to log: never a URI, header or credential.
type ServiceError struct {
	// HTTPStatus is the status of the service's response.
	HTTPStatus int
	// Code is the service's error code, such as "PreconditionFailed", if the
	// backend reports one.
	Code string
	// Message is the service's own description, if the backend reports one
	// it knows names no caller, resource or credential.
	Message string
}

// DescribeServiceError describes the storage service response that err
// carries, if any. Each backend decides what of its errors is safe to
// describe; an error in err's chain from any other source with an
// HTTPStatusCode() int method, as the S3 SDK's response errors have, is
// described by its status alone.
func DescribeServiceError(err error) (ServiceError, bool) {
	if desc, ok := describeGCSError(err); ok {
		return desc, true
	}
	var withStatus interface{ HTTPStatusCode() int }
	if errors.As(err, &withStatus) {
		return ServiceError{HTTPStatus: withStatus.HTTPStatusCode()}, true
	}
	return ServiceError{}, false
}

type ObjectStorage interface {
	GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, object string, reader io.Reader) error
}

// SizedReader is implemented by the readers GetObject returns when the
// backend reported the object's size as it opened it. The GCS and S3 clients
// in this package return one whenever the response carries the size.
type SizedReader interface {
	io.ReadCloser
	// ObjectSize is the whole object's size in bytes, as the backend reported
	// it, or a negative number if it is unknown.
	ObjectSize() int64
}

// ObjectSize returns the size of the object r, a reader GetObject returned,
// reads, and false if its backend did not report one.
func ObjectSize(r io.Reader) (int64, bool) {
	s, ok := r.(SizedReader)
	if !ok {
		return 0, false
	}
	n := s.ObjectSize()
	return n, n >= 0
}

func FetchFromGCS(ctx context.Context, client ObjectStorage, gsURL string) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "fetchFromGCS")
	defer span.End()

	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return nil, fmt.Errorf("while parsing url: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return nil, fmt.Errorf("while getting object bucket=%q object=%q: %w", bucket, object, err)
	}
	defer rc.Close()

	content, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("while reading all content: %w", err)
	}

	return content, nil
}

// Open streams the object at gsURL; the caller must Close the returned reader.
// Unlike FetchFromGCS it does not buffer the whole object in memory.
func Open(ctx context.Context, client ObjectStorage, gsURL string) (io.ReadCloser, error) {
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return nil, fmt.Errorf("while parsing url: %w", err)
	}
	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return nil, fmt.Errorf("while getting object bucket=%q object=%q: %w", bucket, object, err)
	}
	return rc, nil
}

func SendLocalFileToGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (err error) {
	ctx, span := tracer.Start(ctx, "sendLocalFileToGCSWithZstd")
	defer span.End()

	localFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", localFilePath, err)
	}
	defer func() {
		if closeErr := localFile.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from closing localFile", slog.String("localFile", localFilePath), slog.Any("err", err))
			}
		}
	}()

	if err := sendZstd(ctx, client, gsURL, localFile); err != nil {
		return fmt.Errorf("in sendZstd: %w", err)
	}

	return nil
}

// sparseFilePutter marks a backend that can upload a sparse FILE directly, splitting
// compression and upload by file range instead of piping one compressed stream into a
// part splitter. Reports errSparseTooSmall when the file is not worth splitting, in
// which case the caller falls back to the streaming path.
type sparseFilePutter interface {
	PutSparseFile(ctx context.Context, bucket, object string, f *os.File, maxParts int) (writeContentResult, error)
}

// streamingPutter marks an ObjectStorage whose PutObject accepts a non-seekable
// streaming body without buffering (e.g. GCS): implementing the interface is the
// signal, so the marker method is never called. See gcsClient.
type streamingPutter interface{ supportsStreamingPut() }

// writeContentResult reports what writeContent compressed.
type writeContentResult struct {
	// logicalBytes is the total logical size of the source, including the holes
	// for a sparse file.
	logicalBytes int64
	// populatedBytes is the count of bytes actually read + compressed: the non-hole
	// (resident) set for the sparse-extent format, == logicalBytes for a plain stream.
	populatedBytes int64
	// sparse is true when the sparse-extent format was used (the source was a file).
	sparse bool
}

// writeContent compresses content to out, choosing the sparse-extent format for a
// seekable *os.File (compress only the populated extents, skip the holes) or a
// plain zstd stream otherwise. It touches only io, so it is unit-testable without
// an object store, and is shared by the buffered and streaming upload paths.
func writeContent(ctx context.Context, out io.Writer, content io.Reader, workers int) (writeContentResult, error) {
	if f, ok := content.(*os.File); ok {
		logical, populated, err := writeSparseZstd(ctx, out, f, workers)
		if err != nil {
			return writeContentResult{}, err
		}
		return writeContentResult{logicalBytes: logical, populatedBytes: populated, sparse: true}, nil
	}
	logical, err := plainZstd(ctx, out, content, workers)
	if err != nil {
		return writeContentResult{}, err
	}
	return writeContentResult{logicalBytes: logical, populatedBytes: logical}, nil
}

// sendZstd zstd-compresses content and uploads it to gsURL.
//
// The snapshot memory-ranges is the large object here (the whole guest RAM image,
// mostly zero) on the SUSPEND critical path, so we compress with SpeedFastest within
// the shared upload memory budget. High-ratio levels scan the multi-GiB image
// far slower for little size gain on near-zero data, and the decoder auto-detects the level so restore + older
// snapshots are unaffected.
//
// Upload strategy depends on the backend:
//   - Streaming backends (GCS and S3) accept a non-seekable body, so we pipe the compressor
//     straight into PutObject: the compress overlaps the network PUT and we never
//     stage the ~100MiB compressed payload to a temp file.
//   - Other backends receive a seekable temporary file containing the compressed
//     content.
func sendZstd(ctx context.Context, client ObjectStorage, gsURL string, content io.Reader) error {
	u := &Uploader{client: client, budget: defaultUploadBudget}
	return u.sendZstd(ctx, gsURL, content)
}

func (u *Uploader) sendZstd(ctx context.Context, gsURL string, content io.Reader) error {
	client := u.client
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}
	plan, err := u.plan(content)
	if err != nil {
		return err
	}
	if err := u.budget.reserved.Acquire(ctx, plan.memory); err != nil {
		return fmt.Errorf("waiting for upload memory: %w", err)
	}
	defer u.budget.reserved.Release(plan.memory)
	tStart := time.Now()
	if sp, ok := client.(sparseFilePutter); ok {
		if f, isFile := content.(*os.File); isFile {
			res, err := sp.PutSparseFile(ctx, bucket, object, f, plan.parts)
			switch {
			case err == nil:
				slog.InfoContext(ctx, "Compressed zstd upload",
					slog.String("object", object), slog.Bool("sparse", true),
					slog.Bool("ranged", true),
					slog.Int64("logical_bytes", res.logicalBytes),
					slog.Int64("populated_bytes", res.populatedBytes),
					slog.Duration("total", time.Since(tStart)))
				return nil
			case !errors.Is(err, errSparseTooSmall):
				return fmt.Errorf("while putting object %q: %w", object, err)
			}
			// Too small to split: fall through to the streaming path.
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return err
			}
		}
	}
	if _, ok := client.(streamingPutter); ok {
		return sendStreamingZstd(ctx, client, bucket, object, content, tStart, plan.workers)
	}
	return sendBufferedZstd(ctx, client, bucket, object, content, tStart, plan.workers)
}

// sendBufferedZstd compresses content to a seekable temp file, then uploads it.
// Used for backends that require a seekable body; the streaming counterpart is
// sendStreamingZstd.
func sendBufferedZstd(ctx context.Context, client ObjectStorage, bucket, object string, content io.Reader, tStart time.Time, workers int) error {
	tmpFile, err := os.CreateTemp("", "substrate-upload-compress-")
	if err != nil {
		return fmt.Errorf("while creating temp compress file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	t0 := time.Now()
	res, err := writeContent(ctx, tmpFile, content, workers)
	if err != nil {
		return fmt.Errorf("while compressing %q: %w", object, err)
	}
	dCompress := time.Since(t0)

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("while seeking temp file: %w", err)
	}
	if err := client.PutObject(ctx, bucket, object, tmpFile); err != nil {
		return fmt.Errorf("while putting object %q: %w", object, err)
	}
	slog.InfoContext(ctx, "Compressed zstd upload",
		slog.String("object", object), slog.Bool("sparse", res.sparse),
		slog.Int64("logical_bytes", res.logicalBytes), slog.Int64("populated_bytes", res.populatedBytes),
		slog.Duration("compress", dCompress), slog.Duration("total", time.Since(tStart)))
	return nil
}

// sendStreamingZstd compresses content and uploads it in one overlapped pass: a
// goroutine writes the (sparse-extent or plain) zstd stream into an io.Pipe while
// PutObject streams the read end to the object store. No seekable temp file, and
// the compress runs concurrently with the network PUT. Used only for streaming
// backends (GCS and S3); see sendZstd.
func sendStreamingZstd(ctx context.Context, client ObjectStorage, bucket, object string, content io.Reader, tStart time.Time, workers int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		res writeContentResult
		err error
	}
	pr, pw := io.Pipe()
	ch := make(chan result, 1)
	go func() {
		res, err := writeContent(ctx, pw, content, workers)
		// Closing the writer delivers EOF (or the compress error) to PutObject.
		_ = pw.CloseWithError(err)
		ch <- result{res: res, err: err}
	}()

	putErr := client.PutObject(ctx, bucket, object, pr)
	if putErr != nil {
		cancel()
		// PutObject bailed (e.g. mid-stream); unblock the compressor goroutine so it
		// can finish and we don't deadlock on the channel receive below.
		_ = pr.CloseWithError(putErr)
	}
	r := <-ch
	if putErr != nil {
		return fmt.Errorf("while putting object %q: %w", object, putErr)
	}
	if r.err != nil {
		return fmt.Errorf("while compressing %q: %w", object, r.err)
	}
	slog.InfoContext(ctx, "Compressed zstd upload",
		slog.String("object", object), slog.Bool("sparse", r.res.sparse), slog.Bool("streaming", true),
		slog.Int64("logical_bytes", r.res.logicalBytes), slog.Int64("populated_bytes", r.res.populatedBytes),
		slog.Duration("total", time.Since(tStart)))
	return nil
}

// plainZstd writes src to w as a single plain zstd stream and returns the
// uncompressed byte count, using only the reserved compression workers.
func plainZstd(ctx context.Context, w io.Writer, src io.Reader, workers int) (int64, error) {
	zw, err := zstd.NewWriter(w,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(workers))
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(contextWriter{ctx: ctx, dst: zw}, src)
	if err != nil {
		zw.Close()
		return n, err
	}
	return n, zw.Close()
}

func FetchLocalFileFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (err error) {
	ctx, span := tracer.Start(ctx, "fetchLocalFileFromGCSWithZstd")
	defer span.End()

	localFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", localFilePath, err)
	}
	defer func() {
		if closeErr := localFile.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from closing localFile", slog.String("localFile", localFilePath), slog.Any("err", err))
			}
		}
	}()

	if err := localFile.Chmod(0o600); err != nil {
		return fmt.Errorf("in localFile.Chmod(0o600): %w", err)
	}

	if err := fetchFromGCSWithZstd(ctx, client, gsURL, localFile); err != nil {
		return fmt.Errorf("while fetching %q from GCS: %w", gsURL, err)
	}

	return nil
}

// FetchFileFromGCSWithZstd downloads and decompresses into an already-open
// file. The caller retains ownership of localFile.
func FetchFileFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFile *os.File) error {
	ctx, span := tracer.Start(ctx, "fetchFileFromGCSWithZstd")
	defer span.End()

	if err := localFile.Chmod(0o600); err != nil {
		return fmt.Errorf("in localFile.Chmod(0o600): %w", err)
	}
	if err := fetchFromGCSWithZstd(ctx, client, gsURL, localFile); err != nil {
		return fmt.Errorf("while fetching %q from GCS: %w", gsURL, err)
	}
	return nil
}

func fetchFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, out io.Writer) (err error) {
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return fmt.Errorf("while getting object: %w", err)
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from rc.Close", slog.Any("err", closeErr))
			}
		}
	}()

	t0 := time.Now()
	res, err := decodeContent(out, rc)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Decompressed zstd download",
		slog.Bool("sparse", res.sparse), slog.Int64("logical_bytes", res.logicalBytes),
		slog.Int64("written_bytes", res.writtenBytes), slog.Duration("took", time.Since(t0)))
	return nil
}

// decodeContentResult reports what decodeContent decompressed.
type decodeContentResult struct {
	// logicalBytes is the logical size written to out (the original image size).
	logicalBytes int64
	// writtenBytes is the count of non-hole bytes actually written on the sparse
	// file path; 0 on the io.Copy fallback (non-file destination).
	writtenBytes int64
	// sparse is true when the input used the sparse-extent format.
	sparse bool
}

// decodeContent decompresses src into out, auto-detecting the format from the
// leading magic: the sparse-extent format (sparseMagic) vs a plain zstd stream
// (older snapshots, or the non-file upload path). When out is an *os.File the plain
// path writes SPARSE (skips zero blocks → holes) so only the resident set is
// written, not a dense multi-GiB image. It touches only io, so it is unit-testable
// without an object store, mirroring writeContent.
func decodeContent(out io.Writer, src io.Reader) (decodeContentResult, error) {
	magic := make([]byte, len(sparseMagic))
	n, rerr := io.ReadFull(src, magic)
	if rerr == nil && string(magic) == sparseMagic {
		f, ok := out.(*os.File)
		if !ok {
			return decodeContentResult{}, fmt.Errorf("sparse-extent snapshot requires a file destination, got %T", out)
		}
		size, derr := readSparseZstd(f, src) // src is positioned just after the magic
		if derr != nil {
			return decodeContentResult{}, fmt.Errorf("in sparse-extent decode: %w", derr)
		}
		return decodeContentResult{logicalBytes: size, sparse: true}, nil
	}
	if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
		return decodeContentResult{}, fmt.Errorf("while reading object header: %w", rerr)
	}

	// Plain zstd stream: put back the peeked bytes, then decompress.
	r := io.MultiReader(bytes.NewReader(magic[:n]), src)
	zrc, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return decodeContentResult{}, fmt.Errorf("in zstd.NewReader: %w", err)
	}
	defer zrc.Close()
	if f, ok := out.(*os.File); ok {
		size, written, derr := copyZstdSparse(f, zrc)
		if derr != nil {
			return decodeContentResult{}, fmt.Errorf("in sparse decompress: %w", derr)
		}
		return decodeContentResult{logicalBytes: size, writtenBytes: written}, nil
	}
	size, cerr := io.Copy(out, zrc)
	if cerr != nil {
		return decodeContentResult{}, fmt.Errorf("in io.Copy: %w", cerr)
	}
	return decodeContentResult{logicalBytes: size}, nil
}

// copyZstdSparse writes src into dst skipping all-zero blocks, so dst becomes a
// sparse file (the skipped regions are holes). Returns the logical size (total bytes
// consumed from src) and the bytes actually written (non-zero). dst is truncated to
// empty first (so skipped regions are real holes, not stale bytes) and to the
// logical size at the end (so trailing zero regions become a hole and the size is
// exact). dst must be a regular file opened for writing.
func copyZstdSparse(dst *os.File, src io.Reader) (size int64, written int64, err error) {
	// Start from an empty file so the holes we skip can't expose pre-existing bytes:
	// this writes out only the non-zero chunks, it does not overlay onto dst.
	if err := dst.Truncate(0); err != nil {
		return 0, 0, fmt.Errorf("truncating dst: %w", err)
	}
	// 64KiB blocks: a multiple of the 4KiB fs block (so skipped runs align to whole
	// hole-able blocks) while keeping the zero-scan + WriteAt syscall count modest.
	const block = 64 << 10
	buf := make([]byte, block)
	var pos int64
	for {
		n, rerr := io.ReadFull(src, buf)
		if n > 0 {
			chunk := buf[:n]
			if !allZero(chunk) {
				if _, werr := dst.WriteAt(chunk, pos); werr != nil {
					return 0, 0, werr
				}
				written += int64(n)
			}
			pos += int64(n)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return 0, 0, rerr
		}
	}
	// Materialize the exact logical size: extends past the last written byte with a
	// hole when the tail was zero (skipped), and is a no-op otherwise.
	if terr := dst.Truncate(pos); terr != nil {
		return 0, 0, terr
	}
	return pos, written, nil
}

// allZero reports whether b is all zero bytes, checking 8 bytes at a time.
func allZero(b []byte) bool {
	i := 0
	for ; i+8 <= len(b); i += 8 {
		if binary.LittleEndian.Uint64(b[i:]) != 0 {
			return false
		}
	}
	for ; i < len(b); i++ {
		if b[i] != 0 {
			return false
		}
	}
	return true
}

func parseGCSURL(gsURL string) (string, string, error) {
	parsed, err := url.Parse(gsURL)
	if err != nil {
		return "", "", fmt.Errorf("while parsing %q: %w", gsURL, err)
	}

	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}
