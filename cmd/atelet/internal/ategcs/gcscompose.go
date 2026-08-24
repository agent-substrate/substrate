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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
)

// uploadPoolSize is how many storage.Clients the parallel part upload spreads its
// parts over. One client keeps a single HTTP/2 connection per host and multiplexes
// every part onto it, so parts that should be independent share one TCP stream:
// measured on a GKE worker node at 8 streams, 334 MB/s shared against 518 MB/s with a
// connection each.
const uploadPoolSize = 8

// uploadClient returns the client part i should use, or the default client if the pool
// could not be built (a pool failure costs throughput, not correctness).
func (g *gcsClient) uploadClient(ctx context.Context, i int) *storage.Client {
	g.poolOnce.Do(func() {
		for range uploadPoolSize {
			// The clients outlive this call, so they must not hold its cancellation.
			c, err := storage.NewClient(context.WithoutCancel(ctx), g.opts...)
			if err != nil {
				slog.WarnContext(ctx, "Falling back to one client for part uploads", slog.Any("err", err))
				return
			}
			g.pool = append(g.pool, c)
		}
	})
	if len(g.pool) == 0 {
		return g.client
	}
	return g.pool[i%len(g.pool)]
}

// One stream to GCS tops out near 100 MiB/s however it is chunked; several do not
// (measured at 300 MiB: 82-107 MiB/s on one stream, 233-257 on four). So a large object
// is cut into parts, uploaded concurrently, and composed server-side. The parts are
// byte ranges of the same stream, so the result is byte-identical to a single
// PutObject and the download path is unaffected.
const (
	// uploadCompositeMin is the size below which an object goes up as one request.
	// Small objects lose to parts — 24 MiB measured 258-314ms as one request against
	// 310-410ms as two — so this sits at the conservative end of that bracket.
	// uploadPartSize/uploadConcurrency come from s3.go: same peak on both backends.
	uploadCompositeMin = 64 << 20
	// maxComposeSources is GCS's limit on sources per compose call. More parts than
	// this are folded in rounds.
	maxComposeSources = 32
)

// putComposite uploads head followed by rest as concurrent parts, then composes them
// into object. Only for objects past uploadCompositeMin; putSingle handles the rest.
func (g *gcsClient) putComposite(ctx context.Context, bucket, object string, head io.Reader, rest io.Reader) (err error) {
	bkt := g.client.Bucket(bucket)
	// A run id keeps concurrent or retried uploads of the same object from colliding on
	// part names, and makes leftovers from a crash identifiable.
	var idBytes [8]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return fmt.Errorf("while naming upload parts: %w", err)
	}
	runID := hex.EncodeToString(idBytes[:])

	var parts []*storage.ObjectHandle
	defer func() {
		// Parts are scratch either way, and deleting them must not mask the real error.
		for _, p := range parts {
			if delErr := p.Delete(context.WithoutCancel(ctx)); delErr != nil &&
				!errors.Is(delErr, storage.ErrObjectNotExist) && err == nil {
				err = fmt.Errorf("while removing upload part %q: %w", p.ObjectName(), delErr)
			}
		}
	}()

	// The stream is read in order here and whole parts handed to the uploaders; buffers
	// cycle through free so the peak stays at uploadConcurrency of them.
	free := make(chan []byte, uploadConcurrency)
	for range uploadConcurrency {
		free <- make([]byte, uploadPartSize)
	}
	g2, gctx := errgroup.WithContext(ctx)
	g2.SetLimit(uploadConcurrency)

	src := io.MultiReader(head, rest)
	for i := 0; ; i++ {
		var buf []byte
		select {
		case buf = <-free:
		case <-gctx.Done():
			return errors.Join(g2.Wait(), gctx.Err())
		}
		n, readErr := io.ReadFull(src, buf)
		if n > 0 {
			part := bkt.Object(fmt.Sprintf("%s.part-%s-%04d", object, runID, i))
			parts = append(parts, part)
			upPart := g.uploadClient(ctx, i).Bucket(bucket).Object(part.ObjectName())
			data := buf[:n]
			g2.Go(func() error {
				defer func() { free <- buf }()
				return writeObject(gctx, upPart, data, n)
			})
		} else {
			free <- buf
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				break
			}
			return errors.Join(fmt.Errorf("while reading upload part %d: %w", i, readErr), g2.Wait())
		}
	}
	if err := g2.Wait(); err != nil {
		return fmt.Errorf("while uploading parts of %q: %w", object, err)
	}
	return composeAll(ctx, bkt, object, parts, runID)
}

// writeObject writes data to obj in one request.
func writeObject(ctx context.Context, obj *storage.ObjectHandle, data []byte, size int) error {
	w := obj.NewWriter(ctx)
	// The part is already in memory; no reason to let the client cut it up again.
	w.ChunkSize = size
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

// composeAll composes parts into object, folding in rounds when there are more parts
// than GCS accepts in one call. Intermediates are cleaned up as they are consumed.
func composeAll(ctx context.Context, bkt *storage.BucketHandle, object string, parts []*storage.ObjectHandle, runID string) error {
	if len(parts) == 0 {
		return fmt.Errorf("no parts to compose into %q", object)
	}
	round := 0
	for len(parts) > maxComposeSources {
		var next []*storage.ObjectHandle
		for i := 0; i < len(parts); i += maxComposeSources {
			end := min(i+maxComposeSources, len(parts))
			group := parts[i:end]
			if len(group) == 1 {
				next = append(next, group[0])
				continue
			}
			mid := bkt.Object(fmt.Sprintf("%s.compose-%s-%d-%04d", object, runID, round, i))
			if _, err := mid.ComposerFrom(group...).Run(ctx); err != nil {
				return fmt.Errorf("while composing parts of %q: %w", object, err)
			}
			// The sources are dead once folded; the caller's deferred cleanup only
			// knows about the leaf parts.
			for _, s := range group {
				_ = s.Delete(context.WithoutCancel(ctx))
			}
			next = append(next, mid)
		}
		parts = next
		round++
	}
	if len(parts) == 1 {
		// Exactly one part long: copy, since compose needs two or more sources.
		if _, err := bkt.Object(object).CopierFrom(parts[0]).Run(ctx); err != nil {
			return fmt.Errorf("while copying single part into %q: %w", object, err)
		}
		return nil
	}
	if _, err := bkt.Object(object).ComposerFrom(parts...).Run(ctx); err != nil {
		return fmt.Errorf("while composing %q: %w", object, err)
	}
	return nil
}
