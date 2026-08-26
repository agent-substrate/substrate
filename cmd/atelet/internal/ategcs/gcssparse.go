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
	"os"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
)

// partWriterChunk is how much of a part the GCS client buffers before it sends. This
// bounds the memory a parallel upload holds: sparseMaxParts * this.
const partWriterChunk = 16 << 20

// PutSparseFile compresses and uploads a sparse file as parts that are composed back
// into one object, fanning the WHOLE pipeline out by file range: each part reads its
// own extents (ReadAt), compresses them, and streams into its own object over its own
// connection, so every stream starts at once.
//
// Piping one compressed stream into a part splitter instead serializes the fan-out
// behind a single compressor: measured on a GKE worker node, a 541 MB actor took 1.78s
// that way against 87 MB/s-per-stream x 8 streams available from the node.
//
// The parts are consecutive byte ranges of the same sparse-extent stream — part 0
// carries the header, the last carries the end sentinel — so the composed object is
// exactly what the single-stream writer would have produced.
func (g *gcsClient) PutSparseFile(ctx context.Context, bucket, object string, f *os.File) (res writeContentResult, err error) {
	fi, err := f.Stat()
	if err != nil {
		return res, err
	}
	size := fi.Size()
	exts, populated, err := sparseExtents(f, size)
	if err != nil {
		return res, err
	}
	res = writeContentResult{logicalBytes: size, populatedBytes: populated, sparse: true}

	n := sparsePartCount(populated)
	if n < 2 {
		// Too small to be worth splitting; the plain streaming path handles it.
		return res, errSparseTooSmall
	}
	groups := planSparseParts(exts, populated, n)

	var idBytes [8]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return res, fmt.Errorf("while naming upload parts: %w", err)
	}
	runID := hex.EncodeToString(idBytes[:])

	bkt := g.client.Bucket(bucket)
	parts := make([]*storage.ObjectHandle, len(groups))
	for i := range groups {
		parts[i] = bkt.Object(fmt.Sprintf("%s.part-%s-%04d", object, runID, i))
	}
	defer func() {
		for _, p := range parts {
			if delErr := p.Delete(context.WithoutCancel(ctx)); delErr != nil &&
				!errors.Is(delErr, storage.ErrObjectNotExist) && err == nil {
				err = fmt.Errorf("while removing upload part %q: %w", p.ObjectName(), delErr)
			}
		}
	}()

	grp, gctx := errgroup.WithContext(ctx)
	for i, ranges := range groups {
		grp.Go(func() error {
			// Each part uploads over its own connection; see uploadClient.
			obj := g.uploadClient(gctx, i).Bucket(bucket).Object(parts[i].ObjectName())
			w := obj.NewWriter(gctx)
			w.ChunkSize = partWriterChunk
			if err := writeSparsePart(w, f, size, ranges, i == 0, i == len(groups)-1); err != nil {
				_ = w.Close()
				return fmt.Errorf("while writing part %d of %q: %w", i, object, err)
			}
			return w.Close()
		})
	}
	if err := grp.Wait(); err != nil {
		return res, err
	}
	return res, composeAll(ctx, bkt, object, parts, runID)
}

// errSparseTooSmall says the object is not worth splitting, so the caller should fall
// back to the single-stream path.
var errSparseTooSmall = errors.New("sparse file below the parallel-part threshold")
