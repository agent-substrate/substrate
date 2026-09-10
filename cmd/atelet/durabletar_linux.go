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

package main

import (
	"context"
	"io"
	"os"
	"strings"
	"sync/atomic"

	"github.com/agent-substrate/substrate/internal/tarutil"
	"golang.org/x/sync/errgroup"
)

// streamDurableTarEnv turns the streaming durable-dir capture on. Off by
// default: it moves the archive out of ateom's paused window (see
// CheckpointWorkloadRequest.skip_durable_dir_tar) and is worth measuring
// against the staged path before it becomes the only one.
const streamDurableTarEnv = "ATELET_STREAM_DURABLE_TAR"

// streamDurableTarEnabled reports whether this atelet may take the durable-dir
// archive over from ateom. Callers must still check that the actor's runtime
// and checkpoint type allow it; see canStreamDurableDirTar.
func streamDurableTarEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(streamDurableTarEnv))) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	}
	return false
}

// streamDurableDirTar archives srcDir and hands the archive to upload as it is
// produced, so the walk overlaps compression and the network PUT and the
// archive is never staged on disk. It returns the uncompressed archive size for
// the size metric, which is only meaningful when the error is nil.
//
// The archive must be identical to the one ateom would have written, or a
// snapshot taken this way would not restore the same tree: micro-VM archives
// all of the durable-dir mounts dir unfiltered (see ateom-microvm's
// tarDurableVolumes), which is what a nil SkipFunc reproduces here.
func streamDurableDirTar(ctx context.Context, srcDir string, upload func(io.Reader) error) (int64, error) {
	pr, pw := io.Pipe()
	counter := &countingWriter{w: pw}

	var g errgroup.Group
	g.Go(func() error {
		err := tarutil.CreateTo(ctx, counter, srcDir, nil)
		// A nil error closes the write end normally, ending the upload's read
		// at EOF; anything else surfaces at that read, so the upload aborts
		// instead of committing a truncated object.
		pw.CloseWithError(err)
		return err
	})

	uploadErr := upload(pr)
	// Unblocks the walk if the upload stopped reading early, so g.Wait cannot
	// hang on a full pipe.
	pr.CloseWithError(uploadErr)
	tarErr := g.Wait()

	// uploadErr first: when the walk is what failed, the upload's read returned
	// that same error and reports it with the upload's context attached, while
	// tarErr in the reverse case is only io.ErrClosedPipe.
	if uploadErr != nil {
		return 0, uploadErr
	}
	if tarErr != nil {
		return 0, tarErr
	}
	return counter.n.Load(), nil
}

// countingWriter totals the bytes written through it. The archive never exists
// as a file, so this is the only place its size can be observed.
type countingWriter struct {
	w io.Writer
	n atomic.Int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n.Add(int64(n))
	return n, err
}
