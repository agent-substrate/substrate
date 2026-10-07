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
	"errors"
	"io"
	"testing"
	"time"
)

func TestRangedReaderCloseStopsRequests(t *testing.T) {
	head, body := io.Pipe()
	defer head.Close()
	defer body.Close()
	fetchContext := make(chan context.Context, 1)
	r := newRangedReader(t.Context(), downloadChunkSize+1, head, func(ctx context.Context, _ int, _, _ int64, _ []byte) error {
		fetchContext <- ctx
		<-ctx.Done()
		return ctx.Err()
	})
	defer r.Close()

	// Leave the first range blocked waiting for the rest of its body.
	if _, err := io.WriteString(body, "partial"); err != nil {
		t.Fatal(err)
	}
	ctx := <-fetchContext
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatal("Close did not cancel the later range request")
	}

	written := make(chan error, 1)
	go func() {
		_, err := io.WriteString(body, "more")
		written <- err
	}()
	select {
	case err := <-written:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("first range write = %v, want closed pipe", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close left the first range reader open")
	}
}
