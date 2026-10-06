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
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// placeholderObject is the one object written under each snapshot URI.
const placeholderObject = "fake-dataplane-placeholder"

// objectWriter is the part of pkg/objectstorage.ObjectStorage the herder
// uses.
type objectWriter interface {
	PutObject(ctx context.Context, bucket, object string, reader io.Reader) error
}

// delays is how long each AteomHerder call takes before it succeeds: the
// data plane's share of it.
type delays struct {
	run, restore, checkpoint, uploadPausedCheckpoint, terminate time.Duration
}

// herder answers every AteomHerder call with success after its delay, without
// running or saving any workload. Actors it "runs" do not exist.
//
// The one side effect it keeps is a placeholder object under each snapshot
// URI it is asked to write: ate-api-server copies an actor's snapshot when it
// creates a tag, golden tags included, and refuses to copy an empty one.
type herder struct {
	ateletpb.UnimplementedAteomHerderServer

	delays  delays
	storage objectWriter
}

// wait sleeps until start+d, or returns the context's error if the caller
// gives up first.
func wait(ctx context.Context, start time.Time, d time.Duration) error {
	remaining := d - time.Since(start)
	if remaining <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(remaining)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// writePlaceholder writes the placeholder object under snapshotURI. It runs
// inside the call's delay; a write slower than a nonzero delay is logged, since
// the call then takes longer than configured. With no delay, every write
// outlasts it, so there is nothing to report.
func (h *herder) writePlaceholder(ctx context.Context, start time.Time, d time.Duration, snapshotURI string) error {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return fmt.Errorf("while parsing snapshot URI %q: %w", snapshotURI, err)
	}
	bucket, prefix, err := objectstore.BucketPrefix(uri.Prefix())
	if err != nil {
		return err
	}
	if err := h.storage.PutObject(ctx, bucket, prefix+placeholderObject, strings.NewReader("")); err != nil {
		return fmt.Errorf("while writing the placeholder under %s: %w", snapshotURI, err)
	}
	if elapsed := time.Since(start); d > 0 && elapsed > d {
		slog.WarnContext(ctx, "Placeholder write outlasted the configured delay",
			slog.String("snapshot_uri", snapshotURI), slog.Duration("elapsed", elapsed), slog.Duration("delay", d))
	}
	return nil
}

func (h *herder) Run(ctx context.Context, _ *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	if err := wait(ctx, time.Now(), h.delays.run); err != nil {
		return nil, err
	}
	return &ateletpb.RunResponse{}, nil
}

func (h *herder) Restore(ctx context.Context, _ *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	if err := wait(ctx, time.Now(), h.delays.restore); err != nil {
		return nil, err
	}
	return &ateletpb.RestoreResponse{}, nil
}

func (h *herder) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	start := time.Now()
	if req.GetType() == ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL {
		if err := h.writePlaceholder(ctx, start, h.delays.checkpoint, req.GetExternalConfig().GetSnapshotUri()); err != nil {
			return nil, err
		}
	}
	if err := wait(ctx, start, h.delays.checkpoint); err != nil {
		return nil, err
	}
	return &ateletpb.CheckpointResponse{}, nil
}

func (h *herder) UploadPausedCheckpoint(ctx context.Context, req *ateletpb.UploadPausedCheckpointRequest) (*ateletpb.UploadPausedCheckpointResponse, error) {
	start := time.Now()
	if err := h.writePlaceholder(ctx, start, h.delays.uploadPausedCheckpoint, req.GetDestinationSnapshotUri()); err != nil {
		return nil, err
	}
	if err := wait(ctx, start, h.delays.uploadPausedCheckpoint); err != nil {
		return nil, err
	}
	return &ateletpb.UploadPausedCheckpointResponse{}, nil
}

func (h *herder) Terminate(ctx context.Context, _ *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	if err := wait(ctx, time.Now(), h.delays.terminate); err != nil {
		return nil, err
	}
	return &ateletpb.TerminateResponse{}, nil
}
