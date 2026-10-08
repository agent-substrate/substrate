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
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// delays is how long each AteomHerder call takes before it succeeds: the
// data plane's share of it.
type delays struct {
	run, restore, checkpoint, uploadPausedCheckpoint, terminate time.Duration
}

// herder answers every AteomHerder call with success after its delay, without
// running or saving any workload. Actors it "runs" do not exist.
type herder struct {
	ateletpb.UnimplementedAteomHerderServer

	delays delays
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

func (h *herder) Checkpoint(ctx context.Context, _ *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	if err := wait(ctx, time.Now(), h.delays.checkpoint); err != nil {
		return nil, err
	}
	return &ateletpb.CheckpointResponse{}, nil
}

func (h *herder) UploadPausedCheckpoint(ctx context.Context, _ *ateletpb.UploadPausedCheckpointRequest) (*ateletpb.UploadPausedCheckpointResponse, error) {
	if err := wait(ctx, time.Now(), h.delays.uploadPausedCheckpoint); err != nil {
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
