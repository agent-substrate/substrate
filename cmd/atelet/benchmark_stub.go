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

// benchmarkStubHerder answers every AteomHerder call with success after a
// fixed delay, without touching an ateom, the node's disk or object storage.
// It lets a benchmark drive ate-api-server and Postgres at rates the real
// restore and checkpoint path cannot reach. Actors it "runs" do not exist, so
// it must never serve real workloads.
type benchmarkStubHerder struct {
	ateletpb.UnimplementedAteomHerderServer

	// delay stands in for the time a real call holds the actor's lease, so
	// ate-api-server sees the same concurrency it would at that latency.
	delay time.Duration
}

// wait sleeps for the delay, or returns the context's error if the caller
// gives up first.
func (s *benchmarkStubHerder) wait(ctx context.Context) error {
	if s.delay <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(s.delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *benchmarkStubHerder) Run(ctx context.Context, _ *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.RunResponse{}, nil
}

func (s *benchmarkStubHerder) Restore(ctx context.Context, _ *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.RestoreResponse{}, nil
}

func (s *benchmarkStubHerder) Checkpoint(ctx context.Context, _ *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.CheckpointResponse{}, nil
}

func (s *benchmarkStubHerder) UploadPausedCheckpoint(ctx context.Context, _ *ateletpb.UploadPausedCheckpointRequest) (*ateletpb.UploadPausedCheckpointResponse, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.UploadPausedCheckpointResponse{}, nil
}

func (s *benchmarkStubHerder) Terminate(ctx context.Context, _ *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.TerminateResponse{}, nil
}
