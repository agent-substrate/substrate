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
	"errors"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

func uniformDelays(d time.Duration) delays {
	return delays{run: d, restore: d, checkpoint: d, uploadPausedCheckpoint: d, terminate: d}
}

// herderCalls invokes every AteomHerder method the herder implements.
func herderCalls(h *herder) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"Run": func(ctx context.Context) error {
			_, err := h.Run(ctx, &ateletpb.RunRequest{})
			return err
		},
		"Restore": func(ctx context.Context) error {
			_, err := h.Restore(ctx, &ateletpb.RestoreRequest{})
			return err
		},
		"Checkpoint": func(ctx context.Context) error {
			_, err := h.Checkpoint(ctx, &ateletpb.CheckpointRequest{})
			return err
		},
		"UploadPausedCheckpoint": func(ctx context.Context) error {
			_, err := h.UploadPausedCheckpoint(ctx, &ateletpb.UploadPausedCheckpointRequest{})
			return err
		},
		"Terminate": func(ctx context.Context) error {
			_, err := h.Terminate(ctx, &ateletpb.TerminateRequest{})
			return err
		},
	}
}

func TestHerderSucceedsAfterDelay(t *testing.T) {
	const d = 20 * time.Millisecond
	for name, call := range herderCalls(&herder{delays: uniformDelays(d)}) {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			if err := call(context.Background()); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if got := time.Since(start); got < d {
				t.Errorf("%s returned after %v, want at least %v", name, got, d)
			}
		})
	}
}

func TestHerderZeroDelay(t *testing.T) {
	for name, call := range herderCalls(&herder{}) {
		if err := call(context.Background()); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestHerderStopsWhenCallerGivesUp(t *testing.T) {
	for name, call := range herderCalls(&herder{delays: uniformDelays(time.Hour)}) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if err := call(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("%s: got %v, want %v", name, err, context.DeadlineExceeded)
			}
		})
	}
}

func TestHerderUsesEachCallsOwnDelay(t *testing.T) {
	h := &herder{delays: delays{run: time.Hour}}
	if _, err := h.Restore(context.Background(), &ateletpb.RestoreRequest{}); err != nil {
		t.Fatalf("Restore with no delay of its own: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := h.Run(ctx, &ateletpb.RunRequest{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run with an hour's delay: got %v, want %v", err, context.DeadlineExceeded)
	}
}
